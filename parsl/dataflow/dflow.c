/*
 * Notes, FIXMEs and TODOs
 * - TODO A lot of functions return NULL in case
 *   of error, need to change this to set error
 *   using PyErr_SetString and use error codes from
 *   https://docs.python.org/3/c-api/exceptions.html#standard-exceptions
 * - TODO have multiple queues, a queue for waitng & pending
 * - TODO maybe split the executor off into another process and
 *   comunicate with it via a zmq pipe
 * - TODO Maybe add a valid flag that tells whether or not an entry in the task table is valid
 * - TODO Currently task_id = index in table, might want to change this for the sake of space
 */
#include <Python.h>
#include <limits.h>
#include <stdlib.h>
#include <pthread.h>
#include <signal.h>

#define TABLE_INC 10000
#define DEPTAB_INC 10
#define EXEC_COUNT 10

/*
 * Based on States enum in parsl/dataflow/states.py found at
 * https://github.com/Parsl/parsl/blob/master/parsl/dataflow/states.py
 */
enum state{
    unsched=-1,
    pending=0,
    running=2,
    exec_done=3,
    failed=4,
    dep_fail=5,
    launched=7,
    fail_retryable=8,
    memo_done=9,
    joining=10,
    running_ended=11,
};

// task dependency struct
struct task{
    unsigned long id;
    enum state status;
    unsigned long* depends;
    unsigned long depsize;
    unsigned long depcount; // number of task dependent on it
    unsigned long depon; // number of task it depends on
    char* exec_label; // Initialized by PyArg_Parse, who keeps track of memory
    char* func_name;
    double time_invoked;
    int join;
    int valid;

    PyObject* app_fu;
    PyObject* exec_fu;
    PyObject* executor;
    PyObject* func;
    PyObject* args;
    PyObject* kwargs;
};

struct executor{
    PyObject* obj;
    char* label;
};

static int init_tasktable(unsigned long); // allocate initial amont of memory for table
static int resize_tasktable(unsigned long); // change amount of memory in table
static int increment_tasktable(void); // will try to increase table size by TABLE_INC

static int create_task(char*, char*, double, int, PyObject*, PyObject*, PyObject*, PyObject*, PyObject*, PyObject*); // add a task to the dfk
static int delete_task(unsigned long);
static inline struct task* get_task(unsigned long);
static int adddep_task(unsigned long, unsigned long);
static int chstatus_task(unsigned long, enum state);
static enum state getstatus_task(unsigned long);

static int init_threads(void);
static int dest_threads(void);

static PyObject* init_dfk(PyObject*, PyObject*);
static PyObject* dest_dfk(PyObject*);
static PyObject* info_dfk(PyObject*);
static PyObject* info_exec_dfk(PyObject*);
static PyObject* add_executor_dfk(PyObject*, PyObject*);
static PyObject* shutdown_executor_dfk(PyObject*);
static PyObject* info_task(PyObject*, PyObject*);
static PyObject* getfunc_task(PyObject*, PyObject*);
static PyObject* getargs_task(PyObject*, PyObject*);
static PyObject* getkwargs_task(PyObject*, PyObject*);
static PyObject* resdep_task(PyObject*, PyObject*); // resolve dependency
static PyObject* submit(PyObject*, PyObject*);
static PyObject* launch(PyObject*, PyObject*, PyObject*, PyObject*);

struct task* tasktable = NULL; // dag represented as table of task structs
struct executor executors[EXEC_COUNT]; // Array of executor structs that store the label, 10 executor cap right now
unsigned int executorcount= 0;
unsigned long tablesize; // number of tasks table can store
unsigned long taskcount; // number of tasks created

int killswitch_thread = 0;

/*
 * In order to invoke object methods we must provide
 * PyObject_CallMethodObjArgs a PyObject that stores
 * a string of the method name so the following PyObjects
 * are will do so and they will be set in the dfk
 * initialization phase. Likely need to decrement ref
 * counter in destroy dfk phase
 */
PyInterpreterState* pyinterp_state = NULL;
PyObject* pystr_submit = NULL;
PyObject* pystr_shutdown = NULL;
PyObject* pystr_tid = NULL;
PyObject* pystr_update = NULL;
PyObject* pystr_done = NULL;
PyObject* pystr_adc = NULL; // add_done_callback
PyObject* pystr_setfut = NULL;
PyObject* pytyp_appfut = NULL;
PyObject* pyfun_unwrapfut = NULL;
PyObject* pyfun_execsubwrapper = NULL;

static int init_tasktable(unsigned long numtasks){
    tasktable = (struct task*)PyMem_RawMalloc(sizeof(struct task) * numtasks);
    if(tasktable == NULL)
        return -1;
    tablesize = numtasks;
    taskcount = 0;
    // set all of the valid flags to 0
    for(unsigned long i; i < numtasks; i++)
        tasktable[i].valid = 0;
    return 0;
}

static int resize_tasktable(unsigned long numtasks){
    if(numtasks > ULONG_MAX) // check if size is too big
        return -1;

    if(!tasktable) // check if task table has been initialized
        return -1;

    if(!PyMem_RawRealloc(tasktable, numtasks * sizeof(struct task*)))
        return -1;

    tablesize = numtasks;
    return 0;
}

static int increment_tasktable(){
    if(tablesize + TABLE_INC > ULONG_MAX)
        return -1;
    return resize_tasktable(tablesize + TABLE_INC);
}

/*
 * In future create function for deletinrg task to
 * conserve space in the task table. Right now the
 * goal if to implement something super simple and
 * functional so task will not be deleted we will
 * just add new task in the next unused spot
 * Returns the id of the task it created TODO, should be an unsigned long
 */
static int create_task(char* exec_label, char* func_name, double time_invoked, int join, PyObject* app_fu, PyObject* exec_fu, PyObject* executor, PyObject* func, PyObject* args, PyObject* kwargs){
    // check if the table is large enough
    if(taskcount == tablesize)
        if(increment_tasktable() < 0)
            return -1;

    struct task* task = get_task(taskcount);
    task->id = taskcount;
    taskcount++;
    task->status = unsched;
    task->depends = NULL;
    task->depsize = 0;
    task->depcount = 0;
    task->depon = 0;

    task->exec_label = exec_label;
    task->func_name = func_name;
    task->time_invoked = time_invoked;
    task->join = join;

    task->app_fu = app_fu;
    task->exec_fu = exec_fu;
    task->executor = executor;
    task->func = func;
    task->args = args;
    task->kwargs = kwargs;
    task->valid = 1;

    Py_INCREF(task->executor);
    Py_INCREF(task->func);
    Py_INCREF(task->args);
    Py_INCREF(task->kwargs);

    return task->id;
}

static int delete_task(unsigned long task_id){
    struct task* task = get_task(task_id);
    if(!task->valid)
        return -1; // task does not exist or has already been deleted
    if(task->depends){
        PyMem_RawFree(task->depends);
    }

    // FIXME gotta figure out which of these is being deleted before hand, one of these is causing the garbage collector to segfault
    //Py_DECREF(task->app_fu);
    //Py_DECREF(task->exec_fu);
    //Py_DECREF(task->executor);
    //Py_DECREF(task->func);
    //Py_DECREF(task->args);
    //Py_DECREF(task->kwargs);

    task->valid = 0;
    return 0;
}

static inline struct task* get_task(unsigned long task_id){
    return &tasktable[task_id];
}

static int adddep_task(unsigned long task_id, unsigned long dep_id){
    struct task* dep = get_task(dep_id);
    struct task* task = get_task(task_id);
    if(!dep->depends){ // has not malloced for depends array
        dep->depends = (unsigned long*)PyMem_RawMalloc(sizeof(unsigned long) * DEPTAB_INC);
        dep->depsize = DEPTAB_INC;
    }
    else if(dep->depcount >= dep->depsize){ // need to resize the dep count array
        dep->depends = (unsigned long*)PyMem_RawRealloc(dep->depends, sizeof(unsigned long) * (dep->depsize + DEPTAB_INC));
        dep->depsize += DEPTAB_INC;
    }
    dep->depends[dep->depcount] = task_id;
    dep->depcount++;
    task->depon++;
    return 0;
}

static int chstatus_task(unsigned long task_id, enum state status){
    struct task* task = get_task(task_id);
    task->status = status;
    return 0;
}

static enum state getstatus_task(unsigned long task_id){
    struct task* task = get_task(task_id);
    return task->status;
}

static int init_threads(void){
    return 0;
}

static int dest_threads(void){
    killswitch_thread = 1;
    return 0;
}

static PyObject* init_dfk(PyObject* self, PyObject* args){
    unsigned long numtasks;
    if(!PyArg_ParseTuple(args, "kOOO", &numtasks, &pytyp_appfut, &pyfun_unwrapfut, &pyfun_execsubwrapper))
        return NULL;

    if(init_tasktable(numtasks) < 0)
        return PyErr_Format(PyExc_RuntimeError, "CDFK failed to initialize task table");
    pyinterp_state = PyInterpreterState_Get();
    pystr_submit = Py_BuildValue("s", "submit");
    pystr_shutdown = Py_BuildValue("s", "shutdown");
    pystr_tid = Py_BuildValue("s", "tid");
    pystr_update = Py_BuildValue("s", "update");
    pystr_done = Py_BuildValue("s", "done");
    pystr_adc = Py_BuildValue("s", "add_done_callback");
    pystr_setfut = Py_BuildValue("s", "setfut");

    if(init_threads() < 0)
        return PyErr_Format(PyExc_RuntimeError, "CDFK failed to initialize threads");

    return Py_None;
}

/*
 * It is to my current belief that the memory
 * consumed by the task struct(pointers) is managed
 * by the python interpreter but if memory becomes an
 * issue then we should make sure that this is the case
 */
static PyObject* dest_dfk(PyObject* self){
    if(dest_threads() < 0)
        ; // TODO Find something to do when attempt to shutdown threads fails
    if(tasktable != NULL){
        for(unsigned long index = 0; index < tablesize; index++){
            delete_task(index);
        }
        PyMem_RawFree(tasktable);
    }
    tablesize = 0;
    taskcount = 0;

    Py_XDECREF(pystr_submit);
    Py_XDECREF(pystr_shutdown);
    Py_XDECREF(pystr_tid);
    Py_XDECREF(pystr_update);
    Py_XDECREF(pystr_done);
    Py_XDECREF(pystr_adc);
    Py_XDECREF(pystr_setfut);

    return Py_None;
}

static PyObject* info_dfk(PyObject* self){
    return PyUnicode_FromFormat("DFK Info -> Tasktable pointer: %p; Task table size: %i; Task count: %i;", tasktable, tablesize, taskcount);
}

static PyObject* info_exec_dfk(PyObject* self){
    for(unsigned int i = 0; i < executorcount; i++){
        if(PyObject_Print(executors[i].obj, stdout, 0) < 0)
           return PyErr_Format(PyExc_RuntimeError, "CDFK failed to print executor %i from executor table", i);
    }
    return Py_None;
}

static PyObject* add_executor_dfk(PyObject* self, PyObject* args){
    PyObject* executor = NULL;
    char* exec_label = NULL;
    if(executorcount == EXEC_COUNT)
        return PyErr_Format(PyExc_RuntimeError, "CDFK failed to add new executor, %i executors are supported", EXEC_COUNT);
    if(!PyArg_ParseTuple(args, "Os", &executor, &exec_label)) // TODO type check executor object to make sure it isn't a list, tuple, or other iterable
        return NULL;

    executors[executorcount].obj = executor;
    executors[executorcount].label = exec_label;
    executorcount++;
    return Py_None;
}

static PyObject* shutdown_executor_dfk(PyObject* self){
    for(unsigned int i = 0; i < executorcount; i++){
        if(executors[i].obj != NULL)
            PyObject_CallMethodObjArgs(executors[i].obj, pystr_shutdown, NULL);
    }
    return Py_None;
}
static PyObject* info_task(PyObject* self, PyObject* args){
    unsigned long task_id;

    if(!PyArg_ParseTuple(args, "k", &task_id))
        return NULL;

    if(tasktable == NULL) // TODO throw error here
        return PyUnicode_FromFormat("DFK Uninitialized");

    if(task_id >= taskcount) // TODO throw error here
        return PyUnicode_FromFormat("Task unallocated");

    struct task* task = get_task(task_id);

    return PyUnicode_FromFormat("Task %lu -> state: %i; depcount: %lu; exec_label: %s; func_name: %s; time invoked: %i; join: %i", // TODO find how to print float
                                task->id, task->status, task->depcount, task->exec_label, task->func_name, (int)task->time_invoked, task->join);
}

static PyObject* getfunc_task(PyObject* self, PyObject* args){
    unsigned long task_id;

    if(!PyArg_ParseTuple(args, "k", &task_id))
        return NULL;

    struct task* task = get_task(task_id);
    if(!task->valid)
        return PyErr_Format(PyExc_RuntimeError, "Invalid task");
    return task->func;
}

static PyObject* getargs_task(PyObject* self, PyObject* args){
    unsigned long task_id;

    if(!PyArg_ParseTuple(args, "k", &task_id))
        return NULL;

    struct task* task = get_task(task_id);
    if(!task->valid)
        return PyErr_Format(PyExc_RuntimeError, "Invalid task");
    return task->args;
}

static PyObject* getkwargs_task(PyObject* self, PyObject* args){
    unsigned long task_id;

    if(!PyArg_ParseTuple(args, "k", &task_id))
        return NULL;

    struct task* task = get_task(task_id);
    if(!task->valid)
        return PyErr_Format(PyExc_RuntimeError, "Invalid task");
    return task->kwargs;
}

static PyObject* resdep_task(PyObject* self, PyObject* args){ // resolve dependency
    unsigned long task_id, dep_id, depindex;
    struct task* task,* dep;
    if(!PyArg_ParseTuple(args, "k", &task_id)){
        return NULL;
    }
    task = get_task(task_id);
    if(!task->depcount){
        return Py_None;
    }

    for(depindex = 0; depindex < task->depcount; depindex++){
        dep_id = task->depends[depindex];
        dep = get_task(dep_id);
        dep->depon--;
        if(dep->depon > 0)
            continue;

        PyObject* exec_fu = launch(dep->executor, dep->func, dep->args, dep->kwargs);
        if(exec_fu == NULL){
            return NULL;
        }

        chstatus_task(dep_id, launched);
        PyObject* done_callback = PyObject_GetAttr(dep->app_fu, pystr_update);
        PyObject_CallMethodOneArg(exec_fu, pystr_adc, done_callback);
        dep->exec_fu = exec_fu;
        PyObject_CallMethodOneArg(dep->app_fu, pystr_setfut, exec_fu);
        Py_DECREF(done_callback);
    }
    return Py_None;
}

/*
 * TODO When freeing a task will need to decrement the refernce counts
 * of the python objects taken as a argument
 */
static PyObject* submit(PyObject* self, PyObject* args){
    char* func_name;
    int join, i, arglist_len;
    unsigned long task_id, dep_id;
    double time_invoked;
    struct executor exec;
    PyObject* func = NULL,* fargs=NULL,* fkwargs=NULL,* exec_fu=NULL,* arglist = NULL;

    if(!PyArg_ParseTuple(args, "sdpO|OOO", &func_name, &time_invoked, &join, &func, &fargs, &fkwargs, &arglist)){
        return NULL;
    }

    if(join){
        // use the internal executor
        exec = executors[0]; // we assume that executor 0 is always the internal executor
    }
    else{
        // use non internal executor(s)
        exec = executors[(rand() % executorcount-1) + 1];
    }

    if((task_id = create_task(exec.label, func_name, time_invoked, join, Py_None, Py_None, exec.obj, func, fargs, fkwargs)) < 0){
        return PyErr_Format(PyExc_RuntimeError, "CDFK failed to create new task and append it to task table");
    }
    struct task* task = get_task(task_id);

    // check for dependencies in args and kwargs
    arglist_len = PyList_Size(arglist);
    for(i = 0; i < arglist_len; i++){
        PyObject* item = PyList_GetItem(arglist, i);
        if(!PyObject_IsInstance(item, pytyp_appfut))
            continue;
        if(PyObject_IsTrue(PyObject_CallMethodNoArgs(item, pystr_done))){
            // got a dependency that happens to be already done
            continue;
        }
        PyObject* pylong_tid = PyObject_GetAttr(item, pystr_tid);
        dep_id = (unsigned long)PyLong_AsLong(pylong_tid);
        adddep_task(task_id, dep_id);
        chstatus_task(task_id, pending);
    }

    // if no dependencies launch, if depedencies don't
    if(getstatus_task(task_id) == pending){
        PyObject* appfu_arglist = Py_BuildValue("Oi", Py_None, task_id);
        PyObject* app_fu = PyObject_CallObject(pytyp_appfut, appfu_arglist);
        Py_INCREF(app_fu);
        Py_DECREF(appfu_arglist);
        task->app_fu = app_fu;
        return app_fu;
    }
    else{
        // invoke executor submit function
        exec_fu = launch(exec.obj, func, fargs, fkwargs);
        if(exec_fu == NULL){
            return NULL;
        }
        chstatus_task(task_id, launched);
        PyObject* appfu_arglist = Py_BuildValue("Oi", exec_fu, task_id);
        PyObject* app_fu = PyObject_CallObject(pytyp_appfut, appfu_arglist);
        Py_INCREF(app_fu);
        PyObject* done_callback = PyObject_GetAttr(app_fu, pystr_update);
        PyObject_CallMethodOneArg(exec_fu, pystr_adc, done_callback);
        Py_DECREF(appfu_arglist);
        Py_DECREF(done_callback); // TODO Does this need to be here? -> Does PyObject_GetAttr increment refernce counter?
        task->exec_fu = exec_fu;
        task->app_fu = app_fu;
        return app_fu;
    }
}

static PyObject* launch(PyObject* executor, PyObject* func, PyObject* args, PyObject* kwargs){
    PyObject* ret = PyObject_CallFunctionObjArgs(pyfun_unwrapfut, args, kwargs, NULL);
    PyObject* newargs = PyTuple_GetItem(ret, 0);
    PyObject* newkwargs = PyTuple_GetItem(ret, 1);
    PyObject* exec_fu = PyObject_CallFunctionObjArgs(pyfun_execsubwrapper, executor, func, newargs, newkwargs, NULL);

    Py_DECREF(ret);
    return exec_fu;
}

char init_dfk_docs[] = "This method will initialize the dfk. In doing so this method will allocate memory for the dag and reset global state.";
char dest_dfk_docs[] = "This method will destroy the dfk. In doing so this method will dealocate memory for the dag and reset global state.";
char info_dfk_docs[] = "This method prints the global state associated with the dfk.";
char info_exec_dfk_docs[] = "Loops through all the executor PyObjects stored in executors array and prints them";
char add_executor_dfk_docs[] = "This method appends a new executor to the executor table";
char shutdown_executor_dfk_docs[] = "Loops through all the executor PyObjects stored in executors array and invokes their shutdown function";
char submit_docs[] = "Takes in a function and its arguments, creates a task in the dag, and invokes executor.submit";
char info_task_docs[] = "takes as input an id as an int and returns information about a the task with that id";
char getfunc_task_docs[] = "Returns the task's function python object";
char getargs_task_docs[] = "Returns the task's args python object";
char getkwargs_task_docs[] = "Returns the task's keyword args python object";
char resdep_task_docs[] = "Takes in a task id and decrements the depcount of all the task dependent on it";

PyMethodDef cdflow_funcs[] = {
    {"init_dfk", (PyCFunction)init_dfk, METH_VARARGS, init_dfk_docs},
    {"dest_dfk", (PyCFunction)dest_dfk, METH_NOARGS, dest_dfk_docs},
    {"info_dfk", (PyCFunction)info_dfk, METH_NOARGS, info_dfk_docs},
    {"info_exec_dfk", (PyCFunction)info_exec_dfk, METH_NOARGS, info_exec_dfk_docs},
    {"info_task", (PyCFunction)info_task, METH_VARARGS, info_task_docs},
    {"getfunc_task", (PyCFunction)getfunc_task, METH_VARARGS, getfunc_task_docs},
    {"getargs_task", (PyCFunction)getargs_task, METH_VARARGS, getargs_task_docs},
    {"getkwargs_task", (PyCFunction)getkwargs_task, METH_VARARGS, getkwargs_task_docs},
    {"add_executor_dfk", (PyCFunction)add_executor_dfk, METH_VARARGS, add_executor_dfk_docs},
    {"shutdown_executor_dfk", (PyCFunction)shutdown_executor_dfk, METH_NOARGS, shutdown_executor_dfk_docs},
    {"submit", (PyCFunction)submit, METH_VARARGS, submit_docs},
    {"resdep_task", (PyCFunction)resdep_task, METH_VARARGS, resdep_task_docs},
    {NULL}
};

char cdflow_docs[] = "Implementing the DFK in C";

PyModuleDef cdflow_mod = {
    PyModuleDef_HEAD_INIT,
    "backend",
    cdflow_docs,
    -1, // all per interpreter state is global, as a consequence this module cannot support sub interpreters
    cdflow_funcs,
    NULL,
    NULL,
    NULL,
    NULL
};

PyMODINIT_FUNC PyInit_cdflow(void){
    return PyModule_Create(&cdflow_mod);
}
