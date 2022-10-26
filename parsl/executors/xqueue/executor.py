from concurrent.futures import Future
import typeguard
import logging
import threading
import queue
import datetime
import pickle
from typing import Dict, Sequence  # noqa F401 (used in type annotation)
from typing import List, Optional, Tuple, Union
import math

from parsl.serialize import pack_apply_message, deserialize, unpack_apply_message, serialize
from parsl.app.errors import RemoteExceptionWrapper
from parsl.executors.errors import (
    BadMessage, ScalingFailed,
    DeserializationError, SerializationError,
    UnsupportedFeatureError
)

from parsl.utils import RepresentationMixin
from parsl.executors.status_handling import NoStatusHandlingExecutor
from parsl.providers.provider_base import ExecutionProvider
from parsl.process_loggers import wrap_with_logs

from parsl.multiprocessing import ForkProcess, SizedQueue as mpQueue
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)



class XQExecutor(NoStatusHandlingExecutor, RepresentationMixin):

    def __init__(self,
                 label: str = 'XQExecutor',
                 working_dir: Optional[str] = None,
                 worker_debug: bool = False,
                 max_workers: Union[int, float] = float('inf'),
                 prefetch_capacity: int = 0,
                 poll_period: int = 10):
        NoStatusHandlingExecutor.__init__()
        logger.debug("Initializing XQExecutor")

        self.label = label
        self.worker_debug = worker_debug
        self.working_dir = working_dir
        self.max_workers = max_workers 

        self._task_counter = 0
        self.tasks = {}
        self.run_id = None  # set to the correct run_id in dfk
        self.poll_period = poll_period
        self.run_dir = '.'

    def start(self):
        """Create the Interchange process and connect to it.
        """
        self.workers = []
        self.outgoing_qs = []# TODO 
        self.incoming_q = mpQueue()# TODO 

        self.is_alive = True

        self._queue_management_thread = None
        self._start_queue_management_thread()

        # start workers
        for i in range(self.max_workers):
            tq = mpQueue()
            w = ForkProcess(targer=worker, args=(i, self.working_directory, tq, self.incoming_q))
            self.outgoing_qs.append(tq)
            self.workers.append(w)

        logger.debug("Created management thread: {}".format(self._queue_management_thread))

    @wrap_with_logs
    def _queue_management_worker(self):
        """Listen to the queue for task status messages and handle them.

        Depending on the message, tasks will be updated with results, exceptions,
        or updates. It expects the following messages:

        .. code:: python

            {
               "task_id" : <task_id>
               "result"  : serialized result object, if task succeeded
               ... more tags could be added later
            }

            {
               "task_id" : <task_id>
               "exception" : serialized exception object, on failure
            }

        The `None` message is a die request.
        """
        logger.debug("[MTHREAD] queue management worker starting")

        while not self.bad_state_is_set:
            try:
                msg_raw = self.incoming_q.get(timeout=1)

            except queue.Empty:
                logger.debug("[MTHREAD] queue empty")
                # Timed out.
                pass

            except IOError as e:
                logger.exception("[MTHREAD] Caught broken queue with exception code {}: {}".format(e.errno, e))
                return

            except Exception as e:
                logger.exception(f"[MTHREAD] Caught unknown exception: {e}")
                return

            else:
                if msg_raw is None:
                    logger.debug("[MTHREAD] Got None, exiting")
                    return
                else:
                        try:
                            msg = pickle.loads(msg_raw)
                        except pickle.UnpicklingError:
                            raise BadMessage("Message received could not be unpickled")
                        if msg['type'] == 'heartbeat':
                            continue
                        elif msg['type'] == 'result':
                            try:
                                tid = msg['task_id']
                            except Exception:
                                raise BadMessage("Message received does not contain 'task_id' field")

                            if tid == -1 and 'exception' in msg:
                                logger.warning("Executor shutting down due to exception from worker")
                                exception = deserialize(msg['exception'])
                                self.set_bad_state_and_fail_all(exception)
                                break

                            task_fut = self.tasks.pop(tid)

                            if 'result' in msg:
                                result = deserialize(msg['result'])
                                task_fut.set_result(result)

                            elif 'exception' in msg:
                                try:
                                    s = deserialize(msg['exception'])
                                    # s should be a RemoteExceptionWrapper... so we can reraise it
                                    if isinstance(s, RemoteExceptionWrapper):
                                        try:
                                            s.reraise()
                                        except Exception as e:
                                            task_fut.set_exception(e)
                                    elif isinstance(s, Exception):
                                        task_fut.set_exception(s)
                                    else:
                                        raise ValueError(f"Unknown exception-like type received: {type(s)}")
                                except Exception as e:
                                    # TODO could be a proper wrapped exception?
                                    task_fut.set_exception(
                                        DeserializationError(f"Received exception, but handling also threw an exception: {e}"))
                            else:
                                raise BadMessage("Message received is neither result or exception")
                        else:
                            raise BadMessage(f"Message received with unknown type {msg['type']}")

            if not self.is_alive:
                break
        logger.info("[MTHREAD] queue management worker finished")

    def _start_queue_management_thread(self):
        """Method to start the management thread as a daemon.

        Checks if a thread already exists, then starts it.
        Could be used later as a restart if the management thread dies.
        """
        if self._queue_management_thread is None:
            logger.debug("Starting queue management thread")
            self._queue_management_thread = threading.Thread(target=self._queue_management_worker, name="HTEX-Queue-Management-Thread")
            self._queue_management_thread.daemon = True
            self._queue_management_thread.start()
            logger.debug("Started queue management thread")

        else:
            logger.error("Management thread already exists, returning")

    def submit(self, func, resource_specification, *args, **kwargs):
        """Submits work to the outgoing_q.

        The outgoing_q is an external process listens on this
        queue for new work. This method behaves like a
        submit call as described here `Python docs: <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_

        Args:
            - func (callable) : Callable function
            - args (list) : List of arbitrary positional arguments.

        Kwargs:
            - kwargs (dict) : A dictionary of arbitrary keyword args for func.

        Returns:
              Future
        """
        if resource_specification:
            logger.error("Ignoring the resource specification. "
                         "Parsl resource specification is not supported in HighThroughput Executor. "
                         "Please check WorkQueueExecutor if resource specification is needed.")
            raise UnsupportedFeatureError('resource specification', 'HighThroughput Executor', 'WorkQueue Executor')

        if self.bad_state_is_set:
            raise self.executor_exception

        self._task_counter += 1
        task_id = self._task_counter

        # handle people sending blobs gracefully
        args_to_print = args
        if logger.getEffectiveLevel() >= logging.DEBUG:
            args_to_print = tuple([arg if len(repr(arg)) < 100 else (repr(arg)[:100] + '...') for arg in args])
        logger.debug("Pushing function {} to queue with args {}".format(func, args_to_print))

        fut = Future()
        fut.parsl_executor_task_id = task_id
        self.tasks[task_id] = fut

        try:
            fn_buf = pack_apply_message(func, args, kwargs,
                                        buffer_threshold=1024 * 1024)
        except TypeError:
            raise SerializationError(func.__name__)

        msg = {"task_id": task_id,
               "buffer": fn_buf}

        # TODO give it to task_count % worker
        self.outgoing_qs[task_id % len(self.workers)].put(msg)

        # Return the future
        return fut

    @property
    def scaling_enabled(self):
        return False

    def create_monitoring_info(self, status):
        """ Create a msg for monitoring based on the poll status

        """
        return

    def shutdown(self):
        """Shutdown the executor, including all workers and controllers.
        """

        logger.info("Attempting HighThroughputExecutor shutdown")
        # TODO kill the workers
        for w in self.workers:
            w.terminate()
        logger.info("Finished HighThroughputExecutor shutdown attempt")

def execute_task(bufs):
    """Deserialize the buffer and execute the task.

    Returns the result or throws exception.
    """
    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})

    f, args, kwargs = unpack_apply_message(bufs, user_ns, copy=False)

    # We might need to look into callability of the function from itself
    # since we change it's name in the new namespace
    prefix = "parsl_"
    fname = prefix + "f"
    argname = prefix + "args"
    kwargname = prefix + "kwargs"
    resultname = prefix + "result"

    user_ns.update({fname: f,
                    argname: args,
                    kwargname: kwargs,
                    resultname: resultname})

    code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                           argname, kwargname)
    exec(code, user_ns, user_ns)
    return user_ns.get(resultname)


def worker(worker_id, logdir, task_queue, result_queue):
    """

    Put request token into queue
    Get task from task_queue
    Pop request from queue
    Put result into result_queue
    """

    # override the global logger inherited from zthe __main__ process (which
    # usually logs to manager.log) with one specific to this worker.

    wlogger = start_file_logger('{}/worker_{}.log'.format(logdir, worker_id),
                               worker_id,
                               name="worker_log",
                               level=logging.INFO)

    # Sync worker with master
    wlogger.info('Worker {} started'.format(worker_id))

    while True:
        # The worker will receive {'task_id':<tid>, 'buffer':<buf>}
        req = task_queue.get()
        tid = req['task_id']
        wlogger.info("Received task {}".format(tid))

        try:
            result = execute_task(req['buffer'])
            serialized_result = serialize(result, buffer_threshold=1e6)
        except Exception as e:
            wlogger.info('Caught an exception: {}'.format(e))
            result_package = {'type': 'result', 'task_id': tid, 'exception': serialize(RemoteExceptionWrapper(*sys.exc_info()))}
        else:
            result_package = {'type': 'result', 'task_id': tid, 'result': serialized_result}
            wlogger.info("Result: {}".format(result))

        wlogger.info("Completed task {}".format(tid))
        try:
            pkl_package = pickle.dumps(result_package)
        except Exception:
            wlogger.exception("Caught exception while trying to pickle the result package")
            pkl_package = pickle.dumps({'type': 'result', 'task_id': tid,
                                        'exception': serialize(RemoteExceptionWrapper(*sys.exc_info()))
            })

        result_queue.put(pkl_package)
        wlogger.info("All processing finished for task {}".format(tid))


def start_file_logger(filename, rank, name='parsl', level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
       -  None
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d " \
                        "%(process)d %(threadName)s " \
                        "[%(levelname)s]  %(message)s"

    l = logging.getLogger(name)
    l.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    l.addHandler(handler)
    return l
