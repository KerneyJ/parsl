from concurrent.futures import Future
import typeguard
import logging
import threading
import queue
import datetime
import pickle
from multiprocessing import Queue
from typing import Dict, Sequence  # noqa F401 (used in type annotation)
from typing import List, Optional, Tuple, Union
import math

from parsl.serialize import pack_apply_message, deserialize
from parsl.app.errors import RemoteExceptionWrapper
from parsl.executors.high_throughput import zmq_pipes
from parsl.executors.high_throughput import interchange
from parsl.executors.errors import (
    BadMessage, ScalingFailed,
    DeserializationError, SerializationError,
    UnsupportedFeatureError
)

from parsl.executors.status_handling import BlockProviderExecutor
from parsl.providers.provider_base import ExecutionProvider
from parsl.data_provider.staging import Staging
from parsl.addresses import get_all_addresses
from parsl.process_loggers import wrap_with_logs

from parsl.multiprocessing import ForkProcess
from parsl.utils import RepresentationMixin
from parsl.providers import LocalProvider

logger = logging.getLogger(__name__)



class XQExecutor(ParslExecutor):

    def __init__(self,
                 label: str = 'XQExecutor',
                 working_dir: Optional[str] = None,
                 worker_debug: bool = False,
                 max_workers: Union[int, float] = float('inf'),
                 prefetch_capacity: int = 0,
                 poll_period: int = 10):
        logger.debug("Initializing HighThroughputExecutor")

        BlockProviderExecutor.__init__(self, provider=provider, block_error_handler=block_error_handler)
        self.label = label
        self.worker_debug = worker_debug
        self.working_dir = working_dir
        self.workers = max_workers 

        self._task_counter = 0
        self.run_id = None  # set to the correct run_id in dfk
        self.poll_period = poll_period
        self.run_dir = '.'

    def start(self):
        """Create the Interchange process and connect to it.
        """
        self.outgoing_q = # TODO 
        self.incoming_q = # TODO 

        self.is_alive = True

        self._queue_management_thread = None
        self._start_queue_management_thread()

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
                                        DeserializationError(f"Received exception, but handling also threw an exception: {e}")
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
        self.outgoing_q.put(msg)

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
        logger.info("Finished HighThroughputExecutor shutdown attempt")
