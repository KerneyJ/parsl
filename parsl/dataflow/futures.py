"""This module implements the AppFutures.

We have two basic types of futures:
    1. DataFutures which represent data objects
    2. AppFutures which represent the futures on App/Leaf tasks.

"""

from concurrent.futures import Future
import cdflow
import logging
import threading
import warnings
from typing import Optional, Sequence

from parsl.app.futures import DataFuture
from parsl.dataflow.taskrecord import TaskRecord

logger = logging.getLogger(__name__)

# Possible future states (for internal use by the futures package).
PENDING = 'PENDING'
RUNNING = 'RUNNING'
# The future was cancelled by the user...
CANCELLED = 'CANCELLED'
# ...and _Waiter.add_cancelled() was called by a worker.
CANCELLED_AND_NOTIFIED = 'CANCELLED_AND_NOTIFIED'
FINISHED = 'FINISHED'

_STATE_TO_DESCRIPTION_MAP = {
    PENDING: "pending",
    RUNNING: "running",
    CANCELLED: "cancelled",
    CANCELLED_AND_NOTIFIED: "cancelled",
    FINISHED: "finished"
}


class AppFuture(Future):
    """An AppFuture wraps a sequence of Futures which may fail and be retried.

    The AppFuture will wait for the DFK to provide a result from an appropriate
    parent future, through ``parent_callback``. It will set its result to the
    result of that parent future, if that parent future completes without an
    exception. This result setting should cause .result(), .exception() and done
    callbacks to fire as expected.

    The AppFuture will not set its result to the result of the parent future, if
    that parent future completes with an exception, and if that parent future
    has retries left. In that case, no result(), exception() or done callbacks should
    report a result.

    The AppFuture will set its result to the result of the parent future, if that
    parent future completes with an exception and if that parent future has no
    retries left, or if it has no retry field. .result(), .exception() and done callbacks
    should give a result as expected when a Future has a result set

    The parent future may return a RemoteExceptionWrapper as a result
    and AppFuture will treat this an an exception for the above
    retry and result handling behaviour.

    """

    def __init__(self, exec_fu, tid) -> None:
        """Initialize the AppFuture.

        Args:

        KWargs:
             - task_def : The DFK task definition dictionary for the task represented
                   by this future.
        """
        super().__init__()
        self._tid = tid
        self._exec_fu = exec_fu
        self._outputs: Sequence[DataFuture]
        self._outputs = []

    def update(self, future):
        self.set_result(future.result())
        cdflow.resdep_task(self._tid)

    def setfut(self, exec_fu: Future):
        self.exec_fu = exec_fu

    def execfu_done(self):
        if self._exec_fu:
            return self._tid, self._exec_fu.done()
        else:
            return self._tid, None

    @property
    def func(self):
        return cdflow.getfunc_task(self._tid)

    @property
    def args(self):
        return cdflow.getargs_task(self._tid)

    @property
    def kwargs(self):
        return cdflow.getkwargs_task(self._tid)

    @property
    def stdout(self) -> Optional[str]:
        raise NotImplementedError("Stdout not implemented")

    @property
    def stderr(self) -> Optional[str]:
        raise NotImplementedError("Stderr not implemented")

    @property
    def tid(self) -> int:
        return self._tid

    def cancel(self) -> bool:
        raise NotImplementedError("Cancel not implemented")

    def cancelled(self) -> bool:
        return False

    def task_status(self) -> str:
        """
        Query cdfk for task status
        """
        raise NotImplementedError("Task status not implemented")

    @property
    def outputs(self) -> Sequence[DataFuture]:
        return self._outputs
