from parsl.config import Config
from parsl.executors.taskvine import TaskVineExecutor

from parsl.data_provider.http import HTTPInTaskStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.file_noop import NoOpFileStaging


def fresh_config():
    return Config(executors=[TaskVineExecutor(port=9000,
                                              storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()])])
