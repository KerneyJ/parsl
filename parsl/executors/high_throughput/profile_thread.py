import threading
import cProfile

class ProfileThread(threading.Thread):

    def __init__(self, group=None, target=None, name=None, save_dir=".", args=(), kwargs=None):
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs)
        self.save_dir = save_dir

    def run(self):
        try:
            if self._target:
                cProfile.runctx("self._target(*self._args, **self._kwargs)", globals=globals(), locals=locals(), filename=f"{self.save_dir}/{self.name}.pstats")
        finally:
            del self._target, self._args, self._kwargs
