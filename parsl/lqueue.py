import mmap
import pickle
import queue
import os
import time
import traceback

from multiprocessing import shared_memory
from multiprocessing import RLock
from multiprocessing import Pipe

class _SharedMemory(object):

    def __init__(self, size):
        self.buf = mmap.mmap(-1, size, flags=mmap.MAP_SHARED | mmap.MAP_ANONYMOUS)

    def close(self):
        self.buf.close()

# Check for improvement in the range 10% 20% and above
# if there is trouble with parsl then do microbenchmark
class LocklessQueue(object):

    _MAX_ITEM_SIZE = 10000 # items should be <= 10KB
    _HEADER_SIZE = 16 # 1 byte for occupied or not 15 bytes for length
    _BLOCK_SIZE = _MAX_ITEM_SIZE + _HEADER_SIZE
    _BYTES_ORDER = "little" # for the size integer
    
    def __init__(self, maxsize, polltime=60):
        self._maxsize = maxsize
        self._polltime = polltime
        self._size = self._maxsize * LocklessQueue._BLOCK_SIZE
        # self._shm = shared_memory.SharedMemory(create=True, size=self._size)
        self._shm = _SharedMemory(size=self._size)
        self._buffer = self._shm.buf
        self._wptr = 0
        self._rptr = 0

    def get_nopoll(self):
        if self._buffer[self._rptr] == 0:
            raise queue.Empty
        length = int.from_bytes(self._buffer[self._rptr+1:self._rptr+17], LocklessQueue._BYTES_ORDER)
        item_bytes = self._buffer[self._rptr+17:self._rptr+17+length]
        item = pickle.loads(item_bytes)
        self._buffer[self._rptr] = 0
        if self._rptr == (self._maxsize-1) * LocklessQueue._BLOCK_SIZE:
            self._rptr = 0
        else:
            self._rptr += LocklessQueue._BLOCK_SIZE

        return item

    def put_nopoll(self, item):
        # check if queue is full
        # check if the write pointer is looking at the last position in the list try to loop it if so
        if self._buffer[self._wptr] == 255:
            raise queue.Full

        b = pickle.dumps(item)
        if len(b) > LocklessQueue._MAX_ITEM_SIZE: # item too big
            raise BufferError("Item is too big for queue") 

        self._buffer[self._wptr+1:self._wptr+16] = len(b).to_bytes(15, LocklessQueue._BYTES_ORDER)
        self._buffer[self._wptr+17:self._wptr+17+len(b)] = bytearray(b) 
        self._buffer[self._wptr] = 255
        if self._wptr == (self._maxsize - 1) * LocklessQueue._BLOCK_SIZE:
            self._wptr = 0
        else:
            self._wptr += LocklessQueue._BLOCK_SIZE

    def get(self, logger=None): # polls, make a sleep call, do exponential backoff(check fo 1 ms, 2 ms, 4ms, on and on until second
        start = time.time()
        wait_time = 1 / 1000 # 1 millisecond
        while True:
            if (time.time() - start) > self._polltime: # break and raise exception when we've waited for more than 60 seconds
                break
            try:
                item = self.get_nopoll()
                return item
            except:
                if logger:
                    logger.info(f"get failed: rptr {self._rptr} -> {self._buffer[self._rptr]} == 255, qsize = {self.qsize()}")
                time.sleep(wait_time)
                if wait_time < 1:
                    wait_time *= 1.5
                continue
        raise queue.Empty

    def put(self, item, logger=None): # poll change this to wait until there is a spot
        wait_time = 1 / 1000
        tot_time = time.time()
        while True:
            try:
                self.put_nopoll(item)
                break
            except:
                if logger:
                    logger.info(f"put failed: wptr {self._wptr} -> {self._buffer[self._wptr]} == 0, qsize = {self.qsize()}")
                time.sleep(wait_time)
                if wait_time < 1:
                    wait_time *= 1.5
                continue
        if logger:
            logger.info(f"put succeded: total time {time.time() - tot_time}")

    def qsize(self): # TODO implement this
        return 0

    def empty(self):
        return self.buffer[self._rptr] == 0

    def close(self):
        self._shm.close()

    def __str__(self):
        return f"rptr: {self._rptr}; wptr: {self._wptr};\n" \
               f"max items: {self._maxsize}; memory used: {self._size}\n" \
               f"shm name: {self._shm.name}\n"


class PipeQueue(object):

    def __init__(self):
        self._reader, self._writer = Pipe(duplex=False)

    def put_nowait(self, item):
        self._writer.send(item)

    def put(self, item):
        self.put_nowait(item)

    def get_nowait(self):
        return self._reader.recv()

    def get(self):
        return self.get_nowait()

    def qsize(self): # TODO implement
        return 0

    def close(self):
        self._reader.close()
        self._writer.close()

if __name__ == "__main__":
    try:
        l = LocklessQueue(3)
        print(f"read pointer: {self._rptr}; len buffer{len(self._buffer)}")
        l.put(1)
        l.put(2)
        l.put(3)
        print(l.get())
        l.put(4)
        print(l.get())
        print(l.get())
        print(l.get())
        l.close()
    except:
        traceback.print_exc()
        l.close()
