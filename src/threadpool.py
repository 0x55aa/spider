# coding:utf-8
import Queue
import threading
import logging
import sys
import traceback


class WorkerThread(threading.Thread):
    """
    线程类
    """
    def __init__(self, requests_queue, info_queue, poll_timeout, **kargs):
        threading.Thread.__init__(self, **kargs)
        #主要任务队列
        self._requests_queue = requests_queue
        #信息打印队列
        self._info_queue = info_queue
        self._poll_timeout = poll_timeout
        self.setDaemon(True)
        self.state = None
        self.start()

    def run(self):
        while True:
            if self.state:
                break
            #取不到任务超时，会重新循环，来判断任务是否结束
            try:
                func, args, kargs = self._requests_queue.get(True,
                        self._poll_timeout)
            except Queue.Empty:
                continue
            #执行
            try:
                func(*args, **kargs)
                self._requests_queue.task_done()
            except:
                logging.debug(sys.exc_info())
                exc_type, exc_value, exc_traceback = sys.exc_info()
                logging.debug(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

    def stop(self):
        """
        用于结束线程
        """
        self.state = True


class ThreadPool(object):
    """
    线程池类
    workNum 为线程数量

    >>> threadPool = ThreadPool(5)
    >>> a = lambda x: x + 2
    >>> [threadPool.add_task(a, n) for n in range(10)]
    [None, None, None, None, None, None, None, None, None, None]
    >>> import time
    >>> time.sleep(5)
    >>> threadPool.stopThreads()

    """
    def __init__(self, worker_num, poll_timeout=5):
        self._requests_queue = Queue.Queue()
        self._info_queue = Queue.Queue()
        self.workers = []
        #self.worker_num = worker_num
        self.addWorkers(worker_num, poll_timeout)

    def addWorkers(self, worker_num, poll_timeout=5):
        """
        添加线程
        """
        for i in range(worker_num):
            self.workers.append(WorkerThread(self._requests_queue,
                                self._info_queue, poll_timeout))

    def add_task(self, func, *args, **kargs):
        """
        添加任务
        """
        self._requests_queue.put((func, args, kargs))

    def poll(self):
        """
        等待线程结束
        """
        self._requests_queue.join()
        for worker in self.workers:
            worker.join()
        self.workers = []

    def stopThreads(self):
        """
        结束线程
        """
        for thread in self.workers:
            thread.stop()
        self.workers = []


if __name__ == '__main__':
    import doctest
    doctest.testmod()
