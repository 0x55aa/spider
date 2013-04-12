# coding:utf-8
import Queue
import threading
import logging
import sys
import traceback


class WorkerThread(threading.Thread):
    """
    �߳���
    """
    def __init__(self, requests_queue, info_queue, poll_timeout, **kargs):
        threading.Thread.__init__(self, **kargs)
        #��Ҫ�������
        self._requests_queue = requests_queue
        #��Ϣ��ӡ����
        self._info_queue = info_queue
        self._poll_timeout = poll_timeout
        self.setDaemon(True)
        self.state = None
        self.start()

    def run(self):
        while True:
            if self.state:
                break
            #ȡ��������ʱ��������ѭ�������ж������Ƿ����
            try:
                func, args, kargs = self._requests_queue.get(True,
                        self._poll_timeout)
            except Queue.Empty:
                continue
            #ִ��
            try:
                func(*args, **kargs)
                self._requests_queue.task_done()
            except:
                logging.debug(sys.exc_info())
                exc_type, exc_value, exc_traceback = sys.exc_info()
                logging.debug(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

    def stop(self):
        """
        ���ڽ����߳�
        """
        self.state = True


class ThreadPool(object):
    """
    �̳߳���
    workNum Ϊ�߳�����

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
        ����߳�
        """
        for i in range(worker_num):
            self.workers.append(WorkerThread(self._requests_queue,
                                self._info_queue, poll_timeout))

    def add_task(self, func, *args, **kargs):
        """
        �������
        """
        self._requests_queue.put((func, args, kargs))

    def poll(self):
        """
        �ȴ��߳̽���
        """
        self._requests_queue.join()
        for worker in self.workers:
            worker.join()
        self.workers = []

    def stopThreads(self):
        """
        �����߳�
        """
        for thread in self.workers:
            thread.stop()
        self.workers = []


if __name__ == '__main__':
    import doctest
    doctest.testmod()
