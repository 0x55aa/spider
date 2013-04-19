# coding:utf-8
import logging
import urllib2
import threading
import sqlite3
import re
import traceback
import sys
import time
from datetime import datetime
from bs4 import BeautifulSoup

import threadpool


def initlog(logfile, loglevel=3):
    """
    ��־����
    """
    LEVELS = {1: logging.CRITICAL,
              2: logging.ERROR,
              3: logging.WARNING,
              4: logging.INFO,
              5: logging.DEBUG,
              }
    logger = logging.getLogger()
    hdlr = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(LEVELS.get(loglevel, logging.WARNING))
    return logger


class Crawler(object):
    """
    ץȡ��

    >>> options = {'loglevel': 4, 'dbfile': 'data.sql', 'thread_number': 5,'key': '', 'url': 'http://music.baidu.com/','testself': False, 'logfile': 'spider.log', 'deep': 0}
    >>> crawler = Crawler(options)
    >>> crawler.options
    {'url': 'http://music.baidu.com/', 'dbfile': 'data.sql', 'thread_number': 5, 'key': '', 'loglevel': 4, 'testself': False, 'logfile': 'spider.log', 'deep': 0}
    >>> crawler.start()

    """
    def __init__(self, options):
        #ץȡ��������
        self.urls = []
        #����
        self.options = options
        #��־��¼
        self.logger = initlog(self.options['logfile'],
                              self.options['loglevel'])
        #�̳߳�
        self.threadPool = threadpool.ThreadPool(self.options['thread_number'])
        #��
        self.mutex = threading.Lock()
        #�洢���ݿ�
        self.dataBase = SaveDataBase(self.options['dbfile'], self.logger)
        #��������
        self.all_url_num = 1
        #����������
        self.crawl_url_num = 0
        #�����������
        self.save_url_num = 0

    def crawl_page(self, url, deep):
        """
        ץȡҳ�棬������url��
        urlΪҳ���ַ��deepΪ��ǰ��ַ��Ҫ����ץȡ����ȡ�
        """
        self.logger.info('crawl url:%s', url)
        try:
            html = urllib2.urlopen(url).read()
        except Exception, e:
            self.logger.warning(e)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logging.debug(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            return None
        finally:
            #������+1
            if self.mutex.acquire():
                self.crawl_url_num += 1
                self.mutex.release()

        #�Ƿ���Ҫ��������
        if deep != 0:
            deep -= 1
            soup = BeautifulSoup(html)
            allUrl = soup.find_all('a', href=re.compile('^http|^/'))
            if self.options['url'].endswith('/'):
                self.options['url'] = url[:-1]
            for i in allUrl:
                if i['href'].startswith('/'):
                    i['href'] = self.options['url'] + i['href']
                #�����������ȡ��������
                if self.mutex.acquire():
                    if i['href'] not in self.urls:
                        self.urls.append(i['href'])
                        #����+1
                        self.all_url_num += 1
                        #�������������
                        self.threadPool.add_task(self.crawl_page,
                                                 i['href'],
                                                 deep)
                    self.mutex.release()

        self.logger.info('crawl url success')
        #����Ƿ���Ҫ�ؼ��ʹ���
        if self.options['key']:
            self.logger.debug('%s.', self.htmlfilter(html))
            if self.htmlfilter(html):
                self.dataBase.save(url, html)
                #������+1
                if self.mutex.acquire():
                    self.save_url_num += 1
                    self.mutex.release()
        else:
            self.dataBase.save(url, html)
            #������+1
            if self.mutex.acquire():
                self.save_url_num += 1
                self.mutex.release()
        #����Ƿ�ִ��������
        if (self.all_url_num == self.crawl_url_num) and self.threadPool._requests_queue.empty():
            self.stop()

    def htmlfilter(self, html):
        """
        ����Ƿ�����ؼ���
        """
        soup = BeautifulSoup(html)
        self.logger.debug(soup.originalEncoding)
        re_string = self.options['key']
        self.logger.debug('%s.', soup.findAll('meta', content=re.compile(re_string)))
        if soup.findAll('meta', content=re.compile(re_string)):
            return True
        else:
            return False

    def start(self):
        """
        ��������
        """
        #��Ӹ�·��
        self.threadPool.add_task(self.crawl_page,
                                 self.options['url'],
                                 self.options['deep'])
        self.threadPool.poll()

    def stop(self):
        """
        ��������
        """
        self.threadPool.stopThreads()
        self.dataBase.stop()
        self.logger.debug('run success.')


class SaveDataBase(object):
    """
    �洢ץȡ������

    >>> logger = initlog('spider.log', 5)
    >>> dataBase = SaveDataBase('data.sql', logger)
    >>> dataBase.save('http://www.baidu.com/', 'baidubaidubaidubaidubaiduaidu')
    """
    def __init__(self, dbfile, logger):
        self.conn = sqlite3.connect(dbfile, check_same_thread=False)
        self.logger = logger
        #����֧�����Ĵ洢
        self.conn.text_factory = str
        self.cmd = self.conn.cursor()
        self.cmd.execute('''
            create table if not exists data(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url text,
                html text
            )
        ''')
        self.conn.commit()

    def save(self, url, html):
        """
        ����html
        """
        try:
            self.cmd.execute("insert into data (url,html) values (?,?)",
                             (url, html))
            self.conn.commit()
            self.logger.info('insert success.')
            self.logger.debug('surl:%s', url)
        except Exception, e:
            self.logger.warning(e)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logging.debug(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

    def stop(self):
        """
        �ر����ݿ�����
        """
        self.conn.close()
        self.logger.debug('database connect close.')


class PrintInfo(threading.Thread):
    """
    ��ӡ������Ϣ��
    """
    def __init__(self, Crawler):
        threading.Thread.__init__(self)
        self.startTime = datetime.now()
        self.daemon = True
        self.crawler = Crawler
        self.start()

    def run(self):
        while True:
            time.sleep(5)
            out_string = '[+] Crawler %d pages,Totally visited %d Links.\n' % \
                         (self.crawler.crawl_url_num,
                          self.crawler.all_url_num)
            print out_string
            self.crawler.logger.info(out_string)
            time.sleep(5)

    def printEnd(self):
        self.endTime = datetime.now()
        out_string = """ Totally visited %d Links.\n Save %d links.\n
 Start at: %s\n Endate : %s\n Spend time: %s\nFinish!\n\n""" \
        % (self.crawler.all_url_num, self.crawler.save_url_num,
           self.startTime, self.endTime,
           (self.endTime - self.startTime))
        print out_string
        self.crawler.logger.info(out_string)


if __name__ == '__main__':
    import doctest
    doctest.testmod()
