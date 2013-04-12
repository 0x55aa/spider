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
    日志设置
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
    抓取类

    >>> options = {'loglevel': 4, 'dbfile': 'data.sql', 'thread_number': 5,'key': '', 'url': 'http://music.baidu.com/','testself': False, 'logfile': 'spider.log', 'deep': 0}
    >>> crawler = Crawler(options)
    >>> crawler.options
    {'url': 'http://music.baidu.com/', 'dbfile': 'data.sql', 'thread_number': 5, 'key': '', 'loglevel': 4, 'testself': False, 'logfile': 'spider.log', 'deep': 0}
    >>> crawler.start()

    """
    def __init__(self, options):
        #抓取到的链接
        self.urls = []
        #参数
        self.options = options
        #日志记录
        self.logger = initlog(self.options['logfile'],
                              self.options['loglevel'])
        #线程池
        self.threadPool = threadpool.ThreadPool(self.options['thread_number'])
        #锁
        self.mutex = threading.Lock()
        #存储数据库
        self.dataBase = SaveDataBase(self.options['dbfile'], self.logger)
        #总链接数
        self.all_url_num = 1
        #处理链接数
        self.crawl_url_num = 0
        #保存的链接数
        self.save_url_num = 0

    def crawl_page(self, url, deep):
        """
        抓取页面，并分析url。
        url为页面地址，deep为当前地址还要进行抓取到深度。
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
            #处理数+1
            if self.mutex.acquire():
                self.crawl_url_num += 1
                self.mutex.release()

        #是否需要分析链接
        if deep != 0:
            deep -= 1
            soup = BeautifulSoup(html)
            allUrl = soup.find_all('a', href=re.compile('^http|^/'))
            if self.options['url'].endswith('/'):
                self.options['url'] = url[:-1]
            for i in allUrl:
                if i['href'].startswith('/'):
                    i['href'] = self.options['url'] + i['href']
                #上锁，添加已取到的链接
                if self.mutex.acquire():
                    if i['href'] not in self.urls:
                        self.urls.append(i['href'])
                        #总数+1
                        self.all_url_num += 1
                        #添加新链接任务
                        self.threadPool.add_task(self.crawl_page,
                                                 i['href'],
                                                 deep)
                    self.mutex.release()

        self.logger.info('crawl url success')
        #检查是否需要关键词过滤
        if self.options['key']:
            self.logger.debug('%s.', self.htmlfilter(html))
            if self.htmlfilter(html):
                self.dataBase.save(url, html)
                #保存数+1
                if self.mutex.acquire():
                    self.save_url_num += 1
                    self.mutex.release()
        else:
            self.dataBase.save(url, html)
            #保存数+1
            if self.mutex.acquire():
                self.save_url_num += 1
                self.mutex.release()
        #检查是否执行完任务
        if (self.all_url_num == self.crawl_url_num) and self.threadPool._requests_queue.empty():
            self.stop()

    def htmlfilter(self, html):
        """
        检查是否满足关键字
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
        启动任务
        """
        #添加根路径
        self.threadPool.add_task(self.crawl_page,
                                 self.options['url'],
                                 self.options['deep'])
        self.threadPool.poll()

    def stop(self):
        """
        结束任务
        """
        self.threadPool.stopThreads()
        self.dataBase.stop()
        self.logger.debug('run success.')


class SaveDataBase(object):
    """
    存储抓取的数据

    >>> logger = initlog('spider.log', 5)
    >>> dataBase = SaveDataBase('data.sql', logger)
    >>> dataBase.save('http://www.baidu.com/', 'baidubaidubaidubaidubaiduaidu')
    """
    def __init__(self, dbfile, logger):
        self.conn = sqlite3.connect(dbfile, check_same_thread=False)
        self.logger = logger
        #设置支持中文存储
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
        保存html
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
        关闭数据库连接
        """
        self.conn.close()
        self.logger.debug('database connect close.')


class PrintInfo(threading.Thread):
    """
    打印进度信息类
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
