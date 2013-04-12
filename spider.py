# coding:utf-8

import sys
import getopt
import re

from spidermain import Crawler, PrintInfo

VERSION = '0.0.1'
ARG = {'-u url': 'the bigin url,eg. http://www.baidu.com/',
       '-h': 'Display usage information (this message)',
       '-V': 'Print version number and exit',
       '-d deep': 'Number of spider crawling depth.default:0',
       '-l loglevel': 'Number of log level(1-5).default:3',
       '--testself': 'Testself',
       '--thread number': 'Number of multiple requests to make.default:10',
       '-f logfilename': 'The log file name.default:spider.log',
       '--dbfile dbfile': 'To save the crawling data.default:data.sql',
       '--key "key"': "The crawling keyword",
       }
#保存参数
options = {'url': 'http://sina.com.cn',
           'deep': 0,
           'loglevel': 3,
           'logfile': 'spider.log',
           'dbfile': 'data.sql',
           'thread_number': 10,
           'testself': False,
           'key': '',
           }


def usage():
    """
    帮助信息
    """
    print 'Usage: spider.py [options]'
    print 'Options are:'

    for i in ARG:
        print '\t%-12s\t%s' % (i, ARG[i])


def deal_argv():
    """
    参数处理
    """
    if len(sys.argv) < 2:
        print 'spider.py: wrong number of arguments'
        usage()
        sys.exit()
    try:
        longargs = ["help", "thread=", "testself", "key=", "dbfile="]
        opts, args = getopt.getopt(sys.argv[1:], "Vhu:d:l:f:", longargs)
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)
    for o, a in opts:
        if o == "-u":
            #url匹配 ^http://[\d\-a-zA-Z]+(\.[\d\-a-zA-Z]+)*/?$
            p = re.compile('^http://[\d\-a-zA-Z]+(\.[\d\-a-zA-Z]+)*/?$')
            if p.match(a):
                options['url'] = a
            else:
                print 'spider.py: invalid URL.try again'
                usage()
                sys.exit()
        elif o == "-d":
            options['deep'] = int(a)
        elif o == "-l":
            options['loglevel'] = int(a)
        elif o == "-f":
            options['logfile'] = a
        elif o == "--dbfile":
            options['dbfile'] = a
        elif o == "--thread":
            options['thread_number'] = int(a)
        elif o == "--testself":
            options['testself'] = True
        elif o == "--key":
            options['key'] = a
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o == "-V":
            print "this is spider.py,Version %s" % VERSION
            sys.exit()
        else:
            assert False, "unhandled option"
    return options


def main():
    options = deal_argv()
    #print options
    crawler = Crawler(options)
    info = PrintInfo(crawler)
    crawler.start()
    #info.stop()
    info.printEnd()

if __name__ == "__main__":
    main()
