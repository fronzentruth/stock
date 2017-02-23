#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  3 18:38:42 2016

@author: andy
"""

import tushare as ts
import pandas as pd
import threading
import time
import sys
from Queue import Queue
from optparse import OptionParser

class Worker(threading.Thread):  
    """多线程获取"""
    def __init__(self, work_queue, result_queue):   
        threading.Thread.__init__(self)
        self.work_queue = work_queue  
        self.result_queue = result_queue   
        self.start()  

    def run(self):          
        while True:
            func, arg, code_index = self.work_queue.get()  
            res = func(arg, code_index)
            self.result_queue.put(res)  
            if self.result_queue.full():
                res = sorted([self.result_queue.get() for i in range(self.result_queue.qsize())], key=lambda s: s[0], reverse=True)
                #以下为最后的输出结果
                print '***** start *****'
                for obj in res:
                    print obj[1]   
                print '***** end *****\n'
                
            self.work_queue.task_done()  

class Stock(object):

    def __init__(self, code, thread_num):    
        self.code = code  
        self.work_queue = Queue()
        self.threads = []      
        self.__init__thread_poll(thread_num)  

    def __init__thread_poll(self, thread_num):
        self.params = self.code.split(',')
        self.result_queue = Queue(maxsize=len(self.params[::-1]))
        for i in range(thread_num):
            self.threads.append(Worker(self.work_queue, self.result_queue))

    def del_params(self):
        for obj in self.params:
            self.__add_work(obj, self.params.index(obj))

    def __add_work(self, stock_code, code_index):
        self.work_queue.put((self.Get_stock, stock_code, code_index))

    def wait_all_complete(self):
        for thread in self.threads:
            if thread.isAlive():
                thread.join()


    @classmethod      
    def Get_stock(cls,code,code_index):
        df = ts.get_realtime_quotes(code)
        df = df[['code','price','b5_p','b5_v','b4_p','b4_v','b3_p','b3_v','b2_p','b2_v','b1_p','b1_v','a1_p','a1_v','a2_p','a2_v','a3_p','a3_v','a4_p','a4_v','a5_p','a5_v','volume','time']]
        
        return code_index, df             #返回df为股票实时报价，x参数为股票代码,参数可以为一个list




if __name__=='__main__':
    parser = OptionParser() 
    parser.add_option('-c','--stock-code',dest='codes')
    parser.add_option('-s','--sleep-time',dest='sleep_time', default=3, type="int")
    parser.add_option('-t','--thread-num',dest='thread_num', default=3, type='int')
    options, args = parser.parse_args(args=sys.argv[1:])

    t = options.sleep_time

    assert options.codes, "Please enter the stock code!"

    stock = Stock(options.codes, options.thread_num)

    while True:
        stock.del_params()
        time.sleep(t)




