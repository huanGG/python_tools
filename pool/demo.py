#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
"""
a simple demo use multiprocessing.Pool

Authors: changhuan(changhuan1993@gmail.com)
Date:    18/7/04
"""
from multiprocessing import Pool
import Queue
import threading
import time


def test(p):
    time.sleep(0.001)
    if p == 10000:
        return True
    else:
        return False


if __name__ == "__main__":
    result = Queue.Queue()  # 队列
    pool = Pool()

    def pool_th():
        for i in xrange(50000000):  # 这里需要创建执行的子进程非常多
            try:
                result.put(pool.apply_async(test, args=(i,)))
            except Exception as e:
                break

    def result_th():
        while 1:
            a = result.get().get()  # 获取子进程函数执行返回值
            if a:
                print a
                pool.terminate()  # 结束所有子进程
                break
    # 利用多线程，同时运行Pool函数创建执行子进程，以及运行获取子进程返回值函数。
    t1 = threading.Thread(target=pool_th)
    t2 = threading.Thread(target=result_th)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    pool.close()
    pool.join()
