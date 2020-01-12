# -*- coding:utf-8 -*-
"""
进程池、线程池
"""
import os
import sys
import signal
import time
import threading
import multiprocessing
from multiprocessing import Pool

import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


# 1.根据不同的操作系统自己创建、管理进程池
class WorkProcPoolWindows(object):
    """
    Windows进程池，创建进程时，所有上下文变量必须可序列化
    """
    proc_list = []

    def __init__(self, proc_num, func, args=None):
        """
        初始化Windows进程池
        :param proc_num: 进程数量
        :param func: 需要执行的函数
        :param args: 传入func的参数
        """
        self.func = func
        self.args = args
        self.proc_num = proc_num
        self.create_pool()

    def create_pool(self):
        """
        根据进程数创建进程池
        :return:
        """
        for _ in range(self.proc_num):
            self.proc_list.append(self.create_process())

    def create_process(self):
        """
        创建响应进程
        :return:
        """
        if self.args is None:
            p = multiprocessing.Process(target=self.func)
        else:
            p = multiprocessing.Process(target=self.func, args=(self.args,))
        p.daemon = True
        p.start()
        return p

    def check_alive(self):
        """
        检查进程池内的进程是否存在，若不存在则重新创建
        :return:
        """
        self.proc_list = [p if p.is_alive() else self.create_process() for p in self.proc_list]

    def reg_signal(self):
        """
        注册signal
        :return:
        """
        # signal.signal(signal.SIGINT, self.kill)
        signal.signal(signal.SIGTERM, self.kill)

    def kill(self, signum, frame):
        """
        杀死进程
        :param signum:
        :param frame:
        :return:
        """
        logging.info(u"got signal:%d" % signum)
        for pid in self.proc_list:
            os.kill(pid, signal.SIGTERM)
        sys.exit()


class WorkThreadPoolWindows(object):
    """
    Windows线程池类
    """
    thread_list = []

    def __init__(self, thread_num, func, args=None):
        """
        初始化线程池
        :type thread_num: int
        :param thread_num: 线程数
        :param func: 需要执行的函数
        :param args: 传入func的参数
        """
        self.func = func
        self.args = args
        self.thread_num = thread_num
        self.create_pool()

    def create_pool(self):
        """
        创建线程池
        :return:
        """
        for _ in range(self.thread_num):
            t = self.create_thread()
            # time.sleep(1)
            t.start()
            self.thread_list.append(t)

    def create_thread(self):
        """
        创建线程
        :return:
        """
        if self.args is not None:
            t = threading.Thread(target=self.func, args=(self.args,))
        else:
            t = threading.Thread(target=self.func)
        t.daemon = True
        return t

    def check_alive(self):
        """
        检查进程池内的进程是否存在，若不存在则重新创建
        :return:
        """
        self.thread_list = [t if t.isAlive() else self.create_thread() for t in self.thread_list]

    def reg_signal(self):
        """
        注册signal
        :return:
        """
        pass

    def wait_proc(self):
        """
        等待执行完毕
        :return:
        """
        pass


class WorkProcPoolLinux(object):
    """
    Linux进程池类
    """
    process_list = []

    def __init__(self, proc_num, sub_func, args=None):
        """
        初始化Linux进程池
        :type proc_num: int
        :param proc_num: 进程数量
        :param sub_func: 需要执行的函数
        :param args: 传入func的参数
        """
        self.sub_func = sub_func
        self.args = args
        self.proc_num = proc_num
        # 创建进程池
        self.create_pool()

    def create_pool(self):
        """
        创建进程池
        :return:
        """
        index = 0
        while index < self.proc_num:
            index += 1
            pid = os.fork()
            if pid == 0:  # 子进程
                if self.args is not None:
                    self.sub_func(self.args)
                else:
                    self.sub_func()
                sys.exit()
            else:  # 父进程
                self.process_list.append(pid)

    def check_alive(self):
        """
        如果进程异常死亡，则重新启动一个加入进程池，并做一些后续处理
        :return:
        """
        try:
            exit_stat = None
            try:
                # Wait for completion of a child process, and return a tuple
                # containing its pid and exit status indication
                exit_stat = os.wait()
            except OSError as e:
                self.create_pool()
                return None
            logging.info(u"进程退出[%s]" % str(exit_stat))
            print exit_stat
            self.process_list.remove(exit_stat[0])
            pid = os.fork()
            if pid == 0:  # 子进程
                try:
                    if self.args is None:
                        self.sub_func()
                    else:
                        self.sub_func(self.args)
                    sys.exit()
                except Exception as e:
                    sys.exit()
            else:
                self.process_list.append(pid)
        except Exception as e:
            logging.error(u"监控子进程失败[%s]" % e)

    def reg_signal(self):
        """
        捕捉一些常用信号，以便后面能优雅退出
        :return:
        """
        signal.signal(signal.SIGINT, self.kill)
        signal.signal(signal.SIGQUIT, self.kill)
        signal.signal(signal.SIGTERM, self.kill)
        signal.signal(signal.SIGHUP, self.kill)

    def wait_proc(self):
        """
        等待所有子进程结束
        :return:
        """
        pass

    def kill(self, sig, frame):
        """
        杀死所有子进程
        :param sig:
        :param frame:
        :return:
        """
        for pid in self.process_list:
            os.kill(pid, signal.SIGKILL)
        sys.exit()


class WorkProcPool(WorkThreadPoolWindows if os.name == 'nt' else WorkProcPoolLinux):
    """线程/进程池，根据操作系统类型确定具体实现类
    """
    pass


# 2. 使用系统自带的进程池，需要操作系统和python支持
def test(p):
    try:
        time.sleep(1)
        print p
    except KeyboardInterrupt, e:
        pass


if __name__ == "__main__":
    pool = Pool(processes=10)

    for i in range(100):
        p = pool.apply_async(test, args=(i,))
    # p = pool.map_async(test, range(100))
    try:
        result = p.get(0xFFFF)
    except KeyboardInterrupt:
        pool.terminate()
    finally:
        pool.close()
        pool.join()


