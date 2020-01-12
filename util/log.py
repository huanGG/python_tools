#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
"""
This is my logging module which offers two ways to log in multiprocess without modifing the lib.

Authors: changhuan(changhuan1993@gmail.com)
Date:    18/7/04
"""
import os
import time

import logging
from logging.handlers import TimedRotatingFileHandler

# lock = multiprocessing.Lock()


# choice 1: to inherit TimedRotatingFileHandler and override doRollover method
class SafeRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, filename, when='h', interval=1, backupCount=0, encoding=None, delay=False,
                 utc=False):
        TimedRotatingFileHandler.__init__(self, filename, when, interval, backupCount, encoding,
                                          delay, utc)

    def doRollover(self):
        """
        do a rollover; in this case, a date/time stamp is appended to the filename
        when the rollover happens.  However, you want the file to be named for the
        start of the interval, not the current time.  If there is a backup count,
        then we have to get a list of matching filenames, sort them and remove
        the one with the oldest suffix.

        Override,   1. if dfn not exist then do rename
                    2. _open with "a" model
        """
        if self.stream:
            self.stream.close()
            self.stream = None
        # get the time that this sequence started at and make it a TimeTuple
        currentTime = int(time.time())
        # is daylight saving time, 1: is dst , 0: not dst
        dstNow = time.localtime(currentTime)[-1]
        t = self.rolloverAt - self.interval
        if self.utc:
            timeTuple = time.gmtime(t)
        else:
            timeTuple = time.localtime(t)
            dstThen = timeTuple[-1]
            if dstNow != dstThen:
                if dstNow:
                    addend = 3600
                else:
                    addend = -3600
                timeTuple = time.localtime(t + addend)
        dfn = self.baseFilename + "." + time.strftime(self.suffix, timeTuple)
        # if os.path.exists(dfn):
        #    os.remove(dfn)

        # Issue 18940: A file may not have been created if delay is True.
        #        if os.path.exists(self.baseFilename):
        if not os.path.exists(dfn) and os.path.exists(self.baseFilename):
            os.rename(self.baseFilename, dfn)
        if self.backupCount > 0:
            for s in self.getFilesToDelete():
                os.remove(s)
        if not self.delay:
            self.mode = "a"
            self.stream = self._open()
        newRolloverAt = self.computeRollover(currentTime)
        while newRolloverAt <= currentTime:
            newRolloverAt = newRolloverAt + self.interval
        # If DST changes and midnight or weekly rollover, adjust for this.
        if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
            dstAtRollover = time.localtime(newRolloverAt)[-1]
            if dstNow != dstAtRollover:
                if not dstNow:  # DST kicks in before next rollover, so we need to deduct an hour
                    addend = -3600
                else:  # DST bows out before next rollover, so we need to add an hour
                    addend = 3600
                newRolloverAt += addend
        self.rolloverAt = newRolloverAt


# choice 2: to inherit FileHandler.(easy to use)
class SafeFileHandler(logging.FileHandler):
    def __init__(self, filename, mode, encoding=None, delay=0):
        """
        Use the specified filename for streamed logging
        """
        # if codecs is None:
        #     encoding = None
        logging.FileHandler.__init__(self, filename, mode, encoding, delay)
        self.mode = mode
        self.encoding = encoding
        self.suffix = "%Y-%m-%d"
        self.suffix_time = ""

    def emit(self, record):
        """
        Emit a record.

        Always check time
        """
        try:
            if self.check_baseFilename(record):
                self.build_baseFilename()
            logging.FileHandler.emit(self, record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def check_baseFilename(self, record):
        """
        Determine if builder should occur.

        record is not used, as we are just comparing times,
        but it is needed so the method signatures are the same
        """
        timeTuple = time.localtime()

        if self.suffix_time != time.strftime(self.suffix, timeTuple) or not os.path.exists(
                self.baseFilename + '.' + self.suffix_time):
            return 1
        else:
            return 0

    def build_baseFilename(self):
        """
        do builder; in this case,
        old time stamp is removed from filename and
        a new time stamp is append to the filename
        """
        if self.stream:
            self.stream.close()
            self.stream = None

        # remove old suffix
        if self.suffix_time != "":
            index = self.baseFilename.find("." + self.suffix_time)
            if index == -1:
                index = self.baseFilename.rfind(".")
            self.baseFilename = self.baseFilename[:index]

        # add new suffix
        currentTimeTuple = time.localtime()
        self.suffix_time = time.strftime(self.suffix, currentTimeTuple)
        self.baseFilename = self.baseFilename + "." + self.suffix_time

        self.mode = 'a'
        if not self.delay:
            self.stream = self._open()


# class Logger(object):
def init_log(log_dir, file_name_prefix, level=logging.INFO, when="D", backup=7,
             format="%(levelname)s: %(asctime)s: %(filename)s:%(lineno)d * %(process)d %(message)s",
             datefmt="%m-%d %H:%M:%S"):
    """
    init_log - initialize log module
    :param log_dir   The folder where the logs are stored
    :param file_name_prefix    Log file name prefix.
    :param level    msg above the level will be displayed
    :param when    how to split the log file by time interval
    :param backup    how many backup file to keep
    :param format    format of the log
    :param datefmt    format of the datetime
    """
    formatter = logging.Formatter(format, datefmt)
    logger = logging.getLogger()
    logger.setLevel(level)
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    # add handlers if there is no handler
    if not logger.handlers:
        info_log_name = os.path.join(log_dir, file_name_prefix + '.log')
        # handler = logging.handlers.TimedRotatingFileHandler(info_log_name,
        #                                                     when=when,
        #                                                     backupCount=backup)
        handler = SafeFileHandler(info_log_name, 'a')
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        warning_log_name = os.path.join(log_dir, file_name_prefix + '.log.wf')
        handler = SafeFileHandler(warning_log_name, 'a')
        handler.setLevel(logging.WARNING)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
