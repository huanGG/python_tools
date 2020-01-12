#!/usr/bin/env python
# -*- coding:utf-8 -*-
#

"""
mysql helper: pack some common mysql operating functions

Authors: changhuan(changhuan1993@gmail.com)
Date:    2018/7/04
"""
import json

import logging
import MySQLdb

import util.formatter


class MySQL(object):
    """
    pack some common mysql operating functions
    """
    _error_code = ''  # MySQL error code

    _instance = None  # instance of this class
    _conn = None  # connection
    _cur = None  # cursor

    def __init__(self, host, port, user, password, database, charset="utf8"):
        """
        Connect to the database based on the parameters.
        :param host:
        :param port:
        :param user:
        :param password:
        :param database:
        :param charset:
        """
        try:
            self._conn = MySQLdb.connect(host=host,
                                         port=port,
                                         user=user,
                                         passwd=password,
                                         db=database,
                                         charset=charset)
            self._cur = self._conn.cursor()
            self._instance = MySQLdb
        except MySQLdb.Error as e:
            self._error_code = e.args[0]
            error_msg = "fail to connect Mysql database，error_code：{0}.".format(repr(e))
            logging.error(error_msg)

    def query(self, sql):
        """
        excute the query sql
        :param sql:
        :return: a list of dict
        """
        try:
            self._conn.ping(True)
            self._cur = self._conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            logging.info('excute query sql：{0}'.format(sql))
            self._cur.execute(sql)
            result = self._cur.fetchall()
        except MySQLdb.Error as e:
            self._error_code = e.args[0]
            err_msg = "fail to excute query sql，error_code：{0}".format(repr(e))
            logging.error(err_msg)
            result = None
        finally:
            return result

    def update(self, sql):
        """
        excute update sql
        :param sql:
        :return: rows affected
        """
        try:
            self._conn.ping(True)
            logging.info('excute update sql：{0}'.format(sql))
            result = self._cur.execute(sql)
            self._conn.commit()
            return result
        except MySQLdb.Error as e:
            self._error_code = e.args[0]
            err_msg = "fail to excute update sql，error_code：{0}".format(repr(e))
            logging.error(err_msg)
            self.rollback()
            result = -1
        finally:
            return result

    def insert(self, sql):
        """
        excute insert sql
        :param sql:
        :return: Returns the ID generated for an AUTO_INCREMENT column by the previous query.
        """
        try:
            self._conn.ping(True)
            logging.info("excute inset sql：{0}".format(sql))
            self._cur.execute(sql)
            self._conn.commit()
            result = self._conn.insert_id()
        except MySQLdb.Error as e:
            err_msg = "fail to excute insert sql，error_code：{0}".format(repr(e))
            logging.error(err_msg)
            self.rollback()
            result = -1
        finally:
            return result

    def insert_dict(self, sql, data_dict):
        """
        excute insert dict sql
        :param sql:
        :param data_dict:
        :return:
        """
        try:
            self._conn.ping(True)
            self._cur.execute("SET NAMES utf8")
            dict_str = json.dumps(data_dict, encoding='utf8', ensure_ascii=False,
                                  cls=python.util.formatter.BetterJsonEncoder)
            logging.info("excute insert dict sql：{0}, dict：{1}".format(sql, dict_str))
            self._cur.execute(sql, data_dict.values())
            self._conn.commit()
            return True
        except MySQLdb.Error as e:
            err_msg = "fail to excute insert dict sql，error_code：{0}".format(repr(e))
            logging.error(err_msg)
            self.rollback()
            return False

    def fetch_all_rows(self):
        """
        return all result rows.
        :return:
        """
        return self._cur.fetchall()

    def fetch_one_row(self):
        """
        return one row and the cursor move forward one row.When the cursor reaches the end,return None.
        :return:
        """
        return self._cur.fetchone()

    def get_row_count(self):
        """
        return row count
        :return:
        """
        return self._cur.rowcount

    def commit(self):
        """
        commit
        :return:
        """
        self._conn.commit()

    def rollback(self):
        """
        rollback
        :return:
        """
        self._conn.rollback()

    def __del__(self):
        """
        release the resources.（invoked by GC)
        :return:
        """
        try:
            if self._cur:
                self._cur.close()
                self._cur = None
            if self._conn:
                self._conn.close()
                self._conn = None
        except Exception as e:
            logging.error("fail to release，error_code：{0}".format(repr(e)))

    def close(self):
        """
        release the resources manually.
        :return:
        """
        self.__del__()

