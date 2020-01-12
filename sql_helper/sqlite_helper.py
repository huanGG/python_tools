#!/usr/bin/env python
# -*- coding:utf-8 -*-
#

"""
sqlite helper: pack some common sqlite operating functions

Authors: changhuan(changhuan1993@gmail.com)
Date:    2018/7/04
"""
import sqlite3
import logging


class SqliteHelper(object):
    """
    pack some common sqlite operating functions
    """
    _error_code = ''

    _instance = None
    _conn = None
    _cur = None

    def __init__(self, db_path):
        """
        Connect to the database based on db3 path.
        :param db_path: sqlite db3 path
        """
        try:
            self._conn = sqlite3.connect(db_path)
        except sqlite3.Error as e:
            self._error_code = e.args[0]
            error_msg = "fail to connect to db3，error_code：{0}.".format(repr(e))
            logging.error(error_msg)

        def dict_factory(cursor, row):
            """
            make row return in dictionary form
            :param cursor:
            :param row: row in result
            :return: dict
            """
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
                # d[col[0]] = str(row[idx]) if isinstance(row[idx], str) else row[idx]
            return d

        self._conn.row_factory = dict_factory
        self._cur = self._conn.cursor()
        self._instance = sqlite3

    def query(self, sql):
        """
        excute query sql
        :param sql:
        :return: return a list of dict
        """
        try:
            logging.info('excute query sql：{0}'.format(sql))
            self._cur.execute(sql)
            result = self._cur.fetchall()
        except sqlite3.Error as e:
            self._error_code = e.args[0]
            err_msg = "fail to excute query，error_code：{0}".format(repr(e))
            logging.error(err_msg)
            result = None
        finally:
            return result

    def update(self, sql):
        """
        excute update sql
        :param sql:
        :return: True: success , False: fail
        """
        try:
            logging.info('excute update sql：{0}'.format(sql))
            self._cur.execute(sql)
            self._conn.commit()
            result = True
        except sqlite3.Error as e:
            self._error_code = e.args[0]
            err_msg = "fail to excute update sql, error_code：{0}".format(repr(e))
            logging.error(err_msg)
            self.rollback()
            result = False
        finally:
            return result

    def update_dict(self, table_name, search_key, data_dict):
        """
        excute update dict sql
        :param table_name:
        :param search_key:
        :param data_dict:
        :return:
        """
        tmp_list = []
        for key, value in data_dict.iteritems():
            if isinstance(value, unicode) or isinstance(value, str):
                tmp_list.append(key + ' = ' + "'{}'".format(value))
            else:
                tmp_list.append(key + ' = ' + str(value))
        fields_str = ', '.join(tmp_list)
        if isinstance(data_dict[search_key], str):
            search_key_value = data_dict[search_key]
        else:
            search_key_value = str(data_dict[search_key])
        sql = 'UPDATE `' + table_name + '` SET ' + fields_str + ' WHERE ' + \
              search_key + ' = ' + search_key_value
        self.update(sql)

    def insert(self, sql):
        """
        excute update sql
        :param sql:
        :return: True: success , False: fail
        """
        try:
            logging.info("excute insert sql：{0}".format(sql))
            self._cur.execute(sql)
            self._conn.commit()
            result = True
        except sqlite3.Error as e:
            self._error_code = e.args[0]
            err_msg = "执行insert异常，错误：{0}".format(repr(e))
            logging.error(err_msg)
            self.rollback()
            result = False
        finally:
            return result

    def insert_dict(self, table_name, search_key, data_dict):
        """
        update insert dict sql
        :param table_name:
        :param search_key:
        :param data_dict:
        :return: True: success , False: fail
        """
        if isinstance(data_dict[search_key], str):
            search_key_value = data_dict[search_key]
        else:
            search_key_value = str(data_dict[search_key])
            # search_key_value = set_precision(data_dict[search_key], 6)

        query_str = 'SELECT * FROM `' + table_name + '` WHERE ' + search_key + ' = ' + \
                    search_key_value
        query_res = self.query(query_str)
        # if exist, update
        if query_res:
            self.update_dict(table_name, search_key, data_dict)
        # else, insert
        else:
            key_list = []
            value_list = []
            for key, value in data_dict.iteritems():
                key_list.append(key)
                value_list.append(str(value))
            key_str = '(' + ', '.join(key_list) + ')'
            value_str = '(' + ', '.join(value_list) + ')'
            insert_query = 'INSERT INTO `' + table_name + '` ' + key_str + 'VALUES' + value_str
            return self.insert(insert_query)

    def insert_many(self, table_name, data_list, fields_list=None):
        """
        insert multiple lines in one commit.
        :param table_name:
        :param data_list:
        :param fields_list:
        :return:
        """
        if not data_list:
            logging.info("data_list is None")
            return False

        if fields_list:  # The fields need to be consistent with the data_list
            fields_str = '(' + ', '.join(fields_list) + ')'
            question_mark_num = len(fields_list)
            question_mark_list = ['?' for _ in range(question_mark_num)]
            values_str = '(' + ', '.join(question_mark_list) + ')'
            sql = 'INSERT INTO `' + table_name + '` ' + fields_str + 'VALUES ' + values_str

        else:  # field_list is empty，the data_list fields need to be consistent with the table.
            question_mark_num = len(data_list[0])
            question_mark_list = ['?' for _ in range(question_mark_num)]
            values_str = '(' + ', '.join(question_mark_list) + ')'
            sql = 'INSERT INTO `' + table_name + '` VALUES ' + values_str

        try:
            logging.info('excute insert_many, sql: {}'.format(sql))
            self._cur.executemany(sql, data_list)
            self._conn.commit()
            result = True
        except sqlite3.Error as e:
            err_msg = 'fail to excute insert_many, error_code:{}'.format(repr(e))
            logging.error(err_msg)
            self.rollback()
            result = False
        finally:
            return result

    def delete(self, sql):
        """
        excute delete sql
        :param sql:
        :return:
        """
        try:
            logging.info('excute delete sql：{0}'.format(sql))
            self._cur.execute(sql)
            self._conn.commit()
            result = True
        except sqlite3.Error as e:
            self._error_code = e.args[0]
            err_msg = "fail to excute delete sql，error_code：{0}".format(repr(e))
            logging.error(err_msg)
            result = False
        finally:
            return result

    def create_table(self, sql):
        """
        create table
        :param sql:
        :return:
        """
        try:
            logging.info('excute create table sql : {0}'.format(sql))
            self._cur.execute(sql)
            self._conn.commit()
            result = True
        except sqlite3.Error as e:
            self._error_code = e.args[0]
            err_msg = "fail to create table, error_code: {0}".format(repr(e))
            logging.error(err_msg)
            result = False
        finally:
            return result

    def truncate_table(self, sql):
        """
        truncate table
        :param sql:
        :return:
        """
        try:
            logging.info('excute truncate sql: {0}'.format(sql))
            self._cur.execute(sql)
            self._conn.commit()
            result = True
        except sqlite3.Error as e:
            self._error_code = e.args[0]
            err_msg = "truncate error: {0}".format(repr(e))
            logging.error(err_msg)
            result = False
        finally:
            return result

    def get_description(self, table_name):
        """
        get description of table
        :param:
        :return:
        """
        try:
            sql = 'PRAGMA table_info({})'.format(table_name)
            logging.info('excute fields infomation of the table:{0}'.format(sql))
            self._cur.execute(sql)
            return self._cur.fetchall()
        except sqlite3.Error as e:
            self._error_code = e.args[0]
            err_msg = "获取表的字段信息,错误: {0}".format(repr(e))
            logging.error(err_msg)
            return None

    def get_table_names(self):
        """
        get all table names of the db3 database
        :return:
        """
        try:
            sql = 'SELECT name FROM sqlite_master WHERE type=\'table\' ORDER BY name'
            logging.info('excute sql:{0}'.format(sql))
            self._cur.execute(sql)
            return self._cur.fetchall()
        except sqlite3.Error as e:
            self._error_code = e.args[0]
            err_msg = "fail to get all table names，error_code:{0}".format(e)
            logging.error(err_msg)
            return None

    def fetch_all_rows(self):
        """
        return rows
        :return:
        """
        return self._cur.fetchall()

    def fetch_one_row(self):
        """
        return one row
        :return:
        """
        return self._cur.fetchone()

    def get_row_count(self):
        """
        return row affected
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
            if self._conn:
                self._conn.close()
        except Exception as e:
            logging.error("fail to release，error_code：{0}".format(repr(e)))

    def close(self):
        """
        release the resources manually.
        :return:
        """
        self.__del__()


def main():
    """
    测试
     """


if __name__ == '__main__':
    main()
