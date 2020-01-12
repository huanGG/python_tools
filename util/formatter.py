#!/usr/bin/env python
# -*- coding:utf-8 -*-
#

"""
provide functions to format json or numbers.

Authors: changhuan(changhuan1993@gmail.com)
Date:    2018/7/04
"""
import datetime

import json


class BetterJsonEncoder(json.JSONEncoder):
    """
    enhance the original json formatter.
    """
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


def set_precision(value, decimal_num=2):
    """
    set number in specified precsion
    :param value: the number need to be formatted
    :param decimal_num: to control precision
    :return:
    """
    decimal_format = '.{}f'.format(decimal_num)
    return format(value, decimal_format)
