#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
"""
pack some request method.

Authors: changhuan(changhuan1993@gmail.com)
Date:    18/7/04
"""
import logging
import json

import requests
import request.HTTPAdapter

def post_message(self, url=None, data=None, timeout=300):
    """
    post data
    :param url:
    :param data:
    :param timeout:
    :return:
    """
    if url is None or data is None:
        return False
    logging.info("推送消息, url: {}, data: {}".format(url, json.dumps(data)))

    # 增加重试次数
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=3))
    s.mount('https://', HTTPAdapter(max_retries=3))

    i = 0
    while True:
        try:
            ret = s.post(url=url, data=json.dumps(data), timeout=timeout)
            if ret.status_code == 200:
                return ret.content
            else:
                if i > 2:
                    raise Exception("重试3次，出现状态码 {} 错误，".format(ret.status_code))
                i = i + 1
                continue
        except Exception as e:
            err_msg = "推送消息失败：{}, msg: {}".format(url, e.args[0])
            logging.error(err_msg)
            raise Exception(err_msg)

def post_file(self, post_url, file_path, timeout=300):
    """
    post file
    :param post_url:
    :param file_path:
    :param timeout:
    :return:
    """
    if not os.path.exists(file_path):
        raise Exception('file does not exist, it can not post: {}.'.format(file_path))
    files = {'file': open(file_path, 'rb')}
    logging.info("文件上传, url: {}, file_path: {}".format(post_url, file_path))

    # 增加重试次数
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=3))
    s.mount('https://', HTTPAdapter(max_retries=3))

    i = 0
    while True:
        try:
            ret = s.post(url=post_url, files=files,timeout)
            if ret.status_code == 200:
                logging.info("file post succeed, content: {}".format(ret.content))
                return ret.content
            else:
                if i > 2:
                    raise Exception("重试3次，出现状态码 {} 错误，".format(ret.status_code))
                i = i + 1
                continue
        except Exception as e:
            err_msg = "文件上传失败：{}, msg: {}".format(post_url, e.args[0])
            logging.error(err_msg)
            raise Exception(err_msg)


def download_file(self, download_url, file_path, params=None, timeout=300):
    """
    下载文件接口
    :param download_url:
    :param file_path:
    :param params:
    :param timeout:
    :return:
    """
    # 增加重试次数
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=3))
    s.mount('https://', HTTPAdapter(max_retries=3))

    i = 0
    while True:
        try:
            ret = s.get(url=download_url, params=params, timeout)
            if ret.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(ret.content)
                return True
            else:
                if i > 2:
                    raise Exception("重试3次，出现状态码 {} 错误，".format(ret.status_code))
                i = i + 1
                continue
        except Exception as e:
            err_msg = "文件下载失败：{}, msg: {}".format(download_url, e.args[0])
            logging.error(err_msg)
            raise Exception(err_msg)
