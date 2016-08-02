# -*- coding: utf-8 -*-
from config import config

__author__ = 'wgx'

import logging
import os
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filemode='a')

log_path_root = config.log_root_path

# 根据时间log的初始化
t_date_file_path = os.path.join(log_path_root, time.strftime('%Y-%m-%d') + '.log')
date_logger = logging.getLogger('date_logger')
date_file_handler = logging.FileHandler(t_date_file_path)
date_logger.addHandler(date_file_handler)

file_dict = dict()


def log_by_time(t_target_str):
    """
    根据时间，每天产生一份日志
    """
    date_logger.info(t_target_str)


def log_with_filename(filename, content):
    """
    :param content: 内容, 自动转成str
    :type content:
    :param filename: 文件名, 默认保存在record下面
    :type filename: str
    """
    if filename in file_dict:
        file_dict.get(filename).info(str(content))
    else:
        log_file_path = os.path.join(config.log_root_path, filename + '.log')
        file_loger = logging.getLogger('filename' + filename)
        file_handler = logging.FileHandler(log_file_path)
        file_loger.addHandler(file_handler)
        file_loger.info(str(content))
        file_dict[filename] = file_loger
