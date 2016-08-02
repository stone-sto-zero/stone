# -*- coding: utf-8 -*-

from datetime import datetime, date, time

# 基本的日期,时间格式
time_format = '%H:%M:%S'
date_format = '%Y-%m-%d'
datetime_format = date_format + ' ' + time_format

<<<<<<< HEAD
# 用作log输出的
datetime_log_format = '%Y_%m_%d_%H_%M_%S'

=======
>>>>>>> aa8b70edb9aeb433916f4729807c6124ac09ee92
# 一个历史时间, 不会影响当前的数据, 用来填充空数据, 判断是否是无效时间等
date_never_used = '1971-1-1'
time_never_used = '00:00:00'


def resolve_date(date_str):
    """
    从一个str中获取date
    :param date_str: 日期的str
    :type date_str: str
    :return: date
    :rtype: date
    """
    return datetime.strptime(date_str, date_format).date()


def resolve_time(time_str):
    """
    从一个str中获取time
    :param time_str:
    :type time_str: str
    :return: time
    :rtype: time
    """
    return datetime.strptime(time_str, time_format).time()
