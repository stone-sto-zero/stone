# encoding:utf-8

import pandas as pd

from data.info import resolve_dataframe


def find_peak_period(s01, base_down=0.1, shut_speed=0.001, min_down=0.03, max_up=0.05, down_up_percent=0.8, shut_by=0):
    """
    找到所有下跌的区间
    思路:
    1. 下跌开始的判定, 下跌超过base_down认为开始了下跌, base_down随着上升空间的增大而变小(shut_speed), 即涨的越多, 下跌反转判定越容易,
        但是base_down不能小于一个最小值(min_down)
    2. 下跌结束的判定, 两种情况:
        1. 突破最低上升空间要求(max_up), 适用于大牛的情况, 即缓冲一次, 继续向上, 产生的上升继续累积.
        2. 跌到此次上升空间的某一比例(down_up_percent), 认为下跌接近结束.

    :param shut_by: 0 天 1 百分比
    :type shut_by: int
    :param down_up_percent: 下跌结束判定时, 需要下跌占用上升的比例
    :type down_up_percent: float
    :param max_up:
    :type max_up: float
    :param min_down:
    :type min_down: float
    :param shut_speed:
    :type shut_speed: float
    :param base_down: 开始下跌的初始判定
    :type base_down: float
    :param s01: 数据源
    :type s01: pd.Series
    :return: 下跌的起始和结束的点, 起始的理想情况是, 有一定的延迟, 结束的理想情况是, 有些许的超前
    :rtype:list[tuple[str]]
    """
    date_strs = s01.index.values
    for date_str in date_strs:
        print date_str


if __name__ == '__main__':
    fix_frame, s01 = resolve_dataframe()
    find_peak_period(s01)
