# encoding:utf-8
from data.info import resolve_dataframe
import pandas as pd


def select_up_percent(time_from, time_end, up_percent, up_time):
    """
    找出指定时间段内, 有uptime次上涨超过up_percent的st
    :param up_time: 上涨超过的次数
    :type up_time: int
    :param time_from: 起始时间
    :type time_from: str
    :param time_end: 结束时间
    :type time_end: str
    :param up_percent: 上涨要超过的百分比
    :type up_percent: float
    :return: 选出的st
    :rtype: list[str]
    """
    percent_frame = resolve_dataframe(frame_type=1)[0]
    stock_names = percent_frame.columns.values
    res_list = list()
    for stock_name in stock_names:
        pt = percent_frame[stock_name].dropna(axis='index')
        count = 0
        for value in pt.loc[time_from:time_end]:
            if value > up_percent:
                count += 1
                if count >= up_time:
                    res_list.append(stock_name)
                    break

    print 'total : ' + str(len(res_list))
    print res_list
    return res_list


def select_up_percent_continuous(time_from, time_end, up_percent, continuous_days, up_percent_max, last_down):
    """
    找出连续几天，上涨超过up_percent的st
    :param last_down: 最后一天是下跌的
    :type last_down: bool
    :param up_percent_max: 上涨的最大值
    :type up_percent_max: float
    :param time_from:
    :type time_from: str
    :param time_end:
    :type time_end: str
    :param up_percent:
    :type up_percent: float
    :param continuous_days:
    :type continuous_days: int
    :return:
    :rtype: list[str]
    """
    percent_frame = resolve_dataframe(frame_type=1)[0]
    stock_names = percent_frame.columns.values
    res_list = list()
    for stock_name in stock_names:
        pt = percent_frame[stock_name].dropna(axis='index')
        """:type:pd.Series"""
        if last_down and pt.iloc[-1] >= 0:
            continue
        date_strs = pt.loc[time_from:time_end].index.values
        # 最后一天上涨小于一定幅度
        for date_str in date_strs:
            if up_percent_max >= pt.loc[:date_str].iloc[-continuous_days:].sum() >= up_percent:
                res_list.append(stock_name)
                break

    print 'total : ' + str(len(res_list))
    print res_list
    return res_list


if __name__ == '__main__':
    pass
    # 10天3up对应大热点，5天2up对应小热点
    # select_up_percent('2016-08-16', '2016-08-19', 0.05, 2)
    # 找出两天上涨超过0.05的st
    select_up_percent_continuous('2016-08-31', '2016-09-06', 0.04, 2, 0.065, True)
