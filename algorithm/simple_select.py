# encoding:utf-8
from data.info import resolve_dataframe


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


if __name__ == '__main__':
    select_up_percent('2016-08-10', '2016-08-19', 0.05, 3)
