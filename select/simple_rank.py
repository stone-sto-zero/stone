#! encoding:utf-8
from data.info import resolve_dataframe
import numpy as np
import pandas as pd


def select_by_last_n_days_up(fix_frame_source=None, n=3, rankpercent=0.1, target=10, use_last=3):
    """
    根据过去几天的percent排名, 找合适的st
    :param fix_frame_source: 数据源
    :type fix_frame_source: pd.DataFrame
    :param s01_source: 数据源
    :type s01_source: pd.Series
    :param use_last: 假设现在是几天前
    :type use_last: int
    :param rankpercent: 排名的范围
    :type rankpercent: float
    :param target: 选的数量
    :type target: int
    :param n: 几天
    :type n: int
    :return: 返回target数量
    :rtype: list[str]
    """
    if fix_frame_source is not None:
        fix_frame = fix_frame_source
    else:
        fix_frame, s01 = resolve_dataframe(frame_type=1)
    stock_names = fix_frame.columns.values
    ranks = fix_frame.iloc[-n - use_last: -use_last].mean()
    stock_names = sorted(stock_names, key=lambda x: ranks[x] if not np.isnan(ranks[x]) else -1, reverse=True)
    rank_start = int(len(stock_names) * rankpercent)
    count = 0
    res_list = list()
    for index in range(rank_start, len(stock_names)):
        res_list.append(stock_names[index])
        count += 1
        if count >= target:
            break

    return res_list


if __name__ == '__main__':
    # 找3天前rank选中的10个中, 最近三天超过5的数量
    fix_frame, s01 = resolve_dataframe(frame_type=1)
    target_value = 10
    use_last = 15
    tem = 0.1
    rank_percent_list = [res * 0.005 for res in range(0, 200)]
    for rank_percent in rank_percent_list:
        res_list = select_by_last_n_days_up(fix_frame_source=fix_frame, rankpercent=rank_percent, n=3, use_last=use_last,
                                            target=target_value)
        up_count = 0

        for res_st in res_list:
            res_up_total = -10000
            res_down_total = 10000
            total = 1

            for index in range(0, use_last):
                total *= (1+fix_frame[res_st][-use_last + index])
                if total > res_up_total:
                    res_up_total = total
                if total < res_down_total:
                    res_down_total = total

            if res_up_total - res_down_total > tem and res_up_total > 1.05:
                print res_st
                print res_up_total
                print res_down_total
                up_count += 1

        print 'rank percent %f win percent %f' % (rank_percent, float(up_count) / target_value)
    # print select_by_last_n_days_up(use_last=5, rankpercent=0)
