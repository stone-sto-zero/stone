# -*- encoding:utf-8 -*-
import os

import pandas as pd

from account.account import MoneyAccount
from chart.chart_utils import draw_line_chart, default_colors
from config import config
from data.back_result import DBResultMaSta
from data.info import resolve_dataframe
from log.log_utils import log_with_filename


def ema_n(pre_ema_n, close_price, n=12):
    """
    计算ema
    :param pre_ema_n: 前一天的ema
    :type pre_ema_n: float
    :param close_price: 收盘价
    :type close_price: float
    :param n: ema的天数, 默认12
    :type n: int
    :return: ema
    :rtype: float
    """
    return pre_ema_n * (n - 1) / (n + 1) + close_price * 2 / (n + 1)


def diff(pre_ema_small, pre_ema_large, close_price, small_value=12, large_value=26):
    """
    计算diff
    :param large_value: 大的ema的区间长度
    :type large_value: int
    :param small_value: 小的ema的区间长度
    :type small_value: int
    :param pre_ema_small: 前一天的ema12
    :type pre_ema_small: float
    :param pre_ema_large: 前一天的ema26
    :type pre_ema_large: float
    :param close_price:
    :type close_price: float
    :return:
    :rtype: float
    """
    return ema_n(pre_ema_small, close_price, small_value) - ema_n(pre_ema_large, close_price, large_value)


def dea(pre_dea, cur_diff):
    return pre_dea * 8 / 9.0 + cur_diff / 5


def macd_bar(cur_diff, cur_dea):
    return (cur_diff - cur_dea) * 2


def diff_love_dea(stock_series, stock_name, run_tag, need_write_db=True, need_png=False):
    """
    看下diff和dea纠缠的成功率
    :param stock_series: 数据源
    :type stock_series: pd.Series
    :param stock_name: st名称
    :type stock_name:str
    :param run_tag:执行标记
    :type run_tag:str
    :param need_write_db: 是否需要写入数据库
    :type need_write_db: bool
    :param need_png: 是否需要画图, 画出macd的dea和diff, account, st
    :type need_png: bool
    """
    # 扔掉空数据, 提高准确性和效率
    stock_series = stock_series.dropna(axis='index')

    # pre的数据, 认为都是0
    pre_ema12 = 0
    pre_ema26 = 0
    pre_dea = 0

    # 遍历的数据key
    date_strs = stock_series.index.values

    account = MoneyAccount(1000000, run_tag, repo_count=1)
    account_divider = stock_series[0] * 1000 / account.property

    # 图标相关
    account_values = list()
    diff_values = list()
    dea_values = list()

    # log
    log_file_path = os.path.join(config.log_root_path, 'macd_info.log')

    # -1 初始化 0 负数 1 正数
    status = -1

    # 统计信息
    w5c = 0
    w10c = 0
    w15c = 0
    w20c = 0
    w25c = 0
    w30c = 0
    wc = 0
    lc = 0
    maxdd = 10000
    account_max_value = -1

    for date_str in date_strs:

        stock_price = stock_series.loc[date_str]
        cur_ema12 = ema_n(pre_ema12, stock_price, 12)
        cur_ema26 = ema_n(pre_ema26, stock_price, 26)
        cur_diff = cur_ema12 - cur_ema26
        cur_dea = dea(pre_dea, cur_diff)
        cur_macd_bar = macd_bar(cur_diff, cur_dea)

        account.update_with_all_stock_one_line({stock_name: (stock_price, date_str)})

        # 图表相关
        account_values.append(account.property * account_divider / 1000)
        diff_values.append(cur_diff)
        dea_values.append(cur_dea)

        # maxdd
        if account.property > account_max_value:
            account_max_value = account.property
        cur_dd = account.property / account_max_value - 1
        if cur_dd < maxdd:
            maxdd = cur_dd

        if cur_macd_bar > 0:
            if status == 0:
                account.buy_with_repos(stock_name, stock_price, date_str)
            status = 1
        else:
            if status == 1:
                if stock_name in account.stocks.keys():
                    returns = account.stocks[stock_name].return_percent
                    if returns > 0.3:
                        w30c += 1
                    elif 0.25 < returns <= 0.3:
                        w25c += 1
                    elif 0.2 < returns <= 0.25:
                        w20c += 1
                    elif 0.15 < returns <= 0.2:
                        w15c += 1
                    elif 0.1 < returns <= 0.15:
                        w10c += 1
                    elif 0.05 < returns <= 0.1:
                        w5c += 1
                    elif 0 < returns <= 0.05:
                        wc += 1
                    else:
                        lc += 1
                    account.sell_with_repos(stock_name, stock_price, date_str)
            status = 0

        # 把cur变成pre
        pre_ema12 = cur_ema12
        pre_ema26 = cur_ema26
        pre_dea = cur_dea

    total = w30c + w25c + w20c + w15c + w10c + w5c + wc + lc
    wc += w30c + w25c + w20c + w15c + w10c + w5c
    w5p = float(w5c) / total
    w10p = float(w10c) / total
    w15p = float(w15c) / total
    w20p = float(w20c) / total
    w25p = float(w25c) / total
    w30p = float(w30c) / total
    wp = float(wc) / total

    log_with_filename(log_file_path, run_tag + ' wp: ' + str(wp) + ' returns : ' + str(account.returns))

    # 写数据库
    if need_write_db:
        result_db = DBResultMaSta()
        result_db.open()
        result_db.cursor.execute(
            'insert into %s (%s) values ("%s", %d, %d,%f, %d, %f, %d, %f, %d, %f, %d, %f, %d, %f, %d, %f, %f, %f)' % (
                result_db.table_name, ','.join(result_db.columns), run_tag, DBResultMaSta.type_macd, w5c, w5p, w10c,
                w10p, w15c, w15p,
                w20c, w20p, w25c, w25p, w30c, w30p, wc, wp, account.returns,
                maxdd))
        result_db.connection.commit()
        result_db.close()

    if need_png:
        result_path = os.path.join(os.path.dirname(__file__), '../result/')
        result_file_name = 'tmp.png'
        draw_line_chart(date_strs, [stock_series.values, diff_values, dea_values, account_values],
                        ['st', 'diff', 'dea', 'account'], default_colors[:4], result_file_name, output_dir=result_path)

if __name__ == '__main__':
    fix_frame, s01 = resolve_dataframe()
    # stock_names = fix_frame.columns.values
    # already_exist = False
    # for stock_name in stock_names:
    #     run_tag = stock_name + '_macd'
    #     print stock_name
    #     if stock_name == 's603028_ss':
    #         already_exist = True
    #         continue
    #     if not already_exist:
    #         continue
    #     diff_love_dea(fix_frame[stock_name], stock_name, run_tag, need_write_db=True, need_png=False)
    diff_love_dea(s01, 's000001_ss', 'test', need_write_db=False, need_png=True)
