# encoding:utf-8

import pandas as pd
import numpy as np

# 在这里验证所有ma的情况
from account.account import MoneyAccount
from chart.chart_utils import draw_line_chart, default_colors
from config import config
from data.back_result import DBResultMaSta
from data.info import resolve_dataframe
import os

from log.log_utils import log_with_filename


def back_test_ma(stock_series, stock_name, run_tag, m=5, need_write_db=True, need_png=False):
    """
    :param need_png: 是否需要画图, 画图会把st, 相应的均线, 以及account的情况记录下
    :type need_png: bool
    :param need_write_db: 是否需要写入数据库
    :type need_write_db: bool
    :param stock_name: st名称
    :type stock_name: str
    :param run_tag: 标记这次执行
    :type run_tag: str
    :param stock_series: 数据源
    :type stock_series: pd.Series
    :param m: ma长度
    :type m: int
    """
    stock_series = stock_series.dropna(axis='index')

    log_file_path = os.path.join(config.log_root_path, 'ma_info.log')

    date_strs = stock_series.index.values
    account = MoneyAccount(1000000.0, run_tag)
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

    account_values = list()
    stock_values = list()
    stock_mean_values = list()
    account_divider = stock_series.iloc[0] * 1000 / account.property

    for date_str in date_strs:
        # 数量不够, pass
        if len(stock_series.loc[:date_str]) < m:
            continue

        stock_source = stock_series.loc[:date_str][-m:]
        stock_price = stock_series[date_str]
        stock_mean = stock_source.mean()

        account.update_with_all_stock_one_line({stock_name: (stock_price, date_str)})

        # 表格相关
        account_values.append(account.property * account_divider / 1000)
        stock_values.append(stock_price)
        stock_mean_values.append(stock_mean)

        if account.property > account_max_value:
            account_max_value = account.property

        cur_dd = account.property / account_max_value - 1
        if cur_dd < maxdd:
            maxdd = cur_dd

        if stock_price > stock_mean:
            account.buy_with_repos(stock_name, stock_price, date_str, 1)
        else:
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
                account.sell_with_repos(stock_name, stock_price, date_str, 1)

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

    if need_write_db:
        simu_type = -1
        if m == 5:
            simu_type = DBResultMaSta.type_ma5
        elif m == 10:
            simu_type = DBResultMaSta.type_ma10
        elif m == 15:
            simu_type = DBResultMaSta.type_ma15
        elif m == 20:
            simu_type = DBResultMaSta.type_ma20
        elif m == 25:
            simu_type = DBResultMaSta.type_ma25
        elif m == 30:
            simu_type = DBResultMaSta.type_ma30
        result_db = DBResultMaSta()
        result_db.open()
        result_db.cursor.execute(
            'insert into %s (%s) values ("%s", %d, %d,%f, %d, %f, %d, %f, %d, %f, %d, %f, %d, %f, %d, %f, %f, %f)' % (
                result_db.table_name, ','.join(result_db.columns), run_tag, simu_type, w5c, w5p, w10c, w10p, w15c, w15p,
                w20c, w20p, w25c, w25p, w30c, w30p, wc, wp, account.returns,
                maxdd))
        result_db.connection.commit()
        result_db.close()

    if need_png:
        result_path = os.path.join(os.path.dirname(__file__), '../result/')
        result_file_name = 'tmp.png'
        draw_line_chart(date_strs, [stock_values, stock_mean_values, account_values], ['st', 'ma', 'account'],
                        default_colors[:3], result_file_name, output_dir=result_path)


def start_back_test_for_all():
    fix_frame, s01 = resolve_dataframe()
    stock_names = fix_frame.columns
    ma = 5
    already_exist = False
    for stock_name in stock_names:
        if stock_name == 's002493_sz':
            already_exist = True
            continue
        if not already_exist:
            continue
        run_tag = stock_name + '_ma' + str(ma)
        back_test_ma(fix_frame[stock_name], stock_name, run_tag, m=ma)
    back_test_ma(fix_frame['s600171_ss'], 's600171', 'tmp600171', m=20, need_png=True)


if __name__ == '__main__':
    start_back_test_for_all()
