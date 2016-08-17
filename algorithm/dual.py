# encoding:utf-8

# dual thrust 模拟
import os

from account.account import MoneyAccount
from chart.chart_utils import draw_line_chart, default_colors
from config import config
from data.back_result import DBResultMaSta
from data.db.db_helper import DBYahooDay
from data.info import DBInfoCache
import pandas as pd

from log.log_utils import log_with_filename


def dual_thrust(k=0.7, denominator=1, pre_n=1, buy_line=0, need_write_db=True, need_png=False):
    """
    因为这个参数比较多, 所以这个还是独立执行比较好
    :param k: range的系数
    :type k: float
    :param denominator: 分成的仓位总数
    :type denominator: int
    :param pre_n: 采集数据的天数
    :type pre_n: int
    :param buy_line: 采用当天的open为基准线还是前一天的close为基准线, 0 前一天的close, 1 当天的open
    :type buy_line: int
    :param need_write_db:
    :type need_write_db: bool
    :param need_png:
    :type need_png: bool
    """
    # 获取数据
    cache_db = DBInfoCache()
    rate_frame = cache_db.get_fix(frame_type=6).dropna(axis='index', thresh=3)
    open_frame = (cache_db.get_fix(frame_type=2).dropna(axis='index', thresh=3)) * rate_frame
    close_frame = (cache_db.get_fix(frame_type=3).dropna(axis='index', thresh=3)) * rate_frame
    high_frame = (cache_db.get_fix(frame_type=4).dropna(axis='index', thresh=3)) * rate_frame
    low_frame = (cache_db.get_fix(frame_type=5).dropna(axis='index', thresh=3)) * rate_frame

    stock_names = open_frame.columns.values

    log_file_path = os.path.join(config.log_root_path, ('dual_%f_%d_%d_%d.log' % (k, denominator, pre_n, buy_line)))

    # Debug时修改stock_names
    # stock_names = ['s000004_sz', ]

    for stock_name in stock_names:
        o = open_frame[stock_name].dropna(axis='index')
        """:type:pd.Series"""
        c = close_frame[stock_name].dropna(axis='index')
        """:type:pd.Series"""
        h = high_frame[stock_name].dropna(axis='index')
        """:type:pd.Series"""
        l = low_frame[stock_name].dropna(axis='index')
        """:type:pd.Series"""

        run_tag = '%s_dual_k%f_de%d_n%d_bl%d' % (stock_name, k, denominator, pre_n, buy_line)

        account = MoneyAccount(1000000, run_tag, repo_count=denominator)

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

        # 图标相关
        account_divider = c.iloc[0] * 1000 / account.property
        account_values = list()

        date_strs = o.index.values

        for date_str in date_strs:

            # 数据量不够, pass
            if len(o.loc[:date_str]) <= pre_n:
                continue

            # 获取数据空间
            ct = c.loc[:date_str].iloc[-pre_n - 1:-1]
            """:type:pd.Series"""
            ht = h.loc[:date_str].iloc[-pre_n - 1:-1]
            """:type:pd.Series"""
            lt = l.loc[:date_str].iloc[-pre_n - 1:-1]
            """:type:pd.Series"""

            # 找到range
            ran0 = ht.max() - ct.min()
            ran1 = ct.max() - lt.min()
            ran = max(ran0, ran1) * k

            # 确定基准线
            if buy_line == 0:
                bench = o.loc[date_str]
            else:
                bench = c.loc[:date_str].iloc[-2]

            # 更新当前price
            cur = c[date_str]
            account.update_with_all_stock_one_line({stock_name: (cur, date_str)})
            account_values.append(account.property * account_divider / 1000)

            # maxdd
            if account.property > account_max_value:
                account_max_value = account.property
            cur_dd = account.property / account_max_value - 1
            if cur_dd < maxdd:
                maxdd = cur_dd

            # 确定是否操作
            if cur > bench + ran:
                account.buy_with_repos(stock_name, cur, date_str, repo_count=1)
            elif cur < bench - ran:
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
                    account.sell_with_repos(stock_name, cur, date_str, repo_count=1)

        total = w30c + w25c + w20c + w15c + w10c + w5c + wc + lc
        if total == 0:
            log_with_filename(log_file_path, stock_name + ' total is 0')
            continue
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

        # 画图
        if need_png:
            result_path = os.path.join(os.path.dirname(__file__), '../result/dual')
            result_file_name = run_tag
            draw_line_chart(date_strs, [c.values, account_values],
                            ['st', 'account'], default_colors[:2], result_file_name,
                            output_dir=result_path)


if __name__ == '__main__':
    import datetime
    cur = datetime.datetime.now()
    dual_thrust(k=0.7, denominator=24, pre_n=3, buy_line=0, need_write_db=True, need_png=True)
    after = datetime.datetime.now()
    print after - cur
