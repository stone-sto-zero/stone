#! encoding:utf-8
import datetime

import math
import random

from account.account import MoneyAccount
from chart.chart_utils import draw_line_chart, default_colors
from config import config
from data.back_result import DBResult
from data.info import DBInfoCache, resolve_dataframe, resolve_real_dateframe
import numpy as np
import pandas as pd

from log import time_utils
from log.log_utils import log_with_filename


def find_next_group(ma_length=5, tem_length=3, rank_percent=0.382, denominator=4):
    """
    找到下个组合
    :return: 组合
    :rtype: list
    """
    fix_frame, s01 = resolve_dataframe()
    means = fix_frame.iloc[-ma_length:].mean()

    # 排序, 找到0.382
    stock_names = fix_frame.columns.values

    ranks = fix_frame.iloc[-1] / fix_frame.iloc[-tem_length]

    rank_count = 0
    for stock_name in stock_names:
        if not np.isnan(ranks[stock_name]):
            rank_count += 1

    percent_values = fix_frame.iloc[-1] / fix_frame.iloc[-2]
    stock_names = sorted(stock_names, key=lambda x: ranks[x] if not np.isnan(ranks[x]) else -1, reverse=True)
    count = 0
    start_point = int(rank_count * rank_percent)

    print stock_names
    print fix_frame.index.values[-1]
    print start_point
    print len(stock_names)
    for index in range(start_point, len(stock_names)):
        stock_name = stock_names[index]
        if fix_frame[stock_name].iloc[-1] >= means[stock_name] and -0.095 < percent_values[
            stock_names[index]] - 1 < 0.095:
            print index
            print stock_name
            count += 1
        if count >= denominator:
            break


def res_statistic():
    """
    统计各个参数对应情况下的结果, 写入cache数据库
    """
    import os
    res_file_dir_path = os.path.join(os.path.dirname(__file__), '../record')
    result_db = DBResult()
    result_db.open()
    file_count = 0
    for file_name in os.listdir(res_file_dir_path):
        if file_name.endswith('.log'):
            file_path = os.path.join(res_file_dir_path, file_name)
            # 先搞定各个参数的值
            params = file_name.split('_')
            params[-1] = params[-1][:-4]

            denominator = params[1]
            malength = params[3]
            temlength = params[5]
            reweightperiod = params[7] if params[7].strip() != 'None' else '-1'
            winpercent = params[9]
            needups01 = params[11]
            sellafterwieght = '1' if params[13].strip() == 'True' else '0'
            losepercent = params[15]
            rankposition = params[17] if params[17].strip() != 'None' else '-1'
            rankpercent = params[19]
            run_tag = params[20]

            # 然后是读出结果, 包含returns和maxdd
            block_size = '4096'
            with open(file_path, 'r') as fp:
                fp.seek(0, os.SEEK_END)
                cur_pos = fp.tell()
                block_size = min(cur_pos, block_size)
                fp.seek(cur_pos - block_size, os.SEEK_SET)
                block_data = fp.read()
                lines = block_data.split('\n')

                if lines[-3].find('max dd-') < 0:
                    continue

                returns = lines[-4].strip()
                maxdd = lines[-3][6:]

                result_tuple = (
                    denominator, malength, temlength, reweightperiod, winpercent, needups01, sellafterwieght,
                    losepercent, rankposition, rankpercent, maxdd, returns, run_tag,
                )
                print result_tuple
                result_db.cursor.execute('insert into %s (%s)values(%s)' % (
                    result_db.table_relative_strength_zero,
                    ','.join(result_db.relative_strenth_zero_columns),
                    ','.join(result_tuple)
                ))
                result_db.connection.commit()
                file_count += 1
                print file_count

    result_db.close()


def relative_strength_monentum(data_frame, s01, denominator=5, ma_length=1, tem_length=3, reweight_period=None,
                               win_percent=0.1,
                               need_up_s01=None, sell_after_reweight=False, lose_percent=0.2, rank_position=None,
                               rank_percent=0.35, standy_count=0, need_write_db=True):
    """
    第一个按照时间进行的算法, pandas 和numpy还不会用, 先随便写写, 回头一定要认真看看
    这个算法的a股验证: https://www.quantopian.com/algorithms/578dcb3a42af719b300007e4
    :param s01: 上证的数据
    :type s01: pd.Series
    :param data_frame: 数据集
    :type data_frame: pd.DataFrame
    :param need_write_db: 是否需要结果写入数据库
    :type need_write_db: bool
    :param standy_count: 后备资金的份数, 必须小于denominator, 否则测试应该跑不起来
    :type standy_count: int
    :param rank_percent: 在rank中的位置, 以这个位置来选股, 因为有的时候并不一定是涨的最快的表现最好
    :type rank_percent: float
    :param rank_position: 在rank中的位置, 以这个位置来选股, 因为有的时候并不一定是涨的最快的表现最好
    :type rank_position: int
    :param lose_percent: 是否需要止损, 止损点是多少
    :type lose_percent: float
    :param sell_after_reweight: 在reweight之后,是否需要卖出股票
    :type sell_after_reweight: bool
    :param need_up_s01: 买入的时候, 上证指数必须处于n日均线之上
    :type need_up_s01: int
    :param win_percent: 坐实收入的比例r
    :type win_percent: float
    :param reweight_period: 调整持仓比例的时间间隔, 单位是 天
    :type reweight_period: int
    :param ma_length: 在这条均线之上, 认为可以买, 等于1无意义
    :type ma_length: int
    :param tem_length: 积累relative strength 的时间长度, 最小为2, 1的话成长率肯定都是1,
                        段时间的增长率排序, 衡量st的rank, 是决定选股的关键时间,  需要大于均线的时间间隔
    :type tem_length: int
    :param denominator: 权重数量
    :type denominator: int
    """
    time_before = datetime.datetime.now()
    run_tag = 'relative_strength_' + time_before.strftime(time_utils.datetime_log_format) + '_' + str(
        random.randint(1000, 9999))
    fix_frame = data_frame

    date_list = fix_frame.index.values

    # 账户
    account = MoneyAccount(1000000, run_tag, repo_count=denominator)
    if need_write_db:
        account.open()

    # st list
    stock_names = fix_frame.columns.values

    # 用来计数成功和失败的交易次数, 仅限于win_percent和lose_percent 生效的时候
    win_count = 0
    lose_count = 0

    # 作图相关的值, 暂时只需要上证和account
    chart_account_divider = s01[0] * 100 / account.property
    chart_account_value = list()
    # 描述增长率
    chart_account_k250_value = list()
    chart_account_k60_value = list()
    chart_account_k30_value = list()
    chart_s01_value = list()
    import os
    chart_output_dir = os.path.join(config.png_root_path, 'relative_strength')
    if not os.path.exists(chart_output_dir):
        os.system('mkdir -p ' + chart_output_dir)

    # 搞定存图片的文件夹, 这次系统一点, 都保存下来, 用参数命名, 用收益/回撤做标题
    chart_title = 'de_%s_ma_%s_tem_%s_re_%s_win_%s_' \
                  'ups01_%s_sewei_%s_los_%s_rpo_%s_rpc_sdyc_%d_%s_%s' % (
                      str(denominator), str(ma_length), str(tem_length), str(reweight_period), str(win_percent),
                      str(need_up_s01), str(sell_after_reweight), str(lose_percent), str(rank_position), standy_count,
                      str(rank_percent), run_tag)

    # reweight的计数
    reweight_count = 0

    # 回撤
    max_dd = 0
    max_property = 0

    for date_str in date_list:

        # 所有的历史数据
        tem_rows = fix_frame.loc[:date_str]

        # 不管怎么样, chart的值都是需要的
        chart_s01_value.append(s01[date_str])
        # 做一下update, 然后把account的值也放进去, 稍有误差, 不过无非就是操作过程中出现的手续费, 因为当天的价格只有一个
        for stock_name in account.stocks.keys():
            if not np.isnan(tem_rows.loc[date_str, stock_name]):
                account.update_with_all_stock_one_line({stock_name: (tem_rows.loc[date_str, stock_name], date_str)})
        chart_account_value.append(account.property * chart_account_divider / 100)

        # 描述增长率, 250日增长率, 10000倍放大, 查看效果
        if len(chart_account_value) > 250:
            log_with_filename(chart_title, 'k250 : ' + str(
                (account.property / chart_account_value[-250] / 100 * chart_account_divider - 1) * 10000))
            chart_account_k250_value.append(
                (account.property / chart_account_value[-250] / 100 * chart_account_divider - 1) * 10000)
        else:
            log_with_filename(chart_title, 'k250 : 0')
            chart_account_k250_value.append(0)

        # 描述增长率, 60日增长率, 10000倍放大, 查看效果
        if len(chart_account_value) > 60:
            log_with_filename(chart_title, 'k60 : ' + str(
                (account.property / chart_account_value[-60] / 100 * chart_account_divider - 1) * 10000))
            chart_account_k60_value.append(
                (account.property / chart_account_value[-60] / 100 * chart_account_divider - 1) * 10000)
        else:
            log_with_filename(chart_title, 'k60 : 0')
            chart_account_k60_value.append(0)

        # 描述增长率, 30日增长率, 10000倍放大, 查看效果

        if len(chart_account_value) > 30:
            log_with_filename(chart_title, 'k30 : ' + str(
                (account.property / chart_account_value[-30] / 100 * chart_account_divider - 1) * 10000))
            chart_account_k30_value.append(
                (account.property / chart_account_value[-30] / 100 * chart_account_divider - 1) * 10000)
        else:
            log_with_filename(chart_title, 'k30 : 0')
            chart_account_k30_value.append(0)

        # 计算回撤
        if account.property > max_property:
            max_property = account.property
        cur_dd = account.property / max_property - 1
        if cur_dd < max_dd:
            max_dd = cur_dd
        log_with_filename(chart_title, 'max dd : ' + str(max_dd))

        # 开始循环
        # 过去tem_length的
        if len(tem_rows.index) < tem_length:
            continue
        tem_rows = tem_rows[-tem_length:]

        rank_place = 0
        if rank_position:
            rank_place = rank_position

        # 更新账户
        # 最新价格的dict
        # stock_price_lines = dict()
        # for stock_name in account.stocks.keys():
        #     if not np.isnan(tem_rows.loc[date_str, stock_name]):
        #         stock_price_lines[stock_name] = (tem_rows.loc[date_str, stock_name], date_str,)
        #
        # account.update_with_all_stock_one_line(stock_price_lines)

        # 发现收益超过win_percent或者价格已经不在了,就撤
        if win_percent:
            for stock_name in account.stocks.keys():
                if not np.isnan(tem_rows.loc[date_str, stock_name]):
                    if account.stocks[stock_name].return_percent > win_percent:
                        account.sell_with_repos(stock_name, tem_rows.loc[date_str, stock_name], date_str,
                                                account.stock_repos[stock_name])
                        win_count += 1
                        # else:
                        #     if account.stocks[stock_name].return_percent > 0:
                        #         win_count += 1
                        #     else:
                        #         lose_count += 1
                        #     account.sell_with_repos(stock_name, account.stocks[stock_name].cur_price, date_str,
                        #                             account.stock_repos[stock_name])

        # 发现收益已经低于lose_percent, 则补仓
        if lose_percent and standy_count > 0:
            if account.cur_repo_left > 0:
                for stock_name in account.stocks.keys():
                    cur_return = account.stocks[stock_name].return_percent
                    # 如果补仓也还是不能搞定lose(这里只考虑第一次补仓不足的情况
                    # 如果是第二次, 应该是大于1.5*lose_percent, 目测这种情况很少, 且损失无非就是手续费), 放弃
                    if -lose_percent * 2 < cur_return < -lose_percent:
                        if not np.isnan(tem_rows.loc[date_str, stock_name]):
                            account.buy_with_repos(stock_name, tem_rows.loc[date_str, stock_name], date_str, 1)
                            # else:
                            #     if account.stocks[stock_name].return_percent > 0:
                            #         win_count += 1
                            #     else:
                            #         lose_count += 1
                            #     account.sell_with_repos(stock_name, account.stocks[stock_name].cur_price, date_str,
                            #                             account.stock_repos[stock_name])

        # 发现收益低于lose_percent或者价格已经不在了, 就撤
        if lose_percent:
            for stock_name in account.stocks.keys():
                if not np.isnan(tem_rows.loc[date_str, stock_name]):
                    if account.stocks[stock_name].return_percent < -lose_percent:
                        account.sell_with_repos(stock_name, tem_rows.loc[date_str, stock_name], date_str,
                                                account.stock_repos[stock_name])
                        lose_count += 1
                        # else:
                        #     if account.stocks[stock_name].return_percent > 0:
                        #         win_count += 1
                        #     else:
                        #         lose_count += 1
                        #     account.sell_with_repos(stock_name, account.stocks[stock_name].cur_price, date_str,
                        #                             account.stock_repos[stock_name])

        log_with_filename(chart_title, 'win_count, lose_count : %d %d' % (win_count, lose_count))

        # 是否需要根据reweight period进行reweight
        if reweight_period:
            reweight_count += 1
            # 每隔reweight进行一次就可以
            if reweight_count < reweight_period:
                continue
            reweight_count = 0

        # 如果需要上证均线之上, 在这里进行检查, 不满足, 直接跳过
        if need_up_s01:
            if len(s01.loc[:date_str]) > need_up_s01:
                if s01[date_str] < s01.loc[:date_str].iloc[-need_up_s01].mean():
                    continue
            else:
                continue

        # 排序
        ranks = tem_rows.iloc[-1] / tem_rows.iloc[0]

        # 找到排在rank_percent 的st
        rank_count = 0
        if rank_percent:
            # 去除为nan的部分
            for stock_name in stock_names:
                if not np.isnan(ranks[stock_name]):
                    rank_count += 1

            rank_place = int(rank_count * rank_percent)
            log_with_filename(chart_title, 'rank place : ' + str(rank_place))

        stock_names = sorted(stock_names, key=lambda x: ranks[x] if not np.isnan(ranks[x]) else -1, reverse=True)

        # 找出denominator个在均线之上的st, 且不是涨停的
        # 均线
        ma_values = fix_frame.loc[:date_str].iloc[-ma_length:].mean()
        percent_values = fix_frame.loc[:date_str].iloc[-1] / fix_frame.loc[:date_str].iloc[-2]
        # 选出来
        res_weight = list()
        count = 0

        for index in range(rank_place, len(stock_names)):
            if tem_rows.loc[date_str, stock_names[index]] > \
                    ma_values[stock_names[index]] and -0.095 < percent_values[stock_names[index]] - 1 < 0.095:
                res_weight.append(stock_names[index])
                count += 1
            if count >= denominator:
                break

        log_with_filename(chart_title, date_str)
        log_with_filename(chart_title, res_weight)

        # # 调整持仓比例
        # # 先把不在top rank中的给卖了
        if sell_after_reweight:
            for stock_name in account.stocks.keys():
                if stock_name not in res_weight:
                    # 先确认这个st还在, 有可能已经消失了, 如果已经消失, 用最后一次的价格, 直接卖掉
                    if np.isnan(tem_rows.loc[date_str, stock_name]):
                        account.sell_with_repos(stock_name, account.stocks[stock_name].cur_price, date_str,
                                                repo_count=account.stock_repos[stock_name])
                    else:
                        account.sell_with_repos(stock_name, tem_rows.loc[date_str, stock_name], date_str,
                                                repo_count=account.stock_repos[stock_name])

        # 然后把在top rank中, 但是还没买的给补上, 如果买了, 用新的价格update一下
        for stock_name in res_weight:
            if stock_name not in account.stocks.keys():
                if account.cur_repo_left > standy_count:
                    account.buy_with_repos(stock_name, tem_rows.loc[date_str, stock_name], date_str, repo_count=1)
            else:
                account.update_with_all_stock_one_line({stock_name: (tem_rows.loc[date_str, stock_name], date_str)})

        log_with_filename(chart_title, account.returns)

    # 关闭account
    if need_write_db:
        account.close()

    # 写DBResult数据库
    if need_write_db:
        result_db = DBResult()
        result_db.open()
        if not reweight_period:
            reweight_period = -1
        if not rank_position:
            rank_position = -1
        if not rank_percent:
            rank_percent = -1
        need_up = need_up_s01 if need_up_s01 else -1
        sell_after = 1 if sell_after_reweight else 0
        result_db.cursor.execute(
            'insert into %s (%s) values (%d, %d, %d, %d, %f, %d, %d, %f, %d, %f, %d, "%s", "%s", %f, %f)' % (
                result_db.table_relative_strength_zero, ','.join(result_db.relative_strenth_zero_columns),
                denominator, ma_length, tem_length, reweight_period, win_percent, need_up, sell_after,
                lose_percent, rank_position, rank_percent, standy_count, run_tag, date_list[-1], max_dd, account.returns
            ))
        result_db.connection.commit()
        result_db.close()

    # 表格相关

    chart_file_name = 'returns_%f_maxdd_%f_%s' % (account.returns, max_dd, run_tag)
    draw_line_chart(date_list, [chart_s01_value, chart_account_value, chart_account_k250_value, chart_account_k60_value,
                                chart_account_k30_value],
                    ['s01', 'account', 'k250', 'k60', 'k30'], default_colors[:5],
                    chart_file_name, title=chart_title, output_dir=chart_output_dir)

    print chart_file_name
    print chart_output_dir

    # 写log
    log_with_filename(chart_title, account)
    log_with_filename(chart_title, account.returns)
    log_with_filename(chart_title, 'max dd' + str(max_dd))
    log_with_filename(chart_title, 'time cose : ' + str(datetime.datetime.now() - time_before))


if __name__ == '__main__':
    pass
    fix_frame, s01 = resolve_dataframe()
    # # 执行回测, 先注释掉, 跑结果统计
    temlist = range(2, 22)
    malengthlist = [18, 19, 20, 21]
    for tem in temlist:
        for malength in malengthlist:
            relative_strength_monentum(fix_frame, s01, denominator=4, win_percent=0.25,
                                       lose_percent=0.1, rank_percent=0.382, need_up_s01=20, ma_length=malength,
                                       tem_length=tem)
            # find_next_group(ma_length=5, tem_length=3, rank_percent=0.382, denominator=4)

            # debug专用
            # lose_list = [0.1, 0.15, 0.2, 0.25]
            # for lose in lose_list:
            #     relative_strength_monentum(fix_frame, s01, denominator=4, win_percent=0.05, lose_percent=lose,
            #                                rank_percent=0.382,
            #                                need_up_s01=20, need_write_db=False, ma_length=3, tem_length=3)
            # 统计
            # res_statistic()

            # 测试
            # relative_strength_monentum(denominator=5, win_percent=0.2, lose_percent=0.2,
            #                                     rank_percent=0.38)

            # 用最新的数据执行一次, 用于选股
            # fix_frame, s01 = resolve_real_dateframe()
            # print fix_frame
            # relative_strength_monentum(fix_frame, s01, denominator=4, win_percent=0.25, lose_percent=0.1, rank_percent=0.382,
            #                            need_up_s01=20, ma_length=3, need_write_db=False)
