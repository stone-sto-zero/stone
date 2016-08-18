# encoding:utf-8

import pandas as pd

from account.account import MoneyAccount
from data.info import resolve_dataframe, DBInfoCache


class EvaZero(object):
    """
    method概况:
    limit_eva**: 限制条件
    select_eva**: st筛选策略
    opt_eva**: 操作策略
    """

    def __init__(self, run_tag, close_frame, open_frame=None, high_frame=None, low_frame=None, percent_frame=None):
        """
        初始化, 只需要数据就可以, 注意的是需要使用fix_rate修正
        :param percent_frame:
        :type percent_frame: pd.DataFrame
        :param close_frame: close是一定要的, 因为所有的操作一定是基于这个的
        :type close_frame: pd.DataFrame
        :param open_frame:
        :type open_frame: pd.DataFrame
        :param high_frame:
        :type high_frame: pd.DataFrame
        :param low_frame:
        :type low_frame: pd.DataFrame
        """
        super(EvaZero, self).__init__()
        # eva的数据
        self.close_frame = close_frame
        self.open_frame = open_frame
        self.high_frame = high_frame
        self.low_frame = low_frame
        self.percent_frame = percent_frame

        # 统计信息
        self.run_tag = run_tag

        # 系统变化的环境记录, 可能有一些参数, 要在这里手动去改, 后续有需要可以考虑开放到class的接口中
        self.account = MoneyAccount(1000000, run_tag, repo_count=1)
        self.cur_date = ''
        self.cur_select = list()
        self.data_set_close = dict()
        self.data_set_percent = dict()
        self.s01 = None

    def limit_eva_ma_and_k(self, ma_group=(5, 10, 20, 30), k_max=1.0, k_range=(-0.33, 0.33),
                           cell_width=0.006):
        """
        通过均线和斜率限制, 满足以下任意条件则认为可行
        1、站在5日线上，并且5，10, 20， 30的斜率均小于1
        2、四根均线的斜率在-0.33 ~ 0.33 之间
        :param cell_width: x轴一个单位的宽度, 单位percent
                            0.006 斜率大于1的临界点
                            5 0.03 10 0.06 20 0.12 30 0.18
        :type cell_width: float
        :param k_max: 第一个条件中的最大斜率
        :type k_max: float
        :param k_range: 第二个条件中的范围
        :type k_range: tuple[float]
        :param ma_group: 均线组合, 第0个位置是要站上的位置
        :type ma_group: tuple[int]
        :return: 是否满足条件
        :rtype: bool
        """
        # 长度不够
        if len(self.s01.loc[:self.cur_date]) < ma_group[-1]:
            return False

        # 数据不在
        if self.cur_date not in self.s01.index.values:
            return False

        # 第一个条件
        con1 = True
        con2 = False

        # 数据段
        dt = self.s01.loc[:self.cur_date].iloc[-ma_group[-1]:]
        # 各种线的值
        ma_value = list()
        # 前一天的线值
        ma_value_1 = list()
        # 各种线的斜率
        k_values = list()
        cell_length = self.s01.loc[self.cur_date] * cell_width
        for m_value in ma_group:
            ma_value.append(dt[-m_value:].mean())
            ma_value_1.append(dt.iloc[-m_value:-1].mean())
            k_values.append((ma_value[-1] - ma_value_1[-1]) / cell_length)
            if k_values[-1] >= k_max:
                con1 = False
            if k_range[0] <= k_values[-1] <= k_range[1]:
                con2 = True

        for k_value in k_values:
            if k_value >= 1:
                con1 = False
                break

        if con1:
            if self.s01.loc[self.cur_date] <= ma_value[0]:
                con1 = False

        return con1 or con2

    def select_eva_high_and_continue(self, n=7, m=3, a=2, b=0.05, c=0.1, d=0.05, x=2):
        """
        找到拉升后的第一次小幅下降的st
        1.找到前n天，前面n-x天上涨, 后面x天下跌, 前面每隔m天的两个close, 都是后面大于前面的,
            前面n-x天,有a次，上涨超过b，然后当前处于距离最高下跌小于c的股票，且整体上有d的上涨
        2.如果集合够大，要使用随机策略选择，这样多次运行之后，可以求得一个可靠的范围，表明是否有效果
        :return: portfolio
        :rtype: list
        """
        # 在所有的st中找
        self.cur_select = list()
        for stock_name in self.data_set_close.keys():
            data_source = self.data_set_close[stock_name]
            """:type:pd.Series"""

            # 不够n天
            if len(data_source.loc[:self.cur_date]) < n:
                continue
            ct = data_source.loc[:self.cur_date].iloc[-n:]
            """:type: pd.Series"""
            pt = self.data_set_percent[stock_name].loc[:self.cur_date].iloc[-n:]

            # 前面n天的前n-x天, 都是每隔m天, 都是close递增
            is_ok = True
            for index in range(0, n - x - m):
                if ct.iloc[index] > ct.iloc[index + m]:
                    is_ok = False
                    break
            if not is_ok:
                continue

            # 前面n-x天,有a次上涨超过b
            if not len(pt.iloc[:n - x][pt > b]) >= a:
                continue

            # 当前距离最高下跌c
            if not ct.iloc[-1] >= ct.max() * (1 - c):
                continue

            # 整体上涨d
            if not ct.iloc[-1] >= ct.iloc[0] * (1 + d):
                continue

            self.cur_select.append(stock_name)

    def opt_eva_confirm_win_and_fix_lose(self):
        """
        止盈和修补策略
        止盈：    上涨超过a后，出现跌破10日均线可卖
        仓位控制：设置n仓，按照选股策略依次建一仓，如果出现损失，那么当损失到达b时, 或者持续了很久的损失，建仓时优先补仓，
                每次按照当前持仓量1:1补，不够少补也可以，没补一次止盈的a下降a/n个点，最坏情况下，最后平仓卖出
        止损：    仅仅通过大盘的位置进行止损，如果任意均线斜率大于1，进行无理由卖出
        """

    def fix_data_accu(self):
        """
        修复数据的问题
        """
        # 处理数据, 把dataframe转成series的一个dict, 因为有的数据中间包含nan, 导致ma等数据受到影响
        # 暂时只包含close_frame
        stock_names = self.close_frame.columns.values
        for stock_name in stock_names:
            self.data_set_close[stock_name] = self.close_frame[stock_name].dropna(axis='index')
            self.data_set_percent[stock_name] = self.percent_frame[stock_name].dropna(axis='index')
        self.s01 = self.data_set_close['s000001_ss']

    def start_eva(self):
        """
        启动algo(改变eva的时钟), 期望这里进行各种方法的组合, 因为一切的输入都只有数据, 所以过程应该是通用的
        """
        self.fix_data_accu()
        # 开始loop
        date_strs = self.s01.index.values
        for date_str in date_strs:
            # 更新日期
            self.cur_date = date_str
            # 如果日期不满足条件, pass
            if not self.limit_eva_ma_and_k():
                continue

            self.select_eva_high_and_continue()
            self.opt_eva_confirm_win_and_fix_lose()

    def select_for_day(self, pre_n):
        """
        为某一天选股
        """
        self.fix_data_accu()
        self.cur_date = self.s01.index.values[-pre_n]
        print self.cur_date
        self.select_eva_high_and_continue()
        print self.cur_select


if __name__ == '__main__':
    info_cache = DBInfoCache()
    close_frame = info_cache.get_fix(frame_type=0)
    percent_frame = info_cache.get_fix(frame_type=1)
    EvaZero(run_tag='', close_frame=close_frame, percent_frame=percent_frame).select_for_day(80)
