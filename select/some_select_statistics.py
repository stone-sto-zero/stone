# encoding:utf-8

# 这里进行各种选择策略的回测, 选择的结果组合需要足够大, 然后通过随机的方式, 进行多次回测, 最终通过统计方法求取一个最终值.
# 每种选择都应建立自己的结果表, 用来存放参数和结果的各项指标
import random

import datetime
import pandas as pd
import numpy as np
import os
import sys

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, create_engine, Integer, Float, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

from account.account import MoneyAccount
from chart.chart_utils import draw_line_chart, default_colors
from config import config
from data.info import resolve_dataframe, DBInfoCache
from log.log_utils import log_with_filename

BaseTable = declarative_base()


class BaseSelect(object):
    """
    选择的通用接口
    """

    db_path = os.path.join(config.db_root_path, 'result_db.db')

    def __init__(self, ft, pet, pot, sft, spet, spot, close, s_close, volume, s_volume):
        """
        :type s_volume: pd.Series
        :type volume: pd.DataFrame
        :type close: pd.DataFrame
        :type s_close: pd.Series
        :type spot: pd.Series
        :type spet: pd.Series
        :type sft: pd.Series
        :param ft: fix
        :type ft: pd.DataFrame
        :param pet: percent
        :type pet: pd.DataFrame
        :param pot: point
        :type pot: pd.DataFrame
        """
        super(BaseSelect, self).__init__()
        self.volume = volume
        self.s_volume = s_volume
        self.s_close = s_close
        self.close = close
        self.spot = spot
        self.spet = spet
        self.sft = sft
        self.pot = pot
        self.pet = pet
        self.ft = ft

    def get_cur_select(self, cur_date):
        """
        :param cur_date:当前日期
        :type cur_date: str
        :return: 组合
        :rtype: tuple[str]
        """
        return tuple()

    def write_res_to_db(self, returns, maxdd, win_time, lose_time, even_time, win_time_percent, lose_time_percent,
                        run_tag):
        """
        :param run_tag: 执行标识
        :type run_tag: str
        :param lose_time_percent: 输占用百分比
        :type lose_time_percent:float
        :param returns:
        :type returns: float
        :param maxdd:
        :type maxdd: float
        :param win_time: 赢的次数
        :type win_time: int
        :param lose_time: 输的次数
        :type lose_time: int
        :param even_time: 没赢没输就撤的次数
        :type even_time: int
        :param win_time_percent: 赢占用百分比
        :type win_time_percent: float
        """
        pass

    @classmethod
    def _get_engine(cls):
        return create_engine('sqlite:///%s' % cls.db_path)

    @classmethod
    def create_session(cls):
        """
        :rtype:Session
        """
        return sessionmaker(bind=cls._get_engine())()

    @classmethod
    def create_table(cls):
        BaseTable.metadata.create_all(cls._get_engine())


class SelectAllTable(BaseTable):
    __tablename__ = 'select_all'

    id = Column(Integer, primary_key=True)
    returns = Column(Float)
    maxdd = Column(Float)
    win_time = Column(Integer)
    lose_time = Column(Integer)
    even_time = Column(Integer)
    win_time_percent = Column(Float)
    lose_time_percent = Column(Float)
    run_tag = Column(String(200))
    up_s01 = Column(Integer)


class SelectAll(BaseSelect):
    """
    全部可用
    """

    def __init__(self, ft, pet, pot, sft, spet, spot, close, s_close, volume, s_volume, up_s01=20):
        """
        :type up_s01: int
        """
        super(SelectAll, self).__init__(ft, pet, pot, sft, spet, spot, close, s_close, volume, s_volume)
        self.up_s01 = up_s01

    def get_cur_select(self, cur_date):
        if self.up_s01 > 0:
            if self.sft.loc[:cur_date].iloc[-self.up_s01:].mean() > self.sft.loc[cur_date]:
                return tuple()
        return self.ft.loc[cur_date].dropna(axis='index').index.values

    def write_res_to_db(self, returns, maxdd, win_time, lose_time, even_time, win_time_percent, lose_time_percent,
                        run_tag):
        session = self.create_session()
        session.add(
            SelectAllTable(returns=returns, maxdd=maxdd, win_time=win_time, lose_time=lose_time, even_time=even_time,
                           win_time_percent=win_time_percent, lose_time_percent=lose_time_percent,
                           run_tag=run_tag, up_s01=self.up_s01))
        session.commit()


class SelectPointRangeTable(BaseTable):
    __tablename__ = 'select_point_range'

    id = Column(Integer, primary_key=True)
    returns = Column(Float)
    maxdd = Column(Float)
    win_time = Column(Integer)
    lose_time = Column(Integer)
    even_time = Column(Integer)
    win_time_percent = Column(Float)
    lose_time_percent = Column(Float)
    run_tag = Column(String(200))
    point0 = Column(Float)
    point1 = Column(Float)
    last_period = Column(Integer)
    up_s01 = Column(Integer)


class SelectPointRange(BaseSelect):
    """
    选择point在指定范围的st
    """

    def __init__(self, ft, pet, pot, sft, spet, spot, close, s_close, volume, s_volume, point_range=(-0.6, -0.4),
                 last_period=5, up_s01=20):
        super(SelectPointRange, self).__init__(ft, pet, pot, sft, spet, spot, close, s_close, volume, s_volume)
        self.up_s01 = up_s01
        self.last_period = last_period
        self.point_range = point_range

    def get_cur_select(self, cur_date):

        if self.up_s01 > 0:
            if self.sft.loc[:cur_date].iloc[-self.up_s01:].mean() > self.sft.loc[cur_date]:
                return tuple()

        avail_data = self.pot.loc[:cur_date]
        if len(avail_data) < self.last_period:
            return tuple()
        mean_frame = avail_data.iloc[-self.last_period:].sum()
        return mean_frame[mean_frame < self.point_range[1]][mean_frame > self.point_range[0]].dropna(
            axis='index').index.values

    def write_res_to_db(self, returns, maxdd, win_time, lose_time, even_time, win_time_percent, lose_time_percent,
                        run_tag):
        session = self.create_session()
        session.add(SelectPointRangeTable(returns=returns, maxdd=maxdd, win_time=win_time, lose_time=lose_time,
                                          even_time=even_time, win_time_percent=win_time_percent,
                                          lose_time_percent=lose_time_percent, up_s01=self.up_s01,
                                          run_tag=run_tag, point0=self.point_range[0], point1=self.point_range[1],
                                          last_period=self.last_period))
        session.commit()


class SelectAllSmallTable(BaseTable):
    __tablename__ = 'select_all_small'

    id = Column(Integer, primary_key=True)
    returns = Column(Float)
    maxdd = Column(Float)
    win_time = Column(Integer)
    lose_time = Column(Integer)
    even_time = Column(Integer)
    win_time_percent = Column(Float)
    lose_time_percent = Column(Float)
    run_tag = Column(String(200))
    up_s01 = Column(Integer)
    small_range0 = Column(Float)
    small_range1 = Column(Float)


class SelectAllSmall(BaseSelect):
    def __init__(self, ft, pet, pot, sft, spet, spot, close, s_close, volume, s_volume, up_s01=20,
                 small_range=(0, 0.05)):
        """
        :param small_percent: small的比例
        :type small_percent: float
        """
        super(SelectAllSmall, self).__init__(ft, pet, pot, sft, spet, spot, close, s_close, volume, s_volume)
        self.up_s01 = up_s01
        self.small_range = small_range
        self.value_source = self.close * self.volume
        """:type: pd.DataFrame"""

    def get_cur_select(self, cur_date):
        """
        选择close * volumn中最小的一部分
        :param cur_date:
        :type cur_date:
        :rtype: tuple[str]
        """
        if self.up_s01 > 0:
            if self.sft.loc[:cur_date].iloc[-self.up_s01:].mean() > self.sft.loc[cur_date]:
                return tuple()

        # 排序, 开始选择
        # stock_names = self.value_source.columns.values
        # sorted_stock_names = sorted(stock_names,
        #                             key=lambda x: self.value_source.loc[cur_date, x] if not np.isnan(
        #                                 self.value_source.loc[cur_date, x]) and self.volume.loc[
        #                                                                             cur_date, x] > 0 else sys.maxint)


        #
        # return sorted_stock_names[int(len(sorted_stock_names) * self.small_range[0]):int(
        #     len(sorted_stock_names) * self.small_range[1])]

        cur_stock_line = self.value_source.loc[cur_date].dropna()
        cur_stock_line = cur_stock_line[cur_stock_line > 0]
        sorted_stock_names = cur_stock_line.sort_values().index.values
        cur_per_line = self.pet.loc[cur_date]
        cur_over_stock_names = cur_per_line[cur_per_line > 0.095]

        res = [stock_name for stock_name in
               sorted_stock_names[int(len(sorted_stock_names) * self.small_range[0]): int(len(sorted_stock_names) *
                                                                                          self.small_range[1])] if
               stock_name not in cur_over_stock_names]

        # for stock_name in res:
        #     print '------'
        #     print stock_name
        #     print self.close.loc[cur_date, stock_name]
        #     print self.volume.loc[cur_date, stock_name]
        #     print self.value_source.loc[cur_date, stock_name]

        return res

    def write_res_to_db(self, returns, maxdd, win_time, lose_time, even_time, win_time_percent, lose_time_percent,
                        run_tag):
        session = self.create_session()
        session.add(SelectAllSmallTable(returns=returns, maxdd=maxdd, win_time=win_time, lose_time=lose_time,
                                        even_time=even_time, win_time_percent=win_time_percent,
                                        lose_time_percent=lose_time, run_tag=run_tag,
                                        up_s01=self.up_s01, small_range0=self.small_range[0],
                                        small_range1=self.small_range[1]))
        session.commit()


class SelectPercentRangeTable(BaseTable):
    __tablename__ = 'select_per_range'

    id = Column(Integer, primary_key=True)
    returns = Column(Float)
    maxdd = Column(Float)
    win_time = Column(Integer)
    lose_time = Column(Integer)
    even_time = Column(Integer)
    win_time_percent = Column(Float)
    lose_time_percent = Column(Float)
    run_tag = Column(String(200))
    up_s01 = Column(Integer)
    per_range0 = Column(Float)
    per_range1 = Column(Float)


class SelectPercentRange(BaseSelect):
    def __init__(self, ft, pet, pot, sft, spet, spot, close, s_close, volume, s_volume, up_s01=-1,
                 per_range=(-0.03, 0.03)):
        super(SelectPercentRange, self).__init__(ft, pet, pot, sft, spet, spot, close, s_close, volume, s_volume)
        self.up_s01 = up_s01
        self.per_range = per_range

    def get_cur_select(self, cur_date):
        if self.up_s01 > 0:
            if self.sft.loc[:cur_date].iloc[-self.up_s01:].mean() > self.sft.loc[cur_date]:
                return tuple()
        return super(SelectPercentRange, self).get_cur_select(cur_date)

    def write_res_to_db(self, returns, maxdd, win_time, lose_time, even_time, win_time_percent, lose_time_percent,
                        run_tag):
        session = self.create_session()
        session.add(SelectPercentRangeTable(returns=returns, maxdd=maxdd, win_time=win_time, lose_time=lose_time,
                                            even_time=even_time, win_time_percent=win_time_percent,
                                            lose_time_percent=lose_time, run_tag=run_tag,
                                            up_s01=self.up_s01, per_range0=self.per_range[0],
                                            per_range1=self.per_range[1]))
        session.commit()


def test_select(select_cls, give_date):
    """
    用来测试选择方法
    :param select_cls:
    :type select_cls: type
    :param give_date:
    :type give_date: str
    """
    # 搞定数据源, 暂时只包含fix, percent, point, 后续有需要再增加
    ft, sft = resolve_dataframe(frame_type=DBInfoCache.cache_type_fix)
    # stock_names = ft.columns.values
    # print stock_names
    # print ft.loc[give_date, stock_names[3]]
    # sorted_stock_names = sorted(stock_names,
    #                             key=lambda x: ft.loc[give_date, x] if not np.isnan(
    #                                 ft.loc[give_date, x]) else sys.maxint)
    # print sorted_stock_names
    pet, spet = resolve_dataframe(frame_type=DBInfoCache.cache_type_percent)
    pot, spot = resolve_dataframe(frame_type=DBInfoCache.cache_type_point)
    close, s_close = resolve_dataframe(frame_type=DBInfoCache.cache_type_close)
    volume, s_volume = resolve_dataframe(frame_type=DBInfoCache.cache_type_volume)

    select_obj = select_cls(ft, pet, pot, sft, spet, spot, close, s_close, volume, s_volume, up_s01=-1)

    res_series = select_obj.get_cur_select(give_date)

    print res_series
    print len(res_series)


def start_back_test(select_obj, ft, sft, pet, spet, pot, spot, run_time=500, repo_count=5, win_percent=0.1,
                    lose_percent=0.1, max_hold_day=20,
                    need_png=False, need_write_db=True, need_log=True, need_account_db=False):
    """
    所有的过程都包含进来
    :param need_account_db:
    :type need_account_db: bool
    :param need_log:
    :type need_log: bool
    :param lose_percent:
    :type lose_percent: float
    :param max_hold_day: 最长持有时间, 只count有数据的日期
    :type max_hold_day: int
    :param win_percent:
    :type win_percent: float
    :param repo_count: 每次从选择结果组合中, 随机抽取的数量, 尽量不要改, 因为可能成为混淆因子,
    :type repo_count: int
    :param run_time: 随机执行的次数
    :type run_time: int
    :param select_obj: 使用的选择策略
    :type select_obj: BaseSelect
    :param need_png: 是否画图
    :type need_png: bool
    :param need_write_db: 是否写数据库
    :type need_write_db: bool
    """
    date_strs = ft.index.values

    # png和log的存放位置
    cur_png_root_path = os.path.join(config.png_root_path, 'some_select_png')
    if not os.path.exists(cur_png_root_path):
        os.mkdir(cur_png_root_path)
    cur_log_root_path = os.path.join(config.png_root_path, 'some_select_log')
    if not os.path.exists(cur_log_root_path):
        os.mkdir(cur_log_root_path)

    for i in range(0, run_time):
        # 一次随机回测开始
        time_before = datetime.datetime.now()
        print time_before
        # 产生一个标记, 唯一表示这次运行, 这个标记会用来:
        # 1. account的操作记录
        # 2. png的名称
        # 3. 回测结果中保存的一个字段
        run_tag = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + str(random.randint(1000, 9999))
        account = MoneyAccount(500000, run_tag, repo_count=repo_count)

        if need_account_db:
            account.open()

        # 记录数据
        win_count = 0
        lose_count = 0
        even_count = 0
        # 回撤相关
        max_dd = 0
        max_property = 0
        # 图表相关, 记录s01, account, k5, k10, k20, k30, k60, k120, k250
        account_values = list()
        account_divider = sft[0] * 100 / account.property
        k5_values = list()
        k10_values = list()
        k20_values = list()
        k30_values = list()
        k60_values = list()
        k120_values = list()
        k250_values = list()

        # 开始回测
        for date_str in date_strs:
            # 当前值
            cur_price_line = ft.loc[date_str]

            # 选择, 并采用随机的方式过滤
            selected_stocks = select_obj.get_cur_select(date_str)

            if len(selected_stocks) > repo_count:
                selected_stocks = random.sample(selected_stocks, repo_count)

            # 执行仓位变动
            # 更新
            source_dict = dict()
            for stock_name in account.stocks.keys():
                if not np.isnan(cur_price_line.loc[stock_name]):
                    source_dict[stock_name] = (cur_price_line.loc[stock_name], date_str)
                account.stocks[stock_name].hold_days += 1
            account.update_with_all_stock_one_line(source_dict)

            # 卖出
            for stock_name in account.stocks.keys():
                if not np.isnan(cur_price_line.loc[stock_name]):
                    if account.stocks[stock_name].return_percent >= win_percent:
                        account.sell_with_repos(stock_name, cur_price_line.loc[stock_name], date_str,
                                                repo_count=account.stock_repos[stock_name])
                        win_count += 1
                    elif account.stocks[stock_name].return_percent <= -lose_percent:
                        account.sell_with_repos(stock_name, cur_price_line.loc[stock_name], date_str,
                                                repo_count=account.stock_repos[stock_name])
                        lose_count += 1
                    elif account.stocks[stock_name].hold_days >= max_hold_day:
                        account.sell_with_repos(stock_name, cur_price_line.loc[stock_name], date_str,
                                                repo_count=account.stock_repos[stock_name])
                        even_count += 1

            # 买入, 根据剩余仓位
            for index in range(0, account.cur_repo_left):
                if index >= len(selected_stocks):
                    break
                if not np.isnan(cur_price_line.loc[selected_stocks[index]]):
                    account.buy_with_repos(selected_stocks[index], cur_price_line.loc[selected_stocks[index]], date_str,
                                           repo_count=1)

            # 计算回撤
            if account.property > max_property:
                max_property = account.property
            cur_dd = account.property / max_property - 1
            if cur_dd < max_dd:
                max_dd = cur_dd

            # 记录account的值
            account_values.append(account.property * account_divider / 100)

            k5_values.append(account_values[-1] / account_values[-5] if len(account_values) > 5 else 0)
            k10_values.append(account_values[-1] / account_values[-10] if len(account_values) > 10 else 0)
            k20_values.append(account_values[-1] / account_values[-20] if len(account_values) > 20 else 0)
            k30_values.append(account_values[-1] / account_values[-30] if len(account_values) > 30 else 0)
            k60_values.append(account_values[-1] / account_values[-60] if len(account_values) > 60 else 0)
            k120_values.append(account_values[-1] / account_values[-120] if len(account_values) > 120 else 0)
            k250_values.append(account_values[-1] / account_values[-250] if len(account_values) > 250 else 0)

            # 写log
            if need_log:
                log_with_filename(run_tag, '--------------------------------')
                log_with_filename(run_tag, 'date :      ' + date_str)
                log_with_filename(run_tag, 'hold :      ' + str(account.stocks.keys()))
                log_with_filename(run_tag, 'win_count : ' + str(win_count))
                log_with_filename(run_tag, 'lose_count :' + str(lose_count))
                log_with_filename(run_tag, 'even_count :' + str(even_count))
                log_with_filename(run_tag, 'returns :   ' + str(account.returns))
                log_with_filename(run_tag, 'maxdd :     ' + str(max_dd))
                log_with_filename(run_tag, 'k5 :        ' + str(k5_values[-1]))
                log_with_filename(run_tag, 'k10 :       ' + str(k10_values[-1]))
                log_with_filename(run_tag, 'k20 :       ' + str(k20_values[-1]))
                log_with_filename(run_tag, 'k30 :       ' + str(k30_values[-1]))
                log_with_filename(run_tag, 'k60 :       ' + str(k60_values[-1]))
                log_with_filename(run_tag, 'k120 :      ' + str(k120_values[-1]))
                log_with_filename(run_tag, 'k250 :      ' + str(k250_values[-1]))

        print 'time cost : ' + str(datetime.datetime.now() - time_before)

        if need_write_db:
            total_count = float(win_count + lose_count + even_count)
            select_obj.write_res_to_db(account.returns, max_dd, win_count, lose_count, even_count,
                                       float(win_count) / total_count, float(lose_count) / total_count, run_tag)

        if need_account_db:
            account.close()

        if need_png:
            draw_line_chart(date_strs, [sft, account_values],
                            ['s01', 'account'], default_colors[:5],
                            run_tag, title=run_tag, output_dir=cur_png_root_path)

        if need_log:
            # 写log
            log_with_filename(run_tag, account)
            log_with_filename(run_tag, account.returns)
            log_with_filename(run_tag, 'max dd' + str(max_dd))
            log_with_filename(run_tag, 'time cost : ' + str(datetime.datetime.now() - time_before))


if __name__ == '__main__':
    pass
    # 建表
    # BaseSelect.create_table()
    # 测试选择结果
    test_select(SelectAllSmall, '2016-09-14')
    # 开始回测
    # 搞定数据

    # 搞定数据源, 暂时只包含fix, percent, point, 后续有需要再增加
    # ft, sft = resolve_dataframe(frame_type=DBInfoCache.cache_type_fix)
    # pet, spet = resolve_dataframe(frame_type=DBInfoCache.cache_type_percent)
    # pot, spot = resolve_dataframe(frame_type=DBInfoCache.cache_type_point)
    # close, s_close = resolve_dataframe(frame_type=DBInfoCache.cache_type_close)
    # volume, s_volume = resolve_dataframe(frame_type=DBInfoCache.cache_type_volume)
    #
    # # 参数范围
    # source_range = range(0, 10, 1)
    # res_list = [(float(i) / 100, float(i) / 100 + 0.01) for i in source_range]
    #
    # # 回测
    # for small_range in res_list[0:1]:
    #     print small_range
    #     select_obj = SelectAllSmall(ft, pet, pot, sft, spet, spot, close, s_close, volume, s_volume,
    #                                 small_range=(small_range[0], small_range[1]))
    #     start_back_test(select_obj, ft, sft, pet, spet, pot, spot, run_time=400, need_write_db=True, need_png=True,
    #                     need_log=True, need_account_db=True)

    # returns avg写入excel
    # session = SelectPointRange.create_session()
    # res_index = list()
    # res_values = list()
    # for point_range in res_list:
    #     res_index.append(point_range[0])
    #     res_values.append(
    #         session.query(func.avg(SelectPointRangeTable.returns)).filter_by(point0=point_range[0],
    #                                                                          point1=point_range[
    #                                                                              1]).first()[0])
    # res_df = pd.DataFrame(res_values, index=res_index)
    # print res_df
    # res_df.to_excel(os.path.join(config.log_root_path, 'point_0.02_-1.xlsx'))
