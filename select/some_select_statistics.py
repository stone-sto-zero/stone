# encoding:utf-8

# 这里进行各种选择策略的回测, 选择的结果组合需要足够大, 然后通过随机的方式, 进行多次回测, 最终通过统计方法求取一个最终值.
# 每种选择都应建立自己的结果表, 用来存放参数和结果的各项指标
import random

import datetime
import pandas as pd
import numpy as np
import os

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, create_engine, Integer, Float
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

    def __init__(self, ft, pet, pot, sft, spet, spot):
        """
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

    def _create_session(self):
        """
        :rtype:Session
        """
        return sessionmaker(bind=self._get_engine())()

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

    def __init__(self, ft, pet, pot, sft, spet, spot, up_s01=20):
        """
        :type up_s01: int
        """
        super(SelectAll, self).__init__(ft, pet, pot, sft, spet, spot)
        self.up_s01 = up_s01

    def get_cur_select(self, cur_date):
        if self.up_s01 > 0:
            if self.sft.loc[:cur_date].iloc[-self.up_s01:].mean() > self.sft.loc[cur_date]:
                return tuple()
        return self.ft.loc[cur_date].dropna(axis='index').index.values

    def write_res_to_db(self, returns, maxdd, win_time, lose_time, even_time, win_time_percent, lose_time_percent,
                        run_tag):
        session = self._create_session()
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

    def __init__(self, ft, pet, pot, sft, spet, spot, point_range=(4, 5), last_period=5, up_s01=20):
        super(SelectPointRange, self).__init__(ft, pet, pot, sft, spet, spot)
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
        session = self._create_session()
        session.add(SelectPointRangeTable(returns=returns, maxdd=maxdd, win_time=win_time, lose_time=lose_time,
                                          even_time=even_time, win_time_percent=win_time_percent,
                                          lose_time_percent=lose_time_percent, up_s01=self.up_s01,
                                          run_tag=run_tag, point0=self.point_range[0], point1=self.point_range[1],
                                          last_period=self.last_period))
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
    pet, spet = resolve_dataframe(frame_type=DBInfoCache.cache_type_percent)
    pot, spot = resolve_dataframe(frame_type=DBInfoCache.cache_type_point)

    select_obj = select_cls(ft, pet, pot, sft, spet, spot)

    res_series = select_obj.get_cur_select(give_date)
    print res_series
    print len(res_series)


def start_back_test(select_clz, run_time=500, repo_count=5, win_percent=0.1, lose_percent=0.1, max_hold_day=20,
                    need_png=False, need_write_db=True, need_log=True):
    """
    所有的过程都包含进来
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
    :param select_clz: 使用的选择策略
    :type select_clz: type
    :param need_png: 是否画图
    :type need_png: bool
    :param need_write_db: 是否写数据库
    :type need_write_db: bool
    """
    # 搞定数据源, 暂时只包含fix, percent, point, 后续有需要再增加
    ft, sft = resolve_dataframe(frame_type=DBInfoCache.cache_type_fix)
    pet, spet = resolve_dataframe(frame_type=DBInfoCache.cache_type_percent)
    pot, spot = resolve_dataframe(frame_type=DBInfoCache.cache_type_point)
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
        # 确定选择策略
        select_obj = select_clz(ft, pet, pot, sft, spet, spot)
        """:type: BaseSelect"""
        # 产生一个标记, 唯一表示这次运行, 这个标记会用来:
        # 1. account的操作记录
        # 2. png的名称
        # 3. 回测结果中保存的一个字段
        run_tag = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + str(random.randint(1000, 9999))
        account = MoneyAccount(500000, run_tag, repo_count=repo_count)

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

        if need_write_db:
            total_count = float(win_count + lose_count + even_count)
            select_obj.write_res_to_db(account.returns, max_dd, win_count, lose_count, even_count,
                                       float(win_count) / total_count, float(lose_count) / total_count, run_tag)

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
    # test_select(SelectPointRange, '2016-08-26')
    # 开始回测
    start_back_test(SelectPointRange, run_time=200, need_png=False)


