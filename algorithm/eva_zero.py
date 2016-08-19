# encoding:utf-8
import random
from datetime import datetime

import pandas as pd

from account.account import MoneyAccount
from data.info import resolve_dataframe, DBInfoCache


class HoldStockInfo(object):
    """
    临时需要记录的持仓信息
    """

    def __init__(self, stock_name):
        self.stock_name = stock_name
        self.hold_days = 0
        self.is_ready = False


class EvaZero(object):
    """
    method概况:
    limit_eva**: 限制条件
    select_eva**: st筛选策略
    opt_eva**: 操作策略
    """

    def __init__(self, close_frame, open_frame=None, high_frame=None, low_frame=None, percent_frame=None):
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
        self.run_tag = ''

        # 系统变化的环境记录, 可能有一些参数, 要在这里手动去改, 后续有需要可以考虑开放到class的接口中
        self.account = None
        self.cur_date = ''
        self.cur_select = list()
        self.data_set_close = dict()
        """:type:dict[str, pd.Series]"""
        self.data_set_percent = dict()
        self.s01 = None
        self.ma_values = None
        self.k_values = None
        # 给opt用的, 表示现在已经满足可卖的第一个条件, 出现第二个条件卖出
        self.hold_dict = dict()
        """:type:dict[str, HoldStockInfo]"""

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
        # 更新所有持仓的持仓天数
        for stock_name in self.hold_dict.keys():
            self.hold_dict[stock_name].hold_days += 1

        limit_start_time = datetime.now()
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
        self.ma_values = list()
        # 前一天的线值
        ma_value_1 = list()
        # 各种线的斜率
        self.k_values = list()
        cell_length = self.s01.loc[self.cur_date] * cell_width
        for m_value in ma_group:
            self.ma_values.append(dt[-m_value:].mean())
            ma_value_1.append(dt.iloc[-m_value:-1].mean())
            self.k_values.append((self.ma_values[-1] - ma_value_1[-1]) / cell_length)
            if self.k_values[-1] >= k_max:
                con1 = False
            if k_range[0] <= self.k_values[-1] <= k_range[1]:
                con2 = True

        for k_value in self.k_values:
            if k_value >= 1:
                con1 = False
                break

        if con1:
            if self.s01.loc[self.cur_date] <= self.ma_values[0]:
                con1 = False
        print 'limit info : '
        print datetime.now() - limit_start_time
        if con1 or con2:
            res_str = 'ok'
        else:
            res_str = 'pass'
        print self.cur_date + ' is ' + res_str
        print '-------------'
        return con1 or con2

    def select_eva_high_and_continue(self, n=6, m=3, a=2, b=0.05, c=0.1, d=0.05, x=2):
        """
        找到拉升后的第一次小幅下降的st
        1.找到前n天，前面n-x天上涨, 后面x天下跌, 前面每隔m天的两个close, 都是后面大于前面的,
            前面n-x天,有a次，上涨超过b，然后当前处于距离最高下跌小于c的股票，且整体上有d的上涨
        2.如果集合够大，要使用随机策略选择，这样多次运行之后，可以求得一个可靠的范围，表明是否有效果
        :return: portfolio
        :rtype: list
        """
        select_start_time = datetime.now()
        # 在所有的st中找
        self.cur_select = list()
        for stock_name in self.data_set_close.keys():
            data_source = self.data_set_close[stock_name]
            """:type:pd.Series"""

            # 当天没数据, 八成停牌了
            if self.cur_date not in data_source.index:
                continue

            # 已经被选中
            if stock_name in self.cur_select:
                continue

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
        print 'select info:'
        print datetime.now() - select_start_time
        print self.cur_select
        print '---------------'

    def opt_eva_confirm_win_and_fix_lose(self, a=0.1, n=6, b=0.1, m=10, s=10, k_max=1, max_days=25):
        """
        止盈和修补策略
        买入:     按照当前选择, 采用随机的办法建仓, 这样多次执行, 能够更好的验证算法的有效性
        止盈：    上涨超过a后，出现跌破s日(在均线组中的位置为k)均线可卖
        仓位控制：设置n仓，按照选股策略依次建一仓，如果出现损失，那么当损失到达b时, 或者持续了m天的损失，建仓时优先补仓，
                每次按照当前持仓量1:1补，不够少补也可以，没补一次止盈的a下降a/n个点，最坏情况下，最后平仓卖出
        止损：    仅仅通过大盘的位置进行止损，如果任意均线斜率大于k_max，进行无理由卖出
                买入超过max_days, 也撤退, 因为会使选入算法失效, 当然, 这个根据选择算法变化, 看需要
        """
        opt_start_time = datetime.now()
        # 如果没账户, 先开户
        if self.account is None:
            self.account = MoneyAccount(1000000, self.run_tag, repo_count=n)

        depre_stock = list()

        # 设置当前数据不存在的st为不可操作
        for stock_name in self.hold_dict.keys():
            if self.cur_date not in self.data_set_close[stock_name].index:
                depre_stock.append(self.hold_dict.pop(stock_name))

        # 先更新下数据, 判定会用, 统计应该也用
        update_dict = dict()
        for stock_name in self.hold_dict.keys():
            update_dict[stock_name] = (self.data_set_close[stock_name][self.cur_date], self.cur_date)
        self.account.update_with_all_stock_one_line(update_dict)

        # 先卖
        # 大盘均线严重向下, 均线的判定, 在limit中有, 如果没有, 后续再补上
        force_sell = True
        for k_value in self.k_values:
            if k_value < k_max:
                force_sell = False
                break

        ready_to_remove = list()
        # 大盘跪了, 全部撤退, 清空hold
        if force_sell:
            """:type:list[HoldStockInfo]"""
            for stock_name in self.hold_dict.keys():
                if self.account.sell_with_repos(stock_name, self.data_set_close[stock_name].loc[self.cur_date],
                                                self.cur_date, repo_count=self.account.stock_repos[stock_name]):
                    ready_to_remove.append(stock_name)

            for stock_name in ready_to_remove:
                del self.hold_dict[stock_name]

            print 'opt info :'
            print 's01 collapse'
            print '--------------'
            # 一天结束, 设置所有的st都成为可操作
            for hold_stock in depre_stock:
                self.hold_dict[hold_stock.stock_name] = hold_stock
            return

        # 更新所有持仓的持仓天数
        # for stock_name in self.hold_dict.keys():
        #     self.hold_dict[stock_name].hold_days += 1

        # 判定是不是上涨到位了, 到位了更新状态
        for stock_name in self.hold_dict.keys():
            hold_stock = self.hold_dict[stock_name]
            if self.account.stocks[hold_stock.stock_name].return_percent >= a - (
                        self.account.stock_repos[hold_stock.stock_name] - 1) * a / n:
                hold_stock.is_ready = True

        # 判定已经ready_sell的, 即已经上涨超过a了, 现在判定是不是跌破了
        for stock_name in self.hold_dict.keys():
            # 均线的值
            s_ma = self.data_set_close[stock_name].loc[:self.cur_date].iloc[-s:].mean()
            # 小于均线, 撤退
            if self.data_set_close[stock_name][self.cur_date] < s_ma:
                # 卖出成功, 删掉
                if self.account.sell_with_repos(stock_name, self.data_set_close[stock_name].loc[self.cur_date],
                                                self.cur_date, repo_count=self.account.stock_repos[stock_name]):
                    ready_to_remove.append(stock_name)

        # 删除卖出成功的
        for stock_name in ready_to_remove:
            del self.hold_dict[stock_name]

        # 持有太久, 一直没进入ready_to_sell的, 也撤退
        ready_to_remove = list()
        for stock_name in self.hold_dict.keys():
            if self.hold_dict[stock_name].hold_days > max_days and not self.hold_dict[stock_name].is_ready:
                if self.account.sell_with_repos(stock_name, self.data_set_close[stock_name].loc[self.cur_date],
                                                self.cur_date, repo_count=self.account.stock_repos[stock_name]):
                    ready_to_remove.append(stock_name)

        # 删除卖出成功的
        for stock_name in ready_to_remove:
            del self.hold_dict[stock_name]

        # 开始买了, 先把亏大发的补一补
        for stock_name in self.hold_dict.keys():
            if self.account.stocks[stock_name].return_percent < b or (
                            self.hold_dict[stock_name].hold_days > m and
                            self.account.stocks[stock_name].return_percent < 0):
                # 补的时候, 尽力而为, 选择剩余仓位或者已买仓位里面的最小值来补
                buy_repo = min(self.account.cur_repo_left, self.account.stock_repos[stock_name])
                # 发现没仓可补, 那么后续啥也不用干了
                if buy_repo <= 0:
                    print 'opt info :'
                    print self.account.stock_repos
                    print self.account.returns
                    print 'not repo left'
                    print 'hold dict : '
                    for stock_name_tag in self.hold_dict.keys():
                        print stock_name_tag
                    print '--------------'
                    # 一天结束, 设置所有的st都成为可操作
                    for hold_stock in depre_stock:
                        self.hold_dict[hold_stock.stock_name] = hold_stock
                    return
                # 如果买入成功, 加入hold_list
                if self.account.buy_with_repos(stock_name, self.data_set_close[stock_name][self.cur_date],
                                               self.cur_date, repo_count=buy_repo):
                    if stock_name not in self.hold_dict.keys():
                        self.hold_dict[stock_name] = HoldStockInfo(stock_name)

        # 没有仓位了, 结束
        if self.account.cur_repo_left <= 0:
            print 'opt info :'
            print self.account.stock_repos
            print self.account.returns
            print 'hold dict : '
            for stock_name in self.hold_dict.keys():
                print stock_name
            print 'not repo left'
            print '--------------'
            # 一天结束, 设置所有的st都成为可操作
            for hold_stock in depre_stock:
                self.hold_dict[hold_stock.stock_name] = hold_stock
            return

        # 先把已选中, 但是已买入的给删除, 因为现在的选择策略本身就容易出现连续多天重复
        for stock_name in self.cur_select:
            if stock_name in self.account.stocks.keys():
                self.cur_select.remove(stock_name)

        # 执行的买入次数为选中数量和当前剩余仓位的较小值
        buy_count = min(self.account.cur_repo_left, len(self.cur_select))
        for index in range(0, buy_count):
            buy_stock_name = self.cur_select[random.randint(0, len(self.cur_select) - 1)]
            # 买入成功, 加入hold_list
            if self.account.buy_with_repos(buy_stock_name, self.data_set_close[buy_stock_name][self.cur_date],
                                           self.cur_date, repo_count=1):
                if buy_stock_name not in self.hold_dict.keys():
                    self.hold_dict[buy_stock_name] = HoldStockInfo(buy_stock_name)

        # 一天结束, 设置所有的st都成为可操作
        for hold_stock in depre_stock:
            self.hold_dict[hold_stock.stock_name] = hold_stock

        print 'opt info:'
        print datetime.now() - opt_start_time
        print self.account.returns
        print self.account.stock_repos
        print 'hold dict : '
        for stock_name in self.hold_dict.keys():
            print stock_name
        print '-------------------------'

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
        # 统计信息小队
        self.run_tag = ''
        # 开始loop
        date_strs = self.s01.index.values
        for date_str in date_strs:
            # 更新日期
            self.cur_date = date_str

            # 如果日期不满足条件, pass, 这个过程会更新ma和k
            if not self.limit_eva_ma_and_k():
                continue

            # 选
            self.select_eva_high_and_continue()

            # 操作
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
    EvaZero(close_frame=close_frame, percent_frame=percent_frame).select_for_day(1)
