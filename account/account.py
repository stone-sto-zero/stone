#! -*- encoding:utf-8 -*-

# 这里管账户的相关信息
# 账户管钱和持仓的变化, 定义明确参数, 注释表明具体是什么意思
import os
from datetime import date, datetime, time

import sqlite3

from config import config
from data.db.db_helper import DBYahooDay
from data.info import Infos
from log import time_utils
from log.time_utils import resolve_date, time_never_used, resolve_time


class Order(object):
    """
    订单
    """

    order_type_buy = 0
    order_type_sell = 1

    def __init__(self, stock_name, deal_type, price, count, deal_date, deal_time=None):
        """
        :param stock_name: 名称
        :type stock_name: str
        :param deal_type: 买入 0  卖出 1
        :type deal_type: int
        :param price:
        :type price: float
        :param count:
        :type count: int
        :param deal_date:
        :type deal_date:str
        :param deal_time:
        :type deal_time: str
        """
        super(Order, self).__init__()
        # st名称
        self.stock_name = stock_name
        # 类型 0 买入 1 卖出
        self.type = deal_type
        # 交易价格
        self.price = price
        # 交易数量
        self.count = count
        # 交易日期
        self.date = deal_date
        # 交易时间
        if deal_time:
            self.time = deal_time
        else:
            self.time = time_never_used

        # st的交易价值
        self.stock_cost = price * count
        # 税和手续费
        self.tax = self.all_tax()
        # 总流水
        if deal_type == self.order_type_buy:
            self.all_cost = self.stock_cost + self.tax
        else:
            self.all_cost = self.tax

    def __str__(self):
        return '\nstock name: %s\ntype: %s\nprice: %f\ncount:%d\ndate:%s\nall cost: %f\ntax: %f\n' % (
            self.stock_name, 'buy' if self.type == 0 else 'sell', self.price, self.count, self.date, self.all_cost,
            self.tax)

    def all_tax(self):
        """
        所有的手续费, 税什么都都包含了
        """
        # 后续需要精确的时候再实现就可以, 现在有个意思一下就行
        total = 0
        # 如果是卖, 手续费千一
        if self.type == self.order_type_sell:
            total += self.stock_cost * 0.001
        # 过户费, 上证有, 暂时认为都有
        total += 6
        # 手续费, 按照佣金宝来, 万分二点五0.00025
        cost = self.stock_cost * 0.00025
        total += (cost if cost > 5 else 5)
        return total


class HoldStock(object):
    """
    股票的信息, 应该只有变量就可以了
    """

    # 市场规则
    t_plus = 1

    def __init__(self, name):
        """
        :param name: 名称
        :type name: str
        """
        super(HoldStock, self).__init__()
        self.name = name
        # 成本价
        self.cost_price = 0
        # 现价
        self.cur_price = 0
        # 当前持有总数量
        self.count = 0
        # 日期对应的不可卖数量, 字典结构, {日期: 数量}
        self.unavail_stock_count_dict = dict()
        # 可以卖出的数量
        self.avail_count = 0
        # 当前日期, 初始化随便选择一个历史日期
        self.cur_date = '1971-01-01 12:12:12'
        # 当前持仓的总收益, 去除已经卖出的部分, 因为已经结算到cash中了, 规则: cur_price / cost_price - 1
        self.return_percent = 0
        # 持仓天数, 不会自动更新, 用的话, 需要外部维护这个值
        self.hold_days = 0

    def __str__(self):
        return '\nstock name: %s\ncost price: %f\ncur price: %f\ncount: %d\ncur date: %s\nreturns : %f' % (
            self.name, self.cost_price, self.cur_price, self.count, self.cur_date, self.return_percent
        )

    def __eq__(self, other):
        """
        :type other: HoldStock
        :rtype: bool
        """
        return self.name == other.name and self.cost_price == other.cost_price and \
               self.cur_price == other.cost_price and self.count == other.count and \
               self.avail_count == other.avail_count and self.cur_date == other.cur_date and \
               self.return_percent == other.return_percent and self.unavail_stock_count_dict == other.unavail_stock_count_dict

    @property
    def stock_whole_property(self):
        """
        st现在的总价值
        """
        return self.cur_price * self.count

    @property
    def stock_cost_property(self):
        """
        st成本价值
        """
        return self.cost_price * self.count

    def update_avail_by_date(self, update_date_str):
        """
        根据时间更新avail
        :param update_date_str: 日期
        :type update_date_str: str
        """
        # 更新avail信息
        for key in self.unavail_stock_count_dict.keys():
            key_date = resolve_date(key)
            update_date = resolve_date(update_date_str)
            if (update_date - key_date).days >= self.t_plus:
                self.avail_count += self.unavail_stock_count_dict.get(key)
                self.unavail_stock_count_dict.pop(key)

    def refresh_returns(self):
        """
        刷新收益, 成本和现价必须已经刷新
        """
        # 更新收益
        if self.cost_price != 0:
            self.return_percent = (self.cur_price - self.cost_price) / self.cost_price
        else:
            self.return_percent = 0

    def update(self, price, update_date):
        """
        根据日期和时间更新st的信息
        :param price: st的当前价格
        :type price: float
        :param update_date: 当前日期
        :type update_date: str
        """
        # 如果日期是过去的时间, 更新无效
        if update_date < self.cur_date:
            print 'HoldStock update failed since the date is passed.'
            return

        # 记录新的数据
        self.cur_price = price
        self.cur_date = update_date

        # 更新avail信息
        self.update_avail_by_date(update_date)
        self.refresh_returns()

    def buy(self, price, count, update_date):
        """
        买入操作
        :param price: 价格
        :type price: float
        :param count: 数量
        :type count: int
        :param update_date: 日期
        :type update_date: str
        :return: 是否成功
        :rtype: bool
        """
        # 同样如果日期是过去的话, 说明有问题, 买入失败
        if update_date < self.cur_date:
            print 'HoldStock buy failed since the date is passed.'
            return False

        # 记录新的数据
        self.cur_price = price
        self.cur_date = update_date

        # 计算成本
        self.cost_price = (self.cost_price * self.count + self.cur_price * count) / (self.count + count)
        # 数量增加
        self.count += count
        # 计算avail
        if update_date in self.unavail_stock_count_dict:
            self.unavail_stock_count_dict[update_date] += count
        else:
            self.unavail_stock_count_dict[update_date] = count
        self.update_avail_by_date(update_date)

        # 更新收益
        self.refresh_returns()

        return True

    def sell(self, price, count, update_date):
        """
        卖出操作
        :param price: 价格
        :type price: float
        :param count: 数量
        :type count: int
        :param update_date: 日期
        :type update_date:str
        :return: 是否成功
        :rtype: bool
        """
        # 同样如果日期是过去的话, 说明有问题, 买入失败
        if update_date < self.cur_date:
            print 'HoldStock sell failed since the date is passed.'
            return False

        # 更新avail先
        self.update_avail_by_date(update_date)

        # 确认是否够卖
        if self.avail_count < count:
            print 'HoldStock sell failed since the count is not enough'
            return False

        # 记录新的数据
        self.cur_price = price
        self.cur_date = update_date

        # 搞定数量
        self.count -= count
        self.avail_count -= count

        # 因为时间可能变了, 所以还要更新下收益
        self.refresh_returns()

        return True


class MoneyAccount(object):
    """
    账户的信息都在这里了
    在infos的上层
    """

    def __init__(self, cash, run_tag, returns=0.0, repo_count=1):
        """
        :param cash: 本金
        :type cash: float
        :param returns: 废弃掉
        :type returns: float
        :param repo_count:买的时候, 分成的仓位数量
        :type repo_count:int
        """
        super(MoneyAccount, self).__init__()
        # 标识
        self.run_tag = run_tag
        # 账户中可用的现金
        self.cash = cash
        # 创建账户开始, 到目前的总收益, 所有hold_st的总值*returns_percent的加和 / origin_property
        self.returns = returns
        # 账户中的持股情况, 用name做key
        self.stocks = dict()
        """:type: dict[str, HoldStock]"""
        # 账户当前总价值
        self.property = cash
        # 账户原始价值
        self.origin_property = cash
        # 订单的全部信息, 暂时还没想到订单除了能用来做结果展示和debug, 还有其他的什么用
        self.order_list = []
        """:type: list[Order]"""
        # 买卖使用的总份数, 是个常量, 买卖的时候不会发生改变
        self.repo_count = repo_count
        # 剩余的份数, 份数相关的必须使用buy_with_repo和sell_with_repo才会生效
        self.cur_repo_left = repo_count
        # 当前st对应的份数, 加和总数 + 剩余份数 = 总份数
        self.stock_repos = dict()
        """:type: dict[str, int]"""
        self.account_db = None
        """:type: DBAccount"""

    def open(self):
        """
        打开数据库, 如果没打开过的话, 就不写数据库
        """
        if not self.account_db:
            self.account_db = DBAccount()
            self.account_db.create_table_with_run_tag(self.run_tag)
        self.account_db.open(self.run_tag)

    def close(self):
        """
         关闭数据库
        """
        if self.account_db:
            self.account_db.close()

    def __str__(self):
        order_list_str = ''
        for order in self.order_list:
            order_list_str += str(order) + '\n'
        hold_stocks_str = ''
        for hold_stock in self.stocks:
            hold_stocks_str += str(hold_stock) + '\n'
        return '==================================' + \
               '\ncash: %f\n returns: %f\n property: %f\n origin property: %f\nhold stocks: %s\norder list: %s\n' % (
                   self.cash, self.returns, self.property, self.origin_property, hold_stocks_str, order_list_str
               ) + '=================================='

    def update_one_stock(self, stock_name, price, cur_date):
        """
        更新一个st, 更新完毕后,记得手动调用update_self
        :param stock_name:
        :type stock_name: str
        :param price:
        :type price: float
        :param cur_date:
        :type cur_date: str
        """
        if stock_name in self.stocks:
            self.stocks[stock_name].update(price, cur_date)

    def update_with_all_stock_one_line(self, stock_line_dict):
        """
        更新account情况
        :param stock_line_dict: 数据的字典, 格式{stock_name: (stock_price, update_date)}, 注意, 数据实际上只是一天的, 传进来的时候一定要拼好
        :type stock_line_dict: dict[str, tuple[float|str]]
        """
        for stock_name in self.stocks.keys():
            hold_stock = self.stocks.get(stock_name)
            # 判定数据是不是在, 然后更新
            if stock_name in stock_line_dict:
                stock_line = stock_line_dict.get(stock_name)
                hold_stock.update(stock_line[0], stock_line[1])
            else:
                pass
                # print 'werror : stock_line_dict is not enough, may cause something error.'

        # 更新完st, 更新自己
        self.update_self()

    def update_self(self):
        """
        更新account的相关信息, 注意, 需要更新完所有的hold_st之后进行
        当然, 如果st未更新完全,  那么account相应也不会更新完全, 但是还是可以更新
        """
        # 更新完st, 更新自己
        # 计算return和property
        all_stock_property = 0
        to_del_stocks = []
        for stock_name in self.stocks.keys():
            hold_stock = self.stocks.get(stock_name)
            all_stock_property += hold_stock.stock_whole_property

            # 删除count为0的stock
            if hold_stock.count == 0:
                to_del_stocks.append(stock_name)

        self.property = self.cash + all_stock_property
        self.returns = (self.property - self.origin_property) / float(self.origin_property)

        # 执行删除
        for stock_name in to_del_stocks:
            self.stocks.pop(stock_name)

    def buy(self, stock_name, price, count, update_date):
        """
        买入只能更新相关的st, 以及相关st影响到的property, 而不会更新所有的st
        :param stock_name:
        :type stock_name: str
        :param price:
        :type price: float
        :param count:
        :type count: int
        :param update_date:
        :type update_date: str
        :return: 是否成功
        :rtype: bool
        """
        # 创建订单, 不一定会存下来
        create_order = Order(stock_name, Order.order_type_buy, price, count, update_date)

        # 先判定, 是否能买得起
        if self.cash < create_order.all_cost:
            return False

        # 能买的起, 订单加入, cash减去
        self.order_list.append(create_order)
        self.cash -= create_order.all_cost

        #  把买入的st加入stocks
        if stock_name in self.stocks:
            res = self.stocks.get(stock_name).buy(price, count, update_date)
        else:
            stock_info = HoldStock(stock_name)
            res = stock_info.buy(price, count, update_date)
            if res:
                self.stocks[stock_name] = stock_info

        # 买完更新下账户的状态, 当然是partly, 因为不知道其他st的情况
        if res:
            self.update_self()
            if self.account_db:
                self.account_db.save_account(self)
        return res

    def sell(self, stock_name, price, count, update_date):
        """
        卖出只能更新相关的st, 以及相关st影响到的property, 而不会更新所有的st
        :param stock_name:
        :type stock_name: str
        :param price:
        :type price: float
        :param count:
        :type count: int
        :param update_date:
        :type update_date: str
        :return: 是否成功
        :rtype: bool
        """
        # 先判断是否持有这个st, 如果持有, 那么成功与否交给HoldStock去判定就可以
        if stock_name not in self.stocks:
            return False

        # 创建订单
        create_order = Order(stock_name, Order.order_type_sell, price, count, update_date)
        hold_stock = self.stocks.get(stock_name)

        # 卖出成功了
        if hold_stock.sell(price, count, update_date):
            self.order_list.append(create_order)
            self.cash += create_order.stock_cost - create_order.tax
            self.update_self()
            if self.account_db:
                self.account_db.save_account(self)
            return True
        # 失败
        else:
            return False

    def buy_with_cash_percent(self, stock_name, price, percent, buy_date):
        """
        按照A的习惯, count必须是100的整数倍
        :param stock_name:
        :type stock_name: str
        :param price:
        :type price: float
        :param percent: 浮点类型, 乘100以后, 才是百分比
        :type percent: float
        :param buy_date:
        :type buy_date: str
        :return: 是否成功
        :rtype: bool
        """
        count = int(self.cash * percent / price / 100) * 100
        # 小于等于0 提示一下
        if count <= 0:
            print 'wwarning : count is 0, no buying'
            return False
        return self.buy(stock_name, price, count, buy_date)

    def sell_with_hold_percent(self, stock_name, price, percent, sell_date):
        """
        :param stock_name:
        :type stock_name: str
        :param price:
        :type price: float
        :param percent: 浮点类型, 乘100以后, 才是百分比
        :type percent: float
        :param sell_date:
        :type sell_date: str
        :return: 是否成功
        :rtype: bool
        """
        # 判读是否已持仓
        if stock_name not in self.stocks:
            print 'werror : stock ' + stock_name + ' is not hold'
            return False

        hold_stock = self.stocks.get(stock_name)
        count = int(hold_stock.count * percent / 100) * 100

        # 小于等于0 提示一下
        if count <= 0:
            print 'werror : stock ' + stock_name + ' is not sold since the count is 0'
            return False

        return self.sell(stock_name, price, count, sell_date)

    def buy_with_cash_percent_with_line(self, stock_name, percent, stock_line):
        """
        使用yahoo的stock_line的close 来买
        :param percent:
        :type percent: float
        :param stock_name:
        :type stock_name: str
        :param stock_line:
        :type stock_line: list
        :return:
        :rtype: bool
        """
        return self.buy_with_cash_percent(stock_name, stock_line[DBYahooDay.line_close_index], percent,
                                          stock_line[DBYahooDay.line_date_index])

    def sell_with_hold_percent_with_line(self, stock_name, percent, stock_line):
        """
        使用yahoo的stock_line的close 来卖
        :param stock_name:
        :type stock_name: str
        :param percent:
        :type percent: float
        :param stock_line:
        :type stock_line: list
        :return:
        :rtype: bool
        """
        return self.sell_with_hold_percent(stock_name, stock_line[DBYahooDay.line_close_index], percent,
                                           stock_line[DBYahooDay.line_date_index])

    def buy_with_repos(self, stock_name, price, buy_date, repo_count=1):
        """
        使用份数的方式来买
        :param buy_date:
        :type buy_date: str
        :param stock_name:
        :type stock_name: str
        :param price:
        :type price: float
        :param repo_count: 使用的份数
        :type repo_count: int
        :return:
        :rtype: bool
        """
        # 已经全仓
        if self.cur_repo_left == 0:
            return False

        # 如果不够买, 就全部花掉
        if self.cur_repo_left < repo_count:
            repo_count = self.cur_repo_left

        buy_res = self.buy_with_cash_percent(stock_name, price, float(repo_count) / self.cur_repo_left, buy_date)

        # 如果成功, 更新repo的相关状态
        if buy_res:
            self.cur_repo_left -= repo_count
            if stock_name in self.stock_repos:
                self.stock_repos[stock_name] += repo_count
            else:
                self.stock_repos[stock_name] = repo_count

        return buy_res

    def sell_with_repos(self, stock_name, price, sell_date, repo_count=1):
        """
        使用份数的方式来卖
        :param stock_name:
        :type stock_name:str
        :param price:
        :type price: float
        :param sell_date:
        :type sell_date: str
        :param repo_count:
        :type repo_count: int
        :return:
        :rtype: bool
        """
        # 当前未持有这个st
        if stock_name not in self.stock_repos:
            return False

        # 如果不够卖, 就全部卖掉
        if self.stock_repos.get(stock_name) < repo_count:
            repo_count = self.stock_repos.get(stock_name)

        sell_res = self.sell_with_hold_percent(stock_name, price, float(repo_count) / self.stock_repos.get(stock_name),
                                               sell_date)
        # 如果卖出成功, 更新各个repo状态
        if sell_res:
            self.cur_repo_left += repo_count
            self.stock_repos[stock_name] -= repo_count

            # 如果已经不再持有, 就清掉
            if self.stock_repos[stock_name] == 0:
                self.stock_repos.pop(stock_name)

        return sell_res


class DBAccount(object):
    """
    记录模拟过程中, 产生的中间数据
    每次实例化MoneyAccount对象的时候, 需要提供一个run_tag
    结构:
    详情表, 使用run_tag命名, 每次run会产生一张表, 保存这次模拟过程中产生所有中间信息, 订单, 账户相关信息的变动等等, run_tag和DBResult中的run_tag保持一致
    每一行实际上是出现一个order之后的变化, 所以是以order为基准变化, 每次出现order出现一行
    """

    _table_name = 'account'
    # 需要保存的内容
    line_id = 'id'

    line_cash = 'cash'
    line_property = 'property'
    line_returns = 'returns'

    line_order_stock = 'order_stock'
    line_order_type = 'order_type'
    line_order_price = 'order_price'
    line_order_count = 'order_count'
    line_order_date = 'order_date'
    line_order_time = 'order_time'
    line_order_stock_cost = 'stock_cost'
    line_order_all_cost = 'order_all_cost'
    line_order_tax = 'order_tax'

    line_hold_stock = 'hold_stock'
    line_stocks_detail = 'stock_detail'  # HoldStock的str, 把\n都换成II

    account_db_columns = (
        line_cash,
        line_property,
        line_returns,
        line_order_stock,
        line_order_type,
        line_order_price,
        line_order_count,
        line_order_date,
        line_order_time,
        line_order_stock_cost,
        line_order_all_cost,
        line_order_tax,
        line_hold_stock,
        line_stocks_detail,
    )

    def __init__(self):
        super(DBAccount, self).__init__()
        self.connection = None
        """:type: sqlite3.Connection"""
        self.cursor = None
        """:type: sqlite3.Cursor"""
        self.create_db_columns = (
            self.line_id + ' integer primary key',
            self.line_cash + ' double',
            self.line_property + ' double',
            self.line_returns + ' double',
            self.line_order_stock + ' varchar(30)',
            self.line_order_type + ' integer',
            self.line_order_price + ' double',
            self.line_order_count + ' bigint',
            self.line_order_date + ' varchar(30)',
            self.line_order_time + ' varchar(30)',
            self.line_order_stock_cost + ' double',
            self.line_order_all_cost + ' double',
            self.line_order_tax + ' double',
            self.line_hold_stock + ' varchar(1000)',
            self.line_stocks_detail + ' varchar(10000)',
        )
        self._db_path = None

    def open(self, run_tag):
        self._db_path = os.path.join(config.db_root_path, 'account/account_%s.db' % run_tag)
        if not os.path.exists(os.path.dirname(self._db_path)):
            os.mkdir(os.path.dirname(self._db_path))
        self.connection = sqlite3.connect(self._db_path)
        self.cursor = self.connection.cursor()

    def close(self):
        self.cursor.close()
        self.connection.close()

    def create_table_with_run_tag(self, run_tag):
        """
        创建一个run_tag对应的表
        :param run_tag:
        :type run_tag: str
        """
        self.open(run_tag)
        create_sql_str = 'create table %s (%s)' % (self._table_name, ','.join(self.create_db_columns))
        self.cursor.execute(create_sql_str)
        self.close()

    def save_account(self, account):
        """
        保存account的信息, order只会写入最后一个order
        :param account: 账户信息
        :type account: MoneyAccount
        """
        hold_stock = ','.join(account.stocks.keys())
        stock_detail = ''
        for stock_name in account.stocks.keys():
            stock_detail += str(account.stocks[stock_name])

        stock_detail = stock_detail.replace('\n', 'II')

        sql_str = 'insert into %s (%s) values (%f, %f, %f, "%s", %d, %f, %d, "%s", "%s", %f, %f, %f, "%s", "%s")' % (
            self._table_name, ','.join(self.account_db_columns), account.cash, account.property, account.returns,
            account.order_list[-1].stock_name, account.order_list[-1].type, account.order_list[-1].price,
            account.order_list[-1].count, account.order_list[-1].date, account.order_list[-1].time,
            account.order_list[-1].stock_cost, account.order_list[-1].all_cost, account.order_list[-1].all_tax(),
            hold_stock, stock_detail,
        )

        self.cursor.execute(sql_str)
        self.connection.commit()
