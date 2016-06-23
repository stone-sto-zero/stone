#! -*- encoding:utf-8 -*-

# 所有的数据库打开都写在这里, 每个数据库对应一个类
import sqlite3
import traceback
import os

from log.log_utils import log_by_time


class DBBase(object):
    """
    封装基本的数据库操作
    """

    def __init__(self):
        super(DBBase, self).__init__()
        self.cursor = None
        self.connection = None

    def _db_file_path(self):
        """
        数据库存放位置
        :return: 绝对路径
        """
        return ''

    def _open_connection(self):
        """
        打开数据库
        """
        self.connection = sqlite3.connect(self._db_file_path())
        self.cursor = self.connection.cursor()

    def _close_connection(self):
        """
        关闭数据库
        """
        self.cursor.close()
        self.connection.close()

    def _create_table(self, table_name, table_columns):
        """
        创建表
        :param table_name:     表名
        :param table_columns:  列名和描述信息的list
        """
        self.cursor.execute('create table ' + table_name + ' (' + ','.join(table_columns) + ')')

    def insert_into_table(self, table_name, column_names, column_values):
        """
        向某一个表插入数据
        :param table_name:  表名
        :param column_names:       列名
        :param column_values:      列值
        """
        self.cursor.execute(
            'insert into %s (%s) values (%s)' % (table_name, ','.join(column_names), ','.join(column_values)))


class DBSinaMinute(object):
    """
    掌管新浪格式的分钟数据库, 需要注意的是, 这并不是一个数据库, 而是一组数据库的通用接口
    依赖于DBYahooDay, 因为要从中获取所有st的名字
    数据库的命名方式, st名称_年份, 每个数据库只有一张表, 表明和数据库的名字一致
    """

    _db_file_dir = '/Users/wgx/workspace/data/minute/'

    line_id = 'id'  # 0
    line_open = 'open'  # 1
    line_last_close = 'last_close'  # 2
    line_cur = 'cur'  # 3
    line_high = 'high'  # 4
    line_low = 'low'  # 5
    line_try_buy = 'try_buy'  # 6竞买
    line_try_sell = 'try_sell'  # 7  竞卖
    line_volume = 'volume'  # 8 除了100, 如果和yahoo数据混用, 记得确定下yahoo的数据是不是也除了100
    line_volume_money = 'volume_money'  # 9

    # 依次为买1到买5 卖1到卖5的情况
    line_buy1_volume = 'buy1_volume'  # 10
    line_buy1 = 'buy1'  # 11
    line_buy2_volume = 'buy2_volume'  # 12
    line_buy2 = 'buy2'  # 13
    line_buy3_volume = 'buy3_volume'  # 14
    line_buy3 = 'buy3'  # 15
    line_buy4_volume = 'buy4_volume'  # 16
    line_buy4 = 'buy4'  # 17
    line_buy5_volume = 'buy5_volume'  # 18
    line_buy5 = 'buy5'  # 19

    line_sell1_volume = 'sell1_volume'  # 20
    line_sell1 = 'sell1'  # 21
    line_sell2_volume = 'sell2_volume'  # 22
    line_sell2 = 'sell2'  # 23
    line_sell3_volume = 'sell3_volume'  # 24
    line_sell3 = 'sell3'  # 25
    line_sell4_volume = 'sell4_volume'  # 26
    line_sell4 = 'sell4'  # 27
    line_sell5_volume = 'sell5_volume'  # 28
    line_sell5 = 'sell5'  # 29

    line_date = 'date'  # 30
    line_time = 'time'  # 31

    column_dict = {
        line_id: 0,
        line_open: 1,
        line_last_close: 2,
        line_cur: 3,
        line_high: 4,
        line_low: 5,
        line_try_buy: 6,
        line_try_sell: 7,
        line_volume: 8,
        line_volume_money: 9,
        line_buy1_volume: 10,
        line_buy1: 11,
        line_buy2_volume: 12,
        line_buy2: 13,
        line_buy3_volume: 14,
        line_buy3: 15,
        line_buy4_volume: 16,
        line_buy4: 17,
        line_buy5_volume: 18,
        line_buy5: 19,
        line_sell1_volume: 20,
        line_sell1: 21,
        line_sell2_volume: 22,
        line_sell2: 23,
        line_sell3_volume: 24,
        line_sell3: 25,
        line_sell4_volume: 26,
        line_sell4: 27,
        line_sell5_volume: 28,
        line_sell5: 29,
        line_date: 30,
        line_time: 31
    }

    def __init__(self, db_year):
        super(DBSinaMinute, self).__init__()
        self.db_year = db_year
        self.stock_names = DBYahooDay().select_all_stock_names()

    def create_all_db_all_tables(self, create_year):
        """
        创建所有st的表
        :param create_year: 创建的年份
        """
        for stock_name in self.stock_names:
            # 搞定db路径
            db_name = '%s_%s' % (stock_name, create_year)
            db_file_name = db_name + '.db'
            db_file_path = os.path.join(self._db_file_dir, db_file_name)

            # 建表
            db_con = sqlite3.connect(db_file_path)
            db_cursor = db_con.cursor()
            db_columes = (
                self.line_id + ' integer primary key',
                self.line_open + ' double',
                self.line_last_close + ' double',
                self.line_cur + ' double',
                self.line_high + ' double',
                self.line_low + ' double',
                self.line_try_buy + ' double',
                self.line_try_sell +' double',
                self.line_volume + ' bigint',
                self.line_volume_money + ' bigint',
                self.line_buy1_volume + ' bigint',
                self.line_buy1 + ' double',
                self.line_buy2_volume + ' bigint,',
                self.line_buy2 + ' double',
                self.line_buy3_volume + ' bigint',
                self.line_buy3 + ' double',
                self.line_buy4_volume + ' bigint',
                self.line_buy4 + ' double',
                self.line_buy5_volume + ' bigint',
                self.line_buy5 + ' double',
                self.line_sell1_volume + ' bigint',
                self.line_sell1 + ' double',
                self.line_sell2_volume + ' bigint',
                self.line_sell2 + ' double',
                self.line_sell3_volume + ' bigint',
                self.line_sell3 + ' double',
                self.line_sell4_volume + ' bigint',

            )
            db_cursor.execute('create table %s (%s)' % (db_name, ','.join(db_columes)))


class DBYahooDay(DBBase):
    """
    掌管雅虎格式的日数据的数据库
    """

    # 名称表的相关内容
    table_stock_name = 'stock_name'
    stock_name_id = 'id'
    stock_name_name = 'name'

    # st表的列名
    line_id = 'id'  # 0
    line_date = 'date'  # 1
    line_open = 'open'  # 2
    line_high = 'high'  # 3
    line_low = 'low'  # 4
    line_close = 'close'  # 5
    line_volume = 'volume'  # 6
    line_adj_close = 'adj_close'  # 7
    line_percent = 'percent'  # 8
    line_point = 'point'  # 9
    line_divider = 'divider'  # 10

    # st表的列和index的关系
    column_dict = {
        line_id: 0,
        line_date: 1,
        line_open: 2,
        line_high: 3,
        line_low: 4,
        line_close: 5,
        line_volume: 6,
        line_adj_close: 7,
        line_percent: 8,
        line_point: 9,
        line_divider: 10
    }

    def __init__(self):
        super(DBYahooDay, self).__init__()

    def open(self):
        self._open_connection()

    def close(self):
        self._close_connection()

    def _db_file_path(self):
        return '/Users/wgx/workspace/data/db_yahoo_day.db'

    def select_all_stock_names(self):
        """
        查询所有的表名
        :return: 表名的list
        """
        self.open()
        res = [cell[0] for cell in
               self.cursor.execute('select %s from  %s order by %s' % (
                   self.stock_name_name, self.table_stock_name, self.stock_name_name)).fetchall()]
        self.close()
        return res

    def create_stock_name_table(self):
        """
        创建包含股票名称的表, 后续可以增加一些备注字段, 比如首先, 是否可用, 连续可用日期等
        """
        self._open_connection()
        self._create_table(self.table_stock_name,
                           [self.stock_name_id + ' integer primary key', self.stock_name_name + ' varchar(20)'])
        self._close_connection()

    def add_row_to_stock_name_table(self, name):
        """
        想保存表名称的表中插入一行
        :param name: 插入的st名称
        """
        self.insert_into_table(self.table_stock_name, (self.stock_name_name,), ("'%s'" % name,))
        self.connection.commit()

    def create_st_table(self, stock_name):
        """
        创建指定的st表
        :param stock_name: 表名
        """
        table_columns = (
            self.line_id + ' integer primary key',
            self.line_date + ' varchar(20)',
            self.line_open + ' double',
            self.line_high + ' double',
            self.line_low + ' double',
            self.line_close + ' double',
            self.line_volume + ' bigint',
            self.line_adj_close + ' double',
            self.line_percent + ' double',
            self.line_point + ' double',
            self.line_divider + ' integer',
        )
        self._create_table(stock_name, table_columns)

    def check_and_del_empty_table_name(self):
        """
        删除stock_name中对应名称表为空的行以及空表
        """
        self.open()
        stock_lines = self.cursor.execute('select * from ' + self.table_stock_name).fetchall()
        for stock_line in stock_lines:
            stock_name = stock_line[1]
            print stock_name
            try:
                data_lines = self.cursor.execute('select * from ' + stock_name).fetchall()
                if len(data_lines) <= 0:
                    print stock_name + ' table is empty'
                    self.cursor.execute('drop table ' + stock_name)
                    self.connection.commit()
                    print stock_name + ' table removed'
            except:
                print stock_name + ' table not exist'
                traceback.print_exc()
        self.close()

    def check_if_is_done(self, stock_name):
        """
        检查指定的st名称是否done
        :param stock_name: 名称
        :return:           是否done
        """
        res = self.cursor.execute('select %s from %s where %s="%s"' % (
            self.stock_name_name, self.table_stock_name, self.stock_name_name, stock_name))
        return not len(res.fetchall()) == 0

    def select_stock_all_lines(self, stock_name, order=0):
        """
        查询st的所有行, 需要手动打开数据库
        :param stock_name: 名称
        :param order:   是否排序 0 asc 1 desc
        :return: st line list
        """
        sql_str = 'select * from ' + stock_name
        if order == 0:
            sql_str += ' order by date'
        elif order == 1:
            sql_str += ' order by date desc'

        return self.cursor.execute(sql_str).fetchall()

    def update_target_date_percent_and_divider(self, stock_name, date, percent, divider):
        """
        修改指定st的各个字段, 需要手动打开数据库,手动commit
        :param stock_name:名称
        :param percent: 增幅
        """
        self.cursor.execute(
            'update %s set %s=%f,%s=%d where %s="%s"' % (
                stock_name, self.line_percent, percent, self.line_divider, divider, self.line_date, date))

    def update_target_date_point(self, stock_name, date, point):
        """
        修改指定st的point
        :param stock_name:名称
        :param point:评分
        """
        self.cursor.execute(
            'update %s set %s=%f where %s="%s"' % (stock_name, self.line_point, point, self.line_date, date))

    def fill_percent_for_all_stock(self, last_index=0):
        """
        处理percent
        """
        stock_names = self.select_all_stock_names()
        self.open()
        stock_count = 0
        if last_index == 0:
            start_index = 0
        else:
            start_index = last_index - 1
        for stock_name in stock_names:
            stock_lines = self.select_stock_all_lines(stock_name)

            # 用来记录上次出现的价格和日期
            last_close_price = -1
            last_date = None
            last_percent = None
            # 开始干活
            for stock_line in stock_lines[start_index:]:
                # 如果没保存过价格, 那么保存价格, 进入下一轮
                if not last_date:
                    last_date = stock_line[self.column_dict.get(self.line_date)]
                    last_close_price = stock_line[self.column_dict.get(self.line_close)]
                    # 第一天的percent写0, divider写0
                    # self.update_target_date_percent_and_divider(stock_name, last_date, 0, 0)

                # 上次的日期存在, 证明上一行是存在的
                else:
                    # 记录当前的价格, 日期
                    cur_price = stock_line[self.column_dict.get(self.line_close)]
                    cur_date = stock_line[self.column_dict.get(self.line_date)]

                    if last_close_price <= 0:
                        # 上次出现的价格有问题, 写入percent 为0, divider为1
                        self.update_target_date_percent_and_divider(stock_name, cur_date, 0, 1)
                    else:
                        last_percent = cur_price / last_close_price - 1
                        # percent超过了10%或者-10%, 证明出现了问题, percent照常记录, divider为1
                        if last_percent > 0.11 or last_percent < -0.11:
                            self.update_target_date_percent_and_divider(stock_name, cur_date, last_percent, 1)
                        else:
                            # 数据没问题,正常写入,divider为0
                            self.update_target_date_percent_and_divider(stock_name, cur_date, last_percent, 0)

                    # 保存当前日期为上次日期
                    last_date = cur_date
                    last_close_price = cur_price

                log_by_time(stock_name + ' ' + str(last_percent) + ' ' + str(stock_count))
            # 每完成一个st, commit一次
            self.connection.commit()
            stock_count += 1

        self.close()

    def select_stock_lines_by_date(self, stock_name, stock_date):
        """
        找到指定
        :param stock_name 名称
        :param stock_date 日期
        :return stock lines
        """
        return self.cursor.execute(
            'select * from %s where %s="%s"' % (stock_name, self.line_date, stock_date)).fetchall()

    def fill_point_for_all_stock(self, last_index=0):
        """
        处理point
        """
        stock_names = self.select_all_stock_names()
        self.open()
        # 先把三大指数做成字典{date:percent}
        # 指数的list
        # 创业板指数据找不到, 所以暂时用深证替换吧
        tem_names = ('s399001_sz', 's000001_ss')
        tem_ss = dict()
        tem_sz = dict()
        dicts = (tem_sz, tem_ss)
        for i in range(0, 2):
            tem_name = tem_names[i]
            tem_dict = dicts[i]
            stock_lines = self.select_stock_all_lines(tem_name)

            for stock_line in stock_lines:
                # 往字典中放值
                tem_dict[stock_line[self.column_dict.get(self.line_date)]] = stock_line[
                    self.column_dict[self.line_percent]]

        # 搞定每个st的percent
        stock_count = 0
        for stock_name in stock_names:

            # 先判定该用哪个dict
            if stock_name.endswith('ss'):
                use_dict = dicts[1]
            elif stock_name.startswith('s3'):
                use_dict = dicts[0]
            else:
                use_dict = dicts[0]

            # 填充每一行
            stock_lines = self.select_stock_all_lines(stock_name)
            for stock_line in stock_lines[last_index:]:
                cur_date = stock_line[self.column_dict.get(self.line_date)]
                if cur_date in use_dict:
                    cur_point = (stock_line[self.column_dict.get(self.line_percent)] - use_dict.get(cur_date)) * 100
                else:
                    cur_point = 0
                self.update_target_date_point(stock_name, cur_date, cur_point)

                log_by_time(
                    'set point for %s at %s with point %f num %d' % (stock_name, cur_date, cur_point, stock_count))
            stock_count += 1

            # 搞定一个st, commit一次
            self.connection.commit()

        self.close()


if __name__ == '__main__':
    pass
    # 更新前几天的percent
    # yahoo_db = DBYahooDay()
    # yahoo_db.fill_percent_for_all_stock(-1)
    # yahoo_db.fill_point_for_all_stock(-1)
