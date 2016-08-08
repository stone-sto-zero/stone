#! encoding:utf-8
import sqlite3
from math import sqrt

import os

from config import config


class DBResultAna(object):
    def __init__(self):
        super(DBResultAna, self).__init__()
        # 平均数
        self.avg = 0.0
        # 标准差
        self.devi = 0.0
        # 最大值
        self.max = -1
        # 最小值
        self.min = 1000000
        # 各个level对应的百分比
        self.levels = list()
        """:type: list[float]"""
        self.level_names = list()
        """:type: list"""

    def __str__(self):
        level_str = ''
        for index in range(0, len(self.level_names)):
            level_str += ('%f:%f\n' % (self.level_names[index], self.levels[index]))
        level_str += 'else : ' + str(self.levels[-1])
        return "avg:%f\ndevi:%f\nmax:%f\nmin:%f\n%s" % (self.avg, self.devi, self.max, self.min, level_str)


class DBResult(object):
    """
    这里放回测的结果数据
    """
    _db_path = os.path.join(config.db_root_path, 'result_db.db')
    table_relative_strength_zero = 'relative_strength_zero'

    line_id = 'id'
    line_denominator = 'denominator'
    line_malength = 'malength'
    line_temlength = 'temlength'
    line_reweightperiod = 'reweightperiod'
    line_winpercent = 'winpercent'
    line_needups01 = 'needups01'
    line_sellafterreweight = 'sellafterreweight'
    line_losepercent = 'losepercent'
    line_rankposition = 'rankposition'
    line_rankpercent = 'rankpercent'
    line_standycount = 'standycount'
    line_run_tag = 'run_tag'

    line_maxdd = 'maxdd'
    line_returns = 'returns'

    line_id_index = 0
    line_denominator_index = 1
    line_malength_index = 2
    line_temlength_index = 3
    line_reweightperiod_index = 4
    line_winpercent_index = 5
    line_needups01_index = 6
    line_sellafterreweight_index = 7
    line_losepercent_index = 8
    line_rankposition_index = 9
    line_rankpercent_index = 10
    line_standycount_index = 11
    line_run_tag_index = 12
    line_maxdd_index = 13
    line_returns_index = 14

    relative_strenth_zero_columns = (
        line_denominator,
        line_malength,
        line_temlength,
        line_reweightperiod,
        line_winpercent,
        line_needups01,
        line_sellafterreweight,
        line_losepercent,
        line_rankposition,
        line_rankpercent,
        line_standycount,
        line_run_tag,
        line_maxdd,
        line_returns,
    )

    def __init__(self):
        super(DBResult, self).__init__()
        self.connection = None
        """:type:sqlite3.Connection"""
        self.cursor = None
        """:type:sqlite3.Cursor"""

    def open(self):
        self.connection = sqlite3.connect(self._db_path)
        self.cursor = self.connection.cursor()

    def close(self):
        self.cursor.close()
        self.connection.close()

    def create_relative_strength_zero(self):
        self.open()
        db_columns = (
            self.line_id + ' integer primary key',
            self.line_denominator + ' double',
            self.line_malength + ' integer',
            self.line_temlength + ' integer',
            self.line_reweightperiod + ' integer',
            self.line_winpercent + ' double',
            self.line_needups01 + ' integer',
            self.line_sellafterreweight + ' integer',
            self.line_losepercent + ' double',
            self.line_rankposition + ' integer',
            self.line_rankpercent + ' double',
            self.line_standycount + ' integer',
            self.line_run_tag + ' varchar(50)',
            self.line_maxdd + ' double',
            self.line_returns + ' double',
        )
        sql_str = 'create table %s (%s)' % (self.table_relative_strength_zero, ','.join(db_columns))
        self.cursor.execute(sql_str)
        self.close()

    @classmethod
    def analysis_basis_for_one_column(cls, data_list, levels=None):
        """
        分析一项数据, 返回包含平均数, 标准差, 最大值, 最小值,
        :param levels: 结果会包含各个级别占比, 这里给出级别的列表, 必须由小到大排好序, 如果不传, 就不给结果
        :type levels: list[float]
        :param data_list: 数据list
        :type data_list: list[float]
        :return: 返回一个包含各项水平的结构
        :rtype: DBResultAna
        """
        res = DBResultAna()
        level_counts = dict()

        # level
        if levels:
            for level in levels:
                level_counts[level] = 0

        # 平均数, 最大值, 最小值
        for data in data_list:
            res.avg += data
            if data > res.max:
                res.max = data
            if data < res.min:
                res.min = data
            if levels:
                for level in levels:
                    if data < level:
                        level_counts[level] += 1
                        break

        res.avg /= len(data_list)
        # 标准差
        for data in data_list:
            res.devi += ((data - res.avg) * (data - res.avg))
        res.devi /= len(data_list)

        print level_counts

        # level
        if levels:
            res.level_names = levels
            left = 1
            for level in levels:
                percent = float(level_counts[level]) / len(data_list)
                res.levels.append(percent)
                left -= percent

            res.levels.append(left)

        res.devi = sqrt(res.devi)

        return res

    @classmethod
    def analysis_view_for_result(cls, data_list):
        """
        输出一个参数集对应数据结果的各项指标
        :param data_list: 数据结果集合
        :type data_list:list[tuple]
        """
        maxdd_result = cls.analysis_basis_for_one_column([data_line[cls.line_maxdd_index] for data_line in data_list],
                                                         [-0.7, -0.6, -0.5, -0.4])
        return_result = cls.analysis_basis_for_one_column(
            [data_line[cls.line_returns_index] for data_line in data_list],
            [0, 10, 20, 30, 40])
        print '\nreturns : '
        print str(return_result)
        print '\nmaxdd : '
        print str(maxdd_result)


if __name__ == '__main__':
    result_db = DBResult()
    result_db.open()
    data_source_lines = result_db.cursor.execute(
        'SELECT * FROM relative_strength_zero WHERE rankpercent = 0.382').fetchall()
    result_db.close()

    DBResult.analysis_view_for_result(data_source_lines)
