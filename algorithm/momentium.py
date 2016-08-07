# encoding:utf-8

import pandas as pd

from data.info import resolve_dataframe


class BaseState(object):
    """
    所有状态都应包含的信息
    """
    a_state = 0
    b_state = 1
    c_state = 2
    none_state = -1

    def __init__(self, cur_date, cur_value, cur_state, pre_state=None):
        super(BaseState, self).__init__()

        self.start_date = cur_date
        self.start_value = cur_value

        self.high_date = cur_date
        self.high_value = cur_value
        self.low_date = cur_date
        self.low_value = cur_value

        self.end_date = cur_date
        self.end_value = cur_value

        self.period_days = 0
        # 没什么实际作用，只是存着这个临时变量方便在执行的时候保存，因为最后一定会等于end的状态
        # self.cur_date = cur_date
        # self.cur_value = cur_value

        # state 0 ：Ａ　state 1 :　Ｂ　state 2 ：Ｃ
        self.cur_state = cur_state
        self.pre_state = pre_state

    def __str__(self):
        return 'cur_state : %s  pre_state :　%s\n' \
               'start_date : %s  start_value : %f\n' \
               'end_date : %s  end_value : %f\n' \
               'high_date : %s  high_value : %f\n' \
               'low_date : %s  low_value : %f\n' \
               'period : %d' % (
                   BaseState.resolve_state_to_str(self.cur_state), BaseState.resolve_state_to_str(self.pre_state),
                   self.start_date, self.start_value, self.end_date, self.end_value, self.high_date, self.high_value,
                   self.low_date, self.low_value, self.period_days
               )

    @classmethod
    def resolve_state_to_str(cls, state):
        if state == cls.a_state:
            return 'A'
        elif state == cls.b_state:
            return 'B'
        elif state == cls.c_state:
            return 'C'
        else:
            return 'U'

    @property
    def percent(self):
        """
        :return:最终的变化范围
        :rtype: float
        """
        if self.start_value == 0.0:
            print 'State not init'
            return 0.0
        else:
            return self.end_value / self.start_value - 1


def resolve_state_list(data_source, down_start=0.1, shut_speed=0.001, shut_by=0, min_down_start=0.03, up_start=0.08,
                       down_fix=0.8, adjust_range=0.02, adjust_back_period=3, c2b_start=0.02):
    """
    根据数据源，返回相应的state list
    前一个state的end_state一定是下一个state的start_date
    :param c2b_start: 由c转b的起始下降，这个值必须不能太大，因为c本身处于不稳定阶段，也不能太小，否则简单的调整就带出去了
    :type c2b_start: float
    :param min_down_start: 最小的下降起步判定
    :type min_down_start: float
    :param adjust_back_period: 由C到B过去几天都需要是调整状态
    :type adjust_back_period: int
    :param adjust_range: 视为调整的百分比范围
    :type adjust_range: float
    :param down_fix: 上涨之后的下跌修复百分比
    :type down_fix: float
    :param up_start: 上涨的判定起始值
    :type up_start: float
    :param shut_by: 0 天 1 百分比
    :type shut_by: int
    :param shut_speed: down_start的衰减速度
    :type shut_speed: float
    :param down_start: 下跌的判定起始值
    :type down_start: float
    :param data_source: 数据源
    :type data_source: pd.Series
    :return: state list
    :rtype: list[BaseState]
    """
    res_states = list()
    """:type: list[BaseState]"""
    date_strs = data_source.index.values

    # 在这里进行第一个状态的初始化，第一个状态一定是已知的，所以模拟的时候可以考虑排除这个状态，从而提高准确性
    # 第一个状态的确定有利于后面状态的准确性
    first_state = BaseState(date_strs[0], data_source[0], BaseState.a_state)
    res_states.append(first_state)

    # 记录当前的down_start
    cur_down_start = down_start

    for date_str in date_strs:
        cur_price = data_source[date_str]
        cur_state = res_states[-1]
        """:type:BaseState"""

        # 上升以及调整
        if cur_state.cur_state == BaseState.a_state:
            # 价格再次上涨
            # 价格上涨只需做一件事，动态改变down_start，但是要确保大于最小值
            # 如果价格超过最大值，并且是通过percent衰减，那么需要更新cur_down_start
            if cur_price > cur_state.high_value:
                if shut_by == 1:
                    cur_down_start = down_start - (cur_price / cur_state.low_value - 1) * 100 * shut_speed
                    cur_state.high_value = cur_price
                    cur_state.high_date = date_str

            # 如果是通过天来衰减，不管怎样，cur_down_start都需要变
            if shut_by == 0:
                if cur_down_start >= min_down_start + shut_speed:
                    cur_down_start -= shut_speed

            # 需要考虑转跌的情况
            if cur_price <= cur_state.high_value * (1 - cur_down_start):
                # 进入b
                cur_state.end_date = date_str
                cur_state.end_value = cur_price
                new_state = BaseState(date_str, cur_price, BaseState.b_state, cur_state.cur_state)
                res_states.append(new_state)

            # 更新cur_state的其他几个值
            # cur_state.cur_date = date_str
            # cur_state.cur_value = cur_price
            if cur_price < cur_state.low_value:
                cur_state.low_value = cur_price
                cur_state.low_date = date_str

        # 下降
        elif cur_state.cur_state == BaseState.b_state:
            # b可以同时满足到a和c的条件，这里的选择是能c不a，所以先判定c，然后在判定a时，判定是否已经产生了新的状态，是的话就不再判定了
            # 首先，是否有机会到c，两个条件，low_value到达fix， adjust_back_period天处于adjust_range
            # 计算fix：
            # 1、如果前面是c，意味着之前出现过通过fix反转失败，那么要找到一个最低点小于当前最低点的a
            # 2、如果前面不是c，意味着是a，那么这次只要满足上次的上升fix就行，所以，只要最低值小于前面a上升空间的down_fix就行
            fix_point = cur_state.low_value
            if cur_state.pre_state == BaseState.c_state:
                # 倒叙遍历state，找到那个最低点小于当前最低点的a的最低点
                cur_max_value = -1
                for index in range(2, len(res_states)):
                    # 记录最大值
                    if res_states[-index].high_value > cur_max_value:
                        cur_max_value = res_states[-index].high_value

                    # 找到满足条件的最低点
                    if res_states[-index].cur_state == BaseState.a_state:
                        if cur_state.low_value > res_states[-index].low_value:
                            # 找到了这个a
                            fix_point = (cur_max_value - res_states[-index].low_value) * down_fix + res_states[
                                -index].low_value
                            break
            elif cur_state.pre_state == BaseState.a_state:
                fix_point = (res_states[-2].high_value - res_states[-2].low_value) * down_fix + res_states[-2].low_value

            # 进入c的第一个条件达成
            if cur_state.low_value < fix_point:
                # 第二个条件开始
                range_max_value = -1
                range_min_value = 50000
                source_view = data_source.loc[:date_str]
                if len(source_view) >= adjust_back_period:
                    for index in range(1, adjust_back_period):
                        if source_view[-index] > range_max_value:
                            range_max_value = source_view[-index]
                        if source_view[-index] < range_min_value:
                            range_min_value = source_view[-index]
                        if (range_max_value - range_min_value) / range_min_value <= adjust_range:
                            # 第二个条件达成，进入c
                            cur_state.end_value = cur_price
                            cur_state.end_date = date_str
                            new_state = BaseState(date_str, cur_price, BaseState.c_state, cur_state.cur_state)
                            res_states.append(new_state)

            # 是否有机会进入a
            # 必须没有新的状态产生
            if cur_state == res_states[-1]:
                if cur_price > cur_state.low_value:
                    if cur_price / cur_state.low_value - 1 > up_start:
                        # 进入a
                        cur_state.end_value = cur_price
                        cur_state.end_date = date_str
                        new_state = BaseState(date_str, cur_price, BaseState.a_state, cur_state.cur_state)
                        res_states.append(new_state)

            # 更新状态的所有值
            if cur_price < cur_state.low_value:
                cur_state.low_value = cur_price
                cur_state.low_date = date_str
            if cur_price > cur_state.high_value:
                cur_state.high_value = cur_price
                cur_state.high_date = date_str

        # 下降之后的调整
        elif cur_state.cur_state == BaseState.c_state:
            # 考虑进入a
            if (cur_price - cur_state.low_value) / cur_state.low_value > up_start:
                # 进入a
                cur_state.end_value = cur_price
                cur_state.end_date = date_str
                new_state = BaseState(date_str, cur_price, BaseState.a_state, cur_state.cur_state)
                res_states.append(new_state)

            # 考虑进入b
            if (cur_state.high_value - cur_price) / cur_price > c2b_start:
                # 进入b
                cur_state.end_date = date_str
                cur_state.end_value = cur_price
                new_state = BaseState(date_str, cur_price, BaseState.b_state, cur_state.cur_state)
                res_states.append(new_state)

            # 更新各种最高和最低
            if cur_price < cur_state.low_value:
                cur_state.low_value = cur_price
                cur_state.low_date = date_str
            if cur_price > cur_state.high_value:
                cur_state.high_value = cur_price
                cur_state.high_date = date_str

        cur_state.period_days += 1

    return res_states


if __name__ == '__main__':
    fix_frame, s01 = resolve_dataframe()
    res = resolve_state_list(s01, down_start=0.1, shut_speed=0.001, shut_by=0, min_down_start=0.03, up_start=0.08,
                             down_fix=0.8, adjust_range=0.02, adjust_back_period=3, c2b_start=0.02)
    for state in res:
        print state.low_date
        print state.low_value
        print state.period_days
        print str(state)


def find_peak_period(s01, base_down=0.1, shut_speed=0.001, min_down=0.03, max_up=0.05, down_up_percent=0.8, shut_by=0):
    """
    deprecated: 这次的实现不太完善，在这上面改也不是很好改，所以重新写个
    找到所有下跌的区间
    思路:
    1. 下跌开始的判定, 下跌超过base_down认为开始了下跌, base_down随着上升空间的增大而变小(shut_speed), 即涨的越多, 下跌反转判定越容易,
        但是base_down不能小于一个最小值(min_down)
    2. 下跌结束的判定, 两种情况:
        1. 突破最低上升空间要求(max_up), 适用于大牛的情况, 即缓冲一次, 继续向上, 产生的上升继续累积.
        2. 跌到此次上升空间的某一比例(down_up_percent), 认为下跌接近结束.

    :param shut_by: 0 天 1 百分比
    :type shut_by: int
    :param down_up_percent: 下跌结束判定时, 需要下跌占用上升的比例
    :type down_up_percent: float
    :param max_up:
    :type max_up: float
    :param min_down:
    :type min_down: float
    :param shut_speed:
    :type shut_speed: float
    :param base_down: 开始下跌的初始判定
    :type base_down: float
    :param s01: 数据源
    :type s01: pd.Series
    :return: 下跌的起始和结束的点, 起始的理想情况是, 有一定的延迟, 结束的理想情况是, 有些许的超前, 0 bottom, 1 top, [(top, date_str)]
    :rtype:list[tuple]
    """
    s01 = s01.dropna()
    date_strs = s01.index.values
    # 当前处于上升还是下降, 0 上升, 1 下降
    cur_state = 0
    cur_max_price = s01[0]
    cur_min_price = s01[0]
    cur_base_down = base_down
    cur_up_percent = 0
    cur_up_days = 0
    res_dates = list()
    res_dates.append((0, date_strs[0]))
    for date_str in date_strs:
        cur_price = s01[date_str]

        # 处于上升, 做下跌开始的判定
        if cur_state == 0:

            # 虽然在上升, 但是如果价格低于了最低价, 那也更新下最低价
            if cur_price < cur_min_price:
                cur_min_price = cur_price

            # 当前价格高于最高价, 上升继续,
            if cur_price >= cur_max_price:
                cur_up_days += 1
                cur_up_percent = cur_price / cur_min_price - 1

                # 更新base_down
                if shut_by == 0:
                    # 按天更新, 直接减就行
                    if cur_base_down >= min_down + shut_speed:
                        cur_base_down -= shut_speed
                elif shut_by == 1:
                    # 按百分比更新, 算出百分比, 然后更新
                    points = cur_up_percent * 100 * shut_speed
                    if base_down - points > min_down:
                        cur_base_down = base_down - points
                    else:
                        cur_base_down = min_down
                # 更新价格
                cur_max_price = cur_price

            # 当前价格低于最高价, 但是大于下跌的反转价格, 认为仍处于上升中
            elif cur_price >= cur_max_price * (1 - cur_base_down):
                cur_up_days += 1
                # cur_min_price可能变了
                cur_up_percent = cur_max_price / cur_min_price - 1
                # 更新base_down, 只更新天的就行
                if shut_by == 0:
                    if cur_base_down >= min_down + shut_speed:
                        cur_base_down -= shut_speed

            # 进入下跌
            else:
                # 重置cur_min_price
                cur_min_price = cur_price
                cur_state = 1

                res_dates.append((1, date_str))

        # 处于下降, 只要判定什么时候恢复上涨就行了
        elif cur_state == 1:
            # 虽然在下降, 但是也有可能高于当前最高价格
            if cur_price > cur_max_price:
                cur_max_price = cur_price

            # 低于最低价, 依然在下降, 更新最低价
            if cur_price <= cur_min_price:
                # 如果跌了上次上涨的down_up_percent, 恢复上涨模式
                if 1 - cur_price / cur_max_price > cur_up_percent / (1 + cur_up_percent) * down_up_percent:
                    cur_state = 0
                    res_dates.append((0, date_str))
                    # 如果已经有了一定的下得, 则充值max_price
                    cur_max_price = cur_price
                    cur_base_down = base_down
                cur_min_price = cur_price
            # 高于最低价, 并突破max_up, 恢复上升
            elif (cur_price - cur_min_price) / cur_min_price >= max_up:
                cur_state = 0
                res_dates.append((0, date_str))

                cur_max_price = cur_price

    return res_dates

# if __name__ == '__main__':
#     fix_frame, s01 = resolve_dataframe()
#     period_dates = find_peak_period(s01)
#     state = 0
#     value0 = 0
#     value1 = 0
#     win_count = 0
#     lose_count = 0
#     for date_group in period_dates:
#         if date_group[0] == 0:
#             value0 = s01[date_group[1]]
#         else:
#             value1 = s01[date_group[1]]
#             if value1 > value0:
#                 win_count += 1
#                 print "win : " + str(value1 - value0)
#             else:
#                 lose_count += 1
#                 print "lose : " + str(value1 - value0)
#         print date_group
#         print s01[date_group[1]]
#
#     print "win count : " + str(win_count)
#     print "lose count : " + str(lose_count)
