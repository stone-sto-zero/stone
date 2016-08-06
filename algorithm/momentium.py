# encoding:utf-8

import pandas as pd

from data.info import resolve_dataframe


def find_peak_period(s01, base_down=0.1, shut_speed=0.001, min_down=0.03, max_up=0.05, down_up_percent=0.8, shut_by=0):
    """
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
                cur_min_price = cur_price
            # 高于最低价, 并突破max_up, 恢复上升
            elif (cur_price - cur_min_price) / cur_min_price >= max_up:
                cur_state = 0
                res_dates.append((0, date_str))

    return res_dates


if __name__ == '__main__':
    fix_frame, s01 = resolve_dataframe()
    period_dates = find_peak_period(s01)
    state = 0
    value0 = 0
    value1 = 0
    win_count = 0
    lose_count = 0
    for date_group in period_dates:
        if date_group[0] == 0:
            value0 = s01[date_group[1]]
        else:
            value1 = s01[date_group[1]]
            if value1 > value0:
                win_count += 1
                print "win : " + str(value1 - value0)
            else:
                lose_count += 1
                print "lose : " + str(value1 - value0)
        print date_group
        print s01[date_group[1]]

    print "win count : " + str(win_count)
    print "lose count : " + str(lose_count)
