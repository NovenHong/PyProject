#!/usr/bin/python3
# -*- coding: utf-8 -*-
from _decimal import ROUND_HALF_UP
from decimal import Decimal

import mysql.connector
import os
import sys
import redis
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import calendar
from pprint import pprint
import requests
import phpserialize
import json
import hashlib
import time
import argparse

# config = {
#     'user': 'cj655',
#     'password': 'game123456',
#     'host': 'localhost',
#     'database': 'cj655',
#     'charset': 'utf8',
#     "connection_timeout": 60,
#     "use_pure": True
# }

config = {
    'user': 'cj655',
    'password': 'game123456',
    'host': '117.27.139.18',
    'database': 'cj655',
    'charset': 'utf8',
    "connection_timeout": 300,
    "use_pure": True
}

config2 = {
    'user': 'admin',
    'password': 'game123456',
    'host': '120.132.31.222',
    'database': 'cj655',
    'charset': 'utf8',
    "connection_timeout": 300,
    "use_pure": True
}

def getRegNum(where):
    where = phpserialize.dumps(where)
    field = "count(distinct username) reg_num"
    url = "http://dj.cj655.com/api.php?m=player&a=admin_role_array7"
    params = {"where": where, "field": field}
    r = requests.get(url, params)
    d = json.loads(r.text)
    return int(d[0]["reg_num"])

def getEffectiveNum(where):
    where = phpserialize.dumps(where)
    field = "count(distinct username) effective_num"
    url = "http://dj.cj655.com/api.php?m=player&a=admin_role_array7"
    params = {"where": where, "field": field}
    r = requests.get(url, params)
    d = json.loads(r.text)
    return int(d[0]["effective_num"])

# 月投产比
def product():
    print("Start to run Product task,Start Time:%s" % datetime.now())
    task_start_time = time.time()
    for i in range(0, 2):
        d = datetime.now()
        m = d.month - i
        y = d.year
        if m <= 0:
            y = y - 1
            m = m + 12
        d = datetime(y, m, 1)
        startDate = datetime(d.year, d.month, 1)
        endDate = datetime(d.year, d.month, calendar.monthrange(d.year, d.month)[1], 23, 59, 59)
        startTime = int(startDate.timestamp())
        endTime = int(endDate.timestamp())

        querySql = "SELECT count(gs.game_server_id) server_count,gs.game_id,g.game_name " \
                   "FROM gc_game_server as gs LEFT JOIN gc_game as g on gs.game_id = g.game_id " \
                   "WHERE ( (gs.open_time BETWEEN %d AND %d ) ) GROUP BY gs.game_id" % (startTime, endTime)
        cursor.execute(querySql)
        game = cursor.fetchall()

        data = []
        for index, value in enumerate(game):
            server_data = {}
            server_data["game_name"] = value["game_name"]
            server_data["month"] = d.strftime("%Y-%m")
            server_data["server_count"] = value["server_count"]

            # server ids
            querySql = "SELECT game_server_id FROM gc_game_server " \
                       "WHERE ( (open_time BETWEEN %d AND %d ) ) AND game_id=%d" % (
                       startTime, endTime, value["game_id"])
            cursor.execute(querySql)
            server = cursor.fetchall()

            server_ids = []
            for game_server in server:
                server_ids.append(str(game_server['game_server_id']))

            # 注册数
            where = "game_id = %d and server_id in (%s) and add_time between %d and %d" % (
                value["game_id"], ','.join(server_ids), startTime, endTime)
            reg_num = getRegNum(where)
            server_data["reg_num"] = reg_num

            # 有效用户数
            where = "game_id = %d and server_id in (%s) and dabiao_time between %d and %d and is_effective = 1" % (
                value["game_id"], ','.join(server_ids), startTime, endTime)
            effective_num = getEffectiveNum(where)
            server_data["effective_num"] = effective_num

            # 充值金额
            querySql = "SELECT round(sum(money/100),2) charge_sum FROM gc_order " \
                       "WHERE game_id = %d and game_server_id in (%s) AND create_time between %d and %d AND channel = 1 AND status = 1" % (
                value["game_id"], ','.join(server_ids), startTime, endTime)
            cursor.execute(querySql)
            charge_sum = cursor.fetchone()["charge_sum"]
            if charge_sum is None:
                charge_sum = 0
            charge_sum = int(charge_sum)
            server_data["charge_sum"] = charge_sum

            # 投放成本
            cost = effective_num * 40
            server_data["cost"] = cost

            # 投产比
            if cost:
                product_ratio = round((charge_sum / cost) * 100, 2)
            else:
                product_ratio = 0
            server_data["product_ratio"] = str(product_ratio) + "%"

            data.append(server_data)

        redis_key = "cj655_summary_product_%s" % hashlib.md5(str(startTime).encode("UTF-8")).hexdigest()
        r.set(redis_key, json.dumps(data))
        print("Task %s is completed" % d.strftime("%Y-%m"))

    task_end_time = time.time()

    print("Product all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

# 月新老付费比-新增付费
def month_charge(month_length=2):
    print("Start to run Month Charge task,Start Time:%s" % datetime.now())
    task_start_time = time.time()

    for i in range(0, month_length):
        d = datetime.now()
        m = d.month - i
        y = d.year
        if m <= 0:
            y = y - 1
            m = m + 12
        d = datetime(y, m, 1)
        startDate = datetime(d.year, d.month, 1)
        endDate = datetime(d.year, d.month, calendar.monthrange(d.year, d.month)[1], 23, 59, 59)
        startTime = int(startDate.timestamp())
        endTime = int(endDate.timestamp())

        querySql = "SELECT FROM_UNIXTIME(u.reg_time,'%%Y-%%m') month " \
                   "FROM gc_user as u LEFT JOIN gc_user_consume as uc on u.user_id = uc.user_id " \
                   "WHERE ( (uc.create_time BETWEEN %d AND %d ) ) AND ( uc.game_id > 0 ) AND ( uc.coin_type = 1 ) " \
                   "AND ( uc.status = 1 ) AND ( u.reg_time >= 1546272000 ) GROUP BY month" % (startTime, endTime)
        cursor.execute(querySql)
        months = cursor.fetchall()

        data = []
        for index, value in enumerate(months):
            p = time.strptime(value['month'], "%Y-%m")
            month_start_time = int(time.mktime(p))
            month_end_time = int(
                datetime(p.tm_year, p.tm_mon, calendar.monthrange(p.tm_year, p.tm_mon)[1], 23, 59, 59).timestamp())

            month_data = [value['month']]
            for index2, value2 in enumerate(months):
                p2 = time.strptime(value2['month'], "%Y-%m")
                month_start_time2 = int(time.mktime(p2))
                month_end_time2 = int(
                    datetime(p2.tm_year, p2.tm_mon, calendar.monthrange(p2.tm_year, p2.tm_mon)[1], 23, 59,
                             59).timestamp())
                querySql = "SELECT round(sum(uc.coin/100),2) charge_sum " \
                           "FROM gc_user_consume as uc LEFT JOIN gc_user as u ON uc.user_id = u.user_id " \
                           "WHERE (u.reg_time BETWEEN %d AND %d ) AND (uc.create_time BETWEEN %d AND %d ) " \
                           "AND uc.coin_type = 1 AND uc.status = 1" % (
                           month_start_time, month_end_time, month_start_time2, month_end_time2)
                cursor.execute(querySql)
                charge_sum = cursor.fetchone()["charge_sum"]
                if charge_sum is None:
                    charge_sum = 0
                charge_sum = int(charge_sum)
                month_data.append(charge_sum)

            data.append(month_data)

        # pprint(data)
        redis_key = "cj655_summary_month_charge_%s" % hashlib.md5(str(startTime).encode("UTF-8")).hexdigest()
        r.set(redis_key, json.dumps({'data': data, 'months': [i['month'] for i in months]}))
        print("Task %s is completed" % d.strftime("%Y-%m"))

    task_end_time = time.time()
    print("Month Charge all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

# 月新老付费比-月总付费
def month_charge_2(month_length=2):
    print("Start to run Month Charge 2 task,Start Time:%s" % datetime.now())
    task_start_time = time.time()

    for i in range(0, month_length):
        d = datetime.now()
        m = d.month - i
        y = d.year
        if m <= 0:
            y = y - 1
            m = m + 12
        d = datetime(y, m, 1)
        startDate = datetime(d.year, d.month, 1)
        endDate = datetime(d.year, d.month, calendar.monthrange(d.year, d.month)[1], 23, 59, 59)
        startTime = int(startDate.timestamp())
        endTime = int(endDate.timestamp())

        querySql = "SELECT FROM_UNIXTIME(gs.open_time,'%%Y-%%m') month FROM gc_user_consume as uc " \
                   "LEFT JOIN gc_game_server as gs ON uc.game_id = gs.game_id AND uc.game_server_id = gs.game_server_id " \
                   "WHERE (uc.create_time BETWEEN %d AND %d) AND gs.open_time > 0 " \
                   "AND uc.coin_type = 1 AND uc.status = 1 GROUP BY month" % (startTime, endTime)
        cursor.execute(querySql)
        months = cursor.fetchall()

        data = []
        for index, value in enumerate(months):
            p = time.strptime(value['month'], "%Y-%m")
            month_start_time = int(time.mktime(p))
            month_end_time = int(datetime(p.tm_year, p.tm_mon, calendar.monthrange(p.tm_year, p.tm_mon)[1], 23, 59, 59).timestamp())
            querySql = "SELECT game_id,game_server_id FROM gc_game_server " \
                       "WHERE (open_time BETWEEN %d AND %d) AND game_id > 0 AND game_server_id > 0" % (month_start_time, month_end_time)
            cursor.execute(querySql)
            server = cursor.fetchall()

            month_data = [value['month']]
            for index2, value2 in enumerate(months):
                p2 = time.strptime(value2['month'], "%Y-%m")
                month_start_time2 = int(time.mktime(p2))
                month_end_time2 = int(datetime(p2.tm_year, p2.tm_mon, calendar.monthrange(p2.tm_year, p2.tm_mon)[1], 23, 59,59).timestamp())

                querySql = "SELECT round(sum(uc.coin/100),2) charge_sum FROM gc_user_consume as uc " \
                   "LEFT JOIN gc_game_server as gs ON uc.game_id = gs.game_id AND uc.game_server_id = gs.game_server_id " \
                   "WHERE (uc.create_time BETWEEN %d AND %d) AND (gs.open_time BETWEEN %d AND %d) " \
                   "AND uc.coin_type = 1 AND uc.status = 1" % (month_start_time2, month_end_time2, month_start_time, month_end_time)
                cursor.execute(querySql)
                charge_sum = cursor.fetchone()["charge_sum"]
                if charge_sum is None:
                    charge_sum = 0
                charge_sum = int(charge_sum)
                month_data.append(charge_sum)

            data.append(month_data)

        #pprint(data)
        redis_key = "cj655_summary_month_charge_2_%s" % hashlib.md5(str(startTime).encode("UTF-8")).hexdigest()
        r.set(redis_key, json.dumps({'data': data, 'months': [i['month'] for i in months]}))
        print("Task %s is completed" % d.strftime("%Y-%m"))

    task_end_time = time.time()
    print("Month Charge 2 all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

# 月新老付费比-新增付费 以天数汇总
def month_charge_by_day(dates=[]):
    print("Start to run Month Charge By Day task,Start Time:%s" % datetime.now())
    task_start_time = time.time()

    #是否使用默认参数
    is_default = False
    if len(dates) == 0:
        dates = [datetime.now().strftime("%Y-%m")]
        is_default = True


    #今日是否是1日
    if datetime.now().day == 1:
        last_month = datetime.now().today() + relativedelta(months=-1)
        dates = [last_month.strftime("%Y-%m")]

    for date in dates:
        month_date = time.strptime(date, "%Y-%m")
        days = calendar.monthrange(month_date.tm_year, month_date.tm_mon)[1]
        start_time = int(datetime(month_date.tm_year, month_date.tm_mon, 1, 0, 0, 0).timestamp())
        for i in range(1, days+1):
            if is_default:
                yesterday = datetime.now()-timedelta(days=1)
                if yesterday.day != i:continue

            day_date = datetime(month_date.tm_year, month_date.tm_mon, i, 23, 59, 59)
            end_time = int(day_date.timestamp())

            if day_date.strftime("%Y-%m") == datetime.now().strftime("%Y-%m") and \
                    end_time > int(datetime.now().timestamp()): continue

            querySql = "SELECT FROM_UNIXTIME(u.reg_time,'%%Y-%%m') month " \
                       "FROM gc_user as u LEFT JOIN gc_user_consume as uc on u.user_id = uc.user_id " \
                       "WHERE ( (uc.create_time BETWEEN %d AND %d ) ) AND ( uc.game_id > 0 ) AND ( uc.coin_type = 1 ) " \
                       "AND ( uc.status = 1 ) AND ( u.reg_time >= 1546272000 ) GROUP BY month" % (start_time, end_time)
            cursor.execute(querySql)
            months = cursor.fetchall()

            data = []
            for index, value in enumerate(months):
                p = time.strptime(value['month'], "%Y-%m")
                month_start_time = int(time.mktime(p))
                if month_start_time == start_time:
                    month_end_time = end_time
                else:
                    month_end_time = int(
                        datetime(p.tm_year, p.tm_mon, calendar.monthrange(p.tm_year, p.tm_mon)[1], 23, 59, 59).timestamp())

                month_data = [value['month']]
                for index2, value2 in enumerate(months):
                    p2 = time.strptime(value2['month'], "%Y-%m")
                    month_start_time2 = int(time.mktime(p2))
                    if month_start_time2 == start_time:
                        month_end_time2 = end_time
                    else:
                        month_end_time2 = int(
                            datetime(p2.tm_year, p2.tm_mon, calendar.monthrange(p2.tm_year, p2.tm_mon)[1], 23, 59,
                                     59).timestamp())
                    querySql = "SELECT round(sum(uc.coin/100),2) charge_sum " \
                               "FROM gc_user_consume as uc LEFT JOIN gc_user as u ON uc.user_id = u.user_id " \
                               "WHERE (u.reg_time BETWEEN %d AND %d ) AND (uc.create_time BETWEEN %d AND %d ) " \
                               "AND uc.coin_type = 1 AND uc.status = 1" % (
                                   month_start_time, month_end_time, month_start_time2, month_end_time2)
                    cursor.execute(querySql)
                    charge_sum = cursor.fetchone()["charge_sum"]
                    if charge_sum is None:
                        charge_sum = 0
                    charge_sum = int(charge_sum)
                    month_data.append(charge_sum)

                data.append(month_data)

            #pprint(data)
            redis_key = "cj655_summary_month_charge_by_day_%s" % hashlib.md5(str(end_time).encode("UTF-8")).hexdigest()
            r.set(redis_key, json.dumps({'data': data, 'months': [i['month'] for i in months]}))
            print("Task %s is completed" % day_date.strftime("%Y-%m-%d"))

    task_end_time = time.time()
    print("Month Charge By Day all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

# 月新老付费比-月总付费 以天数汇总
def month_charge_2_by_day(dates=[]):
    print("Start to run Month Charge 2 By Day task,Start Time:%s" % datetime.now())
    task_start_time = time.time()

    # 是否使用默认参数
    is_default = False
    if len(dates) == 0:
        dates = [datetime.now().strftime("%Y-%m")]
        is_default = True

    # 今日是否是1日
    if datetime.now().day == 1:
        last_month = datetime.now().today() + relativedelta(months=-1)
        dates = [last_month.strftime("%Y-%m")]

    for date in dates:
        month_date = time.strptime(date, "%Y-%m")
        days = calendar.monthrange(month_date.tm_year, month_date.tm_mon)[1]
        start_time = int(datetime(month_date.tm_year, month_date.tm_mon, 1, 0, 0, 0).timestamp())
        for i in range(1, days + 1):
            if is_default:
                yesterday = datetime.now() - timedelta(days=1)
                if yesterday.day != i: continue

            day_date = datetime(month_date.tm_year, month_date.tm_mon, i, 23, 59, 59)
            end_time = int(day_date.timestamp())

            if day_date.strftime("%Y-%m") == datetime.now().strftime("%Y-%m") and \
                    end_time > int(datetime.now().timestamp()): continue

            querySql = "SELECT FROM_UNIXTIME(gs.open_time,'%%Y-%%m') month FROM gc_user_consume as uc " \
                       "LEFT JOIN gc_game_server as gs ON uc.game_id = gs.game_id AND uc.game_server_id = gs.game_server_id " \
                       "WHERE (uc.create_time BETWEEN %d AND %d) AND gs.open_time > 0 " \
                       "AND uc.coin_type = 1 AND uc.status = 1 GROUP BY month" % (start_time, end_time)
            cursor.execute(querySql)
            months = cursor.fetchall()

            data = []
            for index, value in enumerate(months):
                p = time.strptime(value['month'], "%Y-%m")
                month_start_time = int(time.mktime(p))
                if month_start_time == start_time:
                    month_end_time = end_time
                else:
                    month_end_time = int(
                        datetime(p.tm_year, p.tm_mon, calendar.monthrange(p.tm_year, p.tm_mon)[1], 23, 59, 59).timestamp())

                month_data = [value['month']]
                for index2, value2 in enumerate(months):
                    p2 = time.strptime(value2['month'], "%Y-%m")
                    month_start_time2 = int(time.mktime(p2))
                    if month_start_time2 == start_time:
                        month_end_time2 = end_time
                    else:
                        month_end_time2 = int(
                            datetime(p2.tm_year, p2.tm_mon, calendar.monthrange(p2.tm_year, p2.tm_mon)[1], 23, 59,
                                     59).timestamp())

                    querySql = "SELECT round(sum(uc.coin/100),2) charge_sum FROM gc_user_consume as uc " \
                               "LEFT JOIN gc_game_server as gs ON uc.game_id = gs.game_id AND uc.game_server_id = gs.game_server_id " \
                               "WHERE (uc.create_time BETWEEN %d AND %d) AND (gs.open_time BETWEEN %d AND %d) " \
                               "AND uc.coin_type = 1 AND uc.status = 1" % (
                               month_start_time2, month_end_time2, month_start_time, month_end_time)
                    cursor.execute(querySql)
                    charge_sum = cursor.fetchone()["charge_sum"]
                    if charge_sum is None:
                        charge_sum = 0
                    charge_sum = int(charge_sum)
                    month_data.append(charge_sum)

                data.append(month_data)

            #pprint(data)
            redis_key = "cj655_summary_month_charge_2_by_day_%s" % hashlib.md5(str(end_time).encode("UTF-8")).hexdigest()
            r.set(redis_key, json.dumps({'data': data, 'months': [i['month'] for i in months]}))
            print("Task %s is completed" % day_date.strftime("%Y-%m-%d"))

    task_end_time = time.time()
    print("Month Charge 2 By Day all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

# 月新老付费比-新增付费-渠道汇总
def channel_month_charge(dates=[datetime.now().strftime("%Y-%m")]):
    global conn2, cursor2
    print("Start to run Channel Month Charge task,Start Time:%s" % datetime.now())
    task_start_time = time.time()

    #1号统计上月数据
    day = datetime.now().day
    if day == 1:
        today = datetime.now().today()
        last_month = today + relativedelta(months=-1)
        dates.append(last_month.strftime("%Y-%m"))

    querySql = "SELECT channel_id,account FROM gc_channel WHERE type=1"
    try:
        cursor2.execute(querySql)
    except mysql.connector.Error as e:
        conn2 = mysql.connector.connect(**config2)
        cursor2 = conn2.cursor(dictionary=True)
        cursor2.execute(querySql)
    channels = cursor2.fetchall()

    for date in dates:
        print("Task %s is begin" % date)

        p = time.strptime(date, "%Y-%m")
        startTime = int(time.mktime(p))
        endTime = int(datetime(p.tm_year, p.tm_mon, calendar.monthrange(p.tm_year, p.tm_mon)[1], 23, 59, 59).timestamp())

        datas = {}
        for key, channel in enumerate(channels):
            # if(channel['channel_id'] != 17357):
            #     continue

            querySql = "SELECT FROM_UNIXTIME(u.reg_time,'%%Y-%%m') month " \
                       "FROM gc_user as u LEFT JOIN gc_user_consume as uc on u.user_id = uc.user_id " \
                       "WHERE ( (uc.create_time BETWEEN %d AND %d ) ) AND ( uc.game_id > 0 ) AND ( uc.coin_type = 1 ) " \
                       "AND ( uc.status = 1 ) AND ( u.reg_time >= 1546272000 ) AND u.main_channel_id = %d GROUP BY month" % (
                       startTime, endTime, channel['channel_id'])
            cursor.execute(querySql)
            months = cursor.fetchall()

            data = []
            for index, value in enumerate(months):
                p = time.strptime(value['month'], "%Y-%m")
                month_start_time = int(time.mktime(p))
                month_end_time = int(
                    datetime(p.tm_year, p.tm_mon, calendar.monthrange(p.tm_year, p.tm_mon)[1], 23, 59, 59).timestamp())

                month_data = [value['month']]
                for index2, value2 in enumerate(months):
                    p2 = time.strptime(value2['month'], "%Y-%m")
                    month_start_time2 = int(time.mktime(p2))
                    month_end_time2 = int(
                        datetime(p2.tm_year, p2.tm_mon, calendar.monthrange(p2.tm_year, p2.tm_mon)[1], 23, 59,
                                 59).timestamp())
                    querySql = "SELECT round(sum(uc.coin/100),2) charge_sum " \
                               "FROM gc_user_consume as uc LEFT JOIN gc_user as u ON uc.user_id = u.user_id " \
                               "WHERE (u.reg_time BETWEEN %d AND %d ) AND u.main_channel_id = %d AND (uc.create_time BETWEEN %d AND %d ) " \
                               "AND uc.coin_type = 1 AND uc.status = 1" % (
                                   month_start_time, month_end_time, channel['channel_id'], month_start_time2,
                                   month_end_time2)
                    cursor.execute(querySql)
                    charge_sum = cursor.fetchone()["charge_sum"]
                    if charge_sum is None:
                        charge_sum = 0
                    charge_sum = int(charge_sum)
                    month_data.append(charge_sum)

                data.append(month_data)

            datas[channel['account']] = {'data': data, 'months': [i['month'] for i in months]}

        redis_key = "cj655_summary_channel_month_charge_%s" % hashlib.md5(str(startTime).encode("UTF-8")).hexdigest()
        r.set(redis_key, json.dumps(datas))
        print("Task %s is completed" % date)

    task_end_time = time.time()
    print("Channel Month Charge all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

# 月新老付费比-月总付费-渠道汇总
def channel_month_charge_2(dates=[datetime.now().strftime("%Y-%m")]):
    global conn2, cursor2
    print("Start to run Channel Month Charge 2 task,Start Time:%s" % datetime.now())
    task_start_time = time.time()

    # 1号统计上月数据
    day = datetime.now().day
    if day == 1:
        today = datetime.now().today()
        last_month = today + relativedelta(months=-1)
        dates.append(last_month.strftime("%Y-%m"))

    querySql = "SELECT channel_id,account FROM gc_channel WHERE type=1"
    try:
        cursor2.execute(querySql)
    except mysql.connector.Error as e:
        conn2 = mysql.connector.connect(**config2)
        cursor2 = conn2.cursor(dictionary=True)
        cursor2.execute(querySql)
    channels = cursor2.fetchall()

    for date in dates:
        print("Task %s is begin" % date)

        p = time.strptime(date, "%Y-%m")
        startTime = int(time.mktime(p))
        endTime = int(
            datetime(p.tm_year, p.tm_mon, calendar.monthrange(p.tm_year, p.tm_mon)[1], 23, 59, 59).timestamp())

        datas = {}
        for key, channel in enumerate(channels):
            # if(channel['channel_id'] != 17357):
            #     continue

            querySql = "SELECT FROM_UNIXTIME(gs.open_time,'%%Y-%%m') month FROM gc_user_consume as uc " \
                       "LEFT JOIN gc_game_server as gs ON uc.game_id = gs.game_id AND uc.game_server_id = gs.game_server_id " \
                       "WHERE (uc.create_time BETWEEN %d AND %d) AND gs.open_time > 0 " \
                       "AND uc.coin_type = 1 AND uc.status = 1 AND uc.main_channel_id = %d GROUP BY month" % (
                           startTime, endTime, channel['channel_id'])
            cursor.execute(querySql)
            months = cursor.fetchall()

            data = []
            for index, value in enumerate(months):
                p = time.strptime(value['month'], "%Y-%m")
                month_start_time = int(time.mktime(p))
                month_end_time = int(
                    datetime(p.tm_year, p.tm_mon, calendar.monthrange(p.tm_year, p.tm_mon)[1], 23, 59, 59).timestamp())

                month_data = [value['month']]
                for index2, value2 in enumerate(months):
                    p2 = time.strptime(value2['month'], "%Y-%m")
                    month_start_time2 = int(time.mktime(p2))
                    month_end_time2 = int(
                        datetime(p2.tm_year, p2.tm_mon, calendar.monthrange(p2.tm_year, p2.tm_mon)[1], 23, 59,
                                 59).timestamp())

                    querySql = "SELECT round(sum(uc.coin/100),2) charge_sum FROM gc_user_consume as uc " \
                               "LEFT JOIN gc_game_server as gs ON uc.game_id = gs.game_id AND uc.game_server_id = gs.game_server_id " \
                               "WHERE (uc.create_time BETWEEN %d AND %d) AND (gs.open_time BETWEEN %d AND %d) " \
                               "AND uc.coin_type = 1 AND uc.status = 1 AND uc.main_channel_id = %d" % (
                                   month_start_time2, month_end_time2, month_start_time, month_end_time,
                                   channel['channel_id'])
                    cursor.execute(querySql)
                    charge_sum = cursor.fetchone()["charge_sum"]
                    if charge_sum is None:
                        charge_sum = 0
                    charge_sum = int(charge_sum)
                    month_data.append(charge_sum)

                data.append(month_data)

            datas[channel['account']] = {'data': data, 'months': [i['month'] for i in months]}

        redis_key = "cj655_summary_channel_month_charge_2_%s" % hashlib.md5(str(startTime).encode("UTF-8")).hexdigest()
        r.set(redis_key, json.dumps(datas))
        print("Task %s is completed" % date)

    task_end_time = time.time()
    print("Channel Month Charge 2 all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

# 月新老付费比-新增付费-渠道汇总 以天数汇总
def channel_month_charge_by_day(dates=[]):
    global conn2, cursor2
    print("Start to run Channel Month Charge By Day task,Start Time:%s" % datetime.now())
    task_start_time = time.time()

    # 是否使用默认参数
    is_default = False
    if len(dates) == 0:
        dates = [datetime.now().strftime("%Y-%m")]
        is_default = True

    # 今日是否是1日
    if datetime.now().day == 1:
        last_month = datetime.now().today() + relativedelta(months=-1)
        dates = [last_month.strftime("%Y-%m")]

    querySql = "SELECT channel_id,account FROM gc_channel WHERE type=1"
    try:
        cursor2.execute(querySql)
    except mysql.connector.Error as e:
        conn2 = mysql.connector.connect(**config2)
        cursor2 = conn2.cursor(dictionary=True)
        cursor2.execute(querySql)
    channels = cursor2.fetchall()

    for date in dates:
        print("Task %s is begin" % date)
        month_date = time.strptime(date, "%Y-%m")
        days = calendar.monthrange(month_date.tm_year, month_date.tm_mon)[1]
        start_time = int(datetime(month_date.tm_year, month_date.tm_mon, 1, 0, 0, 0).timestamp())
        for i in range(1, days + 1):
            if is_default:
                yesterday = datetime.now() - timedelta(days=1)
                if yesterday.day != i: continue

            day_date = datetime(month_date.tm_year, month_date.tm_mon, i, 23, 59, 59)
            end_time = int(day_date.timestamp())

            if day_date.strftime("%Y-%m") == datetime.now().strftime("%Y-%m") and \
                    end_time > int(datetime.now().timestamp()): continue

            print("Task %s is processing" % day_date.strftime("%Y-%m-%d"))

            datas = {}
            for key, channel in enumerate(channels):
                querySql = "SELECT FROM_UNIXTIME(u.reg_time,'%%Y-%%m') month " \
                           "FROM gc_user as u LEFT JOIN gc_user_consume as uc on u.user_id = uc.user_id " \
                           "WHERE ( (uc.create_time BETWEEN %d AND %d ) ) AND ( uc.game_id > 0 ) AND ( uc.coin_type = 1 ) " \
                           "AND ( uc.status = 1 ) AND ( u.reg_time >= 1546272000 ) AND u.main_channel_id = %d GROUP BY month" % (
                               start_time, end_time, channel['channel_id'])
                cursor.execute(querySql)
                months = cursor.fetchall()

                data = []
                for index, value in enumerate(months):
                    p = time.strptime(value['month'], "%Y-%m")
                    month_start_time = int(time.mktime(p))
                    if month_start_time == start_time:
                        month_end_time = end_time
                    else:
                        month_end_time = int(
                            datetime(p.tm_year, p.tm_mon, calendar.monthrange(p.tm_year, p.tm_mon)[1], 23, 59, 59).timestamp())

                    month_data = [value['month']]
                    for index2, value2 in enumerate(months):
                        p2 = time.strptime(value2['month'], "%Y-%m")
                        month_start_time2 = int(time.mktime(p2))
                        if month_start_time2 == start_time:
                            month_end_time2 = end_time
                        else:
                            month_end_time2 = int(
                                datetime(p2.tm_year, p2.tm_mon, calendar.monthrange(p2.tm_year, p2.tm_mon)[1], 23, 59,59).timestamp())
                        querySql = "SELECT round(sum(uc.coin/100),2) charge_sum " \
                                   "FROM gc_user_consume as uc LEFT JOIN gc_user as u ON uc.user_id = u.user_id " \
                                   "WHERE (u.reg_time BETWEEN %d AND %d ) AND u.main_channel_id = %d AND (uc.create_time BETWEEN %d AND %d ) " \
                                   "AND uc.coin_type = 1 AND uc.status = 1" % (
                                       month_start_time, month_end_time, channel['channel_id'], month_start_time2, month_end_time2)
                        cursor.execute(querySql)
                        charge_sum = cursor.fetchone()["charge_sum"]
                        if charge_sum is None:
                            charge_sum = 0
                        charge_sum = int(charge_sum)
                        month_data.append(charge_sum)

                    data.append(month_data)

                datas[channel['account']] = {'data': data, 'months': [i['month'] for i in months]}

            redis_key = "cj655_summary_channel_month_charge_by_day_%s" % hashlib.md5(str(end_time).encode("UTF-8")).hexdigest()
            r.set(redis_key, json.dumps(datas))
            print("Task %s is completed" % day_date.strftime("%Y-%m-%d"))

    task_end_time = time.time()
    print("Channel Month Charge By Day all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

# 月新老付费比-月总付费-渠道汇总 以天数汇总
def channel_month_charge_2_by_day(dates=[]):
    global conn2, cursor2
    print("Start to run Channel Month Charge 2 By Day task,Start Time:%s" % datetime.now())
    task_start_time = time.time()

    # 是否使用默认参数
    is_default = False
    if len(dates) == 0:
        dates = [datetime.now().strftime("%Y-%m")]
        is_default = True

    # 今日是否是1日
    if datetime.now().day == 1:
        last_month = datetime.now().today() + relativedelta(months=-1)
        dates = [last_month.strftime("%Y-%m")]

    querySql = "SELECT channel_id,account FROM gc_channel WHERE type=1"
    try:
        cursor2.execute(querySql)
    except mysql.connector.Error as e:
        conn2 = mysql.connector.connect(**config2)
        cursor2 = conn2.cursor(dictionary=True)
        cursor2.execute(querySql)
    channels = cursor2.fetchall()

    for date in dates:
        print("Task %s is begin" % date)
        month_date = time.strptime(date, "%Y-%m")
        days = calendar.monthrange(month_date.tm_year, month_date.tm_mon)[1]
        start_time = int(datetime(month_date.tm_year, month_date.tm_mon, 1, 0, 0, 0).timestamp())
        for i in range(1, days + 1):
            if is_default:
                yesterday = datetime.now() - timedelta(days=1)
                if yesterday.day != i: continue

            day_date = datetime(month_date.tm_year, month_date.tm_mon, i, 23, 59, 59)
            end_time = int(day_date.timestamp())

            if day_date.strftime("%Y-%m") == datetime.now().strftime("%Y-%m") and \
                    end_time > int(datetime.now().timestamp()): continue

            print("Task %s is processing" % day_date.strftime("%Y-%m-%d"))

            datas = {}
            for key, channel in enumerate(channels):
                querySql = "SELECT FROM_UNIXTIME(gs.open_time,'%%Y-%%m') month FROM gc_user_consume as uc " \
                           "LEFT JOIN gc_game_server as gs ON uc.game_id = gs.game_id AND uc.game_server_id = gs.game_server_id " \
                           "WHERE (uc.create_time BETWEEN %d AND %d) AND gs.open_time > 0 " \
                           "AND uc.coin_type = 1 AND uc.status = 1 AND uc.main_channel_id = %d GROUP BY month" % (
                               start_time, end_time, channel['channel_id'])
                cursor.execute(querySql)
                months = cursor.fetchall()

                data = []
                for index, value in enumerate(months):
                    p = time.strptime(value['month'], "%Y-%m")
                    month_start_time = int(time.mktime(p))
                    if month_start_time == start_time:
                        month_end_time = end_time
                    else:
                        month_end_time = int(
                            datetime(p.tm_year, p.tm_mon, calendar.monthrange(p.tm_year, p.tm_mon)[1], 23, 59,
                                     59).timestamp())

                    month_data = [value['month']]
                    for index2, value2 in enumerate(months):
                        p2 = time.strptime(value2['month'], "%Y-%m")
                        month_start_time2 = int(time.mktime(p2))
                        if month_start_time2 == start_time:
                            month_end_time2 = end_time
                        else:
                            month_end_time2 = int(
                                datetime(p2.tm_year, p2.tm_mon, calendar.monthrange(p2.tm_year, p2.tm_mon)[1], 23, 59,
                                         59).timestamp())

                        querySql = "SELECT round(sum(uc.coin/100),2) charge_sum FROM gc_user_consume as uc " \
                                   "LEFT JOIN gc_game_server as gs ON uc.game_id = gs.game_id AND uc.game_server_id = gs.game_server_id " \
                                   "WHERE (uc.create_time BETWEEN %d AND %d) AND (gs.open_time BETWEEN %d AND %d) " \
                                   "AND uc.coin_type = 1 AND uc.status = 1 AND uc.main_channel_id = %d" % (
                                       month_start_time2, month_end_time2, month_start_time, month_end_time, channel['channel_id'])
                        cursor.execute(querySql)
                        charge_sum = cursor.fetchone()["charge_sum"]
                        if charge_sum is None:
                            charge_sum = 0
                        charge_sum = int(charge_sum)
                        month_data.append(charge_sum)

                    data.append(month_data)

                datas[channel['account']] = {'data': data, 'months': [i['month'] for i in months]}

            redis_key = "cj655_summary_channel_month_charge_2_by_day_%s" % hashlib.md5(str(end_time).encode("UTF-8")).hexdigest()
            r.set(redis_key, json.dumps(datas))
            print("Task %s is completed" % day_date.strftime("%Y-%m-%d"))

    task_end_time = time.time()
    print("Channel Month Charge 2 By Day all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

# 产品新老付费比-新增付费
def game_charge(month_length=2):
    print("Start to run Game Charge task,Start Time:%s" % datetime.now())
    task_start_time = time.time()

    for i in range(0, month_length):
        d = datetime.now()
        m = d.month - i
        y = d.year
        if m <= 0:
            y = y - 1
            m = m + 12
        d = datetime(y, m, 1)
        startDate = datetime(d.year, d.month, 1)
        endDate = datetime(d.year, d.month, calendar.monthrange(d.year, d.month)[1], 23, 59, 59)
        startTime = int(startDate.timestamp())
        endTime = int(endDate.timestamp())

        querySql = "SELECT uc.game_id,g.game_name " \
                   "FROM gc_user_consume as uc LEFT JOIN gc_game as g on uc.game_id = g.game_id " \
                   "WHERE ( (uc.create_time BETWEEN %d AND %d ) ) AND ( uc.game_id > 0 ) " \
                   "AND ( uc.coin_type = 1 ) AND ( uc.status = 1 ) GROUP BY uc.game_id" % (startTime, endTime)
        cursor.execute(querySql)
        game = cursor.fetchall()

        querySql = "SELECT FROM_UNIXTIME(u.reg_time,'%%Y-%%m') month " \
                   "FROM gc_user as u LEFT JOIN gc_user_consume as uc on u.user_id = uc.user_id " \
                   "WHERE ( (uc.create_time BETWEEN %d AND %d ) ) AND ( uc.game_id > 0 ) " \
                   "AND ( uc.coin_type = 1 ) AND ( uc.status = 1 ) AND ( u.reg_time >= 1546272000 ) GROUP BY month ORDER BY month desc" % (
                   startTime, endTime)
        cursor.execute(querySql)
        months = cursor.fetchall()

        data = []
        for index, value in enumerate(game):
            game_data = [value["game_name"]]
            for index2, value2 in enumerate(months):
                p = time.strptime(value2['month'], "%Y-%m")
                month_start_time = int(time.mktime(p))
                month_end_time = int(
                    datetime(p.tm_year, p.tm_mon, calendar.monthrange(p.tm_year, p.tm_mon)[1], 23, 59, 59).timestamp())
                where = []
                where.append("(u.reg_time BETWEEN %d AND %d)" % (month_start_time, month_end_time))
                where.append("(uc.create_time BETWEEN %d AND %d)" % (startTime, endTime))
                where.append("uc.coin_type = 1")
                where.append("uc.status = 1")
                where.append("uc.game_id = %d" % value["game_id"])

                querySql = "SELECT round(sum(uc.coin/100),2) charge_sum " \
                           "FROM gc_user_consume as uc LEFT JOIN gc_user as u ON uc.user_id = u.user_id " \
                           "WHERE %s" % (" AND ".join(where))
                cursor.execute(querySql)
                charge_sum = cursor.fetchone()["charge_sum"]
                if charge_sum is None:
                    charge_sum = 0
                charge_sum = int(charge_sum)
                game_data.append(charge_sum)

            data.append(game_data)

        # pprint(data)
        redis_key = "cj655_summary_game_charge_%s" % hashlib.md5(str(startTime).encode("UTF-8")).hexdigest()
        r.set(redis_key, json.dumps({'data': data, 'months': [i['month'] for i in months]}))
        print("Task %s is completed" % d.strftime("%Y-%m"))

    task_end_time = time.time()
    print("Game Charge all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

# 产品新老付费比-月总付费
def game_charge_2(month_length=2):
    print("Start to run Game Charge 2 task,Start Time:%s" % datetime.now())
    task_start_time = time.time()

    for i in range(0, month_length):
        d = datetime.now()
        m = d.month - i
        y = d.year
        if m <= 0:
            y = y - 1
            m = m + 12
        d = datetime(y, m, 1)
        startDate = datetime(d.year, d.month, 1)
        endDate = datetime(d.year, d.month, calendar.monthrange(d.year, d.month)[1], 23, 59, 59)
        startTime = int(startDate.timestamp())
        endTime = int(endDate.timestamp())

        querySql = "SELECT FROM_UNIXTIME(gs.open_time,'%%Y-%%m') month FROM gc_user_consume as uc " \
                   "LEFT JOIN gc_game_server as gs ON uc.game_id = gs.game_id AND uc.game_server_id = gs.game_server_id " \
                   "WHERE (uc.create_time BETWEEN %d AND %d) AND gs.open_time > 0 " \
                   "AND uc.coin_type = 1 AND uc.status = 1 GROUP BY month ORDER BY month desc" % (startTime, endTime)
        cursor.execute(querySql)
        months = cursor.fetchall()

        querySql = "SELECT uc.game_id,g.game_name " \
                   "FROM gc_user_consume as uc LEFT JOIN gc_game as g on uc.game_id = g.game_id " \
                   "WHERE ( (uc.create_time BETWEEN %d AND %d ) ) AND ( uc.game_id > 0 ) " \
                   "AND ( uc.coin_type = 1 ) AND ( uc.status = 1 ) GROUP BY uc.game_id" % (startTime, endTime)
        cursor.execute(querySql)
        game = cursor.fetchall()

        data = []
        for index, value in enumerate(game):
            game_data = [value['game_name']]
            for index2, value2 in enumerate(months):
                p = time.strptime(value2['month'], "%Y-%m")
                month_start_time = int(time.mktime(p))
                month_end_time = int(datetime(p.tm_year, p.tm_mon, calendar.monthrange(p.tm_year, p.tm_mon)[1], 23, 59, 59).timestamp())

                querySql = "SELECT round(sum(uc.coin/100),2) charge_sum FROM gc_user_consume as uc " \
                           "LEFT JOIN gc_game_server as gs ON uc.game_id = gs.game_id AND uc.game_server_id = gs.game_server_id " \
                           "WHERE (uc.create_time BETWEEN %d AND %d) AND (gs.open_time BETWEEN %d AND %d) AND uc.game_id = %d " \
                           "AND uc.coin_type = 1 AND uc.status = 1" % (
                           startTime, endTime, month_start_time, month_end_time, value["game_id"])
                cursor.execute(querySql)
                charge_sum = cursor.fetchone()["charge_sum"]
                if charge_sum is None:
                    charge_sum = 0
                charge_sum = int(charge_sum)
                game_data.append(charge_sum)

            data.append(game_data)

        redis_key = "cj655_summary_game_charge_2_%s" % hashlib.md5(str(startTime).encode("UTF-8")).hexdigest()
        r.set(redis_key, json.dumps({'data': data, 'months': [i['month'] for i in months]}))
        print("Task %s is completed" % d.strftime("%Y-%m"))

    task_end_time = time.time()
    print("Game Charge 2 all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

# 关注角色付费、数量增长情况
def notice_role_new_charge():
    print("Start to run Notice Role New Charge task,Start Time:%s" % datetime.now())
    task_start_time = time.time()

    querySql = "SELECT id,group_name FROM gc_gmgroup"
    cursor.execute(querySql)
    groups = cursor.fetchall()

    for i in range(0, 2):
        d = datetime.now()
        m = d.month - i
        y = d.year
        if m <= 0:
            y = y - 1
            m = m + 12
        d = datetime(y, m, 1)
        startDate = datetime(d.year, d.month, 1)
        endDate = datetime(d.year, d.month, calendar.monthrange(d.year, d.month)[1], 23, 59, 59)
        startTime = int(startDate.timestamp())
        endTime = int(endDate.timestamp())

        data = []
        for group in groups:
            # if group['id'] != 105:
            #     continue

            querySql = "SELECT distinct server_id,game_id FROM gc_gmgroup_server WHERE group_id = %d" % (group['id'])
            cursor.execute(querySql)
            servers = cursor.fetchall()
            for server in servers:
                querySql = "SELECT role_id,create_time FROM gc_gmnotice_role " \
                           "WHERE game_id = %d AND server_id = %d AND status = 3" % (
                           server['game_id'], server['server_id'])
                cursor.execute(querySql)
                notice_roles = cursor.fetchall()

                # 关注角色添加前充值
                charge_sum_1 = 0
                for notice_role in notice_roles:
                    querySql = "SELECT sum(coin/100) charge_sum FROM gc_user_consume WHERE role_id = '%s' AND create_time BETWEEN %d AND %d" \
                               " AND status = 1 AND coin_type = 1 LIMIT 1" % (
                                   notice_role['role_id'], startTime, notice_role['create_time'])
                    cursor.execute(querySql)
                    user_consume = cursor.fetchone()
                    if user_consume['charge_sum'] is not None:
                        charge_sum_1 += int(user_consume['charge_sum'])

                # 关注角色添加后充值
                charge_sum_2 = 0
                for notice_role in notice_roles:
                    querySql = "SELECT sum(coin/100) charge_sum FROM gc_user_consume WHERE role_id = '%s' AND create_time BETWEEN %d AND %d" \
                               " AND status = 1 AND coin_type = 1 LIMIT 1" % (
                                   notice_role['role_id'], notice_role['create_time'], endTime)
                    cursor.execute(querySql)
                    user_consume = cursor.fetchone()
                    if user_consume['charge_sum'] is not None:
                        charge_sum_2 += int(user_consume['charge_sum'])

                # 新增数量
                querySql = "SELECT count(distinct role_id) role_num FROM gc_gmnotice_role " \
                           "WHERE game_id = %d AND server_id = %d AND create_time BETWEEN %d AND %d AND status = 3 LIMIT 1" % (
                               server['game_id'], server['server_id'], startTime, endTime)
                cursor.execute(querySql)
                notice_role = cursor.fetchone()
                role_num = notice_role['role_num']

                server_data = {}
                server_data['game_id'] = server['game_id']
                server_data['server_id'] = server['server_id']
                server_data['group_id'] = group['id']
                server_data['group_name'] = group['group_name']
                server_data['charge_sum_1'] = charge_sum_1
                server_data['charge_sum_2'] = charge_sum_2
                server_data['role_num'] = role_num
                server_data['month'] = d.strftime("%Y-%m")
                server_data['month_time'] = startTime

                data.append(server_data)

        redis_key = "cj655_notice_role_new_charge_%s" % hashlib.md5(str(startTime).encode("UTF-8")).hexdigest()
        r.set(redis_key, json.dumps(data))
        print("Task %s is completed" % d.strftime("%Y-%m"))

    task_end_time = time.time()
    print(
        "Notice Role New Charge all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

#登录异常预警
def exception_login_aware():
    global conn2, cursor2
    print("Start to run Risk ELA task,Start Time:%s" % datetime.now())
    task_start_time = time.time()

    for i in range(0, 2):
        d = datetime.now()
        m = d.month - i
        y = d.year
        if m <= 0:
            y = y - 1
            m = m + 12
        d = datetime(y, m, 1)
        startDate = datetime(d.year, d.month, 1)
        endDate = datetime(d.year, d.month, calendar.monthrange(d.year, d.month)[1], 23, 59, 59)
        startTime = int(startDate.timestamp())
        endTime = int(endDate.timestamp())
        limitTime = int((datetime.today()-timedelta(days=30)).timestamp())

        querySql = "SELECT channel_id,charge_sum,new_charge_sum,effective_num FROM gc_channel_month_count " \
                   "WHERE date_time BETWEEN %d AND %d" % (startTime,endTime)

        cursor.execute(querySql)
        channel_month_count_data = cursor.fetchall()

        querySql = "SELECT channel_id,same_unique_flag_user_count,same_ip_user_count,one_login_effective_user_count FROM gc_channel_user_login " \
                   "WHERE date_time BETWEEN %d AND %d" % (startTime,endTime)

        cursor.execute(querySql)
        channel_user_login_data = cursor.fetchall()

        #渠道3级 会长 组长 推广员
        data = {}
        for type in range(1, 4):
            # querySql = "SELECT channel_id,type,add_time,account,deal_type FROM gc_channel " \
            #            "WHERE add_time <= %d AND status > 0 AND channel_is_delete = 0 AND type = %d ORDER BY add_time DESC LIMIT 20" % (
            #                limitTime,type)
            querySql = "SELECT channel_id,type,add_time,account,deal_type FROM gc_channel " \
                       "WHERE add_time <= %d AND status > 0 AND channel_is_delete = 0 AND type = %d ORDER BY add_time DESC" % (
                           limitTime, type)

            try:
                cursor2.execute(querySql)
            except mysql.connector.Error as e:
                conn2 = mysql.connector.connect(**config2)
                cursor2 = conn2.cursor(dictionary=True)
                cursor2.execute(querySql)
            channel = cursor2.fetchall()

            for index, value in enumerate(channel):
                child_ids = get_pids_by_type(value['channel_id'], value['type'])

                # 计算充值总额 新充值额 有效人数 LTV
                charge_sum = 0
                new_charge_sum = 0
                effective_num = 0

                for value2 in channel_month_count_data:
                    if value2['channel_id'] in child_ids:
                        charge_sum += value2['charge_sum']
                        new_charge_sum += value2['new_charge_sum']
                        effective_num += value2['effective_num']

                channel[index]['charge_sum'] = charge_sum
                channel[index]['new_charge_sum'] = new_charge_sum
                channel[index]['effective_num'] = effective_num
                if channel[index]['effective_num'] > 0:
                    channel[index]['ltv'] = (
                                channel[index]['new_charge_sum'] / channel[index]['effective_num']).quantize(
                        Decimal('0.00'), ROUND_HALF_UP)
                else:
                    channel[index]['ltv'] = 0

                # 计算设备码重复数≥5
                same_unique_flag_user_count = 0

                for value2 in channel_user_login_data:
                    if value2['channel_id'] in child_ids:
                        same_unique_flag_user_count += value2['same_unique_flag_user_count']

                channel[index]['same_unique_flag_user_count'] = same_unique_flag_user_count

                # 计算IP重复数≥10
                same_ip_user_count = 0
                for value2 in channel_user_login_data:
                    if value2['channel_id'] in child_ids:
                        same_ip_user_count += value2['same_ip_user_count']

                channel[index]['same_ip_user_count'] = same_ip_user_count

                # 计算一次登录无二次登录个数
                one_login_effective_user_count = 0
                for value2 in channel_user_login_data:
                    if value2['channel_id'] in child_ids:
                        one_login_effective_user_count += value2['one_login_effective_user_count']

                channel[index]['one_login_effective_user_count'] = one_login_effective_user_count
                if channel[index]['effective_num'] > 0:
                    channel[index]['one_login_effective_user_rate'] = round(
                        channel[index]['one_login_effective_user_count'] / channel[index]['effective_num'] * 100, 2)
                else:
                    channel[index]['one_login_effective_user_rate'] = 0

            data[type] = channel

        redis_key = "cj655_exception_login_aware_%s" % hashlib.md5(str(startTime).encode("UTF-8")).hexdigest()
        r.set(redis_key, json.dumps(data,cls=DecimalEncoder))
        print("Task %s is completed" % d.strftime("%Y-%m"))

    task_end_time = time.time()
    print("Risk ELA all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

#维度异常预警
def exception_dimension_aware():
    global conn2, cursor2
    print("Start to run Risk EDA task,Start Time:%s" % datetime.now())
    task_start_time = time.time()

    for i in range(0, 2):
        d = datetime.now()
        m = d.month - i
        y = d.year
        if m <= 0:
            y = y - 1
            m = m + 12
        d = datetime(y, m, 1)
        startDate = datetime(d.year, d.month, 1)
        endDate = datetime(d.year, d.month, calendar.monthrange(d.year, d.month)[1], 23, 59, 59)
        startTime = int(startDate.timestamp())
        endTime = int(endDate.timestamp())
        limitTime = int((datetime.today() - timedelta(days=30)).timestamp())

        querySql = "SELECT channel_id,effective_num,new_charge_sum,effective_num130_149,reg_num FROM gc_channel_month_count " \
                   "WHERE date_time BETWEEN %d AND %d" % (startTime, endTime)

        cursor.execute(querySql)
        channel_month_count_data = cursor.fetchall()

        #渠道3级 会长 组长 推广员
        data = {}
        for type in range(1, 4):
            # querySql = "SELECT channel_id,type,add_time,account,deal_type FROM gc_channel " \
            #            "WHERE add_time <= %d AND status > 0 AND channel_is_delete = 0 AND type = %d ORDER BY add_time DESC LIMIT 10" % (
            #                limitTime,type)
            querySql = "SELECT channel_id,type,add_time,account,deal_type FROM gc_channel " \
                       "WHERE add_time <= %d AND status > 0 AND channel_is_delete = 0 AND type = %d ORDER BY add_time DESC" % (
                           limitTime, type)

            try:
                cursor2.execute(querySql)
            except mysql.connector.Error as e:
                conn2 = mysql.connector.connect(**config2)
                cursor2 = conn2.cursor(dictionary=True)
                cursor2.execute(querySql)
            channel = cursor2.fetchall()

            for index, value in enumerate(channel):
                child_ids = get_pids_by_type(value['channel_id'], value['type'])

                # 计算有效人数 注册充值总额 130-149有效人数 注册数
                effective_num = 0
                new_charge_sum = 0
                effective_num130_149 = 0
                reg_num = 0

                for value2 in channel_month_count_data:
                    if value2['channel_id'] in child_ids:
                        effective_num += value2['effective_num']
                        new_charge_sum += value2['new_charge_sum']
                        effective_num130_149 += value2['effective_num130_149']
                        reg_num += value2['reg_num']

                channel[index]['effective_num'] = effective_num
                channel[index]['new_charge_sum'] = new_charge_sum
                channel[index]['effective_num130_149'] = effective_num130_149
                channel[index]['reg_num'] = reg_num

                if channel[index]['effective_num'] > 0:
                    channel[index]['ltv'] = (
                            channel[index]['new_charge_sum'] / channel[index]['effective_num']).quantize(
                        Decimal('0.00'), ROUND_HALF_UP)
                    channel[index]['low_level_rate'] = round(channel[index]['effective_num130_149'] / channel[index]['effective_num'] * 100,2)

                else:
                    channel[index]['ltv'] = 0
                    channel[index]['low_level_rate'] = 0

                if channel[index]['reg_num'] > 0:
                    channel[index]['effective_reg_rate'] = round(channel[index]['effective_num'] / channel[index]['reg_num'] * 100,2)
                else:
                    channel[index]['effective_reg_rate'] = 0

            data[type] = channel

        #pprint(data)
        redis_key = "cj655_exception_dimension_aware_%s" % hashlib.md5(str(startTime).encode("UTF-8")).hexdigest()
        r.set(redis_key, json.dumps(data, cls=DecimalEncoder))
        print("Task %s is completed" % d.strftime("%Y-%m"))

    task_end_time = time.time()
    print("Risk EDA all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

#推广账号维度异常标识
def exception_dimension_identify(month_length=2):
    global conn2, cursor2
    print("Start to run Risk EDI task,Start Time:%s" % datetime.now())
    task_start_time = time.time()

    for i in range(0, month_length):
        d = datetime.now()
        m = d.month - i
        y = d.year
        if m <= 0:
            y = y - 1
            m = m + 12
        d = datetime(y, m, 1)
        startDate = datetime(d.year, d.month, 1)
        endDate = datetime(d.year, d.month, calendar.monthrange(d.year, d.month)[1], 23, 59, 59)
        startTime = int(startDate.timestamp())
        endTime = int(endDate.timestamp())
        limitTime = int((datetime.today()-timedelta(days=30)).timestamp())

        lastMonthStartDate = startDate + relativedelta(months=-1)
        lastMonthEndDate = endDate + relativedelta(months=-1)
        lastMonthStartTime = int(lastMonthStartDate.timestamp())
        lastMonthEndTime = int(lastMonthEndDate.timestamp())

        querySql = "SELECT sum(charge_sum) total_charge_sum," \
                   "sum(charge_num) total_charge_num," \
                   "sum(new_charge_sum) total_new_charge_sum," \
                   "sum(new_charge_num) total_new_charge_num," \
                   "sum(new_login_count) total_new_login_count," \
                   "sum(reg_num) total_reg_num FROM gc_channel_month_count " \
                   "WHERE date_time BETWEEN %d AND %d" % (lastMonthStartTime,lastMonthEndTime)
        cursor.execute(querySql)
        total_count_data = cursor.fetchone()

        total = {}
        # 活跃ARPU平均值
        total['login_arpu_ave'] = round(total_count_data['total_new_charge_sum']/total_count_data['total_new_login_count'], 2)
        # 注册ARPU平均值
        total['reg_arpu_ave'] = round(total_count_data['total_new_charge_sum']/total_count_data['total_reg_num'], 2)
        # 活跃付费率平均值
        total['login_charge_rate_ave'] = round(total_count_data['total_new_charge_num']/total_count_data['total_new_login_count']*100, 2)
        # 注册付费率平均值
        total['reg_charge_rate_ave'] = round(total_count_data['total_new_charge_num']/total_count_data['total_reg_num']*100, 2)
        # ARPPU平均值
        total['arppu_ave'] = round(total_count_data['total_charge_sum']/total_count_data['total_charge_num'], 2)


        querySql = "SELECT * FROM gc_channel_month_count WHERE date_time BETWEEN %d AND %d" % (startTime,endTime)

        cursor.execute(querySql)
        channel_month_count_data = cursor.fetchall()

        # 渠道3级 会长 组长 推广员
        data = {}
        for type in range(1, 4):
            # if type != 1:
            #     continue
            # querySql = "SELECT channel_id,type,add_time,account,deal_type FROM gc_channel " \
            #            "WHERE add_time <= %d AND status > 0 AND channel_is_delete = 0 AND type = %d AND channel_id = 1 ORDER BY add_time DESC" % (
            #                limitTime, type)
            querySql = "SELECT channel_id,type,add_time,account,deal_type FROM gc_channel " \
                       "WHERE add_time <= %d AND status > 0 AND channel_is_delete = 0 AND type = %d ORDER BY add_time DESC" % (
                           limitTime, type)

            try:
                cursor2.execute(querySql)
            except mysql.connector.Error as e:
                conn2 = mysql.connector.connect(**config2)
                cursor2 = conn2.cursor(dictionary=True)
                cursor2.execute(querySql)
            channel = cursor2.fetchall()

            for index, value in enumerate(channel):
                child_ids = get_pids_by_type(value['channel_id'], value['type'])

                #当月充值总额 当月充值人数 当月新增充值总额 当月新增充值人数 当月注册数
                #当月有效数 当月130-149有效数 当月登录数 当月新增登录数
                charge_sum = 0
                charge_num = 0
                new_charge_sum = 0
                new_charge_num = 0
                reg_num = 0
                effective_num = 0
                effective_num_130_149 = 0
                login_count = 0
                new_login_count = 0

                for value2 in channel_month_count_data:
                    if value2['channel_id'] in child_ids:
                        charge_sum += value2['charge_sum']
                        charge_num += value2['charge_num']
                        new_charge_sum += value2['new_charge_sum']
                        new_charge_num += value2['new_charge_num']
                        reg_num += value2['reg_num']
                        effective_num += value2['effective_num']
                        effective_num_130_149 += value2['effective_num130_149']
                        login_count += value2['login_count']
                        new_login_count += value2['new_login_count']

                channel[index]['charge_sum'] = charge_sum
                channel[index]['charge_num'] = charge_num
                channel[index]['new_charge_sum'] = new_charge_sum
                channel[index]['new_charge_num'] = new_charge_num
                channel[index]['reg_num'] = reg_num
                channel[index]['effective_num'] = effective_num
                channel[index]['effective_num_130_149'] = effective_num_130_149
                channel[index]['login_count'] = login_count
                channel[index]['new_login_count'] = new_login_count

                #历史活跃LTV
                if effective_num > 0:
                    channel[index]['ltv'] = round(channel[index]['charge_sum'] / channel[index]['effective_num'], 2)
                else:
                    channel[index]['ltv'] = 0

                #当月活跃LTV
                if effective_num > 0:
                    channel[index]['add_ltv'] = round(channel[index]['new_charge_sum'] / channel[index]['effective_num'], 2)
                else:
                    channel[index]['add_ltv'] = 0

                #低等级比率
                if effective_num > 0:
                    channel[index]['low_level_rate'] = round(channel[index]['effective_num_130_149'] / channel[index]['effective_num'] * 100, 2)
                else:
                    channel[index]['low_level_rate'] = 0

                #有效注册比
                if reg_num > 0:
                    channel[index]['effective_reg_rate'] = round(channel[index]['effective_num'] / channel[index]['reg_num'] * 100, 2)
                else:
                    channel[index]['effective_reg_rate'] = 0

                #活跃ARPU
                if new_login_count > 0:
                    channel[index]['login_arpu'] = round(channel[index]['charge_sum'] / channel[index]['new_login_count'], 2)
                else:
                    channel[index]['login_arpu'] = 0

                #注册ARPU
                if reg_num > 0:
                    channel[index]['reg_arpu'] = round(channel[index]['new_charge_sum'] / channel[index]['reg_num'], 2)
                else:
                    channel[index]['reg_arpu'] = 0

                #活跃付费率
                if new_login_count > 0:
                    channel[index]['login_charge_rate'] = round(channel[index]['charge_num'] / channel[index]['new_login_count'] * 100, 2)
                else:
                    channel[index]['login_charge_rate'] = 0

                #注册付费率
                if reg_num > 0:
                    channel[index]['reg_charge_rate'] = round(channel[index]['new_charge_num'] / channel[index]['reg_num'] * 100, 2)
                else:
                    channel[index]['reg_charge_rate'] = 0

                #ARPPU
                if charge_num > 0:
                    channel[index]['arppu'] = round(channel[index]['charge_sum'] / channel[index]['charge_num'], 2)
                else:
                    channel[index]['arppu'] = 0

                #回款率
                if effective_num > 0:
                    channel[index]['repay_rate'] = round(channel[index]['new_charge_sum'] / (channel[index]['effective_num'] * 40) * 100, 2)
                else:
                    channel[index]['repay_rate'] = 0

            data[type] = channel

        redis_key = "cj655_exception_dimension_identify_%s" % hashlib.md5(str(startTime).encode("UTF-8")).hexdigest()
        r.set(redis_key, json.dumps({'data': data, 'total': total}, cls=DecimalEncoder))
        print("Task %s is completed" % d.strftime("%Y-%m"))

    task_end_time = time.time()
    print("Risk EDI all task is completed,Time:%s \n" % timedelta(seconds=task_end_time - task_start_time))

def get_pids_by_type(channel_id,type):
    if type == 3:
        return [channel_id]
    elif type == 2:
        querySql = "SELECT child_id FROM gc_president_pid WHERE parent_id = %d" % (channel_id)
        cursor2.execute(querySql)
        r = cursor2.fetchall()
        if r:
            channel_ids = [i['child_id'] for i in r]
            channel_ids.append(channel_id)
            return channel_ids
        else:
            return [channel_id]
    else:
        querySql = "SELECT child_id FROM gc_president_pid WHERE parent_id = %d" % (channel_id)
        cursor2.execute(querySql)
        r = cursor2.fetchall()
        if r:
            channel_ids = [i['child_id'] for i in r]
            for value in channel_ids:
                querySql = "SELECT child_id FROM gc_president_pid WHERE parent_id = %d" % (value)
                cursor2.execute(querySql)
                r = cursor2.fetchall()
                if r:
                    channel_ids_2 = [i['child_id'] for i in r]
                    channel_ids.extend(channel_ids_2)
            channel_ids.append(channel_id)
            return channel_ids
        else:
            return [channel_id]

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        super(DecimalEncoder, self).default(o)

if __name__ == '__main__':
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor(dictionary=True)

    conn2 = mysql.connector.connect(**config2)
    cursor2 = conn2.cursor(dictionary=True)

    r = redis.Redis(host='localhost', port=6379, db=0)

    parser = argparse.ArgumentParser(description="Async Task")
    parser.add_argument('--task-type', action='store', dest='task_type', default=0, help='run the specify type of task')
    parser.add_argument('--month-length', action='store', dest='month_length', default=2, help='specify length of months')
    parser.add_argument('--task-name', action='store', dest='task_name', default='',help='run the specify name of task')
    parser.add_argument('--month', action='store', dest='month', default='',help='run the specify months of task')
    parser = parser.parse_args()

    task_type = int(parser.task_type)
    month_length = int(parser.month_length)
    task_name = parser.task_name
    month = parser.month

    if task_name != '':
        if month != '':
            eval(task_name)(month.split(','))
        elif month_length > 2:
            eval(task_name)(month_length)
        else:
            eval(task_name)()
        sys.exit(0)

    # 月新老付费比-按天汇总
    if task_type == 1 or task_type == 0:
        if month != '':
            month_charge_by_day(month.split(','))
            month_charge_2_by_day(month.split(','))
            channel_month_charge_by_day(month.split(','))
            channel_month_charge_2_by_day(month.split(','))
        else:
            month_charge_by_day()
            month_charge_2_by_day()
            channel_month_charge_by_day()
            channel_month_charge_2_by_day()

    # 月份新老付费比
    if task_type == 2 or task_type == 0:
        month_charge(month_length)
        month_charge_2(month_length)

    # 产品新老付费比
    if task_type == 3 or task_type == 0:
        game_charge(month_length)
        game_charge_2(month_length)

    # 关注角色付费、数量增长情况
    if task_type == 4 or task_type == 0:
        notice_role_new_charge()

    # 登录异常预警
    # 维度异常预警
    # 推广账号维度异常标识
    if task_type == 5:
        exception_login_aware()
        exception_dimension_aware()
        exception_dimension_identify()

    # 月新老付费比-新增付费-渠道汇总
    # 月新老付费比-月总付费-渠道汇总
    if task_type == 6:
        if month != '':
            channel_month_charge(month.split(','))
            channel_month_charge_2(month.split(','))
        else:
            channel_month_charge()
            channel_month_charge_2()
