#!/usr/bin/python3
#-*- coding: utf-8 -*-

import os
from math import ceil
from pprint import pprint
import mysql.connector
from datetime import datetime,timedelta
import time
import phpserialize
import requests
import xlwt


config = {
      'user': 'root',
      'password': '',
      'host': 'localhost',
      'database': 'cj655',
      'charset': 'utf8',
      "connection_timeout": 60,
      "use_pure": True
}

if os.getenv('OS') == 'Windows_NT':
    config['password'] = ''
else:
    config['password'] = 'game123456'

if  __name__ == '__main__':
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor(dictionary=True)

    sd = datetime(2019, 5, 1)
    start_time = int(sd.timestamp())
    ed = datetime(2019, 12, 25)
    end_time = int(ed.timestamp())+86399

    days = ceil((end_time-start_time)/86400)

    print("Days : %d" % days)

    task_start_time = time.time()

    valid_day = [1,3,7,15,30,60,90,120,150]

    data = []
    for i in range(days-1,-1,-1):
        #if (i+1) not in valid_day:continue

        dd = ed - timedelta(days=i)
        day_start_time = int(dd.timestamp())
        day_end_time = day_start_time+86399
        #print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(day_start_time)))
        #print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(day_end_time)))

        day_data = {}
        day_data["date"] = dd.strftime("%Y-%m-%d")

        querySql = "SELECT count(user_id) reg_num FROM gc_user WHERE reg_time BETWEEN %d AND %d" % (day_start_time,day_end_time)
        cursor.execute(querySql)
        reg_num = cursor.fetchone()["reg_num"]
        if reg_num is None:
            reg_num = 0
        reg_num = int(reg_num)
        day_data["reg_num"] = reg_num

        #print(day_data)
        for ii in range(0,i+1):
            if (ii + 1) not in valid_day: continue
            ad = dd + timedelta(days=ii)
            add_start_time = int(ad.timestamp())
            add_end_time = add_start_time+86399
            #print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(add_start_time)))
            #print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(add_end_time)))
            querySql = "SELECT round(sum(o.money/100),2) charge_sum " \
                       "FROM gc_order as o LEFT JOIN gc_user as u ON o.user_id = u.user_id " \
                       "WHERE (u.reg_time BETWEEN %d AND %d ) AND (o.create_time BETWEEN %d AND %d ) " \
                       "AND o.channel = 1 AND o.status = 1" % (day_start_time, day_end_time, day_start_time, add_end_time)
            cursor.execute(querySql)
            charge_sum = cursor.fetchone()["charge_sum"]
            if charge_sum is None:
                charge_sum = 0
            charge_sum = int(charge_sum)
            if reg_num == 0:
                ltv = 0
            else:
                ltv = round(charge_sum/reg_num,2)
            day_data["ltv_"+str(ii + 1)] = ltv

        data.append(day_data)

    workbook = xlwt.Workbook(encoding='utf-8')
    worksheet = workbook.add_sheet('sheet1')

    worksheet.write(0, 0, '日期')
    worksheet.write(0, 1, '注册数')
    worksheet.write(0, 2, '1日ltv')
    worksheet.write(0, 3, '3日ltv')
    worksheet.write(0, 4, '7日ltv')
    worksheet.write(0, 5, '15日ltv')
    worksheet.write(0, 6, '30日ltv')
    worksheet.write(0, 7, '60日ltv')
    worksheet.write(0, 8, '90日ltv')
    worksheet.write(0, 9, '120日ltv')
    worksheet.write(0, 10, '150日ltv')

    for index, value in enumerate(data):
        worksheet.write((index + 1), 0, value["date"])
        worksheet.write((index + 1), 1, value["reg_num"])
        for key in valid_day:
            key2 = "ltv_"+str(key)
            if key2 in value:
                worksheet.write((index + 1), valid_day.index(key)+2, value[key2])

    excel_dir_name = "./excel"
    if not os.path.exists(excel_dir_name):
        os.mkdir(excel_dir_name)
    file_name = '%s/reg_ltv-%s-%s.xls' % (excel_dir_name, sd.strftime("%Y-%m-%d"), ed.strftime("%Y-%m-%d"))
    #print(file_name)
    if os.path.exists(file_name):
        os.remove(file_name)
    workbook.save(file_name)

    task_end_time = time.time()

    print("Task is completed,time : %s \n" % timedelta(seconds=task_end_time-task_start_time))
    #pprint(data)