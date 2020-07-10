#!/usr/bin/python3
# -*- coding: utf-8 -*-
import configparser
import os
import subprocess
from tkinter import *
from tkinter import ttk, messagebox
import mysql.connector
from pprint import pprint
from mysql.connector import MySQLConnection

config = configparser.ConfigParser()

config.read("config.ini")
server1_config = config.items("server1")
server2_config = config.items("server2")

root = Tk()

root.title('MYSQL Synchronize')
root.iconbitmap('./sync_72px.ico')

Label(root, text="Server1").grid(row=0, columnspan=2)
Label(root, text="host").grid(row=1, sticky=W, padx=5)
Label(root, text="user").grid(row=2, sticky=W, padx=5)
Label(root, text="password").grid(row=3, sticky=W, padx=5)

server1_host_var = StringVar()
server1_user_var = StringVar()
server1_password_var = StringVar()
server1_host_var.set(server1_config[0][1])
server1_user_var.set(server1_config[1][1])
server1_password_var.set(server1_config[2][1])

host1 = Entry(root, textvariable=server1_host_var)
user1 = Entry(root, textvariable=server1_user_var)
password1 = Entry(root, show='*', textvariable=server1_password_var)
host1.grid(row=1, column=1, pady=5)
user1.grid(row=2, column=1, pady=5)
password1.grid(row=3, column=1, pady=5)

# 间隔
Label(root, text="").grid(row=0, column=2, padx=10)

Label(root, text="Server2").grid(row=0, column=3, columnspan=2)
Label(root, text="host").grid(row=1, column=3, sticky=W, padx=5)
Label(root, text="user").grid(row=2, column=3, sticky=W, padx=5)
Label(root, text="password").grid(row=3, column=3, sticky=W, padx=5)

server2_host_var = StringVar()
server2_user_var = StringVar()
server2_password_var = StringVar()
server2_host_var.set(server2_config[0][1])
server2_user_var.set(server2_config[1][1])
server2_password_var.set(server2_config[2][1])

host2 = Entry(root, textvariable=server2_host_var)
user2 = Entry(root, textvariable=server2_user_var)
password2 = Entry(root, show='*', textvariable=server2_password_var)
host2.grid(row=1, column=4, pady=5)
user2.grid(row=2, column=4, pady=5)
password2.grid(row=3, column=4, pady=5)

server1_host = ""
server1_user = ""
server1_password = ""
server1_is_connected = False
server1_is_table_selected = False
server1_is_database_selected = False
server1_database = ""
server1_table = ""
conn1: MySQLConnection


def server1_connect():
    global server1_host, server1_user, server1_password, server1_is_connected
    server1_host = host1.get()
    server1_user = user1.get()
    server1_password = password1.get()
    try:
        global conn1
        conn1 = mysql.connector.connect(**{
            'user': server1_user,
            'password': server1_password,
            'host': server1_host,
            'database': 'mysql',
            'charset': 'utf8',
            "connection_timeout": 60,
            "use_pure": True
        })

    except Exception as e:
        server1_text_var.set("connect fail")
        server1_message.configure(fg="red")
        messagebox.showinfo('Server1', e)
    else:
        server1_text_var.set("connect success")
        server1_message.configure(fg="green")
        cursor = conn1.cursor(dictionary=True)
        cursor.execute("show databases")
        r = cursor.fetchall()
        server1_combo1["value"] = [i['Database'] for i in r]
        server1_is_connected = True


Button(root, text="连接", command=server1_connect).grid(row=4, column=0, sticky=W, padx=5)
server1_text_var = StringVar()
server1_message = Message(root, textvariable=server1_text_var, width=100)
server1_message.grid(row=4, column=1, sticky=W)

server2_host = ""
server2_user = ""
server2_password = ""
server2_is_connected = False
server2_is_table_selected = False
server2_is_database_selected = False
server2_database = ""
server2_table = ""
conn2: MySQLConnection


def server2_connect():
    global server2_host, server2_user, server2_password, server2_is_connected
    server2_host = host2.get()
    server2_user = user2.get()
    server2_password = password2.get()
    try:
        global conn2
        conn2 = mysql.connector.connect(**{
            'user': server2_user,
            'password': server2_password,
            'host': server2_host,
            'database': 'mysql',
            'charset': 'utf8',
            "connection_timeout": 60,
            "use_pure": True
        })

    except Exception as e:
        server2_text_var.set("connect fail")
        server2_message.configure(fg="red")
        messagebox.showinfo('Server2', e)
    else:
        server2_text_var.set("connect success")
        server2_message.configure(fg="green")
        cursor = conn2.cursor(dictionary=True)
        cursor.execute("show databases")
        r = cursor.fetchall()
        server2_combo1["value"] = [i['Database'] for i in r]
        server2_is_connected = True


Button(root, text="连接", command=server2_connect).grid(row=4, column=3, sticky=W, padx=5)
server2_text_var = StringVar()
server2_message = Message(root, textvariable=server2_text_var, width=100)
server2_message.grid(row=4, column=4, sticky=W)


def server1_combo1_select(e):
    database = server1_combo1.get()
    global conn1, server1_database, server1_is_database_selected
    conn1.ping(True, 10, 3)
    cursor = conn1.cursor(dictionary=True)
    cursor.execute("use %s" % database)
    cursor.execute("show tables")
    r = cursor.fetchall()
    server1_combo2["value"] = [i["Tables_in_%s" % database] for i in r]
    server1_database = database
    server1_is_database_selected = True


Label(root, text="数据库").grid(row=5, column=0, padx=5, sticky=W)
server1_combo1 = ttk.Combobox(root)
server1_combo1.grid(row=5, column=1)
server1_combo1.bind("<<ComboboxSelected>>", server1_combo1_select)


def server1_combo2_select(e):
    global server1_is_table_selected, server1_table
    server1_is_table_selected = True
    server1_table = server1_combo2.get()


Label(root, text="数据表").grid(row=6, column=0, padx=5, sticky=W)
server1_combo2 = ttk.Combobox(root)
server1_combo2.grid(row=6, column=1)
server1_combo2.bind("<<ComboboxSelected>>", server1_combo2_select)


def server2_combo1_select(e):
    database = server2_combo1.get()
    global conn2, server2_database, server2_is_database_selected
    conn2.ping(True, 10, 3)
    cursor = conn2.cursor(dictionary=True)
    cursor.execute("use %s" % database)
    cursor.execute("show tables")
    r = cursor.fetchall()
    server2_combo2["value"] = [i["Tables_in_%s" % database] for i in r]
    server2_database = database
    server2_is_database_selected = True


Label(root, text="数据库").grid(row=5, column=3, padx=5, sticky=W)
server2_combo1 = ttk.Combobox(root)
server2_combo1.grid(row=5, column=4)
server2_combo1.bind("<<ComboboxSelected>>", server2_combo1_select)

def server2_combo2_select(e):
    global server2_is_table_selected, server2_table
    server2_is_table_selected = True
    server2_table = server2_combo2.get()


Label(root, text="数据表").grid(row=6, column=3, padx=5, sticky=W)
server2_combo2 = ttk.Combobox(root)
server2_combo2.grid(row=6, column=4)
server2_combo2.bind("<<ComboboxSelected>>", server2_combo2_select)

text = Text(root, width=60, height=15)
text.grid(row=7, columnspan=5, pady=10)

frame = Frame(root)
frame.grid(row=8, columnspan=5)

def compare():
    if not server1_is_connected:
        return messagebox.showinfo('Server1', "server1 is unconnected")
    if not server2_is_connected:
        return messagebox.showinfo('Server2', "server2 is unconnected")
    if not server1_is_table_selected:
        return messagebox.showinfo('Server1', "server1 is nerver selected a table")
    if not server2_is_table_selected:
        return messagebox.showinfo('Server2', "server2 is nerver selected a table")

    shell = "mysqldiff.exe --server1=%s:%s@%s --server2=%s:%s@%s " \
    "%s.%s:%s.%s --changes-for=server2 -d sql --skip-table-options -q" % (
        server1_user,server1_password,server1_host,server2_user,server2_password,server2_host,
        server1_database,server1_table,server2_database,server2_table
    )
    #shell = "mysqldiff.exe --server1=root:@127.0.0.1 --server2=cj655:game123456@117.27.139.18 cj655.gc_table_structure:cj655.gc_table_structure --changes-for=server2 -d sql --skip-table-options -q"
    popen = subprocess.Popen(shell, shell=True, stdout=subprocess.PIPE)
    popen.wait()
    lines = popen.stdout.readlines()
    text.delete('0.0', 'end')
    for index, line in enumerate(lines):
        if index < 3:
            continue
        text.insert(INSERT, line.decode('utf-8'))

Button(frame, text="开始比较", command=compare).grid(row=0, column=0, padx=10)

def sync():
    query_sql = text.get('0.0', 'end').strip()
    if query_sql == "" or not server1_is_connected or not server2_is_connected:
        return messagebox.showinfo('System', "no action")
    global conn2, server2_database
    conn2.ping(True, 10, 3)
    conn2.cmd_query("use %s" % server2_database)
    try:
        conn2.cmd_query(query_sql)
    except Exception as e:
        messagebox.showinfo('System', e)
    else:
        messagebox.showinfo('System', "synchronize success")

Button(frame, text="开始同步", command=sync).grid(row=0, column=1, padx=10)

def create():
    if not server1_is_connected:
        return messagebox.showinfo('Server1', "server1 is unconnected")
    if not server2_is_connected:
        return messagebox.showinfo('Server2', "server2 is unconnected")
    if not server1_is_table_selected:
        return messagebox.showinfo('Server1', "server1 is nerver selected a table")
    if not server2_is_database_selected:
        return messagebox.showinfo('Server2', "server2 is nerver selected a database")

    global conn1,server1_table
    conn1.ping(True, 10, 3)
    cursor = conn1.cursor(dictionary=True)
    cursor.execute("show create table %s" % server1_table)
    r = cursor.fetchone()
    query_sql = r['Create Table']
    text.delete('0.0', 'end')
    text.insert(INSERT, query_sql)

Button(frame, text="生成创表", command=create).grid(row=0, column=2, padx=10)

def save():
    config.set("server1", "host", server1_host_var.get())
    config.set("server1", "user", server1_user_var.get())
    config.set("server1", "password", server1_password_var.get())
    config.set("server2", "host", server2_host_var.get())
    config.set("server2", "user", server2_user_var.get())
    config.set("server2", "password", server2_password_var.get())
    file_write = open("config.ini", "w")
    config.write(file_write)
    file_write.close()

Button(frame, text="保存信息", command=save).grid(row=0, column=3, padx=10)

def center_window(w, h):
    # 获取屏幕 宽、高
    ws = root.winfo_screenwidth()
    hs = root.winfo_screenheight()
    # 计算 x, y 位置
    x = (ws / 2) - (w / 2)
    y = (hs / 2) - (h / 2)
    root.geometry('%dx%d+%d+%d' % (w, h, x, y))


center_window(510, 450)

# root.rowconfigure(0, weight=1)
# root.columnconfigure (0, weight=1)

root.resizable(0, 0)

root.mainloop()
