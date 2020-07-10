#!/usr/bin/python3
#-*- coding: utf-8 -*-

import subprocess

if  __name__ == '__main__':
    popen = subprocess.Popen('git clone https://github.com/NovenHong/GoProject.git /data/htdocs/app/gittest',
                             shell=True, stdout=subprocess.PIPE)
    popen.wait()
    lines = popen.stdout.readlines()
    for line in lines:
        print(line.decode('gbk'))
    print(popen.returncode)  # 128 #0