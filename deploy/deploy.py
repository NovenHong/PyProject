#!/usr/bin/python3
#-*- coding: utf-8 -*-

import subprocess
import os
import time

github_path = 'https://github.com/NovenHong/GoProject.git'

if os.getenv('OS') == 'Windows_NT':
    project_path = 'D://TestGit'
else:
    project_path = '/data/htdocs/app/GoProject'

if  __name__ == '__main__':
    if not os.path.exists(project_path):
        shell = 'git clone %s %s' % (github_path,project_path)
        popen = subprocess.Popen(shell,shell=True, stdout=subprocess.PIPE)
        popen.wait()
        lines = popen.stdout.readlines()
        for line in lines:
            print(line.decode('gbk'))

    print('start to pull latest file from %s' % github_path)
    shell = 'cd %s && git pull origin master' % project_path
    popen = subprocess.Popen(shell, shell=True, stdout=subprocess.PIPE)
    popen.wait()
    lines = popen.stdout.readlines()
    for line in lines:
        print(line.decode('gbk'))
    print('pull latest file success')

    root, dirs, files = os.walk(project_path).__next__()
    for d in dirs:
        if d.find('.git') == 0:
            continue
        dir_path = os.path.join(root, d)
        files = os.walk(dir_path).__next__()[2]
        can_build = False
        for f in files:
            if f.endswith(".go"):
                file_path = os.path.join(dir_path, f)
                stat = os.stat(file_path)
                if time.time() - stat.st_mtime < 60:
                    can_build = True
                    break
        if can_build:
            print('start to build %s mod' % d)
            shell = 'cd %s && set GOOS=linux&& go build %s' % (dir_path,d)
            popen = subprocess.Popen(shell, shell=True, stdout=subprocess.PIPE)
            popen.wait()
            lines = popen.stdout.readlines()
            for line in lines:
                print(line.decode('gbk'))
            if popen.returncode == 0:
                print('build %s mod success' % d)