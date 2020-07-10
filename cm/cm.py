#!/usr/bin/python
# -*- coding: utf-8 -*-
#import commands
import signal
import sys
import threading
import time

cm_status = 1


def run7():
    command = '/data/htdocs/app/cmc --offset 7'
    log_name = '/data/htdocs/app/print_log_cmc_7'
    print('Ready to start cmc, Command:%s' % command)
    time.sleep(5)
    #status, output = commands.getstatusoutput(command)
    file = open(log_name, 'w')
    #file.write('\n\n%s\n\n' % output)
    file.flush()
    file.close()
    print('Execute cmc success, Command:%s' % command)


def run10():
    command = '/data/htdocs/app/cmc --offset 10'
    log_name = '/data/htdocs/app/print_log_cmc_10'
    print('Ready to start cmc, Command:%s' % command)
    time.sleep(5)
    #status, output = commands.getstatusoutput(command)
    file = open(log_name, 'w')
    #file.write('\n\n%s\n\n' % output)
    file.flush()
    file.close()
    print('Execute cmc success, Command:%s' % command)


def handler(signum, frame):
    print('Received signal : %s' % signum)  # 10
    if signum == 10:
        global cm_status
        if cm_status == 1:
            cm_status = 7
        elif cm_status == 7:
            cm_status = 10
        elif cm_status == 10:
            cm_status = 0


def timeout():
    print('CM timeout')
    sys.exit()


if __name__ == '__main__':
    print('Start wait cmc signal')
    signal.signal(signal.SIGUSR1, handler)
    timer = threading.Timer(3600 * 3, timeout)
    timer.start()
    startTime = time.time()
    while True:
        if cm_status == 7:
            run7()
        elif cm_status == 10:
            run10()
        elif cm_status == 0:
            print("All task is done,exit")
            sys.exit()
        endTime = time.time()
        waitTime = (endTime - startTime)
        if waitTime >= 1800:
            startTime = time.time()
            print('No signal received, WaitTime: %fm, CurrentTime: %s' % (
                (waitTime / 60), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
