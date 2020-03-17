# -*- coding: utf-8 -*-

"""

@author: zhenfengjiang

@contact: zhenfengjiang@imdada.cn

@file: utils.py

@time: 2019/8/23 14:56

@desc:

"""

import commands
import datetime
from config import PROJECT_PATH
import os


def generate_sql_file(sql_info):
    tmp_sql_path = PROJECT_PATH + '/tmp/tmp_sql'
    if not os.path.exists(tmp_sql_path):
        os.mkdir(tmp_sql_path)
    with open(tmp_sql_path, 'w') as f:
        f.write(sql_info)
    return tmp_sql_path


def load_sql_data(sql_info, data_path=None):
    """ 执行sql """
    sql_path = generate_sql_file(sql_info)
    if data_path:
        commands.getstatusoutput(
            "rm -rf {data_path}".format(data_path=data_path))
        sql_cmd = "hive -f {sql_path} >> {data_path}" \
            .format(sql_path=sql_path,
                    data_path=data_path)
    else:
        sql_cmd = "hive -f {sql_path}".format(sql_path=sql_path)
    print("notice: command is running:\n%s" % sql_cmd)
    print("notice: this sql in running:\n%s\n" % sql_info)
    commands.getstatusoutput(sql_cmd)


def get_someday(delta=0):
    today = datetime.date.today()
    deltaday = datetime.timedelta(days=delta)
    someday = today - deltaday
    return someday


def get_short_someday(delta=0):
    someday = get_someday(delta)
    return str(someday).replace("-", "")


def process_command_str(cmd_str):
    print("notice: command is running:\n{cmd}".format(cmd=cmd_str))
    commands.getstatusoutput(cmd_str)
