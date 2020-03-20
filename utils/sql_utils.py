# -*- coding: utf-8 -*-


import commands
import datetime
from config import PROJECT_PATH
import os


def generate_sql_file(sql_info):
    """
    把要执行的sql写到文件
    return 文件地址
    """
    tmp_sql_path = PROJECT_PATH + '/tmp/tmp_sql'
    if not os.path.exists(PROJECT_PATH + '/tmp'):
        os.mkdir(PROJECT_PATH + '/tmp')
    with open(tmp_sql_path, 'w') as f:
        f.write(sql_info)
    return tmp_sql_path


def load_sql_data(sql_info, data_path=None):
    """
    执行sql
    sql_info： sql语句
    """
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
