# -*- coding: utf-8 -*-
"""
注：一定要确认create_sample_data(函数是否调用)！！
"""
import commands
from config import PROJECT_DATA_PATH
from utils import load_sql_data
from ml_data.sql_eta_c.load_data import GENERATE_VALI_SAMPLE_SQL, DOWNLOAD_VALI_DATA_TO_LOCAL
import os


def create_sample_data():
    """
    跑sql序列
    :return:
    """
    date_begin = '2020-02-13'
    date_end = '2020-03-13'
    sql_info = GENERATE_VALI_SAMPLE_SQL.replace('{date_begin}', date_begin) \
        .replace('{date_end}', date_end)
    load_sql_data(sql_info)


def load_sample_data_157():
    path_dir = PROJECT_DATA_PATH + "/c_vali_sample_data_dir"
    data_path = PROJECT_DATA_PATH + "/c_vali_sample_data"

    # 1 hive 拉下来的数据会先存到文件夹
    cmd = "rm -rf {path}".format(path=path_dir)
    commands.getstatusoutput(cmd)
    os.mkdir(path_dir)

    # 2 download hive data
    sql_info = DOWNLOAD_VALI_DATA_TO_LOCAL.format(
        local_path=path_dir
    )
    load_sql_data(sql_info)
    print("done: hive -f")

    # 3 del 已有文件
    cmd = "rm -rf {data_path}".format(data_path=data_path)
    commands.getstatusoutput(cmd)

    # 4 写入指定file
    cmd = "cat {data_dir}/* >> {data_path}".format(
        data_dir=path_dir,
        data_path=data_path
    )
    print("cmd run: \n %s" % cmd)
    commands.getstatusoutput(cmd)


if __name__ == '__main__':
    # create_sample_data()  # 慎用！！！会删了所有表重跑.
    load_sample_data_157()
