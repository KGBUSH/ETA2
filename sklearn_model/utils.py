#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2016 xuekun.zhuang <zhuangxuekun@imdada.cn>
# Licensed under the Dada tech.co.ltd - http://www.imdada.cn
import time
import random
import cPickle as pickle
import copy
import collections
import re


def save_object(obj, file_path):
    f = file(file_path, 'wb')
    pickle.dump(obj, f, True)
    f.close()
    print("%s saved successfully!" % file_path)


def load_object(file_path):
    f = file(file_path, 'rb')
    obj = pickle.load(f)
    print("%s loaded successfully!" % file_path)
    f.close()
    return obj


def get_median(data):
    data = copy.deepcopy(data)
    data.sort()
    half = len(data) // 2
    return (data[half] + data[~half]) / 2


class TimeRecorder(object):
    def __init__(self):
        self.tick_time = 0

    def tick(self):
        self.tick_time = time.time()

    def tock(self, process_name):
        now = time.time()
        elpased = now - self.tick_time
        self.tick_time = now
        print("[%s] used time : %8.2fs" % (process_name, elpased))


def sample_balance(X, Y):
    X_index = []
    Y_new = []
    pos_neg_num = collections.Counter(Y)
    sample_type, ratio = get_under_sample_ratio(pos_neg_num[1], pos_neg_num[-1])
    if sample_type == 0:
        return X, Y
    else:
        for index, y in enumerate(Y):
            if y == sample_type:
                if random.random() <= ratio:
                    X_index.append(index)
                    Y_new.append(y)
            else:
                X_index.append(index)
                Y_new.append(y)
    return X[X_index, :], Y_new


def get_under_sample_ratio(pos, neg):
    if abs(float(pos - neg)) / (pos + neg) < 0.05:
        return 0, -1
    else:
        if pos >= neg:
            ratio = float(neg) / pos
            return 1, ratio
        else:
            ratio = float(pos) / neg
            return -1, ratio


class BuildingRecognizer(object):
    PATTERN_RECOGNIZE_BASEMENT = re.compile(ur'-\d楼|-\d层|-\d[fF]')
    PATTERN_RECOGNIZE = re.compile(ur'\d+楼|\d{3,4}$|\d{3,4} |\d{3,4}室|\d{3,4}房|\d+层|\d+[fF]')
    REPLACE_RECOGNIZE = re.compile(ur'楼|室|层|房|[A-Za-z]| ')
    CHINESE_NUM_RECOGNIZE = re.compile(ur'[一二三四五六七八九十]+')
    MINUS_NUM_RECOGNIZE = re.compile(ur'[Bb负]')

    PATTERN_RECOGNIZE_BUILDING = re.compile(ur'\d+号楼|\d+栋|\d+幢')
    REPLACE_RECOGNIZE_BUILDING = re.compile(ur'号楼|栋|幢')

    def get_building_floor(self, address):
        # 将汉语的数字转换成阿拉伯数字
        address = self.CHINESE_NUM_RECOGNIZE.sub(cn2dig, address)
        address = self.MINUS_NUM_RECOGNIZE.sub('-', address)
        res_basement = self.PATTERN_RECOGNIZE_BASEMENT.findall(address)
        res = self.PATTERN_RECOGNIZE.findall(address)
        res = res_basement + res
        if res:
            floor = self.REPLACE_RECOGNIZE.sub("", res[0])
            if len(floor) > 2:
                return int(floor[0:-2])
            else:
                return int(floor)
        else:
            return 0

    def get_building_num(self, address):
        """获取楼号"""
        address = self.CHINESE_NUM_RECOGNIZE.sub(cn2dig, address)
        address = self.MINUS_NUM_RECOGNIZE.sub('-', address)
        building = self.PATTERN_RECOGNIZE_BUILDING.findall(address)
        result_building = -999
        if building:
            result_building = self.REPLACE_RECOGNIZE_BUILDING.sub("", building[0])
            result_building = int(result_building)
        return result_building


class ETABuildingRecognizer(BuildingRecognizer):
    """
    加了几号楼提取
    """
    PATTERN_RECOGNIZE = re.compile(ur'\d+楼|\d{3,4}$|\d{3,4} |\d{3,4}室|\d{3,4}房|\d+层|\d+[A-Za-z]')


if __name__ == '__main__':
    import numpy as np
    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d import Axes3D

    # Create Map
    cm = plt.get_cmap("RdYlGn")

    x = np.random.rand(30)
    y = np.random.rand(30)
    z = np.random.rand(30)
    # col = [cm(float(i)/(29)) for i in xrange(29)] # BAD!!!
    col = [cm(float(i) / (30)) for i in xrange(30)]

    # 2D Plot
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.scatter(x, y, s=10, c=col, marker='o')

    # 3D Plot
    fig = plt.figure()
    ax3D = fig.add_subplot(111, projection='3d')
    ax3D.scatter(x, y, z, s=10, c=col, marker='o')

    plt.show()
