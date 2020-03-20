#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2016 xuekun.zhuang <zhuangxuekun@imdada.cn>
# Licensed under the Dada tech.co.ltd - http://www.imdada.cn
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import time
import random
import cPickle as pickle
import copy
import collections
import re
import pandas as pd
import seaborn as sns


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


def plotImp(model, X_col_name, num=20):
    """seaborn 画特征重要性图"""
    feature_imp = pd.DataFrame({'Value': model.feature_importances_, 'Feature': X_col_name})
    plt.figure(figsize=(8, 5))
    # sns.set(font_scale = 5)
    sns.barplot(x="Value", y="Feature", data=feature_imp.sort_values(by="Value",
                                                                     ascending=False)[0:num])
    plt.title('LightGBM {num} Features (avg over folds)'.format(
        num=X_col_name.__len__()))
    plt.tight_layout()
    plt.show()


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
