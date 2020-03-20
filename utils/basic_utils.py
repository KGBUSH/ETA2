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
import pandas as pd
import seaborn as sns
import numpy as np


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


def error_analysis(predict, ground_truth_vec=None, prefix_title=''):
    """
    直方图统计误差
    :param predict: 预测值：np一维向量
    :param ground_truth_vec: np 统计绝对误差绝对误差率：abs(预测耗时 - 真实耗时)/真实耗时，举例：真实耗时=10min，预测耗时=15min，则绝对误差=5min，绝对误差率=5min/10min， 为50%。
    :param prefix_title:
    :return:
    """
    error_vec = np.abs(predict - ground_truth_vec)
    e50 = round(np.percentile(error_vec, 50), 1)
    e67 = round(np.percentile(error_vec, 67), 1)
    e80 = round(np.percentile(error_vec, 80), 1)
    e95 = round(np.percentile(error_vec, 95), 1)

    # plt.hist(error_vec, 50)
    title = str(error_vec.shape) + "{prefix_title} {e50}@50%, {e67}@67%, {e80}@80%, {e95}@95%".format(
        prefix_title=prefix_title,
        e50=e50,
        e67=e67,
        e80=e80,
        e95=e95
    )
    if ground_truth_vec is None:
        print(title)
        # plt.title(title)
        # plt.show()
    else:
        mape = np.float32(error_vec) / ground_truth_vec
        bigger = mape.mean()  # 平均百分误差率
        bigger30 = (mape > 0.3).sum() / float(mape.shape[0])  # 预估时间的绝对误差率超过30%的订单占比量
        bigger50 = (mape > 0.5).sum() / float(mape.shape[0])  # 预估时间的绝对误差率超过50%的订单占比量
        bigger70 = (mape > 0.7).sum() / float(mape.shape[0])
        print title, \
            ';  误差率>30%:', "%.1f%%" % (bigger30 * 100), \
            ', 误差率>50%:', "%.1f%%" % (bigger50 * 100), \
            ', 误差率>70%:', "%.1f%%" % (bigger70 * 100), \
            ';  平均误差(s)=', int(error_vec.mean()), ', 平均百分误差率%=', "%.1f%%" % (bigger * 100)


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
