# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from utils.basic_utils import error_analysis


def load_csv(csv_path):
    df = pd.read_csv(csv_path)

    # 对照组
    df_blank = df.loc[df['isdowngrade'] == 1, :]
    pred = np.array(df_blank['receivertimevalue'])
    gt = np.array(df_blank['delivery_time2'])
    error_analysis(predict=pred, ground_truth_vec=gt, prefix_title='')

    # 实验组
    df_blank = df.loc[df['isdowngrade'] == 0, :]
    pred = np.array(df_blank['receivertimevalue'])
    gt = np.array(df_blank['delivery_time2'])
    error_analysis(predict=pred, ground_truth_vec=gt, prefix_title='')

    print '#' * 50
    # 对照组
    df_blank = df.loc[df['test_id'] == 'blank', :]
    pred = np.array(df_blank['receivertimevalue'])
    gt = np.array(df_blank['delivery_time2'])
    error_analysis(predict=pred, ground_truth_vec=gt, prefix_title='')

    # 实验组
    df_blank = df.loc[df['test_id'] == 'new', :]
    pred = np.array(df_blank['receivertimevalue'])
    gt = np.array(df_blank['delivery_time2'])
    error_analysis(predict=pred, ground_truth_vec=gt, prefix_title='')


if __name__ == '__main__':
    path = './csv/eta_mini_c.csv'
    load_csv(csv_path=path)
