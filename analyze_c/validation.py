# -*- coding: utf-8 -*-
"""
用新数据（by collect_validation_data.sql）验证模型精度
"""

from config import CLASSIFIER_SRC_C_ROOT, DATA_C_VALI_PATH, TRAIN_LIMIT_NUM
from feature_engineering import FeatureExtractorETAc, NormalEncoder
from utils.basic_utils import TimeRecorder, save_object, load_object, \
    error_analysis, plotImp

import numpy as np
import os


class EtaCPredictModel(object):
    def __init__(self, fea_transformer_path, model_path):
        self.feature_extractor = FeatureExtractorETAc()
        self.feature_extractor.load_fea_preprocessor(fea_transformer_path)
        self.MODEL = load_object(model_path)

    def validate_one_line(self, raw_line):
        """
        还是sql捞出来的数据，有label
        :param raw_line:
        :return: 只有一行： 预测值, gt, 老算法的值
        """
        # 加载样本并进行特征处理
        raw_line = raw_line.strip()
        fea_std_list, goal_list = self.feature_extractor.process_line(raw_line,
                                                                      is_multi_class=False,
                                                                      use_expand=False)
        old = fea_std_list[0][self.feature_extractor.old_label]
        x_test = self.feature_extractor.fea_transformer["dict_vector"].transform(fea_std_list)
        y_predict = self.MODEL.predict(x_test)
        y_predict = NormalEncoder.skewness_recover(y_predict)  # 偏态校正回来
        return y_predict[0], goal_list[0], old


def test():
    pred_list = []
    gt_list = []
    old_list = []
    fea_transformer_path = os.path.join(CLASSIFIER_SRC_C_ROOT, 'lgb_fea_preprocess.pkl')
    model_path = os.path.join(CLASSIFIER_SRC_C_ROOT, 'lgb_model.pkl')
    model = EtaCPredictModel(
        fea_transformer_path=fea_transformer_path,
        model_path=model_path
    )
    data_path = DATA_C_VALI_PATH
    with open(data_path, 'r') as f:
        data_info = f.readlines()

    cnt = 0
    err_cnt = 0
    for row_info in data_info:
        try:
            y_predict, gt, old = model.validate_one_line(row_info)  # 一个数
            pred_list.append(y_predict)
            gt_list.append(gt)
            old_list.append(old)
        except:
            print(cnt)
            err_cnt += 1
            pass
        cnt += 1
    print('总行数=%s; 错误行数=%s' % (cnt, err_cnt))
    error_analysis(predict=np.array(old_list),
                   ground_truth_vec=np.array(gt_list),
                   prefix_title='old算法：')
    error_analysis(predict=np.array(pred_list),
                   ground_truth_vec=np.array(gt_list),
                   prefix_title='离线验证：')


def run():
    # load_data()
    # load_data_update()
    test()


if __name__ == '__main__':
    run()
