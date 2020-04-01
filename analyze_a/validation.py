# -*- coding: utf-8 -*-
"""
用新数据（by collect_validation_data.sql）验证模型精度
读一行 inference一行，非常慢；load整个测试文件用validation2.py
"""

from config import CLASSIFIER_SRC_A_ROOT, DATA_A_VALI_PATH, TRAIN_LIMIT_NUM
from feature_engineering import FeatureExtractorETAa, NormalEncoder
from utils.basic_utils import TimeRecorder, save_object, load_object, \
    error_analysis, plotImp

import numpy as np
import os
from tqdm import tqdm


class EtaAPredictModel(object):
    def __init__(self, fea_transformer_path, model_path_a1, model_path_a2):
        self.feature_extractor = FeatureExtractorETAa()
        self.feature_extractor.load_fea_preprocessor(fea_transformer_path)
        self.MODEL_A1 = load_object(model_path_a1)
        self.MODEL_A2 = load_object(model_path_a2)

    def validate_one_line(self, raw_line):
        """
        还是sql捞出来的数据，有label
        :param raw_line:
        :return: 一行： a1预测值, a1预测值, gt, 老算法a1, 老算法a2
        """
        # 加载样本并进行特征处理
        raw_line = raw_line.strip()
        fea_std_list, goal_list = self.feature_extractor.process_line(raw_line,
                                                                      is_multi_class=False,
                                                                      use_expand=False)
        x_test = self.feature_extractor.fea_transformer["dict_vector"].transform(fea_std_list)  # 也可以用fea_std_list[0],结果完全一样
        y_predict_a1 = self.MODEL_A1.predict(x_test)
        y_predict_a1 = NormalEncoder.skewness_recover(y_predict_a1)  # 偏态校正回来
        y_predict_a2 = self.MODEL_A2.predict(x_test)
        y_predict_a2 = NormalEncoder.skewness_recover(y_predict_a2)  # 偏态校正回来

        old_a1 = fea_std_list[0][self.feature_extractor.old_label_a1]
        old_a2 = fea_std_list[0][self.feature_extractor.old_label_a2]

        return y_predict_a1[0], y_predict_a2[0], goal_list[0][0], goal_list[0][1], old_a1, old_a2


def test():
    time_recorder.tock("Test started !")
    data_path = DATA_A_VALI_PATH
    pred_list = []
    gt_list = []
    old_list = []

    fea_transformer_path = os.path.join(CLASSIFIER_SRC_A_ROOT, 'lgb_fea_preprocess.pkl')
    model_path_a1 = os.path.join(CLASSIFIER_SRC_A_ROOT, 'lgb_model_a1.pkl')
    model_path_a2 = os.path.join(CLASSIFIER_SRC_A_ROOT, 'lgb_model_a2.pkl')

    model = EtaAPredictModel(
        fea_transformer_path=fea_transformer_path,
        model_path_a1=model_path_a1,
        model_path_a2=model_path_a2
    )
    with open(data_path, 'r') as f:
        data_info = f.readlines()

    cnt = 0
    err_cnt = 0
    time_recorder.tock("Model loaded !")
    for i in tqdm(range(len(data_info))):
        row_info = data_info[i]
        try:
            a1_pred, a2_pred, a1_gt, a2_gt, a1_old, a2_old = model.validate_one_line(row_info)  # 一个数
            pred_list.append([a1_pred, a2_pred])
            gt_list.append([a1_gt, a2_gt])
            old_list.append([a1_old, a2_old])
        except:
            print(cnt)
            err_cnt += 1
            pass
        cnt += 1
    time_recorder.tock('总行数=%s; 错误加载行数=%s' % (cnt, err_cnt))
    pred_list = np.array(pred_list)
    gt_list = np.array(gt_list)
    old_list = np.array(old_list)

    for i in range(1, 3):
        print("\n\n\n\n" + "-" * 50 + 'a%s inference' % i + "-" * 50)
        j = i - 1
        error_analysis(predict=old_list[:, j],
                       ground_truth_vec=gt_list[:, j],
                       prefix_title='a%s old算法：' % i)
        error_analysis(predict=pred_list[:, j],
                       ground_truth_vec=gt_list[:, j],
                       prefix_title='a%s 离线验证：' % i)



def run():
    # load_data()
    # load_data_update()
    test()


if __name__ == '__main__':
    print "验证数据path：", DATA_A_VALI_PATH
    time_recorder = TimeRecorder()
    time_recorder.tick()
    run()
