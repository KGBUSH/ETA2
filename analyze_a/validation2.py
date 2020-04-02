# -*- coding: utf-8 -*-
"""
整个文件一起inference
用新数据（by collect_validation_data.sql）验证模型精度
"""

from config import CLASSIFIER_SRC_A_ROOT, DATA_A_VALI_PATH, TRAIN_LIMIT_NUM, SHORT_DISTANCE
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


def test(short_distance=None):
    time_recorder.tock("\n\n\nTest started ! (short_distance = %s)" % short_distance)
    data_path = DATA_A_VALI_PATH

    fea_transformer_path = os.path.join(CLASSIFIER_SRC_A_ROOT, 'lgb_fea_preprocess.pkl')
    model_path_a1 = os.path.join(CLASSIFIER_SRC_A_ROOT, 'lgb_model_a1.pkl')
    model_path_a2 = os.path.join(CLASSIFIER_SRC_A_ROOT, 'lgb_model_a2.pkl')

    model = EtaAPredictModel(
        fea_transformer_path=fea_transformer_path,
        model_path_a1=model_path_a1,
        model_path_a2=model_path_a2
    )

    x_std, gt_list = model.feature_extractor.load_for_inference(sample_file=data_path, short_distance=short_distance)
    y_predict_a1 = model.MODEL_A1.predict(x_std)
    y_predict_a1 = NormalEncoder.skewness_recover(y_predict_a1)  # 偏态校正回来
    y_predict_a2 = model.MODEL_A2.predict(x_std)
    y_predict_a2 = NormalEncoder.skewness_recover(y_predict_a2)  # 偏态校正回来
    time_recorder.tock('inference finish')

    error_analysis(predict=y_predict_a1,
                   ground_truth_vec=gt_list[:, 0],
                   prefix_title='short distance=%sm a1 离线验证：' % short_distance)
    error_analysis(predict=y_predict_a2,
                   ground_truth_vec=gt_list[:, 1],
                   prefix_title='short distance=%sm a2 离线验证：' % short_distance)


def run():
    # load_data()
    # load_data_update()
    test()
    test(short_distance=SHORT_DISTANCE)


if __name__ == '__main__':
    print "验证数据path：", DATA_A_VALI_PATH
    time_recorder = TimeRecorder()
    time_recorder.tick()
    run()
