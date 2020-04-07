# -*- coding: utf-8 -*-

"""
整个文件一起inference
用新数据（by collect_validation_data.sql）验证模型精度

通过全局变量传入 SHORT_DISTANCE，测试短距离的精度
"""

from config import CLASSIFIER_SRC_C_ROOT, DATA_C_VALI_PATH, TRAIN_LIMIT_NUM, SHORT_DISTANCE
from feature_engineering import FeatureExtractorETAc, NormalEncoder
from utils.basic_utils import TimeRecorder, save_object, load_object, \
    error_analysis, plotImp

import numpy as np
import os
from tqdm import tqdm


class EtaCPredictModel(object):
    def __init__(self, fea_transformer_path, model_path_c):
        self.feature_extractor = FeatureExtractorETAc()
        self.feature_extractor.load_fea_preprocessor(fea_transformer_path)
        self.MODEL_C = load_object(model_path_c)


def test(short_distance=None):
    time_recorder.tock("Test started ! (short_distance = %s)" % short_distance)
    data_path = DATA_C_VALI_PATH

    fea_transformer_path = os.path.join(CLASSIFIER_SRC_C_ROOT, 'lgb_fea_preprocess.pkl')
    model_path_c = os.path.join(CLASSIFIER_SRC_C_ROOT, 'lgb_model.pkl')

    model = EtaCPredictModel(
        fea_transformer_path=fea_transformer_path,
        model_path_c=model_path_c,
    )

    x_std, gt_list = model.feature_extractor.load_for_inference(sample_file=data_path)
    y_predict_c = model.MODEL_C.predict(x_std)
    y_predict_c = NormalEncoder.skewness_recover(y_predict_c)  # 偏态校正回来

    time_recorder.tock('inference finish')

    error_analysis(predict=y_predict_c,
                   ground_truth_vec=gt_list,
                   prefix_title='C 离线验证：')


def run():
    # load_data()
    # load_data_update()
    test()


if __name__ == '__main__':
    print "验证数据path：", DATA_C_VALI_PATH
    time_recorder = TimeRecorder()
    time_recorder.tick()
    run()
