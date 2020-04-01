# -*- coding: utf-8 -*-


from config import CLASSIFIER_SRC_A_ROOT, DATA_A_TRAIN_PATH, TRAIN_LIMIT_NUM
from feature_engineering import FeatureExtractorETAa
from utils.basic_utils import TimeRecorder, save_object, load_object, \
    error_analysis, plotImp

from model import BinaryLRModel, LgbRegressionModel
from sklearn.model_selection import train_test_split

import os


class RegressionTrainer(object):
    def __init__(self):
        self.time_recorder = TimeRecorder()
        self.fea_extractor = FeatureExtractorETAa()
        self.model = None
        self.x_train = None
        self.y_train = None
        self.x_test = None
        self.y_test = None
        self.fea_preprocessor_path = None  # 根据模型决定命名

    def run(self, model_type, train_file, need_ini, limit_num):
        self.fea_preprocessor_path = CLASSIFIER_SRC_A_ROOT + "/{type}_fea_preprocess.pkl".format(type=model_type)
        self.time_recorder.tick()
        print("-" * 100 + "LOAD SAMPLES")
        # load
        self.load_sample(train_file, need_ini, limit_num)

        # train_test_split
        self.x_train, self.x_test, self.y_train, self.y_test = train_test_split(self.x_train,
                                                                                self.y_train,
                                                                                test_size=0.2,
                                                                                random_state=40)
        print("-" * 50 + "START TRAIN" + "-" * 50)

        # choose model
        if model_type == "lgb":
            self.model = LgbRegressionModel()
        else:
            print("Please give the model type supported!")

        # train
        self.time_recorder.tock("train started !")
        train_num, train_dim, train_pos, train_neg = self.model.train(self.x_train, self.y_train)
        self.time_recorder.tock("train finished !")

        # save model
        model_path = CLASSIFIER_SRC_A_ROOT + "/%s_model.pkl" % model_type
        self.model.save_model(model_path)

        # test
        y_pred = self.model.test(self.x_test, self.y_test)
        error_analysis(predict=y_pred, ground_truth_vec=self.y_test,
                       prefix_title='ETA_C test data')
        y_pred_train = self.model.test(self.x_train, self.y_train)
        error_analysis(predict=y_pred_train, ground_truth_vec=self.y_train,
                       prefix_title='ETA_C train data')
        self.time_recorder.tock("test finished !")

        # feature importance
        columns_name = self.fea_extractor.fea_transformer["dict_vector"].get_feature_names()
        plotImp(model=self.model.MODEL, X_col_name=columns_name, model_type=self.model.model_type)

    def load_sample(self, train_file, need_ini, limit_num):
        if need_ini:
            self.x_train, self.y_train = self.fea_extractor.load(train_file, limit_num)
            self.time_recorder.tock("sample loaded successfully: "
                                    + str(self.x_train.shape) + str(self.y_train.shape))

            # save x and y
            if not os.path.exists(CLASSIFIER_SRC_A_ROOT):
                os.mkdir(CLASSIFIER_SRC_A_ROOT)
            for ele in [["x_train", self.x_train], ["y_train", self.y_train]]:
                pkl_f = CLASSIFIER_SRC_A_ROOT + "/%s.pkl" % ele[0]
                save_object(ele[1], pkl_f)
            self.fea_extractor.save_fea_preprocessor(self.fea_preprocessor_path)
        else:
            tmp = [["x_train", None], ["y_train", None]]
            for ele in tmp:
                pkl_f = CLASSIFIER_SRC_A_ROOT + "/%s.pkl" % ele[0]
                ele[1] = load_object(pkl_f)
            self.x_train = tmp[0][1]
            self.y_train = tmp[1][1]
            self.fea_extractor.load_fea_preprocessor(self.fea_preprocessor_path)


if __name__ == "__main__":
    print "训练path：", DATA_A_TRAIN_PATH
    trainer = RegressionTrainer()
    trainer.run(
        model_type='lgb',
        train_file=DATA_A_TRAIN_PATH,
        need_ini=True,
        limit_num=TRAIN_LIMIT_NUM
    )
