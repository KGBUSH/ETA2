# -*- coding: utf-8 -*-


from config import CLASSIFIER_SRC_ROOT, PROJECT_PATH, PROJECT_DATA_PATH
from feature_engineering import FeatureExtractorETAc
from utils import TimeRecorder, save_object, load_object
from model import BinaryLRModel
from sklearn.model_selection import train_test_split


class ClassifierTrainer(object):
    def __init__(self):
        self.time_recorder = TimeRecorder()
        self.fea_extractor = FeatureExtractorETAc()

        self.model = None
        self.x_train = None
        self.y_train = None
        self.x_test = None
        self.y_test = None

    def run(self, model_type, train_file, need_ini, limit_num):

        self.fea_preprocessor_path = CLASSIFIER_SRC_ROOT + "/{type}_fea_preprocess.pkl".format(type=model_type)
        self.time_recorder.tick()
        print("-" * 100 + "LOAD SAMPLES")
        # load
        self.load_sample(train_file, need_ini, limit_num)

        # cut
        self.x_train, self.x_test, self.y_train, self.y_test = \
            train_test_split(self.x_train,
                             self.y_train,
                             test_size=0.2,
                             random_state=40)

        print("-" * 50 + "START TRAIN" + "-" * 50)
        # choose model
        if model_type == "lr":
            self.model = BinaryLRModel()
        else:
            print("Please give the model type supported!")

        # train
        self.time_recorder.tock("train started !")
        train_num, train_dim, train_pos, train_neg = \
            self.model.train(self.x_train, self.y_train)
        self.time_recorder.tock("train finished !")

        # save model
        model_path = CLASSIFIER_SRC_ROOT + "/%s_model.pkl" % model_type
        self.model.save_model(model_path)

        # test
        test_num, test_dim, test_pos, test_neg, \
        accuracy, precision, recall, auc = self.model.test(
            self.x_test,
            self.y_test,
            is_draw=True)
        self.time_recorder.tock("test finished !")

        # check output
        self.model.test_prob_threshold(
            self.x_test,
            self.y_test,
            CLASSIFIER_SRC_ROOT + "/{type}_test_threshold.txt".format(
                type=model_type))
        self.model.get_fea_importance(
            CLASSIFIER_SRC_ROOT + "/%s" % model_type,
            self.fea_preprocessor_path)

    def load_sample(self, train_file, need_ini, limit_num):
        if need_ini:
            self.x_train, self.y_train = self.fea_extractor.load(train_file,
                                                                 limit_num)
            # self.x_train = self.x_train.toarray()
            self.time_recorder.tock("sample loaded successfully !")
            for ele in [["x_train", self.x_train], ["y_train", self.y_train]]:
                pkl_f = CLASSIFIER_SRC_ROOT + "/%s.pkl" % ele[0]
                save_object(ele[1], pkl_f)
            self.fea_extractor.save_fea_preprocessor(self.fea_preprocessor_path)
        else:
            tmp = [["x_train", None], ["y_train", None]]
            for ele in tmp:
                pkl_f = CLASSIFIER_SRC_ROOT + "/%s.pkl" % ele[0]
                ele[1] = load_object(pkl_f)
            self.x_train = tmp[0][1]
            self.y_train = tmp[1][1]
            self.fea_extractor.load_fea_preprocessor(self.fea_preprocessor_path)


if __name__ == "__main__":
    trainer = ClassifierTrainer()
    trainer.run(
        model_type='lr',
        train_file=PROJECT_DATA_PATH + '/risk_wenchen_0226_shanghai_from2019.csv',
        need_ini=True,
        limit_num=20000
    )
