# -*- coding: utf-8 -*-

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import collections
import numpy as np
from multiprocessing import cpu_count
from config import PROJECT_PATH
from utils import save_object
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from feature_engineering import FeatureExtractor
from sklearn.metrics import accuracy_score, recall_score, roc_auc_score, \
    roc_curve, auc, precision_score


class Model(object):
    def __init__(self):
        self.MODEL = None
        self.feature_extractor = FeatureExtractor()

    def save_model(self, model_path):
        save_object(self.MODEL, model_path)


class BinaryModel(Model):
    def __init__(self):
        Model.__init__(self)

    def train(self, x_train, y_train, is_warm_start=False):
        title = "%-20s\t%-20s\t%-20s\t%-20s" % (
            "num", "dimension", "pos", "neg")
        print(title)
        fmt = "%-20d\t%-20d\t%-20d\t%-20d"
        pos_neg_stat = collections.Counter(y_train)
        sample_num, dimension, pos, neg = \
            x_train.shape[0], x_train.shape[1], pos_neg_stat[1], pos_neg_stat[
                -1]
        msg = fmt % (sample_num, dimension, pos, neg)
        print(msg)
        if is_warm_start:
            self.MODEL.partial_fit(x_train, y_train, classes=np.array([1, -1]))
        else:
            self.MODEL.fit(x_train, y_train)
        return sample_num, dimension, pos, neg

    def test(self, x_test, y_test, threshold=0.5, is_draw=False):
        print("-" * 100 + "TEST")
        title = "%-20s\t%-20s\t%-20s\t%-20s" % (
            "num", "dimension", "pos", "neg")
        print(title)
        fmt = "%-20d\t%-20d\t%-20d\t%-20d"
        pos_neg_stat = collections.Counter(y_test)
        sample_num, dimension, pos, neg = x_test.shape[0], x_test.shape[1], \
                                          pos_neg_stat[1], pos_neg_stat[-1]
        msg = fmt % (sample_num, dimension, pos, neg)
        print(msg)

        y_predict = self.MODEL.predict_proba(x_test)
        y_predict = self._change_prob_threshold(y_predict, threshold)

        title = "%-20s\t%-20s\t%-20s\t%-20s" % ("accuracy", "precision", "recall", "auc")
        print(title)
        fmt = "%-20.4f\t%-20.4f\t%-20.4f\t%-20.4f"
        y_prob = [ele[1] for ele in self.MODEL.predict_proba(x_test)]

        accuracy, precision, recall, auc = \
            accuracy_score(y_test, y_predict), \
            precision_score(y_test, y_predict), \
            recall_score(y_test, y_predict, average='binary'), \
            roc_auc_score(y_test, y_prob)
        msg = fmt % (100 * accuracy,
                     100 * precision,
                     100 * recall,
                     auc)
        print(msg)

        if is_draw:
            fpr, tpr, thresholds = roc_curve(y_test, y_prob)
            # roc_auc = auc(fpr, tpr)
            plt.plot(fpr, tpr, lw=1, label='ROC (area = %0.2f)' % auc)
            plt.plot([0, 1], [0, 1], '--', color=(0.6, 0.6, 0.6),
                     label='Lucky Line')
            plt.xlim([-0.0, 1.0])
            plt.ylim([-0.0, 1.0])
            plt.xlabel('False Positive Rate')
            plt.ylabel('True Positive Rate')
            plt.title('Order Accept Time Prediction')
            plt.legend(loc="lower right")
            plt.savefig(PROJECT_PATH + '/sklearn_model/resource/AUC.png')
            plt.cla()

        return sample_num, dimension, pos, neg, \
               accuracy, precision, recall, auc

    def predict(self, x_std):
        prob = self.MODEL.predict_proba(x_std)
        return float(prob[0][1])

    def test_prob_threshold(self, x_test, y_test, prob_threshold_file):
        f_out = open(prob_threshold_file, "wb")
        print("-" * 100 + "PROB CHECK")
        title = "%6s%10s%10s%12s%12s%11s%8s%10s" % (
            "prob", "neg_num", "pos_num", "pk_ratio", \
            "accuracy", "precison", "recall", "auc")
        print(title)
        f_out.write(title + "\n")

        t = 0.0
        for i in range(19):
            t += 0.05
            y_predict = self.MODEL.predict_proba(x_test)
            y_predict = self._change_prob_threshold(y_predict, t)
            predict_result = collections.Counter(y_predict)
            y_prob = [ele[1] for ele in self.MODEL.predict_proba(x_test)]

            line = "%6.2f%10d%10d%10.2f%%%10.2f%%%10.2f%%%10.2f%%%10.4f" % (
                t,
                predict_result[-1],  # predict_result是计数器，-1类似字典的key  -1 是没有违章的
                predict_result[1],
                100 * float(predict_result[1]) / (
                        predict_result[-1] + predict_result[1]),
                100 * accuracy_score(y_test, y_predict),
                100 * precision_score(y_test, y_predict),
                100 * recall_score(y_test, y_predict, average='binary'),
                roc_auc_score(y_test, y_prob)
            )
            print(line)
            f_out.write(line + "\n")
        f_out.close()

    def _change_prob_threshold(self, predict, threshold):
        predict_threshold = []
        for item in predict:
            if item[1] >= threshold:
                predict_threshold.append(1)
            else:
                predict_threshold.append(-1)
        return predict_threshold


class BinaryLRModel(BinaryModel):
    def __init__(self):
        Model.__init__(self)
        self.MODEL = LogisticRegression(
            C=1,
            penalty='l2',
            solver='sag',
            tol=1e-4,
            n_jobs=cpu_count()-1,
            verbose=1,
            max_iter=10000,
            class_weight={1: 0.84, -1: 0.16}
        )

    def get_fea_importance(self, dim_analysis_path, fea_preprocessor_path):
        self.feature_extractor.load_fea_preprocessor(fea_preprocessor_path)
        fea_importance_path = dim_analysis_path + ".coef"
        coef_file = open(fea_importance_path, 'w')
        for fea, coef in zip(self.feature_extractor.fea_transformer[
                                 "dict_vector"].feature_names_,
                             self.MODEL.coef_.ravel()):
            coef_file.write("%s\t%s\n" % (fea, coef))
        coef_file.close()
