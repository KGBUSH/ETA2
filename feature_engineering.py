# -*- coding: utf-8 -*-


import numpy as np
import pandas as pd
from tqdm import tqdm
from utils.basic_utils import load_object, save_object, error_analysis
from sklearn.feature_extraction import DictVectorizer
from analyze_c.feature import ETA_C_COLUMNS_DICT
from utils.building_re_utils import ETABuildingRecognizer

BASE_FEATURE_DICT = {
    'transporter_id': 0,  # 没有用了
    'score': 1,  # 等级分
    'grade': 2,  # 等级
    'work_dt_cnt': 3,  # 30单工作天数
    'avg_dis': 4,  # 单均配送距离
    'avg_speed': 5,  # 单均速度
    'invalid_cnt': 6,  # 违章次数
    'not_first_invalid': 7,  # 是否历史违章过
    'invalid_label': 8  # 分类标签
}


class Encoder(object):
    """ Transform feature to standard format
    """
    pass


class NormalEncoder(Encoder):
    """

    """
    MAX_MIN_CONFIG = {
        "score": (0.0, 2000.0),
        "avg_dis": (500.0, 15000),
        "avg_speed": (0.5, 4.0),
        "invalid_cnt": (0, 10),
        "work_dt_cnt": (1, 30),
        "score/avg_speed": (0, 1000),
        "grade/avg_speed": (0, 5),
        "work_dt_cnt*avg_dis": (1, 100)
    }

    def __init__(self):
        Encoder.__init__(self)

    @staticmethod
    def normalize(fea_key, fea_value):
        v_normalized = (fea_value - NormalEncoder.MAX_MIN_CONFIG[fea_key][0]) \
                       / (NormalEncoder.MAX_MIN_CONFIG[fea_key][1] -
                          NormalEncoder.MAX_MIN_CONFIG[fea_key][0])
        if v_normalized > 1:
            return 1.0
        else:
            return v_normalized

    @staticmethod
    def skewness(value_list):
        """
        偏态校正，针对右偏态(长尾) https://blog.csdn.net/liuweiyuxiang/article/details/90233203
        :param value_list: np,df,list 都可以
        :return:
        """
        return np.log1p(value_list)

    @staticmethod
    def skewness_recover(value_list):
        """
        turn back
        """
        return np.expm1(value_list)


class FeatureBase(object):
    def __init__(self):
        self.fea_transformer = {
            "dict_vector": DictVectorizer(),
            "fea_mapping": {}
        }
        self.normal_encoder = NormalEncoder()


class FeatureExtractor(FeatureBase):
    """
    """

    def __init__(self):
        FeatureBase.__init__(self)
        self.CROSS_FEATURES = {
            # "hour": "earning",
            # "label": "earning",
            # "city_id": "earning",
            # "dis_range": "earning",
            # "cargo_type_id": "earning"
        }
        self.need_norm = True

    def load(self, sample_file, limit_num=1e5):
        X = []
        Y = []
        counter = 0  # 记录源文件读了多少行，并不代表有效数据行数
        sample_f = open(sample_file, 'r')
        print("load samples from %s ... ..." % sample_file)
        for i in tqdm(range(int(limit_num))):
            line = sample_f.readline()
            line = line.strip()
            if line == '':
                break
            try:
                fea_std_list, goal_list = self.process_line(line, use_expand=False)
                for fea in fea_std_list:
                    X.append(fea)
                for goal in goal_list:
                    Y.append(goal)
                counter += 1
                if counter > limit_num:
                    break
            except Exception as e:
                print(e)
                pass

        x_std = self.fea_transformer["dict_vector"].fit_transform(X)
        y_std = np.array(Y)
        print('load data finish!')
        return x_std, y_std

    def get_fea_selected(self, items, is_multi_class=False):
        feature_selected = {
            "onehot": {
                "grade": str(items[BASE_FEATURE_DICT["grade"]]),
                "work_dt_cnt": str(items[BASE_FEATURE_DICT["work_dt_cnt"]]),
                "not_first_invalid": str(items[BASE_FEATURE_DICT["not_first_invalid"]]),  # 是否首次，这个特征很重要
            },
            "normal": {
                # "invalid_cnt": float(items[BASE_FEATURE_DICT["invalid_cnt"]]),
                "score": float(items[BASE_FEATURE_DICT["score"]]),
                "avg_dis": float(items[BASE_FEATURE_DICT["avg_dis"]]),
                "avg_speed": float(items[BASE_FEATURE_DICT["avg_speed"]]),
                # "work_dt_cnt": int(items[BASE_FEATURE_DICT["work_dt_cnt"]]),
                "score/avg_speed": float(items[BASE_FEATURE_DICT[
                    "score"]]) / float(items[BASE_FEATURE_DICT["avg_speed"]]),
                "grade/avg_speed": float(items[BASE_FEATURE_DICT[
                    "grade"]]) / float(items[BASE_FEATURE_DICT["avg_speed"]]),
                # "work_dt_cnt*avg_dis": int(items[BASE_FEATURE_DICT[
                #     "work_dt_cnt"]])*float(items[BASE_FEATURE_DICT["avg_dis"]])
            }
        }

        return feature_selected

    def get_fea_std(self, items, is_multi_class):
        feature_selected = self.get_fea_selected(items, is_multi_class)
        fea_std = self.combine_feature_groups(feature_selected, self.need_norm)
        fea_std = self.cross_fea_std(fea_std)
        return fea_std

    def cross_fea_std(self, fea_std):
        new_fea_std = {}
        for k1, v1 in self.CROSS_FEATURES.items():
            for k2, v2 in fea_std.items():
                if k1 in k2:
                    new_k = k2 + v2 + "_" + self.CROSS_FEATURES[k1]
                    new_fea_std[new_k] = fea_std[v1]
        fea_std.update(new_fea_std)
        return fea_std

    def process_line(self, line, is_multi_class=False, use_expand=True):
        fea_std_list = []
        goal_list = []
        try:
            sep = None
            if '\t' in line:
                sep = '\t'
            elif ',' in line:
                sep = ','
            else:
                print line

            items = line.strip().split(sep)
            # origin state
            fea_std = self.get_fea_std(items, is_multi_class)
            class_label = FeatureExtractor.construct_class_label(items[BASE_FEATURE_DICT["invalid_label"]],
                                                                 is_multi_class)
            if class_label != 0:
                fea_std_list.append(fea_std)
                goal_list.append(class_label)

        except Exception as e:
            # print(e)
            pass
        return fea_std_list, goal_list

    @staticmethod
    def construct_class_label(label, is_multi_class=False):
        if int(label) == 1:
            return 1
        else:
            return -1

    def combine_feature_groups(self, feature_selected, need_norm):
        fea_std = {}
        for fea_type, features in feature_selected.iteritems():
            if "normal" == fea_type:
                for k, v in features.iteritems():
                    if need_norm:
                        features[k] = self.normal_encoder.normalize(k, v)
                    else:
                        # features[k] = v  # 本来就是，不需要再赋一次值
                        pass
            if not fea_std:
                fea_std = features
            else:
                fea_std = dict(fea_std, **features)
        return fea_std

    def save_fea_preprocessor(self, fea_transformer_path):
        save_object(self.fea_transformer, fea_transformer_path)

    def load_fea_preprocessor(self, fea_transformer_path):
        self.fea_transformer = load_object(fea_transformer_path)


class FeatureExtractorETAc(FeatureExtractor):
    """
    ETA C 段
    """

    def __init__(self):
        FeatureExtractor.__init__(self)
        self.need_norm = False
        self.label_choose = 'delivery_time2'
        self.old_label = 'percentile_delivery_time_poi'
        self.invalid_field_replace = -1

    # def evaluate_old(self):
    #     old = [item[self.old_label] for item in ]

    def load(self, sample_file, limit_num=1e5):
        X = []
        Y = []
        counter = 0  # 记录源文件读了多少行，并不代表有效数据行数
        with open(sample_file, 'r') as fr:
            lines_num = len(fr.readlines())
        if limit_num == -1:
            limit_num = lines_num

        sample_f = open(sample_file, 'r')
        print("load samples from %s ... ..." % sample_file)
        for i in tqdm(range(int(limit_num))):
            line = sample_f.readline()
            line = line.strip()
            if line == '':
                break
            try:
                fea_std_list, goal_list = self.process_line(line, use_expand=False)
                for fea in fea_std_list:
                    X.append(fea)
                for goal in goal_list:
                    Y.append(goal)
                counter += 1
                if counter > limit_num:
                    break
            except Exception as e:
                print(e)
                pass

        x_std = self.fea_transformer["dict_vector"].fit_transform(X)
        final_features = self.fea_transformer["dict_vector"].get_feature_names()
        print("final features = %s: " % len(final_features), final_features)
        y_std = np.array(Y)
        print('load data finish!')

        # 评估老算法
        old = pd.DataFrame(X).loc[:, self.old_label]
        error_analysis(predict=old, ground_truth_vec=y_std, prefix_title='old_C')
        return x_std, y_std

    def process_line(self, line, is_multi_class=False, use_expand=True):
        fea_std_list = []
        goal_list = []
        try:
            sep = None
            if '\t' in line:
                sep = '\t'
            elif ',' in line:
                sep = ','
            else:
                print line

            line = line.replace('NULL', '-1').replace('Null', '-1').replace('None', '-1')

            items = line.strip().split(sep)

            # origin state
            fea_std = self.get_fea_std(items, is_multi_class)
            label = float(items[ETA_C_COLUMNS_DICT[self.label_choose]])

            # 交付时间大于0
            if label > 0:
                fea_std_list.append(fea_std)
                goal_list.append(label)

        except Exception as e:
            # print(e)
            pass
        if fea_std_list.__len__() == 0:
            pass
        return fea_std_list, goal_list

    def get_fea_selected(self, items, is_multi_class=False):
        """
        ETA C段的特征提取
        :param items:
        :param is_multi_class:
        :return:
        """
        # items 里面已有的先填入
        feature_selected = {
            "onehot": {
                "cargo_type_id": str(items[ETA_C_COLUMNS_DICT["cargo_type_id"]]),
                "city_id": str(items[ETA_C_COLUMNS_DICT["city_id"]]),
            },
            "normal": {
                "cargo_weight": float(items[ETA_C_COLUMNS_DICT["cargo_weight"]]),
                "percentile_delivery_time_poi": float(items[ETA_C_COLUMNS_DICT["percentile_delivery_time_poi"]]),
                "avg_delivery_time_poi": float(items[ETA_C_COLUMNS_DICT["avg_delivery_time_poi"]]),
                "percentile_distance_poi": float(items[ETA_C_COLUMNS_DICT["percentile_distance_poi"]]),
                "std_distance_poi": float(items[ETA_C_COLUMNS_DICT["std_distance_poi"]]),
                "std_delivery_time_poi": float(items[ETA_C_COLUMNS_DICT["std_delivery_time_poi"]]),
                "order_cnt": float(items[ETA_C_COLUMNS_DICT["order_cnt"]]),
                "t_history_order_num": float(items[ETA_C_COLUMNS_DICT["t_history_order_num"]]),
                "t_avg_a1_time": float(items[ETA_C_COLUMNS_DICT["t_avg_a1_time"]]),
                "t_avg_a2_time": float(items[ETA_C_COLUMNS_DICT["t_avg_a2_time"]]),
                "delivery_cnt": float(items[ETA_C_COLUMNS_DICT["delivery_cnt"]]),

                # "avg_delivery_time1": float(items[ETA_C_COLUMNS_DICT["avg_delivery_time1"]]),
                "avg_delivery_time2": float(items[ETA_C_COLUMNS_DICT["avg_delivery_time2"]]),
                # "per_delivery_time1": float(items[ETA_C_COLUMNS_DICT["per_delivery_time1"]]),
                "per_delivery_time2": float(items[ETA_C_COLUMNS_DICT["per_delivery_time2"]]),
                "cnt_peek1": float(items[ETA_C_COLUMNS_DICT["cnt_peek1"]]),
                "cnt_peek2": float(items[ETA_C_COLUMNS_DICT["cnt_peek2"]]),
                "cnt_peek3": float(items[ETA_C_COLUMNS_DICT["cnt_peek3"]]),
                "cnt_peek0": float(items[ETA_C_COLUMNS_DICT["cnt_peek0"]]),
                "per_delivery_time_peek1": float(items[ETA_C_COLUMNS_DICT["per_delivery_time_peek1"]]),
                "per_delivery_time_peek2": float(items[ETA_C_COLUMNS_DICT["per_delivery_time_peek2"]]),
                "per_delivery_time_peek3": float(items[ETA_C_COLUMNS_DICT["per_delivery_time_peek3"]]),
                "per_delivery_time_peek0": float(items[ETA_C_COLUMNS_DICT["per_delivery_time_peek0"]]),
            }
        }

        # 一些需要加工的特征
        feature_selected = FeatureExtractorETAc.add_other_basic_features(feature_selected, items)
        feature_selected = FeatureExtractorETAc.add_current_features(feature_selected)

        return feature_selected

    @staticmethod
    def add_other_basic_features(feature_selected, items):
        """
        添加其他加工之后的特征
        """
        # 时间地址
        hour = pd.Timestamp(items[ETA_C_COLUMNS_DICT["finish_time"]], unit='s', tz='Asia/Shanghai').hour
        # weekday = pd.Timestamp(items[ETA_C_COLUMNS_DICT["finish_time"]], unit='s', tz='Asia/Shanghai').dayofweek
        receiver_address_char_num = items[ETA_C_COLUMNS_DICT["receiver_address"]].__len__()
        feature_selected['onehot']['hour'] = str(hour)  # one-hot必须要是str类型
        feature_selected['normal']['receiver_address_char_num'] = receiver_address_char_num

        # # 几楼  (这个特征没啥用了，由于疫情)
        # build = ETABuildingRecognizer()
        # floor = build.get_building_floor(items[ETA_C_COLUMNS_DICT["receiver_address"]])
        # is_floor_over6 = 1 if floor > 6 else 0
        # feature_selected['normal']['floor'] = floor
        # feature_selected['normal']['is_floor_over6'] = is_floor_over6

        return feature_selected

    @staticmethod
    def add_current_features(feature_selected):
        """
        current 那两个特征
        """
        now_hour = feature_selected['onehot']['hour']
        current_cnt_peek = -1
        current_per_delivery_time_peek = -1
        if (11 <= now_hour < 13) or (18 <= now_hour < 20):
            current_cnt_peek = feature_selected['normal']['cnt_peek1']
            current_per_delivery_time_peek = feature_selected['normal']['per_delivery_time_peek1']
        elif (9 <= now_hour < 11) or (15 <= now_hour < 17) or (20 <= now_hour < 22):
            current_cnt_peek = feature_selected['normal']['cnt_peek2']
            current_per_delivery_time_peek = feature_selected['normal']['per_delivery_time_peek2']
        elif (now_hour < 9) or (23 <= now_hour):
            current_cnt_peek = feature_selected['normal']['cnt_peek3']
            current_per_delivery_time_peek = feature_selected['normal']['per_delivery_time_peek3']
        else:
            current_cnt_peek = feature_selected['normal']['cnt_peek0']
            current_per_delivery_time_peek = feature_selected['normal']['per_delivery_time_peek0']

        feature_selected['normal']['current_cnt_peek'] = current_cnt_peek
        feature_selected['normal']['current_per_delivery_time_peek'] = current_per_delivery_time_peek
        return feature_selected
