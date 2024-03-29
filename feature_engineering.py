# -*- coding: utf-8 -*-


import numpy as np
import pandas as pd
from tqdm import tqdm
from sklearn.feature_extraction import DictVectorizer

from utils.basic_utils import load_object, save_object, error_analysis
from analyze_c.feature import ETA_C_COLUMNS_DICT
from analyze_a.feature import ETA_A_COLUMNS_DICT
from utils.building_re_utils import ETABuildingRecognizer
import config

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
            "dict_vector": DictVectorizer(),  # inference的时候即便缺少特征也可以.transform(X)，特征
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
        self.invalid_field_replace = '-1'

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
        self.old_label_c = 'percentile_delivery_time_poi'

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
        old = pd.DataFrame(X).loc[:, self.old_label_c]
        error_analysis(predict=old, ground_truth_vec=y_std, prefix_title='old_C')
        return x_std, y_std

    def load_for_inference(self, sample_file):
        """
        * 推理阶段用这个函数（整个测试文件加载进来）
        加载文件内容，用已有的特征transformer转换成稀疏编码
        如果传入short_distance，则该load函数只输出short_dist以内的样本
        return: x_std -> scipy.sparse.csr.csr_matrix
                y_std -> numpy.ndarray
        """
        # 1. 按行load文件
        X = []
        Y = []
        counter = 0  # 记录源文件读了多少行，并不代表有效数据行数
        with open(sample_file, 'r') as fr:
            lines_num = len(fr.readlines())

        sample_f = open(sample_file, 'r')
        print("load samples from %s ... ..." % sample_file)
        for i in tqdm(range(int(lines_num))):
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
            except Exception as e:
                print(e)
                pass

        y_std = np.array(Y)
        print('load data finish!')

        # 2. 评估老算法
        X = pd.DataFrame(X)
        all_old_c = X.loc[:, self.old_label_c]
        error_analysis(predict=all_old_c, ground_truth_vec=y_std, prefix_title='old_C')

        # 3. 稀疏存储 X
        x_std = self.fea_transformer["dict_vector"].transform(X.to_dict(orient='records'))  # pandas 转dict再转稀疏矩阵
        final_features = self.fea_transformer["dict_vector"].get_feature_names()
        print("final features = %s: " % len(final_features))

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

            line = line.replace('NULL', self.invalid_field_replace) \
                .replace('Null', self.invalid_field_replace) \
                .replace('None', self.invalid_field_replace)
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

                "delivery_cnt": int(float(items[ETA_C_COLUMNS_DICT["delivery_cnt"]])),
                # "avg_delivery_time1": int(float(items[ETA_C_COLUMNS_DICT["avg_delivery_time1"]])),
                "avg_delivery_time2": int(float(items[ETA_C_COLUMNS_DICT["avg_delivery_time2"]])),
                # "per_delivery_time1": int(float(items[ETA_C_COLUMNS_DICT["per_delivery_time1"]])),
                "per_delivery_time2": int(float(items[ETA_C_COLUMNS_DICT["per_delivery_time2"]])),
                "cnt_peek1": int(float(items[ETA_C_COLUMNS_DICT["cnt_peek1"]])),
                "cnt_peek2": int(float(items[ETA_C_COLUMNS_DICT["cnt_peek2"]])),
                "cnt_peek3": int(float(items[ETA_C_COLUMNS_DICT["cnt_peek3"]])),
                "cnt_peek0": int(float(items[ETA_C_COLUMNS_DICT["cnt_peek0"]])),
                "per_delivery_time_peek1": int(float(items[ETA_C_COLUMNS_DICT["per_delivery_time_peek1"]])),
                "per_delivery_time_peek2": int(float(items[ETA_C_COLUMNS_DICT["per_delivery_time_peek2"]])),
                "per_delivery_time_peek3": int(float(items[ETA_C_COLUMNS_DICT["per_delivery_time_peek3"]])),
                "per_delivery_time_peek0": int(float(items[ETA_C_COLUMNS_DICT["per_delivery_time_peek0"]])),
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
        hour = pd.Timestamp(items[ETA_C_COLUMNS_DICT["finish_time"]]).hour
        # weekday = pd.Timestamp(items[ETA_C_COLUMNS_DICT["finish_time"]], unit='s', tz='Asia/Shanghai').dayofweek
        receiver_address_char_num = unicode(items[ETA_C_COLUMNS_DICT["receiver_address"]], 'utf-8').__len__()
        feature_selected['onehot']['hour'] = str(hour)  # one-hot必须要是str类型
        # feature_selected['normal']['receiver_address_char_num'] = receiver_address_char_num

        # # 几楼  (这个特征没啥用了，由于新冠疫情)
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
        now_hour = int(feature_selected['onehot']['hour'])  # 装的是str
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


class FeatureExtractorETAa(FeatureExtractor):
    """
    ETA A 段
    包含 a1, a2
    """
    dada_speed_map = config.ETA_DADA_SPEED_CITY_GROUP
    default_dada_speed = dada_speed_map.get(0, 4.5)

    def __init__(self):
        # FeatureExtractor.__init__(self)
        super(FeatureExtractorETAa, self).__init__()

        self.need_norm = False
        self.label_choose_a1 = 'a1_time'
        self.label_choose_a2 = 'a2_time'
        self.old_label_a1 = 'old_a1'
        self.old_label_a2 = 'old_a2'

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
        print("final features = %s: " % len(final_features))
        y_std = np.array(Y)
        print('load data finish!')

        # 评估老算法
        all_old_a1 = pd.DataFrame(X).loc[:, self.old_label_a1]
        all_old_a2 = pd.DataFrame(X).loc[:, self.old_label_a2]
        error_analysis(predict=all_old_a1, ground_truth_vec=y_std[:, 0], prefix_title='old_A:a1')
        error_analysis(predict=all_old_a2, ground_truth_vec=y_std[:, 1], prefix_title='old_A:a2')

        return x_std, y_std

    def load_for_inference(self, sample_file, short_distance=None):
        """
        * 推理阶段用这个函数（整个测试文件加载进来）
        加载文件内容，用已有的特征transformer转换成稀疏编码
        如果传入short_distance，则该load函数只输出short_dist以内的样本
        return: x_std -> scipy.sparse.csr.csr_matrix
                y_std -> numpy.ndarray
        """
        # 1. 按行load文件
        X = []
        Y = []
        counter = 0  # 记录源文件读了多少行，并不代表有效数据行数
        with open(sample_file, 'r') as fr:
            lines_num = len(fr.readlines())

        sample_f = open(sample_file, 'r')
        print("load samples from %s ... ..." % sample_file)
        for i in tqdm(range(int(lines_num))):
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
            except Exception as e:
                print(e)
                pass

        y_std = np.array(Y)
        print('load data finish!')

        # 2. 评估老算法
        # 2.1. 汇总测试
        X = pd.DataFrame(X)
        all_old_a1 = X.loc[:, self.old_label_a1]
        all_old_a2 = X.loc[:, self.old_label_a2]
        error_analysis(predict=all_old_a1, ground_truth_vec=y_std[:, 0], prefix_title='old_A:a1')
        error_analysis(predict=all_old_a2, ground_truth_vec=y_std[:, 1], prefix_title='old_A:a2')

        # 2.2. 短距离测试
        if short_distance:
            short_dist = short_distance if short_distance is not None else 100000
            index = X.real_time_line_distance < short_dist
            error_analysis(predict=X.loc[index, self.old_label_a1], ground_truth_vec=y_std[index, 0],
                           prefix_title='short distance=%sm old_A:a1' % short_dist)
            error_analysis(predict=X.loc[index, self.old_label_a2], ground_truth_vec=y_std[index, 1],
                           prefix_title='short distance=%sm old_A:a2' % short_dist)

        # 3. 稀疏存储 X
        index = range(X.shape[0])
        if short_distance:
            index = X.real_time_line_distance <= short_distance
            X = X.loc[index, :].reset_index(drop=True)
            y_std = y_std[index, :]
        x_std = self.fea_transformer["dict_vector"].transform(X.to_dict(orient='records'))  # pandas 转dict再转稀疏矩阵
        final_features = self.fea_transformer["dict_vector"].get_feature_names()
        print("final features = %s: " % len(final_features))

        return x_std, y_std

    def process_line(self, line, is_multi_class=False, use_expand=True):
        """
        return: fea_std_list, goal_list(a1 和a2的标签都在)
        """
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

            line = line.replace('NULL', self.invalid_field_replace) \
                .replace('Null', self.invalid_field_replace) \
                .replace('None', self.invalid_field_replace)

            items = line.strip().split(sep)

            # origin state
            fea_std = self.get_fea_std(items, is_multi_class)
            a1_label = float(items[ETA_A_COLUMNS_DICT[self.label_choose_a1]])
            a2_label = float(items[ETA_A_COLUMNS_DICT[self.label_choose_a2]])

            # 交付时间大于0
            if a1_label > 0 and a2_label > 0:
                fea_std_list.append(fea_std)
                goal_list.append([a1_label, a2_label])

        except Exception as e:
            # print(e)
            pass
        if fea_std_list.__len__() == 0:
            aa = 1
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
                "cargo_type_id": str(items[ETA_A_COLUMNS_DICT["cargo_type_id"]]),
                "city_id": str(items[ETA_A_COLUMNS_DICT["city_id"]]),
                # "supplier_id": str(items[ETA_A_COLUMNS_DICT["supplier_id"]]),  # 没啥鸟用
            },
            "normal": {
                "real_time_line_distance": float(items[ETA_A_COLUMNS_DICT["real_time_line_distance"]]),
                "t_history_order_num": float(items[ETA_A_COLUMNS_DICT["t_history_order_num"]]),
                "t_avg_a1_time": float(items[ETA_A_COLUMNS_DICT["t_avg_a1_time"]]),
                "t_avg_a2_time": float(items[ETA_A_COLUMNS_DICT["t_avg_a2_time"]]),
                "s_history_order_num": float(items[ETA_A_COLUMNS_DICT["s_history_order_num"]]),
                "s_avg_a1_time": float(items[ETA_A_COLUMNS_DICT["s_avg_a1_time"]]),
                "s_avg_a2_time": float(items[ETA_A_COLUMNS_DICT["s_avg_a2_time"]]),
                "cargo_weight": float(items[ETA_A_COLUMNS_DICT["cargo_weight"]]),
            }
        }

        # 一些需要加工的特征
        feature_selected = FeatureExtractorETAa.add_other_basic_features(feature_selected, items)
        feature_selected = FeatureExtractorETAa.add_old_results_as_features(feature_selected)

        return feature_selected

    @staticmethod
    def add_other_basic_features(feature_selected, items):
        """
        添加其他加工之后的特征
        """
        # 时间
        hour = pd.Timestamp(items[ETA_A_COLUMNS_DICT["finish_time"]]).hour
        weekday = pd.Timestamp(items[ETA_A_COLUMNS_DICT["finish_time"]]).dayofweek
        is_weekend = 1 if weekday > 4 else 0
        feature_selected['onehot']['hour'] = str(hour)  # one-hot必须要是str类型
        feature_selected['onehot']['weekday'] = str(weekday)
        feature_selected['onehot']['is_weekend'] = str(is_weekend)

        real_time_line_distance = feature_selected['normal']['real_time_line_distance']
        feature_selected['onehot']['is_over200m'] = '1' if real_time_line_distance > 200 else '0'
        feature_selected['onehot']['is_over20m'] = '1' if real_time_line_distance > 20 else '0'

        return feature_selected

    @staticmethod
    def add_old_results_as_features(feature_selected):
        """
        添加老算法结果特征
        """
        # old a1: 直线距离 / speed
        real_time_line_distance = feature_selected['normal']['real_time_line_distance']
        city_id = feature_selected['onehot']['city_id']
        dada_speed = FeatureExtractorETAa.dada_speed_map.get(int(city_id),
                                                             FeatureExtractorETAa.default_dada_speed)
        old_a1 = real_time_line_distance / dada_speed
        feature_selected['normal']['old_a1'] = old_a1

        # old a2: 商户的平均取货时间
        feature_selected['normal']['old_a2'] = feature_selected['normal']['s_avg_a2_time']

        return feature_selected
