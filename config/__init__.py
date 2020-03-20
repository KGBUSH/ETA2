# -*- coding: utf-8 -*-


from .default import *

try:
    from .local import *
except ImportError:
    pass

import os

PROJECT_PATH = os.path.abspath(
    os.path.join(os.path.abspath(os.path.dirname(__file__)), os.pardir))

# 训练模型存储位置
CLASSIFIER_SRC_C_ROOT = PROJECT_PATH + "/analyze_c/resource"

BASE_FEATURE_ETA_C_DICT = {
    # 'transporter_id': 0,  # 没有用了
    # 'score': 1,  # 等级分
    # 'grade': 2,  # 等级
    # 'work_dt_cnt': 3,  # 30单工作天数
    # 'avg_dis': 4,  # 单均配送距离
    # 'avg_speed': 5,  # 单均速度
    # 'invalid_cnt': 6,  # 违章次数
    # 'not_first_invalid': 7,  # 是否历史违章过
    # 'invalid_label': 8  # 分类标签
}
