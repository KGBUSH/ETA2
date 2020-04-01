# -*- coding: utf-8 -*-
"""
从hive 上下载数据

92062条数据
hive -e 'SELECT * FROM algo_test.dy_eta_c_vali_10;' > /data/dengyang/projects/dataFromEsql/eta_c_0323to0325_at0325.csv



"""

from .default import *

try:
    from .local import *
except ImportError:
    pass

import os

PROJECT_PATH = os.path.abspath(
    os.path.join(os.path.abspath(os.path.dirname(__file__)), os.pardir))

# 训练模型存储位置
CLASSIFIER_SRC_A_ROOT = PROJECT_PATH + "/analyze_a/resource"
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

# dada's mean speed in each city
ETA_DADA_SPEED_CITY_GROUP = {
    0: 4.5,
    1: 4.670976602114164,
    2: 4.817198349879338,
    3: 4.800101683689998,
    4: 4.544530281653771,
    5: 4.9441774258246785,
    6: 4.165272254210252,
    7: 4.799496577336238,
    8: 4.8642904208256645,
    9: 4.650193526194646,
    10: 3.8875846679394064,
    11: 3.7497491469750037,
    12: 4.639689977352436,
    13: 4.459546125852144,
    14: 4.416226698802068,
    15: 4.6933662707989035,
    16: 4.3487566984616794,
    17: 4.409211415510911,
    18: 4.839935651192298,
    19: 5.089973101249108,
    21: 5.016223577352671,
    22: 4.828889865141648,
    23: 4.560015036509587,
    24: 4.480501706783588,
    25: 4.837992099615244,
    26: 4.679076176423293,
    28: 4.924391784667969,
    29: 4.4657377279721775,
    30: 4.456049809089074,
    31: 5.324681043624878,
    32: 5.011953353881836,
    35: 5.349959538533137,
    37: 4.849944591522217,
    38: 3.9762484385417056,
    39: 4.0739441101367655,
    76: 4.2549366198088,
    77: 4.153659022771395,
    135: 4.220209901983088,
    180: 4.808286813589243
}
