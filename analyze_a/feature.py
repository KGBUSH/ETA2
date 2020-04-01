# -*- coding: utf-8 -*-

# C 段的原始文件输入列名
ETA_A_COLUMNS_LIST = [
    'delivery_id',
    'transporter_id',
    'supplier_id',
    'supplier_type_id',
    'distance',
    'cargo_type_id',
    'accept_time',
    'arrive_time',
    'fetch_time',
    'finish_time',
    'a_time',
    'a1_time',
    'a2_time',
    'dada_report_time',
    'row_num',
    'supp_lat',
    'supp_lng',
    'lat',
    'lng',
    'real_time_line_distance',
    'city_id',
    's_geohash',
    't_geohash',
    't_history_order_num',
    't_avg_a1_time',
    't_avg_a2_time',
    's_history_order_num',
    's_avg_a1_time',
    's_avg_a2_time',
    'cargo_weight',
]

ETA_A_COLUMNS_DICT = dict(zip(
    ETA_A_COLUMNS_LIST,
    [i for i in range(len(ETA_A_COLUMNS_LIST))]
))
