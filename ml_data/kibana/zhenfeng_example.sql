-- 解析sql中查询出来的json数据类型

SELECT
  get_json_object(t1.data, '$.order_id') as order_id,
  get_json_object(t1.data, '$.prob') as prob,
  get_json_object(
    get_json_object(t1.data, '$.dynamic_info'),  -- 嵌套的json字典
    '$.time_window_key'
  ) as time_window_key,
  get_json_object(
    get_json_object(t1.data, '$.dynamic_info'),
    '$.overtime_allowance'
  ) as overtime_allowance,
  get_json_object(
    get_json_object(t1.data, '$.dynamic_info'),
    '$.tips'
  ) as tips,
  get_json_object(
    get_json_object(t1.data, '$.dynamic_info'),
    '$.allowance'
  ) as allowance
FROM
  dada_log.dps_biz_log as t1
WHERE
  dt = "2019-09-01"
  AND biz_type = 10564