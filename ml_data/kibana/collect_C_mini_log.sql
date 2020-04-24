-- 1
-- C 段捞日志
DROP TABLE algo_test.dy_eta_c_mini_01;
CREATE TABLE algo_test.dy_eta_c_mini_01 AS
SELECT
  get_json_object(t1.data, '$.recOrderId') as recOrderId,
  get_json_object(t1.data, '$.orderId') as orderId,
  --get_json_object(t1.data, '$.pickupTimeValue') as pickupTimeValue,  -- 这里是str类型
  CAST(
    get_json_object(t1.data, '$.receiverTimeValue') as int
  ) as receiverTimeValue,
  get_json_object(t1.data, '$.isDowngrade') as isDowngrade,
  get_json_object(t1.data, '$.test_id') as test_id,
  get_json_object(t1.data, '$.cityId') as cityId,
  get_json_object(t1.data, '$.transporterId') as transporterId,
--   get_json_object(t1.data, '$.supplierLat') as supplierLat,
  get_json_object(t1.data, '$.now_timestamp') as now_timestamp
FROM
  dada_log.saaty_biz_log as t1
WHERE
  dt = '${data_dt}'  -- 默认是前一天
  AND biz_type = 10610;


-- 2
-- 捞出真实的数据，

-- 1. 捞出order单，拿出C段，和coord合并
-- distinct order_id's count=51738
drop table algo_test.dy_eta_c_vali_mini_01;
create table algo_test.dy_eta_c_vali_mini_01 as
select
  A.*,
  B.t_lat,
  B.t_lng,
  from_unixtime(B.log_time, 'yyyy-MM-dd HH:mm:ss') as dada_report_time,
  --   format_datetime(from_unixtime(B.log_time),'yyyy-MM-dd HH:mm:ss') as dada_report_time,
  B.log_time as dada_report_unixtime,
  row_number() over (
    partition by A.order_id
    order by
      B.log_time asc
  ) as row_num
from
  (
    select
      order_id,
      transporter_id,
      supplier_id,
      receiver_id,
      unix_timestamp(fetch_time, 'yyyy-MM-dd HH:mm:ss') as fetch_time_unix,
      unix_timestamp(finish_time, 'yyyy-MM-dd HH:mm:ss') as finish_time_unix,
      (
        unix_timestamp(finish_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(fetch_time, 'yyyy-MM-dd HH:mm:ss')
      ) as BC_time,
      fetch_time,
      finish_time,
      receiver_lat as r_lat,
      receiver_lng as r_lng,
      udf.get_geo_distance(
        receiver_lng,
        receiver_lat,
        supplier_lng,
        supplier_lat
      ) as s_r_line_distance,
      receiver_address,
      cargo_type_id,
      cargo_weight,
      cargo_amt,
      tips_amt,
      allowance_amt,
      city_id,
      create_dt
    from
      bi_dw.dw_tsp_order a
    where
      create_dt >= date_sub('${data_dt}', 1)
      AND create_dt <= '${data_dt}'
      AND order_status = 4
      AND order_source_from != 'jdMall'
      and order_status = 4
  ) A
  JOIN (
    select
      user_id,
      lat as t_lat,
      lng as t_lng,
      logging_unixtime as log_time
    from
      dada_log.coord_log b -- 大概2，30秒上报一次
    where
      b.log_dt >= date_sub('${data_dt}', 1)
      and b.log_dt <= '${data_dt}'
  ) B ON A.transporter_id = B.user_id
WHERE
  udf.get_geo_distance(
    A.r_lng,
    A.r_lat,
    B.t_lng,
    B.t_lat
  ) < 80 -- 实时直线距离，一定要已经进圈了, mini中必须写死
  AND B.log_time > A.fetch_time_unix -- 取货之后
  AND B.log_time <= A.finish_time_unix + 120; --交付之前！！！！所以就没办法，直接去掉这个条件，就可以用进圈出圈了




--2. 把B.log_time > A.finish_time_unix - { delta_time }这个条件不满足的订单筛除
-- count = 287
drop table algo_test.dy_eta_c_vali_mini_02;
create table algo_test.dy_eta_c_vali_mini_02 as
select
  *
from
  algo_test.dy_eta_c_vali_mini_01
where
  row_num = 1
  and unix_timestamp(dada_report_time, 'yyyy-MM-dd HH:mm:ss') < finish_time_unix - 800;  --第一条上报时间开始到finish_time超过800秒，mini中需要写死



--3. 把刚才那些进圈后用时很久的订单剔除
drop table algo_test.dy_eta_c_vali_mini_03;
create table algo_test.dy_eta_c_vali_mini_03 as
select
  A.*
from
  algo_test.dy_eta_c_vali_mini_01 as A
  left outer join algo_test.dy_eta_c_vali_mini_02 as Invalid on (
    A.order_id = Invalid.order_id
    and A.transporter_id = Invalid.transporter_id
    and A.receiver_id = Invalid.receiver_id
  )
where
  Invalid.order_id is null;  --不要忘了加这个条件




--4 (重要)这里对之前的表做group by，得到交付时间和交付上报条数,
-- 1001-1014的order量是678475
drop table algo_test.dy_eta_c_vali_mini_04;
create table algo_test.dy_eta_c_vali_mini_04 as
select
  *
from
  (
    select
      order_id,
      receiver_id,
      supplier_id,
      transporter_id,
      r_lng,
      r_lat,
      s_r_line_distance,
      finish_time,
      -- 2019-10-10 09:33:13
      city_id,
      -- delivery_time1: finish_time - 进圈第一条上报时间
      (
        unix_timestamp(finish_time, 'yyyy-MM-dd HH:mm:ss') - min(dada_report_unixtime)
      ) as delivery_time1,
      -- delivery_time2：圈内最后一条时间-进圈第一条上报时间
      max(dada_report_unixtime) - min(dada_report_unixtime) as delivery_time2,
      -- ****其实还是用进圈出圈比较好，后面考虑做一个delivery_time3 -- 学坤说还是用finish_time
      count(1) as point_cnt,
      -- 进了交付圈之后上报几个点
      receiver_address,
      cargo_type_id,
      cargo_weight
    from
      algo_test.dy_eta_c_vali_mini_03
    group by
      order_id,
      receiver_id,
      supplier_id,
      transporter_id,
      finish_time,
      r_lng,
      r_lat,
      s_r_line_distance,
      receiver_address,
      cargo_type_id,
      cargo_weight,
      city_id
  ) a
where
  point_cnt > 1
  and delivery_time1 > 0
  and delivery_time2 > delivery_time1;




-- 3
-- join
drop table algo_test.dy_eta_c_mini_03;
create table algo_test.dy_eta_c_mini_03 as
select
  t1.*,
  t2.delivery_time2,
  t2.delivery_time1,
  t2.receiver_address,
  t2.cargo_type_id,
  t2.cargo_weight
from
  algo_test.dy_eta_c_mini_01 as t1
  inner join algo_test.dy_eta_c_vali_mini_04 as t2 on (
    t1.orderId = t2.order_id
    and t1.transporterId = t2.transporter_id
  );
