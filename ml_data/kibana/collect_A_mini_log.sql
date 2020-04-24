-- 1
-- A段捞日志
DROP TABLE algo_test.dy_eta_a_mini_01;
CREATE TABLE algo_test.dy_eta_a_mini_01 AS
SELECT
  get_json_object(t1.data, '$.recOrderId') as recOrderId,
  get_json_object(t1.data, '$.orderId') as orderId,
  --get_json_object(t1.data, '$.pickupTimeValue') as pickupTimeValue,  -- 这里是str类型
  CAST(
    get_json_object(t1.data, '$.pickupTimeValue') as int
  ) as pickupTimeValue,
  get_json_object(t1.data, '$.isDowngrade') as isDowngrade,
  get_json_object(t1.data, '$.test_id') as test_id,
  get_json_object(t1.data, '$.cityId') as cityId,
  get_json_object(t1.data, '$.transporterId') as transporterId,
  get_json_object(t1.data, '$.supplierLat') as supplierLat,
  get_json_object(t1.data, '$.now_timestamp') as now_timestamp
FROM
  dada_log.saaty_biz_log as t1
WHERE
  dt = '${data_dt}'  -- 默认是前一天
  AND biz_type = 10604;


-- 2
-- 捞出真实的数据，其实就是dy_eta_a_vali_01表
drop table algo_test.dy_eta_a_mini_02;
create table algo_test.dy_eta_a_mini_02 as
select
  *
from
  (
    select
      deli.delivery_id,
      deli.transporter_id,
      deli.supplier_id,
      deli.supplier_type_id,
      deli.distance,  -- s->r, huangyingxuan说这个是骑行距离
      deli.cargo_type_id,
      deli.accept_time,
      deli.arrive_time,
      deli.fetch_time,
      deli.finish_time,
      (
        unix_timestamp(fetch_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(accept_time, 'yyyy-MM-dd HH:mm:ss')
      ) as A_time,
      (
        unix_timestamp(arrive_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(accept_time, 'yyyy-MM-dd HH:mm:ss')
      ) as A1_time,
      (
        unix_timestamp(fetch_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(arrive_time, 'yyyy-MM-dd HH:mm:ss')
      ) as A2_time,
      from_unixtime(coord.log_time, 'yyyy-MM-dd HH:mm:ss') as dada_report_time,
      row_number() over (
        partition by deli.delivery_id
        order by
          coord.log_time asc
      ) as row_num,
      --商户和达达的位置, 后面调map API
      deli.supp_lat,
      deli.supp_lng,
      coord.lat,
      coord.lng,
      -- 实时直线距离
      udf.get_geo_distance(
        deli.supp_lng,
        deli.supp_lat,
        coord.lng,
        coord.lat
      ) as real_time_line_distance,
      deli.city_id
    from
      (
        select
          a.*,
          b.supplier_lat as supp_lat,
          b.supplier_lng as supp_lng,
          b.supplier_type_id
        from
          (
            -- 运单表，只拿少数样本
            select
              delivery_id,
              transporter_id,
              supplier_id,
              distance,
              cargo_type_id,
              accept_time,
              unix_timestamp(accept_time, 'yyyy-MM-dd HH:mm:ss') as accept_time_unix,
              arrive_time,
              unix_timestamp(arrive_time, 'yyyy-MM-dd HH:mm:ss') as arrive_time_unix,
              fetch_time,
              unix_timestamp(fetch_time, 'yyyy-MM-dd HH:mm:ss') as fetch_time_unix,
              finish_time,
              unix_timestamp(finish_time, 'yyyy-MM-dd HH:mm:ss') as finish_time_unix,
              city_id
            from
              (
                select
                  *
                from
                  bi_dw.dw_tsp_delivery_order
                where
                  create_dt >= date_sub('${data_dt}', 1)
                  and create_dt <= '${data_dt}'
                  and unix_timestamp(arrive_time, 'yyyy-MM-dd HH:mm:ss') <> -62170185600
                  and unix_timestamp(fetch_time, 'yyyy-MM-dd HH:mm:ss') <> -62170185600
                  and distance > 0
                  and delivery_status = 4
                  and supplier_type_id != 5 -- 过滤C端用户
              ) tmp
          ) a
          inner join(
            -- 商家表，主要是位置信息
            select
              supplier_id,
              supplier_lat,
              supplier_lng,
              supplier_type_id
            from
              bi_dw.dw_usr_supplier
            where
              supplier_type_id != 5 -- 过滤C端用户
              and supplier_lat > 0
              and supplier_lng > 0
          ) b ON a.supplier_id = b.supplier_id
      ) deli
      inner join (
        select
          user_id,
          lat,
          lng,
          logging_unixtime as log_time
        from
          dada_log.coord_log b
        where
          b.log_dt >= date_sub('${data_dt}', 1)
          and b.log_dt <= '${data_dt}'
          and lat > 0
          and lng > 0
      ) coord ON coord.user_id = deli.transporter_id
    WHERE
      deli.accept_time_unix <= coord.log_time
      and deli.fetch_time_unix >= coord.log_time -- 只考虑取货阶段
  ) all
where
  row_num = 1 --只拿初始接单的那条数据（记录接单时，到店的距离）
  and A1_time <= 1000
  and A2_time <=1200
  and real_time_line_distance < 5000
  and 6 * A1_time > real_time_line_distance;  --过滤掉加速都连直线距离都到不了的



-- 3
-- join
drop table algo_test.dy_eta_a_mini_03;
create table algo_test.dy_eta_a_mini_03 as
select
  recorderid,
  orderid,
  pickuptimevalue,
  isdowngrade,
  test_id cityid,
  transporterid,
  supplier_id,
  supplier_type_id,
  cargo_type_id,
  accept_time,
  arrive_time fetch_time,
  real_time_line_distance,
  a_time,
  a1_time,
  a2_time city_id
from
  algo_test.dy_eta_a_mini_01 as t1
  inner join algo_test.dy_eta_a_mini_02 as t2 on (
    t1.orderId = t2.delivery_id
    and t1.transporterId = t2.transporter_id
  );

