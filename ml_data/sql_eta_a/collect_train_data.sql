-- 训练数据 ETA A段训练数据，python不会直接运行这个

set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.exec.reducers.bytes.per.reducer=180000000;
set hive.exec.parallel=true;
set hive.auto.convert.join = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.dynamic.partitions.pernode=2000;


-- 1 根据振锋的sql，晒出几条supplier的平均pickup时间，和实际接单时间相比
#define label='订单开始时间',${cal_dt1}='2020-03-15';
#define label='订单结束时间',${cal_dt2}='2020-03-22';
#define label='骑手商家直线距离最小值',${real_time_line_distance_min}=0;

drop table algo_test.dy_eta_a_train_01;
create table algo_test.dy_eta_a_train_01 as
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
              distance,  -- 骑行距离，发单的时候调了高德或者百度算的
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
                  create_dt >= $ { cal_dt1 }
                  and create_dt <= $ { cal_dt2 } --and city_id = $ { city }
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
              --city_id = $ { city }
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
          b.log_dt >= $ { cal_dt1 }
          and b.log_dt <= $ { cal_dt2 } --and b.cityid = $ { city }
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

select count(*) as c1_, count(distinct delivery_id) as cd_1
from algo_test.dy_eta_a_train_01;  --11961727




--2 捞出t->s 直线距离超过real_time_line_distance_min的订单
drop table algo_test.dy_eta_a_train_02;
create TABLE algo_test.dy_eta_a_train_02 AS
select
  *
from
  algo_test.dy_eta_a_train_01
where
  real_time_line_distance >= $ { real_time_line_distance_min };
select count(*) as c2, count(distinct delivery_id) as cd2
from algo_test.dy_eta_a_train_02;



-- 3 大合并 num=7023858, 后面直接用这个切后面几天来test
drop table algo_test.dy_eta_a_train_03;
create TABLE algo_test.dy_eta_a_train_03 AS
select
  dc.*,
  udf.get_geo_hash(supp_lng, supp_lat, 7) as s_geohash,  -- 商户geohash，下面是达达的geohash
  udf.get_geo_hash(lng, lat, 7) as t_geohash,
  t_info.history_order_num as t_history_order_num,
  t_info.avg_a1_time as t_avg_a1_time,
  t_info.avg_a2_time as t_avg_a2_time,
  s_info.history_order_num as s_history_order_num,
  s_info.avg_a1_time as s_avg_a1_time,
  s_info.avg_a2_time as s_avg_a2_time,
  B.cargo_weight
--   W.rain_fall,
--   W.air_temperature,
--   W.wind_power
from
  algo_test.dy_eta_a_train_02 as dc
  inner join algo_test.dy_transporter_history_delivery_city0_filter as t_info on dc.transporter_id = t_info.transporter_id
  and dc.city_id = t_info.city_id
  inner join algo_test.dy_supplier_history_delivery_city0_filter as s_info on dc.supplier_id = s_info.supplier_id
  and dc.city_id = s_info.city_id
  inner join bi_dw.dw_tsp_order as B on dc.delivery_id = B.order_id;
  --inner join bi_dw.dw_tsp_log_order_weather as W on dc.delivery_id = W.order_id;

select count(*) as c3, count(distinct delivery_id) as cd3
from algo_test.dy_eta_a_train_03; --5825003