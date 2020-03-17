# -*- coding: utf-8 -*-


GENERATE_TRAIN_SAMPLE_SQL = """

set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.exec.reducers.bytes.per.reducer=180000000;
set hive.exec.parallel=true;
set hive.auto.convert.join = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.dynamic.partitions.pernode=2000;


-- 1. 捞出order单，拿出C段，和coord合并
#define label='指定开始时间',${data_dt1}='{date_begin}';
#define label='指定结束时间',${data_dt2}='{date_end}';
#define label='进圈距离',${distance}=80;  --学坤订的50米参数
-- distinct order_id's count=51738
drop table algo_test.dy_eta_c_01;
create table algo_test.dy_eta_c_01 as
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
--       to_unixtime(CAST(fetch_time AS timestamp)) as fetch_time_unix,
      unix_timestamp(fetch_time, 'yyyy-MM-dd HH:mm:ss') as fetch_time_unix,
--       to_unixtime(CAST(finish_time AS timestamp)) as finish_time_unix,
      unix_timestamp(finish_time, 'yyyy-MM-dd HH:mm:ss') as finish_time_unix,
      (
        unix_timestamp(finish_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(fetch_time, 'yyyy-MM-dd HH:mm:ss')
      ) as BC_time,
--       (
--         to_unixtime(CAST(finish_time AS timestamp)) - to_unixtime(CAST(fetch_time AS timestamp))
--       ) as BC_time,
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
      (
        select
          a.*
        from
          bi_dw.dw_tsp_order a
        where
          create_dt >= $ { data_dt1 }
          AND create_dt <= $ { data_dt2 }
          AND order_status = 4
          AND order_source_from != 'jdMall'
          --AND city_id = 1
          and unix_timestamp(finish_time, 'yyyy-MM-dd HH:mm:ss') <> 1548950400  --select to_unixtime(CAST('2019-02-01 00:00:00' AS timestamp));
          and unix_timestamp(fetch_time, 'yyyy-MM-dd HH:mm:ss') <> 1548950400 --and distance > 0  -- order表 没有distance
          and order_status = 4 --and supplier_type_id != 5 -- 过滤C端用户, 应该要考虑C端用户
      ) tmp
  ) A
  JOIN (
    select
      user_id,
      lat as t_lat,
      lng as t_lng,
      logging_unixtime as log_time
    from
      dada_log.coord_log b  -- 大概2，30秒上报一次
    where
      b.log_dt >= $ { data_dt1 }
      AND b.log_dt <= $ { data_dt2 }
      --and cityid = 1
  ) B ON A.transporter_id = B.user_id
WHERE
  udf.get_geo_distance(
    A.r_lng,
    A.r_lat,
    B.t_lng,
    B.t_lat
  ) < $ { distance } -- 实时直线距离，一定要已经进圈了
  --AND B.log_time > A.finish_time_unix - { delta_time } -- 1 为什么要设置一个delta？
  AND B.log_time > A.fetch_time_unix -- 取货之后
  AND B.log_time <= A.finish_time_unix; --交付之前




--2. 把B.log_time > A.finish_time_unix - { delta_time }这个条件不满足的订单筛除
#define label='超时时间',${delta_time}=800; -- 进圈后超过800秒都没有交付成功，这种订单剔除,之前振锋用的1000，其实大于800秒的也不到1%
-- count = 287
drop table algo_test.dy_eta_c_02;
create table algo_test.dy_eta_c_02 as
select
  *
from
  algo_test.dy_eta_c_01
where
  row_num = 1
  and unix_timestamp(dada_report_time, 'yyyy-MM-dd HH:mm:ss') < finish_time_unix - $ { delta_time };



--3. 把刚才那些进圈后用时很久的订单剔除
drop table algo_test.dy_eta_c_03;
create table algo_test.dy_eta_c_03 as
select
  A.*
from
  algo_test.dy_eta_c_01 as A
  left outer join algo_test.dy_eta_c_02 as Invalid on (
    A.order_id = Invalid.order_id
    and A.transporter_id = Invalid.transporter_id
    and A.receiver_id = Invalid.receiver_id
  )
where
  Invalid.order_id is null;  --不要忘了加这个条件
select count(*) as c3, count(distinct(order_id)) as cd3
from algo_test.dy_eta_c_03;



--4 (重要)这里对之前的表做group by，得到交付时间和交付上报条数,
-- 1001-1014的order量是678475
drop table algo_test.dy_eta_c_04;
create table algo_test.dy_eta_c_04 as
select
  A.order_id,
  A.receiver_id,
  A.supplier_id,
  A.transporter_id,
  A.r_lng,
  A.r_lat,
  A.s_r_line_distance,
  --A.A_time,
  A.finish_time,  -- 2019-10-10 09:33:13
  A.city_id,
  -- deliver_time1: finish_time - 进圈第一条上报时间
  (unix_timestamp(A.finish_time, 'yyyy-MM-dd HH:mm:ss') - min(A.dada_report_unixtime)) as deliver_time1,
  -- deliver_time2：圈内最后一条时间-进圈第一条上报时间
  max(A.dada_report_unixtime) - min(A.dada_report_unixtime) as deliver_time2,
  -- ****其实还是用进圈出圈比较好，后面考虑做一个delivery_time3 -- 学坤说还是用finish_time
  count(1) as point_cnt,
  A.receiver_address,
  A.cargo_type_id,
  A.cargo_weight
from
  algo_test.dy_eta_c_03 as A
group by
  A.order_id,
  A.receiver_id,
  A.supplier_id,
  A.transporter_id,
  A.finish_time,
  A.r_lng,
  A.r_lat,
  A.s_r_line_distance,
  --A.A_time,
  A.receiver_address,
  A.cargo_type_id,
  A.cargo_weight,
  A.city_id;



--5.  订单绑定到POI信息
drop table algo_test.dy_eta_c_05;
create table algo_test.dy_eta_c_05 as
select
  T1.*,
  T2.poi_id,
  T2.poi_name,
  T2.distance,
  T2.poi_lat,
  T2.poi_lng,
  case
    when d_poi.poi_id is null then 0
    else 1
  end as is_hard_poi,
  case
    when d_poi.difficulty is null then 0 --下面是left outer join,这里要处理NULL
    else d_poi.difficulty
  end as difficulty
from
  (
    select
      *,
      concat(cast(round(a.r_lng, 5) as string) , '_', cast(round(a.r_lat, 5)as string)) as lng_lat --distinct 22万 -- 必须要保留5位小数
    from
      algo_test.dy_eta_c_04 a
	where point_cnt > 2 and deliver_time1 > 15
  ) T1 --66w
  inner join (
    select
      concat(cast(round(lng, 5) as string), '_', cast(round(lat, 5) as string)) as lng_lat,
      poi_name,
      poi_id,
      distance,
      poi_lat,
      -- 这个poi经纬度的小数不止5位
      poi_lng
    from
      algo_db.poi_info
    where
      --a.create_dt >= '2019-10-01'  -- 如果数据之前有了，就不会再插入数据
      --AND a.create_dt <= '2019-10-14'
      poi_name is not null
      and poi_type = 'landmark_l2' --and a.city_id = 1
  ) T2 on T1.lng_lat = T2.lng_lat
  left join algo_db.hubble_poi_difficulty_info as d_poi on d_poi.poi_id = T2.poi_id;




-- 6* api_algo_poi_statistic_Info  poi统计信息，数据库要用, poi 的count要大于等于5
drop table algo_test.dy_eta_c_06_poi_statistics;
create table algo_test.dy_eta_c_06_poi_statistics as
select
  row_number() over() as id,
  a.poi_id,
  --a.poi_name,
  percentile(int(a.deliver_time1), 0.5) as percentile_delivery_time_poi,
  -- 这个楼的历史平均交付时间，下面会做达达的交付时间
  round(avg(a.deliver_time1), 1) as avg_delivery_time_poi,
  percentile(int(a.distance), 0.5) as percentile_distance_poi,
  stddev(a.distance) as std_distance_poi,
  stddev(a.deliver_time1) as std_delivery_time_poi,
  --加入方差
  -- 这个poi周围的收货人的平均收货时间
  count(a.order_id) as order_cnt,
  --这个poi周围有多少订单
  a.city_id,
  a.poi_lat,
  a.poi_lng,
  from_unixtime(
    cast(current_timestamp() AS BIGINT),
    --当前时间
    'yyyy-MM-dd HH:mm:ss'
  ) AS create_time,
  from_unixtime(
    cast(current_timestamp() AS BIGINT),
    'yyyy-MM-dd HH:mm:ss'
  ) AS update_time
from
  algo_test.dy_eta_c_05 a
group by
  a.poi_id,
  --a.poi_name,
  --和这个poi有关的订单group
  a.city_id,
  a.poi_lat,
  a.poi_lng
having order_cnt >= 5;





--7 计算 POI 级别均值、中位数等服务时效，****放开distance的限制***
-- key 加入 poi的经纬度
-- 加入方差，transporter_id等数据
-- 加入supplier_id，s_r直线距离
drop table algo_test.dy_eta_c_07;
create table algo_test.dy_eta_c_07 as
select
  A.order_id,
  A.supplier_id,
  A.transporter_id,
  A.receiver_id,
  A.s_r_line_distance,
  A.finish_time,
  A.receiver_address,
  A.cargo_type_id,
  A.cargo_weight,
  --A.cargo_amt,
  --A.tips_amt,
  --A.allowance_amt,
  A.r_lng,
  A.r_lat,
  B.poi_id,
  --B.poi_name,
  B.poi_lat,
  B.poi_lng,
  A.is_hard_poi,
  A.difficulty,
  A.distance as receiver_poi_distance,  --当前收货人到poi距离
  B.percentile_delivery_time_poi,  --下面三个都是poi的历史数据
  B.avg_delivery_time_poi,
  B.percentile_distance_poi,
  B.std_distance_poi,
  B.std_delivery_time_poi,
  B.order_cnt,
  B.city_id,
  A.deliver_time1,
  A.deliver_time2,
  abs(B.percentile_delivery_time_poi - A.deliver_time1) as error1,  -- POI中位统计时间
  abs(B.percentile_delivery_time_poi - A.deliver_time2) as error2
from
  (
    select
      a.*
    from
      algo_test.dy_eta_c_05 a
  ) A
  join algo_test.dy_eta_c_06_poi_statistics as B on (  -- 上面做的统计表
    A.poi_id = B.poi_id
    --and A.poi_name = B.poi_name
    and A.city_id = B.city_id
    and A.poi_lat = B.poi_lat
    and A.poi_lng = B.poi_lng
  );




--8. 把transporter的信息合入进来，做到algo_db中, 最后又转存到algo_test
drop table algo_test.dy_eta_c_08;
create table algo_test.dy_eta_c_08 as
select
  bef.*,
  T.history_order_num as t_history_order_num,
  T.avg_a1_time as t_avg_a1_time,
  T.avg_a2_time as t_avg_a2_time
from
  algo_test.dy_eta_c_07 as bef
  inner join algo_test.dy_transporter_history_delivery_city0_filter as T on (
    bef.transporter_id = T.transporter_id
    and bef.city_id = T.city_id
  );




-- 9 hive 运行，对达达做peek平均交付时间统计, 又加上了各时间阶段阶段  数据库
drop table algo_test.dy_eta_c_09_peek;
create table algo_test.dy_eta_c_09_peek AS
select
  row_number() over() as id,
  -- 自增
  a.*,
  from_unixtime(
    cast(current_timestamp() AS BIGINT),
    --当前时间
    'yyyy-MM-dd HH:mm:ss'
  ) AS create_time,
  from_unixtime(
    cast(current_timestamp() AS BIGINT),
    'yyyy-MM-dd HH:mm:ss'
  ) AS update_time
from (
    select
      a.transporter_id,
      a.city_id,
      count(1) as delivery_cnt,
      --好像其他地方也有历史订单的统计，这个是交付成功（又进过圈）的样本数量
      cast(avg(deliver_time1) as int) as avg_delivery_time1,
      cast(avg(deliver_time2) as int) as avg_delivery_time2,
      percentile(deliver_time1, 0.5) as per_delivery_time1,
      percentile(deliver_time2, 0.5) as per_delivery_time2,

      count(case when peek_time = 1 then 1 end) as cnt_peek1, -- 统计高峰阶段的订单量, 午餐晚餐
      count(case when peek_time = 2 then 1 end) as cnt_peek2, -- 统计高峰之间的闲时阶段的订单量
      count(case when peek_time = 3 then 1 end) as cnt_peek3, -- 统计半夜到清晨阶段的订单量
      count(case when peek_time = 0 then 1 end) as cnt_peek0, -- 统计其他阶段的订单量

      percentile((case when peek_time = 1 then deliver_time1 end), 0.5) as per_delivery_time1_peek1,
      percentile((case when peek_time = 2 then deliver_time1 end), 0.5) as per_delivery_time1_peek2,
      percentile((case when peek_time = 3 then deliver_time1 end), 0.5) as per_delivery_time1_peek3,
      percentile((case when peek_time = 0 then deliver_time1 end), 0.5) as per_delivery_time1_peek0

    from
      (
        select
          tmp1.*,
          case
            when ((finish_hour >= 11 and finish_hour < 13) or (finish_hour >= 18 and finish_hour < 20)) then 1
            when ((finish_hour >= 9 and finish_hour < 11) or (finish_hour >= 15 and finish_hour < 17)  or (finish_hour >= 20 and finish_hour < 22)) then 2
            when (finish_hour < 9 or finish_hour >= 23) then 3  --半夜
            else 0
          end as peek_time
        from
          (
            select
              tmp.*,
              hour(finish_time) as finish_hour
            from
              algo_test.dy_eta_c_08 as tmp
          ) as tmp1
      ) a
    group by
      transporter_id,
      city_id
) a;




--10 拼接
drop table algo_test.dy_eta_c_10;
create table algo_test.dy_eta_c_10 AS
select
  a.*,
  b.delivery_cnt,
  b.avg_delivery_time1,
  b.avg_delivery_time2,
  b.per_delivery_time1,
  b.per_delivery_time2,
  b.cnt_peek1,
  b.cnt_peek2,
  b.cnt_peek3,
  b.cnt_peek0,
  b.per_delivery_time1_peek1,
  b.per_delivery_time1_peek2,
  b.per_delivery_time1_peek3,
  b.per_delivery_time1_peek0
from
  algo_test.dy_eta_c_08 as a
  inner join algo_test.dy_eta_c_09_peek as b on (
    a.transporter_id = b.transporter_id
    and a.city_id = b.city_id
  );
select
  count(distinct order_id) as cd11,
  count(*) as c11
from
  algo_test.dy_eta_c_10;
"""

DOWNLOAD_TRAIN_DATA_TO_LOCAL = """
-- 下载数据到本地指定目录
INSERT overwrite LOCAL directory "{local_path}" ROW format delimited fields 
terminated BY "\t" COLLECTION ITEMS TERMINATED BY "\n"
SELECT * FROM algo_test.dy_eta_c_10;
"""
