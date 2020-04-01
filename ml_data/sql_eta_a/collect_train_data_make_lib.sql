-- ETA C段训练数据，python不会直接运行这个
-- 主要用于生成那两个库


set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.exec.reducers.bytes.per.reducer=180000000;
set hive.exec.parallel=true;
set hive.auto.convert.join = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.dynamic.partitions.pernode=2000;



-- 1 运单表,时间久，主要用来做历史统计用
#define label='统计开始时间',${dt1}='2019-12-01';  --之前是半年，现在也用半年，去统计商家和骑手的历史数据
#define label='统计结束时间',${dt2}='2020-03-15';
--#define label='city',${city_id}=1;
drop table algo_test.dy_order_city0;
create TABLE algo_test.dy_order_city0 AS
select
  delivery_id,
  supplier_id,
  transporter_id,
  accept_time,
  city_id,
  (
    unix_timestamp(fetch_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(arrive_time, 'yyyy-MM-dd HH:mm:ss')
  ) as A2_time,
  (
    unix_timestamp(arrive_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(accept_time, 'yyyy-MM-dd HH:mm:ss')
  ) as A1_time,
   (
    unix_timestamp(fetch_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(accept_time, 'yyyy-MM-dd HH:mm:ss')
  ) as A_time
from
  bi_dw.dw_tsp_delivery_order
where
  create_dt >= $ { dt1 }
  and create_dt <= $ { dt2 } --and city_id = $ { city_id }
  and unix_timestamp(arrive_time, 'yyyy-MM-dd HH:mm:ss') > 1556621020
  and unix_timestamp(fetch_time, 'yyyy-MM-dd HH:mm:ss') > 1556621020
  and delivery_source_from != 'jdMall' -- 可以不要这个条件，因为jdMall的arrive_time是无效值
  and distance > 0
  and delivery_status = 4
  and supplier_type_id != 5;



-- 2
--商家，历史接单数
drop table algo_test.dy_supplier_history_delivery_city0;
create TABLE algo_test.dy_supplier_history_delivery_city0 AS
select
  row_number() over() as id,  -- 自增
  supplier_id,
  count(*) as history_order_num,
  avg(A1_time) as avg_a1_time,
  avg(A2_time) as avg_a2_time,
  city_id,
  from_unixtime(
    cast(current_timestamp() as BIGINT),  --当前时间
    'yyyy-MM-dd HH:mm:ss'
  ) as create_time,
  from_unixtime(
    cast(current_timestamp() as BIGINT),
    'yyyy-MM-dd HH:mm:ss'
  ) as update_time
from
  algo_test.dy_order_city0
group by
  supplier_id,
  city_id;
select count(*) as c3_s, count(distinct supplier_id) as cd3_s
from algo_test.dy_supplier_history_delivery_city0;  --214285	213392，说明还真有不同city同一个supplier_id
--0305 ：229810


--把supplier对应多个city这种跨城市商户删除
drop table algo_test.dy_supplier_history_delivery_city0_filter;
create table algo_test.dy_supplier_history_delivery_city0_filter as
select
  A.*
from
  algo_test.dy_supplier_history_delivery_city0 as A
  left outer join (
    select
      supplier_id,
      count(*)
    from
      algo_test.dy_supplier_history_delivery_city0
    group by
      supplier_id
    having  -- group by 之后不能用where
      count(*) > 1
  ) as Invalid on A.supplier_id = Invalid.supplier_id
where
  Invalid.supplier_id is null;--不要忘了加这个条件
select count(*) as c3_s, count(distinct supplier_id) as cd3_s
from algo_test.dy_supplier_history_delivery_city0_filter;  --212711	212711



-- 3
--dada，历史接单数
drop table algo_test.dy_transporter_history_delivery_city0;
create TABLE algo_test.dy_transporter_history_delivery_city0 AS
select
  row_number() over() as id,
  transporter_id,
  count(*) as history_order_num,
  avg(A1_time) as avg_a1_time,
  avg(A2_time) as avg_a2_time,
  city_id,
  from_unixtime(
    cast(current_timestamp() as BIGINT),
    'yyyy-MM-dd HH:mm:ss'
  ) as create_time,
  from_unixtime(
    cast(current_timestamp() as BIGINT),
    'yyyy-MM-dd HH:mm:ss'
  ) as update_time
from
  algo_test.dy_order_city0
group by
  transporter_id,
  city_id;
select count(*) as c3_t, count(distinct transporter_id) as cd3_t
from algo_test.dy_transporter_history_delivery_city0; --252618	246557，和上面的同理

-- 把dada对应多个city这种跨城市骑士删除
drop table algo_test.dy_transporter_history_delivery_city0_filter;
create table algo_test.dy_transporter_history_delivery_city0_filter as
select
  A.*
from
  algo_test.dy_transporter_history_delivery_city0 as A
  left outer join (
    select
      transporter_id,
      count(*)
    from
      algo_test.dy_transporter_history_delivery_city0
    group by
      transporter_id
    having  -- 不能用where
      count(*) > 1
  ) as Invalid on A.transporter_id = Invalid.transporter_id
where
  Invalid.transporter_id is null;--不要忘了加这个条件
select count(*) as c3_t, count(distinct transporter_id) as cd3_t
from algo_test.dy_transporter_history_delivery_city0_filter;  --240692	240692


