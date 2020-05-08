CREATE TABLE `algo_poi_statistic_Info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
  `poi_id` varchar(100) NOT NULL DEFAULT '0' COMMENT 'POI编号',
  `percentile_delivery_time_poi` float NOT NULL DEFAULT '0' COMMENT '该poi的中位数交付时间',
  `avg_delivery_time_poi` float NOT NULL DEFAULT '0' COMMENT '该poi的平均交付时间',
  `percentile_distance_poi` float NOT NULL DEFAULT '0' COMMENT '收货地与poi的中位数距离',
  `std_distance_poi` float NOT NULL DEFAULT '0' COMMENT '距离方差',
  `std_delivery_time_poi` float NOT NULL DEFAULT '0' COMMENT '交付时间方差',
  `order_cnt` int(11) NOT NULL DEFAULT '0' COMMENT '交付数量',
  `city_id` int(11) NOT NULL DEFAULT '0' COMMENT '城市ID',
  `poi_lat` float NOT NULL DEFAULT '0' COMMENT '纬度',
  `poi_lng` float NOT NULL DEFAULT '0' COMMENT '经度',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_poi_city_lat_lng` (`poi_id`,`city_id`,`poi_lat`,`poi_lng`)
) ENGINE=InnoDB AUTO_INCREMENT=65 DEFAULT CHARSET=utf8mb4 COMMENT='poi交付相关表';