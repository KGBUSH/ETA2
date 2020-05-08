CREATE TABLE `algo_supplier_delivery_history` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
  `supplier_id` bigint(20) NOT NULL DEFAULT '-1' COMMENT '商家id',
  `history_order_num` float NOT NULL DEFAULT '0' COMMENT '商家历史接单数',
  `avg_a1_time` float NOT NULL DEFAULT '0' COMMENT '商家平均a1',
  `avg_a2_time` float NOT NULL DEFAULT '0' COMMENT '商家平均a2',
  `city_id` int(11) NOT NULL DEFAULT '0' COMMENT '城市ID',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_supplier_id` (`supplier_id`)
) ENGINE=InnoDB AUTO_INCREMENT=458785 DEFAULT CHARSET=utf8mb4 COMMENT='商家历史信息表';