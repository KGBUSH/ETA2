CREATE TABLE `algo_transporter_delivery_history` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
  `transporter_id` bigint(20) NOT NULL DEFAULT '-1' COMMENT '达达id',
  `history_order_num` float NOT NULL DEFAULT '0' COMMENT '达达历史接单数',
  `avg_a1_time` float NOT NULL DEFAULT '0' COMMENT '达达平均a1',
  `avg_a2_time` float NOT NULL DEFAULT '0' COMMENT '达达平均a2',
  `city_id` int(11) NOT NULL DEFAULT '0' COMMENT '城市ID',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_transporter_id` (`transporter_id`)
) ENGINE=InnoDB AUTO_INCREMENT=741674 DEFAULT CHARSET=utf8mb4 COMMENT='达达历史信息表';