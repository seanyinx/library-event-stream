CREATE TABLE IF NOT EXISTS `library_event_stream` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `cluster_name` varchar(100) DEFAULT 'cluster name',
  `binlog_position` varchar(256) DEFAULT 'binlog position',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '生成时间',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `cluster_name_index` (`cluster_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
