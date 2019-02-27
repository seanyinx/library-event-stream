CREATE TABLE IF NOT EXISTS `test_table` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `status` varchar(10) NOT NULL DEFAULT 'NEW' COMMENT '监听事件类型NEW SENT',
  `content` mediumtext COMMENT '消息内容',
  `topic` varchar(100) DEFAULT '',
  `tag` varchar(32) DEFAULT '',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '生成时间',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

insert into `listener_event` ( `status`, `content`, `topic`, `tag`) values ('NEW', 'test1', 'bob', 'alice');
insert into `listener_event` ( `status`, `content`, `topic`, `tag`) values ('NEW', 'test2', 'jack', 'alice');

insert into `test_table` ( `status`, `content`, `topic`, `tag`, `create_time`, `update_time`) values ( 'NEW', 'test1', 'bob', 'alice', '2018-08-17 10:21:31', '2018-08-17 10:23:45');
update `test_table` set status = 'SENT' where `topic` = 'jack' and `tag` = 'alice';

insert into `listener_event` ( `status`, `content`, `topic`, `tag`) values ('NEW', 'test3', 'bob', 'jack');
insert into `listener_event` ( `status`, `content`, `topic`, `tag`) values ('NEW', 'test4', 'john', 'bob');

select * from `test_table` where `topic` = 'jack' and `tag` = 'alice';
delete from `test_table` where `topic` = 'jack' and `tag` = 'alice';

update `listener_event` set status = 'SENT' where `topic` = 'jack' and `tag` = 'alice';
select * from `listener_event` where `topic` = 'jack' and `tag` = 'alice';
delete from `listener_event` where `topic` = 'jack' and `tag` = 'alice';

START TRANSACTION;
update `listener_event` set status = 'SENT' where `topic` = 'jack' and `tag` = 'alice';
insert into `listener_event` ( `status`, `content`, `topic`, `tag`) values ('NEW', 'test5', 'lucy', 'john');
COMMIT;
