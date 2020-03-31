CREATE TABLE `database_group` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `name` varchar(255) NOT NULL COMMENT '名称',
  `instance_name` varchar(255) NOT NULL COMMENT '实例名称',
  `database_list` varchar(512) NOT NULL COMMENT '数据库名称',
  `creator` varchar(255) NOT NULL COMMENT '创建人账号',
  `modifier` varchar(255) NOT NULL COMMENT '修改人账号',
  `is_deleted` int(1) DEFAULT '0' COMMENT '是否删除',
  `create_time` datetime(6) NOT NULL COMMENT '创建时间',
  `update_time` datetime(6) NOT NULL COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_name` (`name`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4;


INSERT INTO `archery`.`auth_permission`(`id`, `name`, `content_type_id`, `codename`) VALUES (158, 'Can add batch SQL工单', 33, 'add_batch_sqlworkflow');
INSERT INTO `archery`.`auth_permission`(`id`, `name`, `content_type_id`, `codename`) VALUES (159, '提交SQL上线批量工单', 26, 'sql_batch_submit');
