INSERT INTO `archery`.`auth_permission`(`id`, `name`, `content_type_id`, `codename`) VALUES (160, '菜单 数据导入', 26, 'menu_dataimport');
INSERT INTO `archery`.`auth_permission`(`id`, `name`, `content_type_id`, `codename`) VALUES (161, '数据导入can import data', 26, 'data_import');



/**单号导入历史记录表*/
CREATE TABLE `id_import_history` (
  `bid` varchar(64) NOT NULL COMMENT '批次ID',
  `file_name` varchar(255) NOT NULL COMMENT '文件名称',
  `instance_name` varchar(255) NOT NULL COMMENT '实例名称',
  `db_name` varchar(255) NOT NULL COMMENT '数据库名称',
  `tb_name` varchar(255) NOT NULL COMMENT '表名称',
  `record_count` bigint(20) NOT NULL COMMENT '记录数量',
  `type` int(1) NOT NULL COMMENT '导入类型 1:表示单号导入   2:表示数据导入',
  `is_valid` int(1) DEFAULT '1' COMMENT '是否有效 0.失败， 1.成功， 2.进行中',
  `creator` varchar(255) NOT NULL COMMENT '创建人账号',
  `create_time` datetime(6) NOT NULL COMMENT '创建时间',
  PRIMARY KEY (`bid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/**单号导入表， 在选中的实例上手动创建 for mysql*/
CREATE TABLE `id_import_record` (
  `bid` varchar(64) NOT NULL COMMENT '批次ID',
  `oid` varchar(128) NOT NULL COMMENT '单号ID',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`bid`, `oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


/**单号导入表， 在选中的实例上手动创建 for ads*/
CREATE TABLE `id_import_record` (
  `bid` varchar  COMMENT '批次ID',
  `oid` varchar  COMMENT '单号ID',
  `create_time` timestamp COMMENT '创建时间',
  PRIMARY KEY (`bid`, `oid`)
) PARTITION BY HASH KEY (`bid`) PARTITION NUM 256
TABLEGROUP table_group_name
OPTIONS (UPDATETYPE='realtime');