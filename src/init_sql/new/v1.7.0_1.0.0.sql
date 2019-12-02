--  添加db_name (保存mongodb的数据库名称)
ALTER TABLE `archery`.`sql_instance` ADD COLUMN `db_name` varchar(64) NULL AFTER `db_type`;