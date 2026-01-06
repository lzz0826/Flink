CREATE DATABASE test;

USE test;

CREATE TABLE IF NOT EXISTS `admin_admin` (
    `id` varchar(40) NOT NULL COMMENT 'ID',
    `equipment_name` varchar(40) DEFAULT NULL,
    `temperature` varchar(40) NOT NULL COMMENT 'temperature',
    `time` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'time',
    PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

