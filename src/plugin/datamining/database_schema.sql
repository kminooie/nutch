SET @UserName = 'dmuser';
SET @PassWord = 'dmpass';
SET @DBName = 'knowledge_base';


CREATE SCHEMA IF NOT EXISTS knowledge_base CHARSET=utf8 COLLATE=utf8_unicode_ci;

GRANT ALL  ON knowledge_base.* TO 'dmuser'@'%' IDENTIFIED BY 'dmpass';

FLUSH PRIVILEGES;

USE knowledge_base;

DROP TABLE IF EXISTS `hosts`;
DROP TABLE IF EXISTS `urls`;
DROP TABLE IF EXISTS `nodes`;
DROP TABLE IF EXISTS `frequency`;


CREATE TABLE `hosts` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `domain` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `domain_UNIQUE` (`domain`)
) ENGINE=TokuDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci `compression`='tokudb_zlib';


CREATE TABLE `urls` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `host_id` int unsigned NOT NULL,
  `path` varchar(512) COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `host` (`host_id`,`path`)
) ENGINE=TokuDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci `compression`='tokudb_zlib';


CREATE TABLE `nodes` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `host_id` int unsigned NOT NULL,
  `hash` int NOT NULL,
  `xpath` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `index2` (`host_id`,`hash`,`xpath`)
) ENGINE=TokuDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci `compression`='tokudb_zlib';


CREATE TABLE `frequency` (
  `node_id` bigint unsigned NOT NULL,
  `url_id` int unsigned NOT NULL,
  PRIMARY KEY (`node_id`,`url_id`)
) ENGINE=TokuDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci `compression`='tokudb_zlib';


