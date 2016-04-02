use knowledge_base;

drop table `hosts`;
drop table `urls`;
drop table `nodes`;
drop table `frequency`;


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


