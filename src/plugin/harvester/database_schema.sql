

CREATE SCHEMA IF NOT EXISTS nutch_harvester_db CHARSET=utf8 COLLATE=utf8_unicode_ci;

GRANT ALL  ON nutch_harvester_db.* TO 'dmuser'@'%' IDENTIFIED BY 'dmpass';
GRANT ALL  ON nutch_harvester_db.* TO 'huser'@'%' IDENTIFIED BY 'hpass';

FLUSH PRIVILEGES;

USE nutch_harvester_db;

-- DROP TABLE IF EXISTS `hosts`;
-- DROP TABLE IF EXISTS `urls`;
-- DROP TABLE IF EXISTS `nodes`;
DROP VIEW IF EXISTS `frequency`;


CREATE TABLE IF NOT EXISTS `nodes` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `host_id` int NOT NULL,
  `xpath_id` int NOT NULL,
  `hash` int NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `index2` ( `host_id`, `xpath_id`, `hash` )
) ENGINE=TokuDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci `compression`='tokudb_zlib';


CREATE TABLE IF NOT EXISTS `urls` (
  `node_id` bigint unsigned NOT NULL,
  `url_id` int  NOT NULL,
  PRIMARY KEY (`node_id`,`url_id`)
) ENGINE=TokuDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci `compression`='tokudb_zlib';


CREATE VIEW `frequency` AS 
	SELECT n.id node_id, host_id, xpath_id, hash, count( url_id ) fq 
	FROM nodes n JOIN urls u ON ( u.node_id = n.id ) GROUP BY u.node_id ;


 
