DROP TABLE IF EXISTS ces0010;
CREATE TABLE ces0010 (
  `CPBPDIV` varchar(10) NOT NULL DEFAULT '',
  `CPBPCD` varchar(10) NOT NULL DEFAULT '',
  `CPBPNM` varchar(50) NOT NULL DEFAULT '',
  `CPBPVTYP` enum('T','E') NOT NULL DEFAULT 'T',
  `CPBPV` varchar(200) DEFAULT '',
  `CLEVEL` decimal(1,0) NOT NULL DEFAULT '1',
  `PUNIT` varchar(50) NOT NULL DEFAULT '',
  `REMARK` varchar(200) NOT NULL DEFAULT '',
  `USEYN` varchar(1) NOT NULL DEFAULT '',
  `INIDNO` varchar(20) DEFAULT '',
  `INNAME` varchar(30) DEFAULT '',
  `INTIM` datetime DEFAULT CURRENT_TIMESTAMP,
  `UPIDNO` varchar(20) DEFAULT '',
  `UPNAME` varchar(30) DEFAULT '',
  `UPTIM` datetime DEFAULT NULL,
  PRIMARY KEY (`CPBPDIV`,`CPBPCD`) USING BTREE,
  KEY `IDX_CPBPDIV` (`CPBPDIV`,`CPBPCD`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS ces0010;
CREATE TABLE ces0010 (
  `CPBPVTYP` enum('T','E') NOT NULL DEFAULT 'T'
);
desc ces0010;
insert into ces0010 values(default);
select * from ces0010;

DROP TABLE IF EXISTS ces0010;
CREATE TABLE ces0010 (
  `CPBPVTYP` enum('T','E') NOT NULL DEFAULT 'E'
);
desc ces0010;
insert into ces0010 values(default);
select * from ces0010;

DROP TABLE IF EXISTS ces0010;
CREATE TABLE ces0010 (
  `CPBPVTYP` enum('T','E') NOT NULL DEFAULT '1'
);
desc ces0010;
insert into ces0010 values(default);
select * from ces0010;

DROP TABLE IF EXISTS ces0010;
CREATE TABLE ces0010 (
  `CPBPVTYP` enum('T','E') NOT NULL DEFAULT 2
);
desc ces0010;
insert into ces0010 values(default);
select * from ces0010;

create table t_enum(a int,b enum('1','2','3','4','5'));
show create table t_enum;