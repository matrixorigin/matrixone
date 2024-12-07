drop database if exists test_show;
create database test_show;

CREATE TABLE `t1` (
      `abc` varchar(255) NOT NULL,
      KEY `uidx` (`abc`)
);
SHOW INDEX FROM `t1`;

drop database if exists test_show;