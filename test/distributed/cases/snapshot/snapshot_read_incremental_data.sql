CREATE TABLE LINEITEM(
L_LINEITEM_ID BIGINT NOT NULL,
L_ORDERKEY    BIGINT NOT NULL,
L_PARTKEY     INTEGER NOT NULL,
L_SUPPKEY     INTEGER NOT NULL,
L_LINENUMBER  INTEGER NOT NULL,
L_QUANTITY    DECIMAL(15,2) NOT NULL,
L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
L_DISCOUNT    DECIMAL(15,2) NOT NULL,
L_TAX         DECIMAL(15,2) NOT NULL,
L_RETURNFLAG  VARCHAR(1) NOT NULL,
L_LINESTATUS  VARCHAR(1) NOT NULL,
L_SHIPDATE    DATE NOT NULL,
L_COMMITDATE  DATE NOT NULL,
L_RECEIPTDATE DATE NOT NULL,
L_SHIPINSTRUCT CHAR(25) NOT NULL,
L_SHIPMODE     CHAR(10) NOT NULL,
L_COMMENT      VARCHAR(44) NOT NULL,
PRIMARY KEY (L_LINEITEM_ID)
);

use tpch;
insert into lineitem values
(1,1,156,4,1,17,17954.55,0.04,0.02,"N","O","1996-03-13","1996-02-12","1996-03-22","DELIVER IN PERSON","TRUCK","egular courts above the"),
(2,1,68,9,2,36,34850.16,0.09,0.06,"N","O","1996-04-12","1996-02-28","1996-04-20","TAKE BACK RETURN","MAIL","ly final dependencies: slyly bold "),
(3,1,64,5,3,8,7712.48,0.10,0.02,"N","O","1996-01-29","1996-03-05","1996-01-31","TAKE BACK RETURN","REG AIR","riously. regular, express dep"),
(4,1,3,6,4,28,25284.00,0.09,0.06,"N","O","1996-04-21","1996-03-30","1996-05-16","NONE","AIR","lites. fluffily even de"),
(5,1,25,8,5,24,22200.48,0.10,0.04,"N","O","1996-03-30","1996-03-14","1996-04-01","NONE","FOB"," pending foxes. slyly re"),
(6,1,16,3,6,32,29312.32,0.07,0.02,"N","O","1996-01-30","1996-02-07","1996-02-03","DELIVER IN PERSON","MAIL","arefully slyly ex"),
(7,2,107,2,1,38,38269.80,0.00,0.05,"N","O","1997-01-28","1997-01-14","1997-02-02","TAKE BACK RETURN","RAIL","ven requests. deposits breach a"),
(8,3,5,2,1,45,40725.00,0.06,0.00,"R","F","1994-02-02","1994-01-04","1994-02-23","NONE","AIR","ongside of the furiously brave acco"),
(9,3,20,10,2,49,45080.98,0.10,0.00,"R","F","1993-11-09","1993-12-20","1993-11-24","TAKE BACK RETURN","RAIL"," unusual accounts. eve"),
(10,3,129,8,3,27,27786.24,0.06,0.07,"A","F","1994-01-16","1993-11-22","1994-01-23","DELIVER IN PERSON","SHIP","nal foxes wake. ");
select count(*) from lineitem;
create snapshot snapshot_lineitem for account sys;
create snapshot snapshot_lineitem_incremental for cluster;
insert into lineitem values
(11,3,30,5,4,2,1860.06,0.01,0.06,"A","F","1993-12-04","1994-01-07","1994-01-01","NONE","TRUCK","y. fluffily pending d"),
(12,3,184,5,5,28,30357.04,0.04,0.00,"R","F","1993-12-14","1994-01-10","1994-01-01","TAKE BACK RETURN","FOB","ages nag slyly pending"),
(13,3,63,8,6,26,25039.56,0.10,0.02,"A","F","1993-10-29","1993-12-18","1993-11-04","TAKE BACK RETURN","RAIL","ges sleep after the caref"),
(14,4,89,10,1,30,29672.40,0.03,0.08,"N","O","1996-01-10","1995-12-14","1996-01-18","DELIVER IN PERSON","REG AIR","- quickly regular packages sleep. idly"),
(15,5,109,10,1,15,15136.50,0.02,0.04,"R","F","1994-10-31","1994-08-31","1994-11-20","NONE","AIR","ts wake furiously "),
(16,5,124,5,2,26,26627.12,0.07,0.08,"R","F","1994-10-16","1994-09-25","1994-10-19","NONE","FOB","sts use slyly quickly special instruc"),
(17,5,38,4,3,50,46901.50,0.08,0.03,"A","F","1994-08-08","1994-10-13","1994-08-26","DELIVER IN PERSON","AIR","eodolites. fluffily unusual"),
(18,6,140,6,1,37,38485.18,0.08,0.03,"A","F","1992-04-27","1992-05-15","1992-05-02","TAKE BACK RETURN","TRUCK","p furiously special foxes"),
(19,7,183,4,1,12,12998.16,0.07,0.03,"N","O","1996-05-07","1996-03-13","1996-06-03","TAKE BACK RETURN","FOB","ss pinto beans wake against th"),
(20,7,146,3,2,9,9415.26,0.08,0.08,"N","O","1996-02-01","1996-03-02","1996-02-19","TAKE BACK RETURN","SHIP","es. instructions");
select count(*) from lineitem {snapshot = 'snapshot_lineitem_incremental'};
select count(*) from lineitem {snapshot = 'snapshot_lineitem'};
insert into lineitem values
(21,7,95,8,3,46,45774.14,0.10,0.07,"N","O","1996-01-15","1996-03-27","1996-02-03","COLLECT COD","MAIL"," unusual reques"),
(22,7,164,5,4,28,29796.48,0.03,0.04,"N","O","1996-03-21","1996-04-08","1996-04-20","NONE","FOB",". slyly special requests haggl"),
(23,7,152,4,5,38,39981.70,0.08,0.01,"N","O","1996-02-11","1996-02-24","1996-02-18","DELIVER IN PERSON","TRUCK","ns haggle carefully ironic deposits. bl"),
(24,7,80,10,6,35,34302.80,0.06,0.03,"N","O","1996-01-16","1996-02-23","1996-01-22","TAKE BACK RETURN","FOB","jole. excuses wake carefully alongside of "),
(25,7,158,3,7,5,5290.75,0.04,0.02,"N","O","1996-02-10","1996-03-26","1996-02-13","NONE","FOB","ithely regula"),
(26,32,83,4,1,28,27526.24,0.05,0.08,"N","O","1995-10-23","1995-08-27","1995-10-26","TAKE BACK RETURN","TRUCK","sleep quickly. req"),
(27,32,198,10,2,32,35142.08,0.02,0.00,"N","O","1995-08-14","1995-10-07","1995-08-27","COLLECT COD","AIR","lithely regular deposits. fluffily "),
(28,32,45,2,3,2,1890.08,0.09,0.02,"N","O","1995-08-07","1995-10-07","1995-08-23","DELIVER IN PERSON","AIR"," express accounts wake according to the"),
(29,32,3,8,4,4,3612.00,0.09,0.03,"N","O","1995-08-04","1995-10-01","1995-09-03","NONE","REG AIR","e slyly final pac"),
(30,32,86,7,5,44,43387.52,0.05,0.06,"N","O","1995-08-28","1995-08-20","1995-09-14","DELIVER IN PERSON","AIR","symptotes nag according to the ironic depo");
select count(*) from lineitem {snapshot = 'snapshot_lineitem_incremental'};
select count(*) from lineitem {snapshot = 'snapshot_lineitem'};
drop snapshot snapshot_lineitem;
drop snapshot snapshot_lineitem_incremental;
drop table lineitem;

drop table if exists rs01;
create table rs01 (col1 int, col2 decimal(6), col3 varchar(30));
insert into rs01 values (1, null, 'database');
insert into rs01 values (2, 38291.32132, 'database');
insert into rs01 values (3, null, 'database management system');
insert into rs01 values (4, 10, null);
insert into rs01 values (1, -321.321, null);
insert into rs01 values (2, -1, null);
select count(*) from rs01;

drop snapshot if exists sp01;
create snapshot sp01 for account sys;
select count(*) from rs01 {snapshot = 'sp01'};
insert into rs01 values (2, -1, null);
insert into rs01 values (1, -321.321, null);
select * from rs01;
select count(*) from rs01 {snapshot = 'sp01'};
select * from rs01 {snapshot = 'sp01'};

restore account sys from snapshot sp01;
select * from rs01;
select count(*) from rs01;
select count(*) from rs01 {snapshot = 'sp01'};

drop snapshot sp01;
drop table rs01;
-- @ignore:1
show snapshots;
