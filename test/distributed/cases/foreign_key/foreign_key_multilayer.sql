CREATE TABLE part_fk(
P_PARTKEY     INTEGER NOT NULL,
P_NAME        VARCHAR(55) NOT NULL,
P_MFGR        CHAR(25) NOT NULL,
P_BRAND       CHAR(10) NOT NULL,
P_TYPE        VARCHAR(25) NOT NULL,
P_SIZE        INTEGER NOT NULL,
P_CONTAINER   CHAR(10) NOT NULL,
P_RETAILPRICE DECIMAL(15,2) NOT NULL,
P_COMMENT     VARCHAR(23) NOT NULL,
PRIMARY KEY (P_PARTKEY)
);
insert into part_fk values(199,"pink wheat powder burlywood snow","Manufacturer#5","Brand#52","MEDIUM BURNISHED BRASS",49,"LG BOX",2097.99,". special deposits hag");

CREATE TABLE region_fk(
R_REGIONKEY  INTEGER NOT NULL,
R_NAME       CHAR(25) NOT NULL,
R_COMMENT    VARCHAR(152),
PRIMARY KEY (R_REGIONKEY)
);
insert into region_fk values(2,"ASIA","ges. thinly even pinto beans ca");

CREATE TABLE NATION_fk(
N_NATIONKEY  INTEGER NOT NULL,
N_NAME       CHAR(25) NOT NULL,
N_REGIONKEY  INTEGER NOT NULL,
N_COMMENT    VARCHAR(152),
PRIMARY KEY (N_NATIONKEY),constraint fk_n foreign key(N_REGIONKEY) REFERENCES region_fk(R_REGIONKEY)
);
insert into nation_fk values(13,"VIETNAM",2,"hely enticingly express accounts. even, final");

CREATE TABLE supplier_fk(
S_SUPPKEY     INTEGER NOT NULL,
S_NAME        CHAR(25) NOT NULL,
S_ADDRESS     VARCHAR(40) NOT NULL,
S_NATIONKEY   INTEGER NOT NULL,
S_PHONE       CHAR(15) NOT NULL,
S_ACCTBAL     DECIMAL(15,2) NOT NULL,
S_COMMENT     VARCHAR(101) NOT NULL,
PRIMARY KEY (S_SUPPKEY),constraint fk_s foreign key(S_NATIONKEY) REFERENCES nation_fk(N_NATIONKEY)
);
insert into supplier_fk values(9991,"Supplier#000009991","RnP1Z uvwftshFtf",13,"23-451-948-8464",6785.10,". furiously pending accounts b");

CREATE TABLE PARTSUPP_fk(
PS_PARTKEY     INTEGER NOT NULL,
PS_SUPPKEY     INTEGER NOT NULL,
PS_AVAILQTY    INTEGER NOT NULL,
PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
PS_COMMENT     VARCHAR(199) NOT NULL,
PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY),constraint fk_p1 foreign key(PS_PARTKEY) REFERENCES part_fk(P_PARTKEY),constraint fk_p2 foreign key(PS_SUPPKEY) REFERENCES supplier_fk(S_SUPPKEY)
);
insert into PARTSUPP_fk values (199,9991,7872,606.64," according to the final pinto beans: carefully silent requests sleep final");

CREATE TABLE customer_fk(
C_CUSTKEY     INTEGER NOT NULL,
C_NAME        VARCHAR(25) NOT NULL,
C_ADDRESS     VARCHAR(40) NOT NULL,
C_NATIONKEY   INTEGER NOT NULL,
C_PHONE       CHAR(15) NOT NULL,
C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
C_MKTSEGMENT  CHAR(10) NOT NULL,
C_COMMENT     VARCHAR(117) NOT NULL,
PRIMARY KEY (C_CUSTKEY),constraint fk_c foreign key(C_NATIONKEY) REFERENCES nation_fk(N_NATIONKEY)
);
insert into customer_fk values(12,"Customer#000149992","iwjVf1MZno1",13,"16-684-999-8810",3417.45,"AUTOMOBILE","luffily final requests integrate slyly. furiously special warhorses are furiously alongside o");

CREATE TABLE orders_fk(
O_ORDERKEY       BIGINT NOT NULL,
O_CUSTKEY        INTEGER NOT NULL,
O_ORDERSTATUS    CHAR(1) NOT NULL,
O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
O_ORDERDATE      DATE NOT NULL,
O_ORDERPRIORITY  CHAR(15) NOT NULL,
O_CLERK          CHAR(15) NOT NULL,
O_SHIPPRIORITY   INTEGER NOT NULL,
O_COMMENT        VARCHAR(79) NOT NULL,
PRIMARY KEY (O_ORDERKEY),constraint fk_o foreign key(O_CUSTKEY) REFERENCES customer_fk(C_CUSTKEY));
insert into orders_fk values(5999968,12,"F",354575.46,"1992-12-24","3-MEDIUM","Clerk#000000736",0, "cajole blithely ag");

CREATE TABLE lineitem_fk(
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
PRIMARY KEY (L_ORDERKEY, L_LINENUMBER),constraint fk_l1 foreign key(L_ORDERKEY) REFERENCES ORDERS_fk(o_orderkey),constraint fk_l2 foreign key(L_PARTKEY) REFERENCES PART_fk(P_PARTKEY),constraint fk_l3 foreign key(L_SUPPKEY) REFERENCES SUPPLIER_fk(S_SUPPKEY));
insert into lineitem_fk values(5999968,199,9991,2,46,63179.16,0.08,0.06,"R","F","1993-09-16","1993-09-21","1993-10-02","COLLECT COD","RAIL","dolites wake");

-- update constraint /Master and Slave Tables
update nation_fk set n_nationkey=10 where n_nationkey=13;
update lineitem_fk set l_partkey=2 where l_suppkey=9991;
update PARTSUPP_fk set PS_PARTKEY=40;
update orders_fk set O_ORDERKEY=1 where O_CUSTKEY=12;
update supplier_fk set s_suppkey=11 where s_nationkey=13;
update customer_fk set c_nationkey=6;
update part_fk set p_partkey=200 where P_RETAILPRICE=2097.99;
update region_fk set r_regionkey=5 where r_name="ASIA";

--delete constraint/Master and Slave Tables
delete from nation_fk where n_nationkey=13;
select * from SUPPLIER_fk;
select * from CUSTOMER_fk;
delete from lineitem_fk where l_suppkey=9991;
select * from partsupp_fk;
select * from orders_fk;
insert into lineitem_fk values(5999968,199,9991,2,46,63179.16,0.08,0.06,"R","F","1993-09-16","1993-09-21","1993-10-02","COLLECT COD","RAIL","dolites wake");
select * from lineitem_fk;
delete from PARTSUPP_fk;
select * from part_fk;
select * from LINEITEM_fk;
insert into PARTSUPP_fk values (199,9991,7872,606.64," according to the final pinto beans: carefully silent requests sleep final");
select * from PARTSUPP_fk;
delete from orders_fk  where O_CUSTKEY=12;
select * from  orders_fk ;
select * from customer_fk;
select * from orders_fk;
delete from supplier_fk  where s_nationkey=13;
select * from partsupp_fk;
select * from nation_fk;
select * from supplier_fk;
delete from customer_fk;
select * from nation_fk;
select * from orders_fk;
delete from part_fk where P_RETAILPRICE=2097.99;
select * from PARTSUPP_fk;
delete from region_fk  where r_name="ASIA";
select * from nation_fk;

-- insert constraint /Master and Slave Tables
insert into region_fk values(3,"ASIA","ges. thinly even pinto beans ca");
select * from region_fk;
insert into nation_fk values(1,"VIETNAM",2,"hely enticingly express accounts. even, final");
insert into part_fk values(200,"pink wheat powder burlywood snow","Manufacturer#5","Brand#52","MEDIUM BURNISHED BRASS",49,"LG BOX",2097.99,". special deposits hag");
insert into supplier_fk values(10000,"Supplier#000009991","RnP1Z uvwftshFtf",14,"23-451-948-8464",6785.10,". furiously pending accounts b");
insert into PARTSUPP_fk values (200,10000,7872,606.64," according to the final pinto beans: carefully silent requests sleep final");
insert into customer_fk values(14,"Customer#000149992","iwjVf1MZno1",15,"16-684-999-8810",3417.45,"AUTOMOBILE","luffily final requests integrate slyly. furiously special warhorses are furiously alongside o");
insert into orders_fk values(1,14,"F",354575.46,"1992-12-24","3-MEDIUM","Clerk#000000736",0, "cajole blithely ag");
insert into lineitem_fk values(1,200,10000,2,46,63179.16,0.08,0.06,"R","F","1993-09-16","1993-09-21","1993-10-02","COLLECT COD","RAIL","dolites wake");

-- drop constraint /Master and Slave Tables
drop table if exists region_fk;
drop table if exists nation_fk;
drop table if exists part_fk;
drop table if exists supplier_fk;
drop table if exists partsupp_fk;
drop table if exists customer_fk;
drop table if exists orders_fk;
drop table if exists lineitem_fk;

drop table if exists lineitem_fk;
drop table if exists orders_fk;
drop table if exists partsupp_fk;
drop table if exists customer_fk;
drop table if exists supplier_fk;
drop table if exists nation_fk;
drop table if exists region_fk;
drop table if exists part_fk;

CREATE TABLE part_fk(
P_PARTKEY     INTEGER NOT NULL,
P_NAME        VARCHAR(55) NOT NULL,
P_MFGR        CHAR(25) NOT NULL,
P_BRAND       CHAR(10) NOT NULL,
P_TYPE        VARCHAR(25) NOT NULL,
P_SIZE        INTEGER NOT NULL,
P_CONTAINER   CHAR(10) NOT NULL,
P_RETAILPRICE DECIMAL(15,2) NOT NULL,
P_COMMENT     VARCHAR(23) NOT NULL,
PRIMARY KEY (P_PARTKEY)
);
insert into part_fk values(199,"pink wheat powder burlywood snow","Manufacturer#5","Brand#52","MEDIUM BURNISHED BRASS",49,"LG BOX",2097.99,". special deposits hag");

CREATE TABLE region_fk(
R_REGIONKEY  INTEGER NOT NULL,
R_NAME       CHAR(25) NOT NULL,
R_COMMENT    VARCHAR(152),
PRIMARY KEY (R_REGIONKEY)
);
insert into region_fk values(2,"ASIA","ges. thinly even pinto beans ca");

CREATE TABLE NATION_fk(
N_NATIONKEY  INTEGER NOT NULL,
N_NAME       CHAR(25) NOT NULL,
N_REGIONKEY  INTEGER NOT NULL,
N_COMMENT    VARCHAR(152),
PRIMARY KEY (N_NATIONKEY),constraint fk_n foreign key(N_REGIONKEY) REFERENCES region_fk(R_REGIONKEY)on delete CASCADE on update CASCADE
);
insert into nation_fk values(13,"VIETNAM",2,"hely enticingly express accounts. even, final");

CREATE TABLE supplier_fk(
S_SUPPKEY     INTEGER NOT NULL,
S_NAME        CHAR(25) NOT NULL,
S_ADDRESS     VARCHAR(40) NOT NULL,
S_NATIONKEY   INTEGER NOT NULL,
S_PHONE       CHAR(15) NOT NULL,
S_ACCTBAL     DECIMAL(15,2) NOT NULL,
S_COMMENT     VARCHAR(101) NOT NULL,
PRIMARY KEY (S_SUPPKEY),constraint fk_s foreign key(S_NATIONKEY) REFERENCES nation_fk(N_NATIONKEY)on delete CASCADE on update CASCADE
);
insert into supplier_fk values(9991,"Supplier#000009991","RnP1Z uvwftshFtf",13,"23-451-948-8464",6785.10,". furiously pending accounts b");

CREATE TABLE PARTSUPP_fk(
PS_PARTKEY     INTEGER NOT NULL,
PS_SUPPKEY     INTEGER NOT NULL,
PS_AVAILQTY    INTEGER NOT NULL,
PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
PS_COMMENT     VARCHAR(199) NOT NULL,
PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY),constraint fk_p1 foreign key(PS_PARTKEY) REFERENCES part_fk(P_PARTKEY)on delete CASCADE on update CASCADE,constraint fk_p2 foreign key(PS_SUPPKEY) REFERENCES supplier_fk(S_SUPPKEY) on delete CASCADE on update CASCADE
);
insert into PARTSUPP_fk values (199,9991,7872,606.64," according to the final pinto beans: carefully silent requests sleep final");

CREATE TABLE customer_fk(
C_CUSTKEY     INTEGER NOT NULL,
C_NAME        VARCHAR(25) NOT NULL,
C_ADDRESS     VARCHAR(40) NOT NULL,
C_NATIONKEY   INTEGER NOT NULL,
C_PHONE       CHAR(15) NOT NULL,
C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
C_MKTSEGMENT  CHAR(10) NOT NULL,
C_COMMENT     VARCHAR(117) NOT NULL,
PRIMARY KEY (C_CUSTKEY),constraint fk_c foreign key(C_NATIONKEY) REFERENCES nation_fk(N_NATIONKEY) on delete CASCADE on update CASCADE
);
insert into customer_fk values(12,"Customer#000149992","iwjVf1MZno1",13,"16-684-999-8810",3417.45,"AUTOMOBILE","luffily final requests integrate slyly. furiously special warhorses are furiously alongside o");

CREATE TABLE orders_fk(
O_ORDERKEY       BIGINT NOT NULL,
O_CUSTKEY        INTEGER NOT NULL,
O_ORDERSTATUS    CHAR(1) NOT NULL,
O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
O_ORDERDATE      DATE NOT NULL,
O_ORDERPRIORITY  CHAR(15) NOT NULL,
O_CLERK          CHAR(15) NOT NULL,
O_SHIPPRIORITY   INTEGER NOT NULL,
O_COMMENT        VARCHAR(79) NOT NULL,
PRIMARY KEY (O_ORDERKEY),constraint fk_o foreign key(O_CUSTKEY) REFERENCES customer_fk(C_CUSTKEY) on delete CASCADE on update CASCADE);
insert into orders_fk values(5999968,12,"F",354575.46,"1992-12-24","3-MEDIUM","Clerk#000000736",0, "cajole blithely ag");

CREATE TABLE lineitem_fk(
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
PRIMARY KEY (L_ORDERKEY, L_LINENUMBER),constraint fk_l1 foreign key(L_ORDERKEY) REFERENCES ORDERS_fk(o_orderkey)on delete CASCADE on update CASCADE,constraint fk_l2 foreign key(L_PARTKEY,L_SUPPKEY) REFERENCES PARTSUPP_fk(PS_PARTKEY,PS_SUPPKEY) on delete CASCADE on update CASCADE);
insert into lineitem_fk values(5999968,199,9991,2,46,63179.16,0.08,0.06,"R","F","1993-09-16","1993-09-21","1993-10-02","COLLECT COD","RAIL","dolites wake");

-- update constraint
update nation_fk set n_nationkey=10 where n_nationkey=13;
select * from supplier_fk;
select * from customer_fk;
update lineitem_fk set l_partkey=2 where l_suppkey=9991;
select * from PARTSUPP_fk;
update PARTSUPP_fk set PS_PARTKEY=40;
select * from lineitem_fk;
update orders_fk set O_ORDERKEY=1 where O_CUSTKEY=12;
select * from lineitem_fk;
update supplier_fk set s_suppkey=11 where s_nationkey=10;
select * from supplier_fk;
select PS_SUPPKEY from PARTSUPP_fk ;
update customer_fk set c_nationkey=20;
update part_fk set p_partkey=200 where P_RETAILPRICE=2097.99;
select PS_PARTKEY from PARTSUPP_fk;
update region_fk set r_regionkey=5 where r_name="ASIA";
select N_REGIONKEY from NATION_fk;

--delete constraint
select * from nation_fk;
delete from nation_fk where n_nationkey=10;
select * from SUPPLIER_fk;
select * from CUSTOMER_fk;
select * from lineitem_fk;
delete from lineitem_fk where l_suppkey=9991;
select * from partsupp_fk;
select * from orders_fk;
insert into lineitem_fk values(5999968,199,9991,2,46,63179.16,0.08,0.06,"R","F","1993-09-16","1993-09-21","1993-10-02","COLLECT COD","RAIL","dolites wake");
delete from PARTSUPP_fk;
select * from part_fk;
select * from LINEITEM_fk;
insert into PARTSUPP_fk values (199,9991,7872,606.64," according to the final pinto beans: carefully silent requests sleep final");
delete from orders_fk  where O_CUSTKEY=12;
select * from customer_fk;
select * from lineitem_fk;
insert into orders_fk values(5999968,12,"F",354575.46,"1992-12-24","3-MEDIUM","Clerk#000000736",0, "cajole blithely ag");
select * from orders_fk;
delete from supplier_fk  where s_nationkey=10;
select * from partsupp_fk;
select * from nation_fk;
select * from supplier_fk;
insert into supplier_fk values(10000,"Supplier#000009991","RnP1Z uvwftshFtf",13,"23-451-948-8464",6785.10,". furiously pending accounts b");
delete from customer_fk;
select * from nation_fk;
select * from orders_fk;
insert into customer_fk values(12,"Customer#000149992","iwjVf1MZno1",13,"16-684-999-8810",3417.45,"AUTOMOBILE","luffily final requests integrate slyly. furiously special warhorses are furiously alongside o");
delete from part_fk where P_RETAILPRICE=2097.99;
select * from PARTSUPP_fk;
insert into part_fk values(199,"pink wheat powder burlywood snow","Manufacturer#5","Brand#52","MEDIUM BURNISHED BRASS",49,"LG BOX",2097.99,". special deposits hag");
delete from region_fk  where r_name="ASIA";
select * from nation_fk;
insert into region_fk values(2,"ASIA","ges. thinly even pinto beans ca");
insert into region_fk values(3,"ASIA","ges. thinly even pinto beans ca");
select * from region_fk;
insert into nation_fk values(1,"VIETNAM",2,"hely enticingly express accounts. even, final");
insert into part_fk values(200,"pink wheat powder burlywood snow","Manufacturer#5","Brand#52","MEDIUM BURNISHED BRASS",49,"LG BOX",2097.99,". special deposits hag");

insert into supplier_fk values(10000,"Supplier#000009991","RnP1Z uvwftshFtf",14,"23-451-948-8464",6785.10,". furiously pending accounts b");
insert into PARTSUPP_fk values (200,10000,7872,606.64," according to the final pinto beans: carefully silent requests sleep final");
insert into customer_fk values(14,"Customer#000149992","iwjVf1MZno1",15,"16-684-999-8810",3417.45,"AUTOMOBILE","luffily final requests integrate slyly. furiously special warhorses are furiously alongside o");
insert into orders_fk values(1,14,"F",354575.46,"1992-12-24","3-MEDIUM","Clerk#000000736",0, "cajole blithely ag");
insert into lineitem_fk values(1,200,10000,2,46,63179.16,0.08,0.06,"R","F","1993-09-16","1993-09-21","1993-10-02","COLLECT COD","RAIL","dolites wake");
