use tpch;
create snapshot tpch_snapshot for account sys;
create snapshot tpch_cluster for cluster;
drop table if exists customer;
drop table if exists lineitem;
drop table if exists nation;
drop table if exists orders;
drop table if exists part;
drop table if exists partsupp;
drop table if exists supplier;
select sleep(10);
-- customer
select count(*) from customer {snapshot = 'tpch_snapshot'};
select count(*) from customer {snapshot = 'tpch_snapshot'};
select count(*) from customer {snapshot = 'tpch_snapshot'};
select count(*) from customer {snapshot = 'tpch_snapshot'};
select count(*) from customer {snapshot = 'tpch_snapshot'};

-- lineitem
select count(*) from lineitem {snapshot = 'tpch_snapshot'};
select count(*) from lineitem {snapshot = 'tpch_snapshot'};
select count(*) from lineitem {snapshot = 'tpch_snapshot'};
select count(*) from lineitem {snapshot = 'tpch_snapshot'};
select count(*) from lineitem {snapshot = 'tpch_snapshot'};

-- nation
select count(*) from nation {snapshot = 'tpch_snapshot'};
select count(*) from nation {snapshot = 'tpch_snapshot'};
select count(*) from nation {snapshot = 'tpch_snapshot'};
select count(*) from nation {snapshot = 'tpch_snapshot'};
select count(*) from nation {snapshot = 'tpch_snapshot'};

-- orders
select count(*) from orders {snapshot = 'tpch_snapshot'};
select count(*) from orders {snapshot = 'tpch_snapshot'};
select count(*) from orders {snapshot = 'tpch_snapshot'};
select count(*) from orders {snapshot = 'tpch_snapshot'};
select count(*) from orders {snapshot = 'tpch_snapshot'};

-- part
select count(*) from part {snapshot = 'tpch_snapshot'};
select count(*) from part {snapshot = 'tpch_snapshot'};
select count(*) from part {snapshot = 'tpch_snapshot'};
select count(*) from part {snapshot = 'tpch_snapshot'};
select count(*) from part {snapshot = 'tpch_snapshot'};

-- partsupp
select count(*) from partsupp {snapshot = 'tpch_snapshot'};
select count(*) from partsupp {snapshot = 'tpch_snapshot'};
select count(*) from partsupp {snapshot = 'tpch_snapshot'};
select count(*) from partsupp {snapshot = 'tpch_snapshot'};
select count(*) from partsupp {snapshot = 'tpch_snapshot'};

-- supplier
select count(*) from supplier {snapshot = 'tpch_snapshot'};
select count(*) from supplier {snapshot = 'tpch_snapshot'};
select count(*) from supplier {snapshot = 'tpch_snapshot'};
select count(*) from supplier {snapshot = 'tpch_snapshot'};
select count(*) from supplier {snapshot = 'tpch_snapshot'};

-- restore
restore account sys database tpch from snapshot tpch_snapshot;
select count(*) from customer;
select count(*) from customer;
select count(*) from customer;

select count(*) from lineitem;
select count(*) from lineitem;
select count(*) from lineitem;

select count(*) from nation;
select count(*) from nation;
select count(*) from nation;

select count(*) from orders;
select count(*) from orders;
select count(*) from orders;

select count(*) from part;
select count(*) from part;
select count(*) from part;

select count(*) from partsupp;
select count(*) from partsupp;
select count(*) from partsupp;

select count(*) from supplier;
select count(*) from supplier;
select count(*) from supplier;

drop database tpch;
select sleep(10);

select count(*) from tpch.customer {snapshot = 'tpch_snapshot'};
select count(*) from tpch.customer {snapshot = 'tpch_snapshot'};
select count(*) from tpch.customer {snapshot = 'tpch_snapshot'};
select count(*) from tpch.customer {snapshot = 'tpch_snapshot'};
select count(*) from tpch.customer {snapshot = 'tpch_snapshot'};

select count(*) from tpch.lineitem {snapshot = 'tpch_snapshot'};
select count(*) from tpch.lineitem {snapshot = 'tpch_snapshot'};
select count(*) from tpch.lineitem {snapshot = 'tpch_snapshot'};
select count(*) from tpch.lineitem {snapshot = 'tpch_snapshot'};
select count(*) from tpch.lineitem {snapshot = 'tpch_snapshot'};

select count(*) from tpch.nation {snapshot = 'tpch_snapshot'};
select count(*) from tpch.nation {snapshot = 'tpch_snapshot'};
select count(*) from tpch.nation {snapshot = 'tpch_snapshot'};
select count(*) from tpch.nation {snapshot = 'tpch_snapshot'};
select count(*) from tpch.nation {snapshot = 'tpch_snapshot'};

select count(*) from tpch.orders {snapshot = 'tpch_snapshot'};
select count(*) from tpch.orders {snapshot = 'tpch_snapshot'};
select count(*) from tpch.orders {snapshot = 'tpch_snapshot'};
select count(*) from tpch.orders {snapshot = 'tpch_snapshot'};
select count(*) from tpch.orders {snapshot = 'tpch_snapshot'};

select count(*) from tpch.part {snapshot = 'tpch_snapshot'};
select count(*) from tpch.part {snapshot = 'tpch_snapshot'};
select count(*) from tpch.part {snapshot = 'tpch_snapshot'};
select count(*) from tpch.part {snapshot = 'tpch_snapshot'};
select count(*) from tpch.part {snapshot = 'tpch_snapshot'};

select count(*) from tpch.partsupp {snapshot = 'tpch_snapshot'};
select count(*) from tpch.partsupp {snapshot = 'tpch_snapshot'};
select count(*) from tpch.partsupp {snapshot = 'tpch_snapshot'};
select count(*) from tpch.partsupp {snapshot = 'tpch_snapshot'};
select count(*) from tpch.partsupp {snapshot = 'tpch_snapshot'};

select count(*) from tpch.supplier {snapshot = 'tpch_snapshot'};
select count(*) from tpch.supplier {snapshot = 'tpch_snapshot'};
select count(*) from tpch.supplier {snapshot = 'tpch_snapshot'};
select count(*) from tpch.supplier {snapshot = 'tpch_snapshot'};
select count(*) from tpch.supplier {snapshot = 'tpch_snapshot'};

restore account sys database tpch from snapshot tpch_snapshot;

select count(*) from tpch.customer;
select count(*) from tpch.customer;
select count(*) from tpch.customer;

select count(*) from tpch.lineitem;
select count(*) from tpch.lineitem;
select count(*) from tpch.lineitem;

select count(*) from tpch.nation;
select count(*) from tpch.nation;
select count(*) from tpch.nation;

select count(*) from tpch.orders;
select count(*) from tpch.orders;
select count(*) from tpch.orders;

select count(*) from tpch.part;
select count(*) from tpch.part;
select count(*) from tpch.part;

select count(*) from tpch.partsupp;
select count(*) from tpch.partsupp;
select count(*) from tpch.partsupp;

select count(*) from tpch.supplier;
select count(*) from tpch.supplier;
select count(*) from tpch.supplier;

drop snapshot tpch_snapshot;

select count(*) from tpch.customer {snapshot = 'tpch_cluster'};
select count(*) from tpch.customer {snapshot = 'tpch_cluster'};
select count(*) from tpch.customer {snapshot = 'tpch_cluster'};
select count(*) from tpch.customer {snapshot = 'tpch_cluster'};
select count(*) from tpch.customer {snapshot = 'tpch_cluster'};

select count(*) from tpch.lineitem {snapshot = 'tpch_cluster'};
select count(*) from tpch.lineitem {snapshot = 'tpch_cluster'};
select count(*) from tpch.lineitem {snapshot = 'tpch_cluster'};
select count(*) from tpch.lineitem {snapshot = 'tpch_cluster'};
select count(*) from tpch.lineitem {snapshot = 'tpch_cluster'};

select count(*) from tpch.nation {snapshot = 'tpch_cluster'};
select count(*) from tpch.nation {snapshot = 'tpch_cluster'};
select count(*) from tpch.nation {snapshot = 'tpch_cluster'};
select count(*) from tpch.nation {snapshot = 'tpch_cluster'};
select count(*) from tpch.nation {snapshot = 'tpch_cluster'};

select count(*) from tpch.orders {snapshot = 'tpch_cluster'};
select count(*) from tpch.orders {snapshot = 'tpch_cluster'};
select count(*) from tpch.orders {snapshot = 'tpch_cluster'};
select count(*) from tpch.orders {snapshot = 'tpch_cluster'};
select count(*) from tpch.orders {snapshot = 'tpch_cluster'};

select count(*) from tpch.part {snapshot = 'tpch_cluster'};
select count(*) from tpch.part {snapshot = 'tpch_cluster'};
select count(*) from tpch.part {snapshot = 'tpch_cluster'};
select count(*) from tpch.part {snapshot = 'tpch_cluster'};
select count(*) from tpch.part {snapshot = 'tpch_cluster'};

select count(*) from tpch.partsupp {snapshot = 'tpch_cluster'};
select count(*) from tpch.partsupp {snapshot = 'tpch_cluster'};
select count(*) from tpch.partsupp {snapshot = 'tpch_cluster'};
select count(*) from tpch.partsupp {snapshot = 'tpch_cluster'};
select count(*) from tpch.partsupp {snapshot = 'tpch_cluster'};

select count(*) from tpch.supplier {snapshot = 'tpch_cluster'};
select count(*) from tpch.supplier {snapshot = 'tpch_cluster'};
select count(*) from tpch.supplier {snapshot = 'tpch_cluster'};
select count(*) from tpch.supplier {snapshot = 'tpch_cluster'};
select count(*) from tpch.supplier {snapshot = 'tpch_cluster'};

drop snapshot tpch_cluster;
