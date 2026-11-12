-- Persisted secondary-index backfill must preserve unary NULL predicates.
drop database if exists secondary_index_null_predicates;
create database secondary_index_null_predicates;
use secondary_index_null_predicates;

create table voucher_line_details (
    voucher_line_id varchar(255) primary key,
    voucher_header_id varchar(255),
    ebeln varchar(255),
    kdauf varchar(255),
    index idx_voucher_header_id (voucher_header_id)
);
create table purchase_payment_detail (bukrs text, gjahr text, belnr text, ebeln text);
create table bill_collection (rbukrs varchar(20), gjahr varchar(20), belnr varchar(20), VBELN varchar(255));
insert into voucher_line_details values
('4000001024-1000-2025-000001','4000001024-1000-2025','P100','S100'),
('4000001024-1000-2025-000002','4000001024-1000-2025','P100','S100'),
('4000001024-1000-2025-000003','4000001024-1000-2025','',NULL),
('4000001024-1000-2025-000004','4000001024-1000-2025',NULL,''),
('4000001024-1000-2025-000005','4000001024-1000-2025','P_ONLY','S_ONLY'),
('2700000627-3000-2026-000001','2700000627-3000-2026','P200','S200'),
('2700000627-3000-2026-000002','2700000627-3000-2026','',NULL),
('2700000627-3000-2026-000003','2700000627-3000-2026',NULL,''),
('other','other','WRONG','WRONG');
insert into purchase_payment_detail values
('1000','2025','4000001024','P100'), ('1000','2025','4000001024','P300'),
('1000','2025','4000001024',NULL), ('1000','2025','4000001024',''),
('3000','2026','2700000627','P200'), ('3000','2026','2700000627','P400'),
('3000','2025','4000001024','WRONG');
insert into bill_collection values
('1000','2025','4000001024','S100'), ('1000','2025','4000001024','S300'),
('1000','2025','4000001024',NULL), ('1000','2025','4000001024',''),
('3000','2026','2700000627','S200'), ('3000','2026','2700000627','S400'),
('3000','2025','4000001024','WRONG');
select mo_ctl('dn', 'flush', 'secondary_index_null_predicates.voucher_line_details');
select mo_ctl('dn', 'flush', 'secondary_index_null_predicates.purchase_payment_detail');
select mo_ctl('dn', 'flush', 'secondary_index_null_predicates.bill_collection');

-- Explicitly verify the public path reaches index backfill.
-- @regex("Index Table Scan",true)
explain select ebeln from voucher_line_details force index (idx_voucher_header_id)
where voucher_header_id='4000001024-1000-2025' and ebeln is not null and ebeln<>'';
select voucher_line_id from voucher_line_details force index (idx_voucher_header_id)
where voucher_header_id='4000001024-1000-2025' and ebeln is null order by voucher_line_id;
select voucher_line_id from voucher_line_details ignore index (idx_voucher_header_id)
where voucher_header_id='4000001024-1000-2025' and ebeln is null order by voucher_line_id;

-- Query form 1: original, exact count, and preview.
SELECT DISTINCT ebeln AS `采购订单号` FROM (SELECT vld.ebeln AS ebeln FROM voucher_line_details vld WHERE vld.voucher_header_id = '4000001024-1000-2025' AND vld.ebeln IS NOT NULL AND vld.ebeln != ''
UNION
SELECT ppd.ebeln AS ebeln FROM purchase_payment_detail ppd WHERE ppd.bukrs = '1000' AND ppd.gjahr = '2025' AND ppd.belnr = '4000001024' AND ppd.ebeln IS NOT NULL AND ppd.ebeln != '') t;
SELECT COUNT(1) AS __query_sql_total_count FROM (
SELECT DISTINCT ebeln AS `采购订单号` FROM (SELECT vld.ebeln AS ebeln FROM voucher_line_details vld WHERE vld.voucher_header_id = '4000001024-1000-2025' AND vld.ebeln IS NOT NULL AND vld.ebeln != ''
UNION
SELECT ppd.ebeln AS ebeln FROM purchase_payment_detail ppd WHERE ppd.bukrs = '1000' AND ppd.gjahr = '2025' AND ppd.belnr = '4000001024' AND ppd.ebeln IS NOT NULL AND ppd.ebeln != '') t
) AS query_sql_count_source;
SELECT * FROM (
SELECT DISTINCT ebeln AS `采购订单号` FROM (SELECT vld.ebeln AS ebeln FROM voucher_line_details vld WHERE vld.voucher_header_id = '4000001024-1000-2025' AND vld.ebeln IS NOT NULL AND vld.ebeln != ''
UNION
SELECT ppd.ebeln AS ebeln FROM purchase_payment_detail ppd WHERE ppd.bukrs = '1000' AND ppd.gjahr = '2025' AND ppd.belnr = '4000001024' AND ppd.ebeln IS NOT NULL AND ppd.ebeln != '') t
) AS query_sql_preview_source LIMIT 200;

-- Query form 2: original, exact count, and preview.
SELECT DISTINCT kdauf AS sales_order FROM voucher_line_details WHERE voucher_header_id = '2700000627-3000-2026' AND kdauf IS NOT NULL AND kdauf <> ''
UNION
SELECT DISTINCT VBELN AS sales_order FROM bill_collection WHERE rbukrs = '3000' AND gjahr = '2026' AND belnr = '2700000627' AND VBELN IS NOT NULL AND VBELN <> '';
SELECT COUNT(1) AS __query_sql_total_count FROM (
SELECT DISTINCT kdauf AS sales_order FROM voucher_line_details WHERE voucher_header_id = '2700000627-3000-2026' AND kdauf IS NOT NULL AND kdauf <> ''
UNION
SELECT DISTINCT VBELN AS sales_order FROM bill_collection WHERE rbukrs = '3000' AND gjahr = '2026' AND belnr = '2700000627' AND VBELN IS NOT NULL AND VBELN <> ''
) AS query_sql_count_source;
SELECT * FROM (
SELECT DISTINCT kdauf AS sales_order FROM voucher_line_details WHERE voucher_header_id = '2700000627-3000-2026' AND kdauf IS NOT NULL AND kdauf <> ''
UNION
SELECT DISTINCT VBELN AS sales_order FROM bill_collection WHERE rbukrs = '3000' AND gjahr = '2026' AND belnr = '2700000627' AND VBELN IS NOT NULL AND VBELN <> ''
) AS query_sql_preview_source LIMIT 200;

-- Query form 3: original, exact count, and preview.
SELECT DISTINCT vld.kdauf AS sales_order FROM voucher_line_details vld WHERE vld.voucher_header_id = '4000001024-1000-2025' AND vld.kdauf IS NOT NULL AND vld.kdauf != ''
UNION
SELECT DISTINCT bc.VBELN AS sales_order FROM bill_collection bc WHERE bc.rbukrs = '1000' AND bc.gjahr = '2025' AND bc.belnr = '4000001024' AND bc.VBELN IS NOT NULL AND bc.VBELN != '';
SELECT COUNT(1) AS __query_sql_total_count FROM (
SELECT DISTINCT vld.kdauf AS sales_order FROM voucher_line_details vld WHERE vld.voucher_header_id = '4000001024-1000-2025' AND vld.kdauf IS NOT NULL AND vld.kdauf != ''
UNION
SELECT DISTINCT bc.VBELN AS sales_order FROM bill_collection bc WHERE bc.rbukrs = '1000' AND bc.gjahr = '2025' AND bc.belnr = '4000001024' AND bc.VBELN IS NOT NULL AND bc.VBELN != ''
) AS query_sql_count_source;
SELECT * FROM (
SELECT DISTINCT vld.kdauf AS sales_order FROM voucher_line_details vld WHERE vld.voucher_header_id = '4000001024-1000-2025' AND vld.kdauf IS NOT NULL AND vld.kdauf != ''
UNION
SELECT DISTINCT bc.VBELN AS sales_order FROM bill_collection bc WHERE bc.rbukrs = '1000' AND bc.gjahr = '2025' AND bc.belnr = '4000001024' AND bc.VBELN IS NOT NULL AND bc.VBELN != ''
) AS query_sql_preview_source LIMIT 200;

-- Query form 4: original, exact count, and preview.
SELECT DISTINCT ebeln AS po_order FROM voucher_line_details WHERE voucher_header_id = '2700000627-3000-2026' AND ebeln IS NOT NULL AND ebeln <> ''
UNION
SELECT DISTINCT ebeln AS po_order FROM purchase_payment_detail WHERE bukrs = '3000' AND gjahr = '2026' AND belnr = '2700000627' AND ebeln IS NOT NULL AND ebeln <> '';
SELECT COUNT(1) AS __query_sql_total_count FROM (
SELECT DISTINCT ebeln AS po_order FROM voucher_line_details WHERE voucher_header_id = '2700000627-3000-2026' AND ebeln IS NOT NULL AND ebeln <> ''
UNION
SELECT DISTINCT ebeln AS po_order FROM purchase_payment_detail WHERE bukrs = '3000' AND gjahr = '2026' AND belnr = '2700000627' AND ebeln IS NOT NULL AND ebeln <> ''
) AS query_sql_count_source;
SELECT * FROM (
SELECT DISTINCT ebeln AS po_order FROM voucher_line_details WHERE voucher_header_id = '2700000627-3000-2026' AND ebeln IS NOT NULL AND ebeln <> ''
UNION
SELECT DISTINCT ebeln AS po_order FROM purchase_payment_detail WHERE bukrs = '3000' AND gjahr = '2026' AND belnr = '2700000627' AND ebeln IS NOT NULL AND ebeln <> ''
) AS query_sql_preview_source LIMIT 200;

-- Query form 5: original, exact count, and preview.
SELECT DISTINCT vld.ebeln AS po_number FROM voucher_line_details vld WHERE vld.voucher_header_id = '4000001024-1000-2025' AND vld.ebeln IS NOT NULL AND vld.ebeln != ''
UNION
SELECT DISTINCT ppd.ebeln AS po_number FROM purchase_payment_detail ppd WHERE ppd.bukrs = '1000' AND ppd.gjahr = '2025' AND ppd.belnr = '4000001024' AND ppd.ebeln IS NOT NULL AND ppd.ebeln != '';
SELECT COUNT(1) AS __query_sql_total_count FROM (
SELECT DISTINCT vld.ebeln AS po_number FROM voucher_line_details vld WHERE vld.voucher_header_id = '4000001024-1000-2025' AND vld.ebeln IS NOT NULL AND vld.ebeln != ''
UNION
SELECT DISTINCT ppd.ebeln AS po_number FROM purchase_payment_detail ppd WHERE ppd.bukrs = '1000' AND ppd.gjahr = '2025' AND ppd.belnr = '4000001024' AND ppd.ebeln IS NOT NULL AND ppd.ebeln != ''
) AS query_sql_count_source;
SELECT * FROM (
SELECT DISTINCT vld.ebeln AS po_number FROM voucher_line_details vld WHERE vld.voucher_header_id = '4000001024-1000-2025' AND vld.ebeln IS NOT NULL AND vld.ebeln != ''
UNION
SELECT DISTINCT ppd.ebeln AS po_number FROM purchase_payment_detail ppd WHERE ppd.bukrs = '1000' AND ppd.gjahr = '2025' AND ppd.belnr = '4000001024' AND ppd.ebeln IS NOT NULL AND ppd.ebeln != ''
) AS query_sql_preview_source LIMIT 200;

-- Newly added columns have no metadata in older persisted objects.
-- Keep both predicates conservative until residual row evaluation.
alter table voucher_line_details add column added varchar(30);
select count(*) from voucher_line_details where added is null;
select count(*) from voucher_line_details where added is not null;
select voucher_line_id from voucher_line_details force index (idx_voucher_header_id)
where voucher_header_id='4000001024-1000-2025' and added is null order by voucher_line_id;
select voucher_line_id from voucher_line_details force index (idx_voucher_header_id)
where voucher_header_id='4000001024-1000-2025' and added is not null order by voucher_line_id;
-- Quoted aliases and columns can both contain dots. NULL pruning must use
-- the intended column, and ambiguous flattened names must keep residual filtering.
create table null_aliases (id int primary key, v int, `a.v` int);
insert into null_aliases values (1, NULL, 9), (2, 2, NULL);
select mo_ctl('dn', 'flush', 'secondary_index_null_predicates.null_aliases');
select id from null_aliases as x where x.id=1 and x.v is null;
select id from null_aliases as `x.y` where `x.y`.id=1 and `x.y`.v is null;
select id from null_aliases as `x.y` where `x.y`.id=2 and `x.y`.v is not null;
select id from null_aliases as `x.y` where `x.y`.id=2 and `x.y`.`a.v` is null;
select id from null_aliases as `x.y` where `x.y`.id=1 and `x.y`.`a.v` is not null;
select id from null_aliases where id=2 and `a.v` is null;
select id from null_aliases as x where x.id=2 and x.`a.v` is null;
-- A non-leading primary key must never borrow the first column's ZoneMap.
create table nonleading_pk (k int, id int primary key, v int);
insert into nonleading_pk values (100, 1, NULL), (200, 2, 2);
select mo_ctl('dn', 'flush', 'secondary_index_null_predicates.nonleading_pk');
select id from nonleading_pk as x where x.id=1 and x.v is null;
select id from nonleading_pk as `x.y` where `x.y`.id=1 and `x.y`.v is null;
select id from nonleading_pk as `x.y` where `x.y`.id<2 and `x.y`.v is null;
select id from nonleading_pk as `x.y` where `x.y`.id<=1 and `x.y`.v is null;
select id from nonleading_pk as `x.y` where `x.y`.id>0 and `x.y`.v is null;
select id from nonleading_pk as `x.y` where `x.y`.id>=1 and `x.y`.v is null;
select id from nonleading_pk as `x.y` where `x.y`.id between 1 and 2 and `x.y`.v is null;
select id from nonleading_pk as `x.y` where `x.y`.id in (1,2) and `x.y`.v is null;
select id from nonleading_pk as `x.y` where `x.y`.id=2 and `x.y`.v is not null;
select id from nonleading_pk as `x.y`
where (`x.y`.id=1 and `x.y`.v is null) or (`x.y`.id=2 and `x.y`.v is not null) order by id;
drop database secondary_index_null_predicates;
