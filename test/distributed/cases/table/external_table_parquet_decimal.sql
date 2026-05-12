drop database if exists parquet_decimal_db;
create database parquet_decimal_db;
use parquet_decimal_db;

create external table catalog_returns (cr_item_sk int, cr_net_loss decimal(12,2)) infile{'filepath'='$resources/load_data/decimal.parq', 'format'='parquet'};

select cr_item_sk, cr_net_loss from catalog_returns order by cr_item_sk;
select sum(cr_net_loss) as total_loss from catalog_returns;

drop database parquet_decimal_db;
