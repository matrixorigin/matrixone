drop account if exists view_columns_subscriber;
create account view_columns_subscriber admin_name = 'admin' identified by '111';
drop publication if exists view_columns_publication;
drop database if exists view_columns_source;
create database view_columns_source;
create table view_columns_source.source_table (value bigint);
create view view_columns_source.published_view as
select value from view_columns_source.source_table;
create publication view_columns_publication database view_columns_source account view_columns_subscriber;

-- @session:id=1&user=view_columns_subscriber:admin&password=111
create database view_columns_subscription from sys publication view_columns_publication;
select * from view_columns_subscription.published_view;
desc view_columns_subscription.published_view;
show columns from view_columns_subscription.published_view;
drop database view_columns_subscription;
-- @session

drop publication view_columns_publication;
drop database view_columns_source;
drop account view_columns_subscriber;
