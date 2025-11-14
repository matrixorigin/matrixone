-- @ignore:0
select mo_ctl("cn", "prefetch-on-subscribed", "'^mo_catalog\.mo_tables$'");
-- @ignore:0
select mo_ctl("cn", "prefetch-on-subscribed", "'^mysql\..*$','^test1\..*$'");
-- @ignore:0
select mo_ctl("cn", "prefetch-on-subscribed", "'.*'");
-- @ignore:0
select mo_ctl("cn", "prefetch-on-subscribed", "'clean all'");