-- @separator:table
select mo_ctl('dn','DiskCleaner','force_gc');
-- @ignore:0,1
select mo_ctl('dn','DiskCleaner','details');
-- @ignore:0,1
select mo_ctl('dn','DiskCleaner','verify');
