-- @suit
-- @case
-- @desc: issue #25908 - file pruning matches exact virtual columns and rejects mixed physical filters
-- @label:bvt

drop table if exists issue_25908_ext;
create external table issue_25908_ext (
    account_id varchar(32),
    accounting varchar(32),
    customer_account varchar(32),
    payload varchar(32)
) infile{'filepath'='$resources/external_table_file/issue_25908.csv'}
fields terminated by ',' lines terminated by '\n';

select account_id, payload
from issue_25908_ext
where issue_25908_ext.account_id = 'normal';

select accounting, payload
from issue_25908_ext
where accounting = 'cash';

select customer_account, payload
from issue_25908_ext
where customer_account = 'customer-a';

select payload
from issue_25908_ext as account_source
where account_source.payload = 'second';

select count(*) as cnt
from issue_25908_ext
where account_id = 'missing';

select count(*) as cnt
from issue_25908_ext
where __mo_filepath = account_id;

drop table issue_25908_ext;
