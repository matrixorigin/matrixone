drop table if exists issue_24211_load_empty_numeric;
create table issue_24211_load_empty_numeric(
  id int,
  qty int,
  amount decimal(7,2),
  profit decimal(7,2),
  label varchar(32)
);

load data infile '$resources/load_data/issue_24211_empty_numeric.csv'
into table issue_24211_load_empty_numeric
fields terminated by '|'
lines terminated by '\n'
parallel 'true';

select
  id,
  qty,
  amount,
  profit,
  label
from issue_24211_load_empty_numeric
order by id;

select
  sum(qty),
  sum(amount),
  sum(ifnull(profit, 0)),
  count(profit)
from issue_24211_load_empty_numeric;

drop table if exists issue_24211_load_empty_numeric_nonparallel;
create table issue_24211_load_empty_numeric_nonparallel(
  id int,
  qty int,
  amount decimal(7,2),
  profit decimal(7,2),
  label varchar(32)
);

load data infile '$resources/load_data/issue_24211_empty_numeric.csv'
into table issue_24211_load_empty_numeric_nonparallel
fields terminated by '|'
lines terminated by '\n';

select
  id,
  qty,
  amount,
  profit,
  label
from issue_24211_load_empty_numeric_nonparallel
order by id;

drop table issue_24211_load_empty_numeric_nonparallel;
drop table issue_24211_load_empty_numeric;
