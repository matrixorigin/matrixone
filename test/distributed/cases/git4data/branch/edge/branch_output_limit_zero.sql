-- DATA BRANCH DIFF OUTPUT LIMIT must obey zero and adjacent boundaries.

drop database if exists branch_output_limit_zero;
create database branch_output_limit_zero;
use branch_output_limit_zero;

create table base(id int primary key, v int);
insert into base values (1, 10), (2, 20), (3, 30);
data branch create table child from base;
update child set v = v + 1;
insert into child values (4, 40);

-- Zero rows, both with and without column projection.
data branch diff child against base output limit 0;
data branch diff child against base columns (id) output limit 0;

-- Adjacent and upper boundaries retain their existing behavior.
data branch diff child against base output limit 1;
data branch diff child against base output limit 4;
data branch diff child against base output limit 5;

drop database branch_output_limit_zero;
