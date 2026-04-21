drop table if exists vector_ivfflat_include_phase1;

create table vector_ivfflat_include_phase1(
  id int primary key,
  embedding vecf32(3),
  title varchar(20),
  category int
);

insert into vector_ivfflat_include_phase1 values
  (1, "[1,2,3]", "alpha", 10),
  (2, "[2,3,4]", "beta", 20),
  (3, "[3,4,5]", "gamma", 30);

create index idx_ivf_include using ivfflat on vector_ivfflat_include_phase1(embedding)
  lists=2 op_type "vector_l2_ops" include (title, category);

show create table vector_ivfflat_include_phase1;
show index from vector_ivfflat_include_phase1;

drop table vector_ivfflat_include_phase1;
