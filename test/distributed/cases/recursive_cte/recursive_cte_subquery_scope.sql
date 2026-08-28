with recursive r(n) as (
    select 1
    union all
    select n + 1
    from r
    where exists (select 1 from r nested)
      and n < 3
)
select * from r;

select 'after exists error' as service_status;

with recursive r(n) as (
    select 1
    union all
    select (
        select nested.n + 1
        from r nested
        where nested.n = r.n
    )
    from r
    where n < 3
)
select * from r;

select 'after scalar error' as service_status;

with recursive r(n) as (
    select 1
    union all
    select n + 1
    from r
    where n < 3
      and exists (
        with recursive x(m) as (
            select 1
            union all
            select x.m + 1
            from x join r z on false
        )
        select 1 from x
      )
)
select * from r;

select 'after nested recursive error' as service_status;

with recursive r(n) as (
    select 1
    union all
    select n + 1
    from r
    where exists (select 1 where r.n < 3)
)
select * from r order by n;
