-- Public SQL conversion of the JSON/VARIANT semantic baseline from issue
-- #27374. The workflow uses a generated 10K-row fixture; BVT keeps the same
-- edge dimensions in a small deterministic fixture so it can run quickly.
--
-- JSON parser indexes (key, value) tuples. It is not a free-text MATCH index;
-- JSON path comparisons are the supported indexed query surface.

drop database if exists json_variant_baseline_bvt;
create database json_variant_baseline_bvt;
use json_variant_baseline_bvt;

set experimental_fulltext2_index = 1;

create table observations (
    id bigint not null,
    trace_id varchar(128) not null,
    case_tag varchar(64) not null,
    payload json null,
    primary key (id)
);

create table sql_null_observations (
    id bigint primary key,
    payload json null
);

create table malformed_observations (
    raw_data json not null
);

-- C001-C008: missing paths, JSON null, scalar types, arrays, deep paths,
-- large values, key reordering, and path evolution.
insert into observations values
    (1, 'trace-edge-missing', 'missing_path',
        '{"kind":"edge","attr":{"present":"yes"}}'),
    (2, 'trace-edge-json-null', 'json_null',
        '{"kind":"edge","optional":null}'),
    (3, 'trace-edge-int', 'mixed_int',
        '{"kind":"mixed","value":42}'),
    (4, 'trace-edge-string', 'mixed_string',
        '{"kind":"mixed","value":"42","other":"deployment"}'),
    (5, 'trace-edge-bool', 'mixed_bool',
        '{"kind":"mixed","value":true}'),
    (6, 'trace-edge-array', 'array_object',
        '{"kind":"edge","items":[1,"two",{"name":"three"}],"attr":{"array_length":3}}'),
    (7, 'trace-edge-deep', 'deep_nesting',
        '{"kind":"edge","deep":{"level_1":{"level_2":{"level_3":{"level_4":{"level_5":{"level_6":{"level_7":{"level_8":"leaf"}}}}}}}}}'),
    (9, 'trace-edge-order-a', 'key_order_a',
        '{"kind":"order","first":1,"second":2,"nested":{"a":3,"b":4}}'),
    (10, 'trace-edge-order-b', 'key_order_b',
        '{"nested":{"b":4,"a":3},"second":2,"kind":"order","first":1}'),
    (11, 'trace-edge-evolution-old', 'path_evolution_old',
        '{"kind":"evolution","schema_version":1,"old_path":"old"}'),
    (12, 'trace-edge-evolution-new', 'path_evolution_new',
        '{"kind":"evolution","schema_version":2,"new_path":"new","old_path":"old"}'),
    (13, 'trace-generated-000013', 'generated',
        '{"kind":"generated","value":13,"message":"timeout retry deployment rollback","attr":{"tenant":"tenant_06","release_ring":"stable","version":2},"items":[2,{"rank":0}]}'),
    (14, 'trace-generated-000014', 'generated',
        '{"kind":"generated","value":14,"message":"deployment only","attr":{"tenant":"tenant_00","release_ring":"canary","version":3},"items":[3,{"rank":1}]}');

-- Keep the workflow's 64 KiB large-value boundary without checking in a
-- generated fixture file.
insert into observations values
    (8, 'trace-edge-large', 'large_value',
        json_object('kind', 'edge', 'large', concat(repeat('L', 65535), 'L')));

-- C001: accepted row count.
select count(*) as accepted_rows from observations;

-- C002: a missing path and a present JSON null remain distinguishable.
select id,
       json_contains_path(payload, 'one', '$.optional') as path_present,
       json_type(json_extract(payload, '$.optional')) as optional_type,
       json_extract(payload, '$.optional') as optional_value
from observations
where id in (1, 2)
order by id;

-- C003: integer, string, and boolean JSON scalar types retain their type.
select id,
       json_type(json_extract(payload, '$.value')) as value_type,
       json_extract(payload, '$.value') as value
from observations
where id in (3, 4, 5)
order by id;

-- C004: public array expansion through UNNEST.
select u.*
from observations as t, unnest(t.payload, '$.items') as u
where t.id = 6
order by u.seq;

-- C005: deep path extraction.
select json_unquote(json_extract(payload,
       '$.deep.level_1.level_2.level_3.level_4.level_5.level_6.level_7.level_8')) as leaf
from observations
where id = 7;

-- C006: large value preservation.
select char_length(json_unquote(json_extract(payload, '$.large'))) as value_length
from observations
where id = 8;

-- C007: key order does not change projected values.
select id,
       json_extract(payload, '$.first') as first_value,
       json_extract(payload, '$.second') as second_value,
       json_extract(payload, '$.nested.a') as nested_a,
       json_extract(payload, '$.nested.b') as nested_b
from observations
where id in (9, 10)
order by id;

-- C008: old and new paths can coexist during schema evolution.
select id,
       json_extract(payload, '$.old_path') as old_path,
       json_extract(payload, '$.new_path') as new_path
from observations
where id in (11, 12)
order by id;

-- C009: SQL NULL and JSON null are separate states.
insert into sql_null_observations values
    (1, null),
    (2, cast('null' as json));
select id,
       payload is null as is_sql_null,
       json_type(payload) as json_type
from sql_null_observations
order by id;

-- C010: array cardinality through the same public UNNEST path.
select count(*) as element_count
from observations as t, unnest(t.payload, '$.items') as u
where t.id = 6;

-- C011: aggregation over an extracted JSON path.
select json_unquote(json_extract(payload, '$.kind')) as kind,
       count(*) as row_count
from observations
group by kind
order by kind;

-- C012: portable text retrieval remains available without a native text
-- parser. Only row 13 contains the ordered deployment/rollback phrase.
select count(*) as message_rows
from observations
where json_unquote(json_extract(payload, '$.message')) like '%deployment%rollback%';

-- C014: a JSON tuple index is deliberately not a free-text MATCH index. The
-- old benchmark expected this to equal C012, which is stale after #27821.
create fulltext2 index ft_payload on observations(payload) with parser json;
select count(*) as bare_match_rows
from observations
where match(payload) against('deployment' in boolean mode);

-- JSON path equality is the indexed/native replacement and remains key-aware.
select id
from observations
where json_extract_string(payload, '$.message') = 'timeout retry deployment rollback'
order by id;
select id
from observations
where json_extract_string(payload, '$.other') = 'deployment'
order by id;

-- C013: malformed JSON is rejected and cannot add a row.
insert into malformed_observations values ('{"id":"not-an-integer","payload":');
select count(*) as accepted_rows from malformed_observations;

-- C015: direct JSON INSERT persists typed values.
insert into observations (id, trace_id, case_tag, payload)
values (20001, 'trace-direct-insert', 'direct_insert',
        cast('{"kind":"insert","value":123}' as json));
select id,
       json_unquote(json_extract(payload, '$.kind')) as kind,
       json_extract(payload, '$.value') as value
from observations
where id = 20001;

drop database json_variant_baseline_bvt;
