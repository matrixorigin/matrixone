-- #30570: MODUMP QUERY_RESULT for an unknown id returns a deterministic error
-- and the same connection stays usable for the next statement. (Positive
-- suffix behavior is asserted by the unit tests and the end-to-end evidence;
-- the qid of a saved result is nondeterministic, so a static BVT file cannot
-- pin it.)
modump query_result '00000000-0000-0000-0000-00000000dead' into '/tmp/m3_modump_bvt_error_000000000000000000000000dead.jsonl';
select 'alive-after-modump-error' as t;
select 1;
