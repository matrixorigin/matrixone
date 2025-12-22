create database if not exists vec_session_var_db;
use vec_session_var_db;
CREATE TABLE `mini_vector_data` (`id` VARCHAR(64) NOT NULL, `text` TEXT DEFAULT NULL, `vec` vecf32(8) DEFAULT NULL COMMENT '8-dim embedding vector', `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`));
CREATE INDEX `idx_vec` USING ivfflat ON `mini_vector_data` (`vec`) lists = 16 op_type 'vector_l2_ops';
INSERT INTO mini_vector_data (id, text, vec) VALUES ('id1','hello world','[0.12,-0.33,0.51,0.27,-0.48,0.19,0.72,-0.11]');
INSERT INTO mini_vector_data (id, text, vec) VALUES ('id2','greeting message','[0.44,0.09,-0.31,0.62,0.18,-0.55,0.21,0.33]');
INSERT INTO mini_vector_data (id, text, vec) VALUES ('id3','vector search test','[-0.21,0.73,-0.12,0.51,-0.44,0.09,0.14,0.82]');

-- Default behavior (no session var)
select id, text, vec from mini_vector_data order by l2_distance(vec, '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]') limit 1;

-- Set session variable
set pre_filter_for_vector_test = 1;

-- Test 1: Should trigger pushdown path (variable logic)
select id, text, vec from mini_vector_data order by l2_distance(vec, '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]') limit 1;

-- Test 2: mode=force should take priority (disable index)
select id, text, vec from mini_vector_data order by l2_distance(vec, '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]') limit 1 by rank with option 'mode=force';

-- Test 3: mode=post should take priority over variable (disable pushdown)
select id, text, vec from mini_vector_data order by l2_distance(vec, '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]') limit 1 by rank with option 'mode=post';

-- Test 4: mode=pre should explicitly enable pushdown (consistent with variable)
select id, text, vec from mini_vector_data order by l2_distance(vec, '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]') limit 1 by rank with option 'mode=pre';

set pre_filter_for_vector_test = 0;

drop database vec_session_var_db;
