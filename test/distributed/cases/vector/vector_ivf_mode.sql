create database if not exists dd1;
use dd1;
CREATE TABLE `mini_vector_data` (`id` VARCHAR(64) NOT NULL, `text` TEXT DEFAULT NULL, `vec` vecf32(8) DEFAULT NULL COMMENT '8-dim embedding vector', `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`));
CREATE INDEX `idx_vec` USING ivfflat ON `mini_vector_data` (`vec`) lists = 16 op_type 'vector_l2_ops';
INSERT INTO mini_vector_data (id, text, vec) VALUES ('id1','hello world','[0.12,-0.33,0.51,0.27,-0.48,0.19,0.72,-0.11]');
INSERT INTO mini_vector_data (id, text, vec) VALUES ('id2','greeting message','[0.44,0.09,-0.31,0.62,0.18,-0.55,0.21,0.33]');
INSERT INTO mini_vector_data (id, text, vec) VALUES ('id3','vector search test','[-0.21,0.73,-0.12,0.51,-0.44,0.09,0.14,0.82]');
INSERT INTO mini_vector_data (id, text, vec) VALUES ('id4','example data','[0.51,0.25,0.09,-0.33,0.74,-0.28,0.41,0.17]');
INSERT INTO mini_vector_data (id, text, vec) VALUES ('id5','embedding sample','[0.08,-0.41,0.66,0.12,0.53,-0.15,0.38,-0.22]');
INSERT INTO mini_vector_data (id, text, vec) VALUES ('id6','short text','[0.32,0.71,-0.26,0.14,-0.03,0.66,-0.41,0.19]');
INSERT INTO mini_vector_data (id, text, vec) VALUES ('id7','random note','[-0.55,0.12,0.44,-0.19,0.27,0.51,0.11,-0.03]');
INSERT INTO mini_vector_data (id, text, vec) VALUES ('id8','semantic item','[0.23,-0.14,0.82,0.39,-0.52,0.08,0.26,0.73]');
INSERT INTO mini_vector_data (id, text, vec) VALUES ('id9','testing vectors','[0.61,0.33,-0.48,0.09,0.12,0.54,-0.29,0.41]');
INSERT INTO mini_vector_data (id, text, vec) VALUES ('id10','additional entry','[0.19,-0.22,0.33,0.77,-0.04,0.12,0.58,-0.31]');
WITH q AS (SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist FROM mini_vector_data) SELECT * FROM q ORDER BY dist LIMIT 5 by rank with option 'mode=pre';
WITH q AS (SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist FROM mini_vector_data) SELECT * FROM q ORDER BY dist LIMIT 5 by rank with option 'mode=post';
WITH q AS (SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist FROM mini_vector_data) SELECT * FROM q ORDER BY dist LIMIT 5 by rank with option 'mode=force';
CREATE TABLE mini_embed_data (id VARCHAR(64) NOT NULL, embedding VECF32(8) DEFAULT NULL, content TEXT DEFAULT NULL, description VARCHAR(255) DEFAULT NULL, file_id VARCHAR(64) DEFAULT NULL, score FLOAT DEFAULT NULL, disabled TINYINT DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id), KEY idx_file_id (file_id));
CREATE INDEX idx_vec_embedding USING ivfflat ON mini_embed_data (embedding) LISTS = 16 OP_TYPE 'vector_cosine_ops';
INSERT INTO mini_embed_data (id, embedding, content, description, file_id, score, disabled) VALUES('id01','[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]','ai is artificial intelligence','what is ai?','file01',0.0,0);
INSERT INTO mini_embed_data (id, embedding, content, description, file_id, score, disabled) VALUES('id02','[0.01,0.03,0.05,0.07,0.09,0.11,0.13,0.15]','sql is structured query language','what is sql?','file01',0.0,0);
INSERT INTO mini_embed_data (id, embedding, content, description, file_id, score, disabled) VALUES('id03','[0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2]','it stores high dimensional vectors','what is vector db?','file02',0.0,0);
INSERT INTO mini_embed_data (id, embedding, content, description, file_id, score, disabled) VALUES('id04','[0.11,0.22,0.33,0.44,0.55,0.66,0.77,0.88]','mysql is a relational database','what is mysql?','file02',0.0,0);
INSERT INTO mini_embed_data (id, embedding, content, description, file_id, score, disabled) VALUES('id05','[0.12,0.32,0.52,0.72,0.11,0.31,0.51,0.71]','an ai assistant by openai','what is chatgpt?','file01',0.0,0);
SELECT mini_embed_data.id, mini_embed_data.content, mini_embed_data.description, mini_embed_data.embedding, mini_embed_data.file_id, mini_embed_data.disabled FROM mini_embed_data WHERE mini_embed_data.file_id IN ('file01','file02') AND mini_embed_data.embedding IS NOT NULL AND (mini_embed_data.disabled IS NULL OR mini_embed_data.disabled = false) ORDER BY cosine_distance(mini_embed_data.embedding,"[0.12,0.55,0.33,0.88,0.22,0.44,0.66,0.11]") DESC LIMIT 10 by rank with option 'mode=pre';
SELECT mini_embed_data.id, mini_embed_data.content, mini_embed_data.description, mini_embed_data.embedding, mini_embed_data.file_id, mini_embed_data.disabled FROM mini_embed_data WHERE mini_embed_data.file_id IN ('file01','file02') AND mini_embed_data.embedding IS NOT NULL AND (mini_embed_data.disabled IS NULL OR mini_embed_data.disabled = false) ORDER BY cosine_distance(mini_embed_data.embedding,"[0.12,0.55,0.33,0.88,0.22,0.44,0.66,0.11]") DESC LIMIT 10 by rank with option 'mode=post';
SELECT mini_embed_data.id, mini_embed_data.content, mini_embed_data.description, mini_embed_data.embedding, mini_embed_data.file_id, mini_embed_data.disabled FROM mini_embed_data WHERE mini_embed_data.file_id IN ('file01','file02') AND mini_embed_data.embedding IS NOT NULL AND (mini_embed_data.disabled IS NULL OR mini_embed_data.disabled = false) ORDER BY cosine_distance(mini_embed_data.embedding,"[0.12,0.55,0.33,0.88,0.22,0.44,0.66,0.11]") DESC LIMIT 10 by rank with option 'mode=force';
-- ============================================================================
-- Additional Test Cases for IVF Mode
-- ============================================================================

-- Test Case: mode=pre with different LIMIT sizes (testing over-fetch factor)
-- Small limit (< 10): should use 5x over-fetch
WITH q AS (SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist FROM mini_vector_data WHERE id LIKE 'id%') SELECT * FROM q ORDER BY dist LIMIT 3 by rank with option 'mode=pre';

-- Medium limit (10-50): should use 2x over-fetch
WITH q AS (SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist FROM mini_vector_data WHERE id LIKE 'id%') SELECT * FROM q ORDER BY dist LIMIT 8 by rank with option 'mode=pre';

-- Test Case: mode=post with different LIMIT sizes
WITH q AS (SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist FROM mini_vector_data WHERE id LIKE 'id%') SELECT * FROM q ORDER BY dist LIMIT 3 by rank with option 'mode=post';

-- Test Case: mode=force (disable vector index) with filters
WITH q AS (SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist FROM mini_vector_data WHERE id LIKE 'id%') SELECT * FROM q ORDER BY dist LIMIT 3 by rank with option 'mode=force';

-- Test Case: mode=pre with multiple filters (complex WHERE clause)
SELECT mini_embed_data.id, mini_embed_data.content FROM mini_embed_data WHERE mini_embed_data.file_id IN ('file01','file02') AND mini_embed_data.embedding IS NOT NULL AND mini_embed_data.disabled = 0 AND mini_embed_data.score = 0.0 ORDER BY cosine_distance(mini_embed_data.embedding,"[0.12,0.55,0.33,0.88,0.22,0.44,0.66,0.11]") DESC LIMIT 3 by rank with option 'mode=pre';

-- Test Case: mode=post with multiple filters
SELECT mini_embed_data.id, mini_embed_data.content FROM mini_embed_data WHERE mini_embed_data.file_id IN ('file01','file02') AND mini_embed_data.embedding IS NOT NULL AND mini_embed_data.disabled = 0 AND mini_embed_data.score = 0.0 ORDER BY cosine_distance(mini_embed_data.embedding,"[0.12,0.55,0.33,0.88,0.22,0.44,0.66,0.11]") DESC LIMIT 3 by rank with option 'mode=post';

-- Test Case: mode=pre without filters (no over-fetch needed)
SELECT id, text FROM mini_vector_data ORDER BY l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') LIMIT 3 by rank with option 'mode=pre';

-- Test Case: mode=post without filters
SELECT id, text FROM mini_vector_data ORDER BY l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') LIMIT 3 by rank with option 'mode=post';

-- Test Case: ASC ordering with mode=pre
WITH q AS (SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist FROM mini_vector_data) SELECT * FROM q ORDER BY dist ASC LIMIT 3 by rank with option 'mode=pre';

-- Test Case: DESC ordering with cosine_distance and mode=pre
WITH q AS (SELECT id, text, cosine_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist FROM mini_vector_data) SELECT * FROM q ORDER BY dist DESC LIMIT 3 by rank with option 'mode=pre';

-- Test Case: LIMIT 1 (edge case for smallest limit)
WITH q AS (SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist FROM mini_vector_data WHERE id LIKE 'id%') SELECT * FROM q ORDER BY dist LIMIT 1 by rank with option 'mode=pre';

-- Test Case: Large LIMIT (>= 200, should use 1.2x over-fetch)
SELECT mini_embed_data.id FROM mini_embed_data WHERE mini_embed_data.file_id IN ('file01','file02') ORDER BY cosine_distance(mini_embed_data.embedding,"[0.12,0.55,0.33,0.88,0.22,0.44,0.66,0.11]") DESC LIMIT 5 by rank with option 'mode=pre';

-- Test Case: mode=pre with NULL check in WHERE
SELECT id, content FROM mini_embed_data WHERE embedding IS NOT NULL AND file_id = 'file01' ORDER BY cosine_distance(embedding,"[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]") LIMIT 2 by rank with option 'mode=pre';

-- Test Case: mode=post with NULL check
SELECT id, content FROM mini_embed_data WHERE embedding IS NOT NULL AND file_id = 'file01' ORDER BY cosine_distance(embedding,"[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]") LIMIT 2 by rank with option 'mode=post';

-- Test Case: Default mode (no mode specified, should behave like mode=post)
WITH q AS (SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist FROM mini_vector_data WHERE id LIKE 'id%') SELECT * FROM q ORDER BY dist LIMIT 3;

-- Test Case: mode=pre with OFFSET
WITH q AS (SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist FROM mini_vector_data) SELECT * FROM q ORDER BY dist LIMIT 3 OFFSET 2 by rank with option 'mode=pre';

-- Test Case: mode=post with OFFSET
WITH q AS (SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist FROM mini_vector_data) SELECT * FROM q ORDER BY dist LIMIT 3 OFFSET 2 by rank with option 'mode=post';

-- ============================================================================
-- Test Cases: Vector Index + Regular Index Combination
-- ============================================================================

-- Create table with vector index + single regular index
CREATE TABLE vec_with_regular_idx (
    id INT PRIMARY KEY,
    category VARCHAR(50),
    status INT,
    score FLOAT,
    vec vecf32(8)
);

CREATE INDEX idx_category ON vec_with_regular_idx(category);
CREATE INDEX idx_vec_regular USING ivfflat ON vec_with_regular_idx(vec) lists=8 op_type 'vector_l2_ops';

INSERT INTO vec_with_regular_idx VALUES
(1, 'A', 1, 5.0, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'),
(2, 'A', 1, 4.5, '[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]'),
(3, 'B', 1, 4.0, '[0.3,0.4,0.5,0.6,0.7,0.8,0.9,0.1]'),
(4, 'B', 2, 3.5, '[0.4,0.5,0.6,0.7,0.8,0.9,0.1,0.2]'),
(5, 'C', 1, 3.0, '[0.5,0.6,0.7,0.8,0.9,0.1,0.2,0.3]'),
(6, 'C', 2, 2.5, '[0.6,0.7,0.8,0.9,0.1,0.2,0.3,0.4]'),
(7, 'A', 2, 2.0, '[0.7,0.8,0.9,0.1,0.2,0.3,0.4,0.5]'),
(8, 'B', 1, 1.5, '[0.8,0.9,0.1,0.2,0.3,0.4,0.5,0.6]');

-- Test Case: mode=pre with regular index filter (category)
SELECT id, category, score FROM vec_with_regular_idx
WHERE category = 'A'
ORDER BY l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]')
LIMIT 3 by rank with option 'mode=pre';

-- Test Case: mode=post with regular index filter
SELECT id, category, score FROM vec_with_regular_idx
WHERE category = 'A'
ORDER BY l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]')
LIMIT 3 by rank with option 'mode=post';

-- Test Case: mode=force with regular index filter
SELECT id, category, score FROM vec_with_regular_idx
WHERE category = 'A'
ORDER BY l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]')
LIMIT 3 by rank with option 'mode=force';

-- Create table with multiple regular indexes
CREATE TABLE vec_with_multi_idx (
    id INT PRIMARY KEY,
    category VARCHAR(50),
    status INT,
    priority INT,
    score FLOAT,
    vec vecf32(8)
);

CREATE INDEX idx_category_multi ON vec_with_multi_idx(category);
CREATE INDEX idx_status ON vec_with_multi_idx(status);
CREATE INDEX idx_priority ON vec_with_multi_idx(priority);
CREATE INDEX idx_vec_multi USING ivfflat ON vec_with_multi_idx(vec) lists=8 op_type 'vector_l2_ops';

INSERT INTO vec_with_multi_idx VALUES
(1, 'A', 1, 1, 5.0, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'),
(2, 'A', 1, 2, 4.5, '[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]'),
(3, 'B', 1, 1, 4.0, '[0.3,0.4,0.5,0.6,0.7,0.8,0.9,0.1]'),
(4, 'B', 2, 1, 3.5, '[0.4,0.5,0.6,0.7,0.8,0.9,0.1,0.2]'),
(5, 'C', 1, 2, 3.0, '[0.5,0.6,0.7,0.8,0.9,0.1,0.2,0.3]'),
(6, 'C', 2, 1, 2.5, '[0.6,0.7,0.8,0.9,0.1,0.2,0.3,0.4]'),
(7, 'A', 2, 2, 2.0, '[0.7,0.8,0.9,0.1,0.2,0.3,0.4,0.5]'),
(8, 'B', 1, 1, 1.5, '[0.8,0.9,0.1,0.2,0.3,0.4,0.5,0.6]'),
(9, 'A', 1, 1, 1.0, '[0.9,0.1,0.2,0.3,0.4,0.5,0.6,0.7]'),
(10, 'B', 2, 2, 0.5, '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]');

-- Test Case: mode=pre with multiple regular index filters (category + status)
SELECT id, category, status FROM vec_with_multi_idx
WHERE category = 'A' AND status = 1
ORDER BY l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]')
LIMIT 3 by rank with option 'mode=pre';

-- Test Case: mode=post with multiple regular index filters
SELECT id, category, status FROM vec_with_multi_idx
WHERE category = 'A' AND status = 1
ORDER BY l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]')
LIMIT 3 by rank with option 'mode=post';

-- Test Case: mode=pre with three index filters (category + status + priority)
SELECT id, category, status, priority FROM vec_with_multi_idx
WHERE category = 'B' AND status = 1 AND priority = 1
ORDER BY l2_distance(vec, '[0.35,0.45,0.55,0.65,0.75,0.85,0.95,0.15]')
LIMIT 2 by rank with option 'mode=pre';

-- Test Case: mode=post with three index filters
SELECT id, category, status, priority FROM vec_with_multi_idx
WHERE category = 'B' AND status = 1 AND priority = 1
ORDER BY l2_distance(vec, '[0.35,0.45,0.55,0.65,0.75,0.85,0.95,0.15]')
LIMIT 2 by rank with option 'mode=post';

-- Test Case: mode=pre with range condition on indexed column
SELECT id, category, status FROM vec_with_multi_idx
WHERE category IN ('A', 'B') AND status = 1
ORDER BY l2_distance(vec, '[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]')
LIMIT 4 by rank with option 'mode=pre';

-- Test Case: mode=post with range condition
SELECT id, category, status FROM vec_with_multi_idx
WHERE category IN ('A', 'B') AND status = 1
ORDER BY l2_distance(vec, '[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]')
LIMIT 4 by rank with option 'mode=post';

-- Test Case: mode=pre with mixed indexed and non-indexed filters
SELECT id, category, score FROM vec_with_multi_idx
WHERE category = 'A' AND score > 2.0
ORDER BY l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]')
LIMIT 3 by rank with option 'mode=pre';

-- Test Case: mode=post with mixed filters
SELECT id, category, score FROM vec_with_multi_idx
WHERE category = 'A' AND score > 2.0
ORDER BY l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]')
LIMIT 3 by rank with option 'mode=post';

-- Test Case: Composite index scenario
CREATE TABLE vec_with_composite_idx (
    id INT PRIMARY KEY,
    category VARCHAR(50),
    status INT,
    vec vecf32(8),
    INDEX idx_composite(category, status)
);

CREATE INDEX idx_vec_composite USING ivfflat ON vec_with_composite_idx(vec) lists=8 op_type 'vector_l2_ops';

INSERT INTO vec_with_composite_idx VALUES
(1, 'A', 1, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'),
(2, 'A', 1, '[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]'),
(3, 'A', 2, '[0.3,0.4,0.5,0.6,0.7,0.8,0.9,0.1]'),
(4, 'B', 1, '[0.4,0.5,0.6,0.7,0.8,0.9,0.1,0.2]'),
(5, 'B', 2, '[0.5,0.6,0.7,0.8,0.9,0.1,0.2,0.3]');

-- Test Case: mode=pre with composite index (both columns in WHERE)
SELECT id, category, status FROM vec_with_composite_idx
WHERE category = 'A' AND status = 1
ORDER BY l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]')
LIMIT 2 by rank with option 'mode=pre';

-- Test Case: mode=post with composite index
SELECT id, category, status FROM vec_with_composite_idx
WHERE category = 'A' AND status = 1
ORDER BY l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]')
LIMIT 2 by rank with option 'mode=post';

-- Test Case: mode=pre with partial composite index usage (only first column)
SELECT id, category FROM vec_with_composite_idx
WHERE category = 'A'
ORDER BY l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]')
LIMIT 3 by rank with option 'mode=pre';

-- ============================================================================
-- Test Cases: UNION with Vector Index and mode=pre
-- ============================================================================

-- Test Case: Simple UNION with mode=pre on same table
(SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist 
 FROM mini_vector_data 
 WHERE id IN ('id1', 'id2', 'id3')
 ORDER BY l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') 
 LIMIT 2 by rank with option 'mode=pre')
UNION
(SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist 
 FROM mini_vector_data 
 WHERE id IN ('id7', 'id8', 'id9')
 ORDER BY l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') 
 LIMIT 2 by rank with option 'mode=pre')
ORDER BY dist LIMIT 3;

-- Test Case: UNION ALL with mode=pre (allows duplicates)
(SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist 
 FROM mini_vector_data 
 WHERE id LIKE 'id%'
 ORDER BY l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') 
 LIMIT 3 by rank with option 'mode=pre')
UNION ALL
(SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist 
 FROM mini_vector_data 
 WHERE text LIKE '%test%'
 ORDER BY l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') 
 LIMIT 2 by rank with option 'mode=pre')
ORDER BY dist LIMIT 5;

-- Test Case: UNION with mode=pre and mode=post (mixed modes)
(SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist 
 FROM mini_vector_data 
 WHERE id IN ('id1', 'id2', 'id3', 'id4')
 ORDER BY l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') 
 LIMIT 2 by rank with option 'mode=pre')
UNION
(SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist 
 FROM mini_vector_data 
 WHERE id IN ('id6', 'id7', 'id8', 'id9')
 ORDER BY l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') 
 LIMIT 2 by rank with option 'mode=post')
ORDER BY dist LIMIT 4;

-- Test Case: UNION with mode=pre on different tables (mini_vector_data and mini_embed_data)
(SELECT id, text AS content, l2_distance(vec, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') AS dist 
 FROM mini_vector_data 
 ORDER BY l2_distance(vec, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') 
 LIMIT 2 by rank with option 'mode=pre')
UNION
(SELECT id, content, cosine_distance(embedding, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') AS dist 
 FROM mini_embed_data 
 ORDER BY cosine_distance(embedding, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') 
 LIMIT 2 by rank with option 'mode=pre')
ORDER BY dist LIMIT 3;

-- Test Case: UNION with mode=pre and complex WHERE conditions
(SELECT id, category, l2_distance(vec, '[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]') AS dist 
 FROM vec_with_multi_idx 
 WHERE category = 'A' AND status = 1
 ORDER BY l2_distance(vec, '[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]') 
 LIMIT 2 by rank with option 'mode=pre')
UNION
(SELECT id, category, l2_distance(vec, '[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]') AS dist 
 FROM vec_with_multi_idx 
 WHERE category = 'B' AND status = 1
 ORDER BY l2_distance(vec, '[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]') 
 LIMIT 2 by rank with option 'mode=pre')
ORDER BY dist LIMIT 3;

-- Test Case: Three-way UNION with mode=pre
(SELECT id, category, l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]') AS dist 
 FROM vec_with_regular_idx 
 WHERE category = 'A'
 ORDER BY l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]') 
 LIMIT 2 by rank with option 'mode=pre')
UNION
(SELECT id, category, l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]') AS dist 
 FROM vec_with_regular_idx 
 WHERE category = 'B'
 ORDER BY l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]') 
 LIMIT 2 by rank with option 'mode=pre')
UNION
(SELECT id, category, l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]') AS dist 
 FROM vec_with_regular_idx 
 WHERE category = 'C'
 ORDER BY l2_distance(vec, '[0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85]') 
 LIMIT 1 by rank with option 'mode=pre')
ORDER BY dist LIMIT 4;

-- Test Case: UNION with mode=pre using CTE (WITH clause)
WITH q1 AS (
  SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist 
  FROM mini_vector_data 
  WHERE id IN ('id1', 'id2', 'id3')
  ORDER BY dist 
  LIMIT 2 by rank with option 'mode=pre'
),
q2 AS (
  SELECT id, text, l2_distance(vec, '[0.1,-0.2,0.3,0.4,-0.1,0.2,0.0,0.5]') AS dist 
  FROM mini_vector_data 
  WHERE id IN ('id8', 'id9', 'id10')
  ORDER BY dist 
  LIMIT 2 by rank with option 'mode=pre'
)
SELECT * FROM q1
UNION
SELECT * FROM q2
ORDER BY dist LIMIT 3;

-- Test Case: UNION ALL with mode=pre and different LIMIT sizes (testing over-fetch)
(SELECT id, content, cosine_distance(embedding, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') AS dist 
 FROM mini_embed_data 
 WHERE file_id = 'file01'
 ORDER BY cosine_distance(embedding, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') 
 LIMIT 1 by rank with option 'mode=pre')
UNION ALL
(SELECT id, content, cosine_distance(embedding, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') AS dist 
 FROM mini_embed_data 
 WHERE file_id = 'file02'
 ORDER BY cosine_distance(embedding, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') 
 LIMIT 2 by rank with option 'mode=pre')
ORDER BY dist DESC LIMIT 3;

-- Test Case: UNION with mode=pre and DESC ordering
(SELECT id, text, l2_distance(vec, '[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]') AS dist 
 FROM mini_vector_data 
 WHERE id LIKE 'id1%'
 ORDER BY l2_distance(vec, '[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]') DESC
 LIMIT 2 by rank with option 'mode=pre')
UNION
(SELECT id, text, l2_distance(vec, '[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]') AS dist 
 FROM mini_vector_data 
 WHERE id LIKE 'id%' AND id NOT LIKE 'id1%'
 ORDER BY l2_distance(vec, '[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]') DESC
 LIMIT 3 by rank with option 'mode=pre')
ORDER BY dist DESC LIMIT 4;

-- Test Case: UNION with mode=pre and OFFSET
(SELECT id, category, status, l2_distance(vec, '[0.3,0.4,0.5,0.6,0.7,0.8,0.9,0.1]') AS dist 
 FROM vec_with_multi_idx 
 WHERE status = 1
 ORDER BY l2_distance(vec, '[0.3,0.4,0.5,0.6,0.7,0.8,0.9,0.1]') 
 LIMIT 3 OFFSET 1 by rank with option 'mode=pre')
UNION
(SELECT id, category, status, l2_distance(vec, '[0.3,0.4,0.5,0.6,0.7,0.8,0.9,0.1]') AS dist 
 FROM vec_with_multi_idx 
 WHERE status = 2
 ORDER BY l2_distance(vec, '[0.3,0.4,0.5,0.6,0.7,0.8,0.9,0.1]') 
 LIMIT 2 by rank with option 'mode=pre')
ORDER BY dist LIMIT 4;

-- Test Case: UNION with mode=pre, mode=post, and mode=force (all three modes)
(SELECT id, text, l2_distance(vec, '[0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2]') AS dist 
 FROM mini_vector_data 
 WHERE id IN ('id1', 'id2')
 ORDER BY l2_distance(vec, '[0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2]') 
 LIMIT 1 by rank with option 'mode=pre')
UNION ALL
(SELECT id, text, l2_distance(vec, '[0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2]') AS dist 
 FROM mini_vector_data 
 WHERE id IN ('id3', 'id4')
 ORDER BY l2_distance(vec, '[0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2]') 
 LIMIT 1 by rank with option 'mode=post')
UNION ALL
(SELECT id, text, l2_distance(vec, '[0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2]') AS dist 
 FROM mini_vector_data 
 WHERE id IN ('id5', 'id6')
 ORDER BY l2_distance(vec, '[0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2]') 
 LIMIT 1 by rank with option 'mode=force')
ORDER BY dist LIMIT 3;

-- Test Case: UNION with composite index and mode=pre
(SELECT id, category, status, l2_distance(vec, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') AS dist 
 FROM vec_with_composite_idx 
 WHERE category = 'A' AND status = 1
 ORDER BY l2_distance(vec, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') 
 LIMIT 1 by rank with option 'mode=pre')
UNION
(SELECT id, category, status, l2_distance(vec, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') AS dist 
 FROM vec_with_composite_idx 
 WHERE category = 'B'
 ORDER BY l2_distance(vec, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') 
 LIMIT 2 by rank with option 'mode=pre')
ORDER BY dist LIMIT 3;

drop table if exists vec_with_regular_idx;
drop table if exists vec_with_multi_idx;
drop table if exists vec_with_composite_idx;
drop database if exists dd1;
