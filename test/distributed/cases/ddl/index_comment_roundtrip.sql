-- Index comments must survive both public DDL entry points, SHOW metadata, and
-- recreation through CREATE TABLE LIKE. The doubled backslash in the SQL
-- spelling represents one semantic backslash in the comment.
DROP TABLE IF EXISTS index_comment_inline_replay;
DROP TABLE IF EXISTS index_comment_inline;
CREATE TABLE index_comment_inline (
    id INT PRIMARY KEY,
    note VARCHAR(64),
    KEY idx_note(note) COMMENT 'index''s comment\\with unicode 维度'
);
SHOW INDEX FROM index_comment_inline;
SHOW CREATE TABLE index_comment_inline;
CREATE TABLE index_comment_inline_replay LIKE index_comment_inline;
SHOW INDEX FROM index_comment_inline_replay;
SHOW CREATE TABLE index_comment_inline_replay;

DROP TABLE IF EXISTS index_comment_alter_replay;
DROP TABLE IF EXISTS index_comment_alter;
CREATE TABLE index_comment_alter (
    id INT PRIMARY KEY,
    note VARCHAR(64)
);
ALTER TABLE index_comment_alter ADD KEY idx_note(note) COMMENT 'index''s comment\\with unicode 维度';
SHOW INDEX FROM index_comment_alter;
SHOW CREATE TABLE index_comment_alter;
CREATE TABLE index_comment_alter_replay LIKE index_comment_alter;
SHOW INDEX FROM index_comment_alter_replay;
SHOW CREATE TABLE index_comment_alter_replay;

DROP TABLE index_comment_inline_replay;
DROP TABLE index_comment_inline;
DROP TABLE index_comment_alter_replay;
DROP TABLE index_comment_alter;
