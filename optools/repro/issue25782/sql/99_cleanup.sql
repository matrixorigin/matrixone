USE issue25782;
SELECT table_cnt FROM table_stats('issue25782.issue25782_build', 'refresh', 'full') g;
SELECT table_cnt FROM table_stats('issue25782.issue25782_probe', 'refresh', 'full') g;
DROP TABLE IF EXISTS issue25782_probe;
DROP TABLE IF EXISTS issue25782_build;
DROP DATABASE IF EXISTS issue25782;
