-- #33234: a NULL in another peer group must not change which ranked rows are stored.
DROP TABLE IF EXISTS peer_rank_result, peer_rank_scores;
CREATE TABLE peer_rank_scores (id INT PRIMARY KEY, k INT, s INT);
INSERT INTO peer_rank_scores VALUES (1,2,NULL),(2,2,0),(3,2,-2),(4,2,81);
-- Assert rank as a value so result-set normalization cannot mask a wrong order.
SELECT id, ROW_NUMBER() OVER (ORDER BY s IS NULL, s DESC, id) AS pos FROM peer_rank_scores;
CREATE TABLE peer_rank_result (id INT);
INSERT INTO peer_rank_result
SELECT id FROM (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY s IS NULL, s DESC, id) AS rn
    FROM peer_rank_scores
) ranked WHERE rn <= 2;
SELECT id FROM peer_rank_result ORDER BY id;
DROP TABLE peer_rank_result, peer_rank_scores;
