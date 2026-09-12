-- Issue #28158: a correlated scalar aggregate must not aggregate unrelated
-- rows after decorrelation.  The execution checks below also protect the
-- duplicate/multiplicity contract of the optimizer's key-domain join.

DROP DATABASE IF EXISTS issue_28158_reg;
CREATE DATABASE issue_28158_reg;
USE issue_28158_reg;

CREATE TABLE tasks (
    workspace_id VARCHAR(20) NOT NULL,
    task_id VARCHAR(20) NOT NULL,
    state VARCHAR(20) NOT NULL,
    PRIMARY KEY (workspace_id, task_id)
);

CREATE TABLE events (
    event_id INT NOT NULL,
    workspace_id VARCHAR(20) NOT NULL,
    task_id VARCHAR(20) NOT NULL,
    seq INT NOT NULL,
    visible BOOL NOT NULL,
    a2a_state VARCHAR(20) NOT NULL,
    PRIMARY KEY (event_id),
    KEY task_event_key (workspace_id, task_id)
);

INSERT INTO tasks VALUES
    ('w1', 'same', 'running'),
    ('w2', 'same', 'running'),
    ('w1', 'empty', 'running');

INSERT INTO events VALUES
    (1, 'w1', 'same', 1, TRUE, 'completed'),
    (2, 'w1', 'same', 2, TRUE, 'completed'),
    (3, 'w2', 'same', 100, TRUE, 'completed'),
    (4, 'w1', 'same', 3, FALSE, 'completed'),
    (5, 'w1', 'same', 4, TRUE, 'pending');

-- Only task_id is correlated.  The two workspaces deliberately share that
-- value; this partial-key control remains outside the point-domain rewrite
-- and verifies that the original workspace-agnostic correlation is preserved.
SELECT t.workspace_id, t.task_id,
       (SELECT COUNT(*)
          FROM events e
         WHERE e.task_id = t.task_id
           AND e.visible = TRUE) AS event_count,
       (SELECT SUM(e.seq)
          FROM events e
         WHERE e.task_id = t.task_id
           AND e.visible = TRUE) AS seq_sum
  FROM tasks t
 WHERE t.task_id = 'same'
 ORDER BY t.workspace_id;

-- All composite primary-key columns are correlated and fixed to one row.
-- COUNT/SUM/MAX protect the multiplicity and empty-input contracts.
SELECT t.workspace_id, t.task_id,
       (SELECT COUNT(*)
          FROM events e
         WHERE e.workspace_id = t.workspace_id
           AND e.task_id = t.task_id
           AND e.visible = TRUE
           AND e.a2a_state IN ('completed', 'failed')) AS event_count,
       (SELECT SUM(e.seq)
          FROM events e
         WHERE e.workspace_id = t.workspace_id
           AND e.task_id = t.task_id
           AND e.visible = TRUE
           AND e.a2a_state IN ('completed', 'failed')) AS seq_sum,
       (SELECT MAX(e.seq)
          FROM events e
         WHERE e.workspace_id = t.workspace_id
           AND e.task_id = t.task_id
           AND e.visible = TRUE
           AND e.a2a_state IN ('completed', 'failed')) AS max_seq
  FROM tasks t
 WHERE t.workspace_id = 'w1'
   AND t.task_id = 'same';

-- No matching event rows: COUNT is zero, SUM/MAX are NULL, and the outer
-- task row is retained by the surrounding LEFT JOIN shape.
SELECT t.workspace_id, t.task_id,
       (SELECT COUNT(*)
          FROM events e
         WHERE e.workspace_id = t.workspace_id
           AND e.task_id = t.task_id
           AND e.visible = TRUE
           AND e.a2a_state IN ('completed', 'failed')) AS event_count,
       (SELECT SUM(e.seq)
          FROM events e
         WHERE e.workspace_id = t.workspace_id
           AND e.task_id = t.task_id
           AND e.visible = TRUE
           AND e.a2a_state IN ('completed', 'failed')) AS seq_sum,
       (SELECT MAX(e.seq)
          FROM events e
         WHERE e.workspace_id = t.workspace_id
           AND e.task_id = t.task_id
           AND e.visible = TRUE
           AND e.a2a_state IN ('completed', 'failed')) AS max_seq
  FROM tasks t
  LEFT JOIN events next_event
    ON next_event.workspace_id = t.workspace_id
   AND next_event.task_id = t.task_id
   AND next_event.seq > 0
 WHERE t.workspace_id = 'w1'
   AND t.task_id = 'empty'
 ORDER BY next_event.seq;

DROP DATABASE issue_28158_reg;
