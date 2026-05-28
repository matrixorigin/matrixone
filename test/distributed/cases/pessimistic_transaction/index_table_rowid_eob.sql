-- Test for index table __mo_rowid Name2ColIndex consistency bug
-- This test verifies that DML operations on index tables created within the same transaction work correctly
-- Bug: When CREATE INDEX is executed in a transaction, the index table's Name2ColIndex map was not synced
-- with the Cols array when __mo_rowid was appended, causing ExpectedEOB errors during subsequent DML operations.

drop table if exists mowl_workflow_cases;

CREATE TABLE mowl_workflow_cases (
    id VARCHAR(36) NOT NULL,
    task_id VARCHAR(36) DEFAULT NULL,
    PRIMARY KEY (id),
    KEY idx_task_id (task_id)
);

INSERT INTO mowl_workflow_cases VALUES ('case-001', 'task-001');
INSERT INTO mowl_workflow_cases VALUES ('case-002', 'task-002');

-- Test 1: ALTER TABLE ADD COLUMN + CREATE INDEX + UPDATE in same transaction
BEGIN;
ALTER TABLE mowl_workflow_cases ADD COLUMN scheduler_visible TINYINT(1) NOT NULL DEFAULT 1;
CREATE INDEX idx_cases_scheduler ON mowl_workflow_cases (scheduler_visible);
UPDATE mowl_workflow_cases SET scheduler_visible = 0 WHERE id = 'case-001';
COMMIT;

-- Verify the UPDATE succeeded
SELECT id, scheduler_visible FROM mowl_workflow_cases ORDER BY id;

-- Test 2: CREATE INDEX + DELETE in same transaction
BEGIN;
CREATE INDEX idx_cases_task ON mowl_workflow_cases (task_id);
DELETE FROM mowl_workflow_cases WHERE id = 'case-002';
COMMIT;

-- Verify the DELETE succeeded
SELECT COUNT(*) FROM mowl_workflow_cases;

drop table mowl_workflow_cases;
