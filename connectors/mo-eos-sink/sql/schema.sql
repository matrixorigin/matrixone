-- Execute in a dedicated MatrixOne database before starting the connector.
-- Identity columns are byte-exact: VARCHAR under utf8mb4_general_ci is unsafe.
CREATE TABLE mo_sink_ledger (
    scope VARBINARY(128) NOT NULL,
    event_id VARBINARY(256) NOT NULL,
    digest_version VARCHAR(64) NOT NULL,
    payload_digest CHAR(64) NOT NULL,
    first_topic VARBINARY(249) NOT NULL,
    first_partition INT NOT NULL,
    first_offset BIGINT NOT NULL,
    PRIMARY KEY (scope, event_id)
);

CREATE TABLE mo_sink_checkpoint (
    scope VARBINARY(128) NOT NULL,
    topic VARBINARY(249) NOT NULL,
    partition_id INT NOT NULL,
    topic_id VARBINARY(22) NULL,
    last_offset BIGINT NOT NULL,
    owner_epoch BIGINT NOT NULL,
    PRIMARY KEY (scope, topic, partition_id)
);

CREATE TABLE mo_sink_totals (
    scope VARBINARY(128) PRIMARY KEY,
    n BIGINT NOT NULL
);

CREATE TABLE mo_sink_budget (
    scope VARBINARY(128) PRIMARY KEY,
    used_rows BIGINT NOT NULL,
    used_bytes BIGINT NOT NULL,
    max_rows BIGINT NOT NULL,
    max_bytes BIGINT NOT NULL
);

-- Example for scope "orders", topic "mo_eos_events", partition 0.
-- Add one checkpoint row per actual topic partition before assignment.
INSERT INTO mo_sink_totals VALUES (UNHEX('6f7264657273'), 0);
INSERT INTO mo_sink_budget VALUES (UNHEX('6f7264657273'), 0, 0, 1000000, 1073741824);
INSERT INTO mo_sink_checkpoint(scope,topic,partition_id,topic_id,last_offset,owner_epoch) VALUES (
    UNHEX('6f7264657273'), UNHEX('6d6f5f656f735f6576656e7473'), 0, NULL, -1, 0);
