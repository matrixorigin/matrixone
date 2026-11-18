-- Run as sys:moadmin. The first initialization leaves collection disabled.
CREATE DATABASE IF NOT EXISTS mo_change_watch;

CREATE TABLE IF NOT EXISTS mo_change_watch.state (
    singleton TINYINT UNSIGNED PRIMARY KEY,
    recovery_epoch UUID NOT NULL,
    enabled BOOL NOT NULL
);

CREATE TABLE IF NOT EXISTS mo_change_watch.watch (
    observer_account_id INT UNSIGNED NOT NULL,
    source_account_id INT UNSIGNED NOT NULL,
    database_name VARCHAR(256) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    grant_id UUID NOT NULL,
    PRIMARY KEY (observer_account_id, source_account_id, database_name, table_name)
);

-- Retries preserve the existing epoch and admission state.
INSERT IGNORE INTO mo_change_watch.state (singleton, recovery_epoch, enabled)
VALUES (1, UUID(), FALSE);

-- Enable only after validating the serving CNs and TN and reconciling all grants.
-- UPDATE mo_change_watch.state SET enabled = TRUE WHERE singleton = 1;

-- Before a restore or clone, block new change-watch queries on every serving
-- CN and drain or cancel in-flight calls. A restore can bring back an enabled
-- state row. Keep ingress blocked until the steps below are complete.
-- UPDATE mo_change_watch.state SET enabled = FALSE WHERE singleton = 1;
-- Replace the restored watch rows with the provisioning service's current grants.
-- UPDATE mo_change_watch.state SET recovery_epoch = UUID() WHERE singleton = 1;
-- Verify the new epoch and grants before reenabling and releasing ingress.
