-- Copyright 2026 Matrix Origin
-- Licensed under the Apache License, Version 2.0.
-- Run this assertion in the same operator-controlled session immediately
-- before resetting the MongoDB live collection. It fails closed unless the
-- committed watermark has passed the archive cutoff or a verified replay
-- source has been registered.

select assert(
    exists(
        select 1
          from telemetry_ingest_control
         where source_name = 'telemetry_events'
           and archive_cutoff is not null
           and (committed_high >= archive_cutoff or archive_replay_ready)
    ),
    'MongoDB live reset blocked: watermark has not passed archive cutoff and no verified replay source is ready'
);
