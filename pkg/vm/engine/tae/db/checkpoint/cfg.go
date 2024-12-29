package checkpoint

import "time"

type CheckpointCfg struct {
	/* incremental checkpoint configurations */

	// min count of transaction to trigger incremental checkpoint
	// exception: force incremental checkpoint ignore min count
	MinCount int64

	// extra reserved wal entry count for incremental checkpoint
	IncrementalReservedWALCount uint64

	// min interval to trigger incremental checkpoint
	// exception: force incremental checkpoint ignore interval
	IncrementalInterval time.Duration

	/* global checkpoint configurations */

	// increment checkpoint min count to trigger global checkpoint
	// exception: force global checkpoint ignore min count
	GlobalMinCount int64

	// history duration to keep for a global checkpoint
	GlobalHistoryDuration time.Duration
}
