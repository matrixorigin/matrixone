// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoint

import (
	"encoding/json"
	"time"
)

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

func (cfg CheckpointCfg) String() string {
	b, _ := json.Marshal(&cfg)
	return string(b)
}

func (cfg *CheckpointCfg) FillDefaults() {
	if cfg.IncrementalInterval <= 0 {
		cfg.IncrementalInterval = time.Minute
	}
	if cfg.MinCount <= 0 {
		cfg.MinCount = 10000
	}
	if cfg.GlobalMinCount <= 0 {
		cfg.GlobalMinCount = 40
	}
	if cfg.GlobalHistoryDuration < 0 {
		cfg.GlobalHistoryDuration = 0
	}
}
