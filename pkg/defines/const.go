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

package defines

import "math"

// Header information.
const (
	OKHeader          byte = 0x00
	ErrHeader         byte = 0xff
	EOFHeader         byte = 0xfe
	LocalInFileHeader byte = 0xfb
)

const (
	SharedFileServiceName  = "SHARED"
	LocalFileServiceName   = "LOCAL"
	ETLFileServiceName     = "ETL"
	StandbyFileServiceName = "STANDBY"
	TmpFileServiceName     = "TMP"
	// sub fileservices
	SpillFileServiceName = "__spill"
)

const (
	MORPCMinVersion    int64 = math.MinInt64
	MORPCVersion1      int64 = 1
	MORPCVersion2      int64 = 2
	MORPCVersion3      int64 = 3  // start from 1.3.0
	MORPCVersion4      int64 = 4  // start from 2.0.1
	MORPCVersion5      int64 = 5  // assignment-aware CHAR/VARCHAR casts
	MORPCVersion6      int64 = 6  // ordered aggregate pipeline configuration
	MORPCVersion7      int64 = 7  // structured CHECK constraint metadata and enforcement
	MORPCVersion8      int64 = 8  // versioned exact runtime-filter key contract
	MORPCVersion9      int64 = 9  // AUTO_INCREMENT epoch-fenced commit
	MORPCVersion10     int64 = 10 // persisted appendable-object abort metadata
	MORPCVersion11     int64 = 11 // bounded Sorted64 membership-filter wire format
	MORPCVersion12     int64 = 12 // prepared-parameter provenance in remote process metadata and aggregate trailers
	MORPCVersion13     int64 = 13 // lossless v2 prefix-index metadata
	MORPCVersion14     int64 = 14 // utf8mb4 text MIN/MAX collation semantics
	MORPCVersion15     int64 = 15 // CHECK metadata in rename-column alter requests
	MORPCVersion16     int64 = 16 // information_schema CHECK_CONSTRAINTS table function
	MORPCVersion17     int64 = 17 // ordered-set percentile aggregate IDs
	MORPCVersion18     int64 = 18 // prepared-parameter binary-string metadata
	MORPCVersion19     int64 = 19 // remote bounded partition Top-N operator
	MORPCVersion20     int64 = 20 // owner-local lock snapshots and table-scoped remote unlock
	MORPCLatestVersion       = MORPCVersion20
)

// DefaultLockWaitTimeoutSeconds is shared by the frontend default and by
// distributed-pipeline compatibility code. Keeping one value is important for
// rolling upgrades: an older receiver ignores LockWaitTimeoutSet, so a newer
// sender represents an explicit clear with this positive fallback in the
// legacy LockWaitTimeout field instead of sending zero and reviving a stale
// transaction override on the receiver.
const DefaultLockWaitTimeoutSeconds int64 = 120
