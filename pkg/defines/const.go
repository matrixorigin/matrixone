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
	MORPCVersion3      int64 = 3 // start from 1.3.0
	MORPCVersion4      int64 = 4 // start from 2.0.1
	MORPCVersion5      int64 = 5 // start from 4.0.5
	MORPCLatestVersion       = MORPCVersion5
)

// DefaultLockWaitTimeoutSeconds is shared by the frontend default and by
// distributed-pipeline compatibility code. Keeping one value is important for
// rolling upgrades: an older receiver ignores LockWaitTimeoutSet, so a newer
// sender represents an explicit clear with this positive fallback in the
// legacy LockWaitTimeout field instead of sending zero and reviving a stale
// transaction override on the receiver.
const DefaultLockWaitTimeoutSeconds int64 = 120
