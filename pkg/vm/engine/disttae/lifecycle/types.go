// Copyright 2026 Matrix Origin
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

package lifecycle

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// DiscoveryCursor is an O(1) scheduling hint. It is never a correctness proof.
type DiscoveryCursor struct {
	Snapshot       types.TS
	LastObjectName objectio.ObjectNameShort
	HasLastObject  bool
	Wrapped        bool
}

type DiscoveryLimits struct {
	MaxObjects   int
	MaxMetaBytes uint64
	MaxDuration  time.Duration
}

type DiscoveryRequest struct {
	Snapshot         types.TS
	Now              time.Time
	Cursor           DiscoveryCursor
	LastFullScanAt   time.Time
	FullScanInterval time.Duration
	Limits           DiscoveryLimits
}

type Candidate struct {
	Snapshot types.TS
	Source   objectio.ObjectEntry
}

type DiscoveryPage struct {
	Candidates          []Candidate
	Next                DiscoveryCursor
	EndOfCycle          bool
	MetaBytes           uint64
	StartedFullScanAt   time.Time
	CompletedFullScanAt time.Time
}
