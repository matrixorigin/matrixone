// Copyright 2022 Matrix Origin
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

package memtable

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
)

type Time struct {
	Timestamp timestamp.Timestamp
	Statement int
}

func Now(clock clock.Clock) Time {
	now, _ := clock.Now()
	return Time{
		Timestamp: now,
	}
}

func (t *Time) Tick() {
	t.Statement++
}

func (t Time) After(t2 Time) bool {
	if t.Timestamp.Greater(t2.Timestamp) {
		return true
	}
	if t.Timestamp.Less(t2.Timestamp) {
		return false
	}
	return t.Statement > t2.Statement
}

func (t Time) Before(t2 Time) bool {
	if t.Timestamp.Less(t2.Timestamp) {
		return true
	}
	if t.Timestamp.Greater(t2.Timestamp) {
		return false
	}
	return t.Statement < t2.Statement
}

func (t Time) String() string {
	return fmt.Sprintf("Time{%d, %d, %d}",
		t.Timestamp.PhysicalTime,
		t.Timestamp.LogicalTime,
		t.Statement,
	)
}

func (t Time) IsZero() bool {
	return t.Timestamp.IsEmpty() && t.Statement == 0
}

func (t Time) ToTxnTS() types.TS {
	return types.BuildTS(
		t.Timestamp.PhysicalTime,
		t.Timestamp.LogicalTime,
	)
}
