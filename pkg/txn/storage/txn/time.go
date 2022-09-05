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

package txnstorage

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type Time struct {
	Timestamp timestamp.Timestamp
	Statement int
}

func (t Time) Next() Time {
	t.Statement++
	return t
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
