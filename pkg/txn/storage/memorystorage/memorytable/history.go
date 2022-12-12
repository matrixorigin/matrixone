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

package memorytable

import (
	"sort"
)

type history[
	K Ordered[K],
	V any,
] struct {
	EndTime  Time
	EndState *tableState[K, V]
	NewLogs  []*logEntry[K, V]
}

// EraseHistory erases history before specified time
func (t *Table[K, V, R]) EraseHistory(before Time) {
	t.Lock()
	defer t.Unlock()
	i := sort.Search(len(t.history), func(i int) bool {
		return before.Equal(t.history[i].EndTime) ||
			before.Less(t.history[i].EndTime)
	})
	if i < len(t.history) {
		t.history = t.history[i:]
	}
}
