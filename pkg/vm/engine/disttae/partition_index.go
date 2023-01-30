// Copyright 2023 Matrix Origin
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

package disttae

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memorytable"
)

type PartitionIndexEntry struct {
	Name      memorytable.Text
	Tuple     memorytable.Tuple
	Timestamp timestamp.Timestamp
	Op        memorytable.Uint

	Batch  *batch.Batch
	Offset int
}

func comparePartitionIndexEntry(a, b *PartitionIndexEntry) bool {
	if a.Name.Less(b.Name) {
		return true
	} else if b.Name.Less(a.Name) {
		return false
	}
	if a.Tuple.Less(b.Tuple) {
		return true
	} else if b.Tuple.Less(a.Tuple) {
		return false
	}
	if a.Timestamp.Less(b.Timestamp) {
		return true
	} else if b.Timestamp.Less(a.Timestamp) {
		return false
	}
	if a.Op.Less(b.Op) {
		return true
	} else if b.Op.Less(a.Op) {
		return false
	}
	return false
}

func (p *Partition) Index(
	name memorytable.Text,
	pivot memorytable.Tuple,
	ts timestamp.Timestamp,
) (ret *PartitionIndexEntry) {
	// check last op before or at ts
	p.index.DescendLessOrEqual(&PartitionIndexEntry{
		Name:      name,
		Tuple:     pivot,
		Timestamp: ts,
	}, func(entry *PartitionIndexEntry) bool {
		if entry.Name != name {
			return false
		}
		if entry.Tuple.Less(pivot) || pivot.Less(entry.Tuple) {
			return false
		}
		if entry.Timestamp.Greater(ts) {
			return true
		}
		if entry.Timestamp.Equal(ts) {
			if entry.Op == opInsert {
				// not visible yet
				return true
			} else {
				// deleted
				return false
			}
		}
		// entryTS is before ts
		if entry.Op == opInsert {
			// still visible
			ret = entry
		}
		return false
	})
	return
}
