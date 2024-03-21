// Copyright 2024 Matrix Origin
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

package trace

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

type tableFilters struct {
	filters []EntryFilter
}

func (f *tableFilters) isEmpty() bool {
	return f == nil || len(f.filters) == 0
}

// filter return true means the data need to be skipped.
// Return true cases:
// 1. filters is empty
// 2. all filter skipped
func (f *tableFilters) filter(data *EntryData) bool {
	if f.isEmpty() {
		return true
	}

	skipped := true
	for _, f := range f.filters {
		if !f.Filter(data) {
			skipped = false
			break
		}
	}
	return skipped
}

// NewKeepTableFilter returns a filter that only keeps the specified table and columns.
// Note: always keep pk column if pk index is specified.
func NewKeepTableFilter(
	id uint64,
	column []string) EntryFilter {
	f := &tableEntryFilter{id: id}
	f.columns = make(map[string]struct{})
	for _, c := range column {
		if c == "" {
			continue
		}
		f.columns[c] = struct{}{}
	}
	return f
}

type tableEntryFilter struct {
	id      uint64
	columns map[string]struct{}
}

func (f *tableEntryFilter) Filter(entry *EntryData) bool {
	if entry.id != f.id {
		return true
	}

	if len(entry.columns) == 0 {
		return false
	}

	// only insert filter columns
	if len(f.columns) > 0 &&
		entry.needFilterColumns() {
		newColumns := entry.columns[:0]
		newVecs := entry.vecs[:0]
		for i, attr := range entry.columns {
			_, ok := f.columns[attr]
			// keep row id and completed pk forever
			if ok ||
				isRowIDColumn(attr) ||
				isComplexColumn(complexPKColumnName) {
				newColumns = append(newColumns, attr)
				newVecs = append(newVecs, entry.vecs[i])
			}
		}
		entry.columns = newColumns
		entry.vecs = newVecs
	}

	if entry.commitVec != nil &&
		entry.entryType == api.Entry_Delete {
		newColumns := entry.columns[:0]
		newVecs := entry.vecs[:0]

		newColumns = append(newColumns, rowIDColumn)
		newVecs = append(newVecs, entry.vecs[0])

		if len(entry.vecs) == 3 {
			newColumns = append(newColumns, deletePKColumn)
			newVecs = append(newVecs, entry.vecs[2])
		}

		entry.columns = newColumns
		entry.vecs = newVecs
	}
	return false
}

func (f *tableEntryFilter) Name() string {
	return fmt.Sprintf("table[%d]: %+v\n", f.id, f.columns)
}

type allTableFilter struct {
}

func (f *allTableFilter) Filter(entry *EntryData) bool {
	return false
}

func (f *allTableFilter) Name() string {
	return "all"
}
