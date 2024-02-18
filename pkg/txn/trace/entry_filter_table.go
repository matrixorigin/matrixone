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

// NewKeepEntryFilter returns a filter that only keeps the specified table and columns.
// Note: always keep pk column if pk index is specified.
func NewKeepEntryFilter(
	keepTableID uint64,
	keepColumns []string) EntryFilter {
	f := &tableEntryFilter{id: keepTableID}
	f.columns = make(map[string]struct{})
	for _, c := range keepColumns {
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

	if len(f.columns) > 0 {
		entry.columns = entry.columns[:0]
		entry.vecs = entry.vecs[:0]
		for i, attr := range entry.columns {
			_, ok := f.columns[attr]
			if ok {
				entry.columns = append(entry.columns, attr)
				entry.vecs = append(entry.vecs, entry.vecs[i])
			}
		}
	}
	return false
}

// NewSkipTableFilter returns a filter that skip all table's filter.
func NewSkipTableFilter(
	keepTableID uint64) EntryFilter {
	f := &skipTableEntryFilter{id: keepTableID}
	return f
}

type skipTableEntryFilter struct {
	id uint64
}

func (f *skipTableEntryFilter) Filter(entry *EntryData) bool {
	return entry.id == f.id
}
