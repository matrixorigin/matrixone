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

package logtailreplay

import (
	"bytes"
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func (p *PartitionState) PKExistInMemBetween(
	from types.TS,
	to types.TS,
	keys [][]byte,
) (bool, bool) {
	iter := p.primaryIndex.NewIter()
	pivot := RowEntry{
		Time: types.BuildTS(math.MaxInt64, math.MaxUint32),
	}
	idxEntry := &PrimaryIndexEntry{}
	defer iter.Close()

	for _, key := range keys {

		idxEntry.Bytes = key

		for entry, ok := iter.Seek(idxEntry); ok; entry, ok = iter.Next() {

			if !bytes.Equal(entry.Bytes, key) {
				break
			}

			if entry.Time.GreaterEq(&from) {
				return true, false
			}

			//some legacy deletion entries may not be indexed since old TN maybe
			//don't take pk in log tail when delete row , so check all rows for changes.
			pivot.BlockID = entry.BlockID
			pivot.RowID = entry.RowID
			rowIter := p.rows.NewIter()
			seek := false
			for {
				var row RowEntry
				if !seek {
					seek = true
					row, ok = rowIter.Seek(pivot)
					if !ok {
						break
					}
				} else {
					row, ok = rowIter.Next()
					if !ok {
						break
					}
				}
				if row.BlockID.Compare(entry.BlockID) != 0 {
					break
				}
				if !row.RowID.Equal(entry.RowID) {
					break
				}
				if row.Time.GreaterEq(&from) {
					rowIter.Close()
					return true, false
				}
			}
			rowIter.Close()
		}

		iter.Rewind()
	}

	p.shared.Lock()
	lastFlushTimestamp := p.shared.lastFlushTimestamp
	p.shared.Unlock()
	if lastFlushTimestamp.LessEq(&from) {
		return false, false
	}
	return false, true
}
