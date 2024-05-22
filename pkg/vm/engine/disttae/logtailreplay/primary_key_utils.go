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

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func (p *PartitionState) PKExistInMemBetween(
	from types.TS,
	to types.TS,
	keys [][]byte,
) (bool, bool) {

	objIter := p.dataRows.Copy().Iter()
	defer objIter.Release()
	objTombstoneIter := p.tombstones.Copy().Iter()
	defer objTombstoneIter.Release()

	//Notice that the number of appendable object in memory is not very large,
	//so don't be too worried about the performance.
	for ok := objIter.First(); ok; ok = objIter.Next() {
		objItem := objIter.Item()

		// data row iterator for object.
		dataRowIter := objItem.DataRows.Load().Copy().Iter()
		defer dataRowIter.Release()

		hasTombstone := false
		ret := objTombstoneIter.Seek(ObjTombstoneRowEntry{
			ShortObjName: objItem.ShortObjName})
		if ret {
			item := objTombstoneIter.Item()
			if bytes.Compare(item.ShortObjName[:], objItem.ShortObjName[:]) == 0 {
				hasTombstone = true
			}
		}

		for _, key := range keys {

			idxEntry := DataRowEntry{
				PK: key,
			}

			for ok := dataRowIter.Seek(idxEntry); ok; ok = dataRowIter.Next() {

				entry := dataRowIter.Item()

				if !bytes.Equal(entry.PK, key) {
					break
				}

				if entry.Time.GreaterEq(&from) {
					return true, false
				}

				if !hasTombstone {
					break
				}

				tombstoneIter := objTombstoneIter.Item().
					Tombstones.Load().Copy().Iter()
				defer tombstoneIter.Release()

				if ok = tombstoneIter.Seek(TombstoneRowEntry{
					BlockID: entry.RowID.CloneBlockID(),
					Offset:  entry.RowID.GetRowOffset(),
				}); !ok {
					continue
				}
				tombstone := tombstoneIter.Item()
				if tombstone.BlockID.Compare(entry.RowID.CloneBlockID()) != 0 ||
					tombstone.Offset != entry.RowID.GetRowOffset() {
					continue
				}
				if tombstone.Time.GreaterEq(&from) {
					return true, false
				}

			}

			dataRowIter.First()

		}

		objTombstoneIter.First()

	}

	p.shared.Lock()
	lastFlushTimestamp := p.shared.lastFlushTimestamp
	p.shared.Unlock()
	if lastFlushTimestamp.LessEq(&from) {
		return false, false
	}
	return false, true
}
