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

//func (p *PartitionState) PKExistInMemBetween(
//	from types.TS,
//	to types.TS,
//	keys [][]byte,
//) (bool, bool) {
//	iter := p.primaryIndex.Copy().Iter()
//	pivot := RowEntry{
//		Time: types.BuildTS(math.MaxInt64, math.MaxUint32),
//	}
//	idxEntry := &PrimaryIndexEntry{}
//	defer iter.Release()
//
//	for _, key := range keys {
//
//		idxEntry.Bytes = key
//
//		for ok := iter.Seek(idxEntry); ok; ok = iter.Next() {
//
//			entry := iter.Item()
//
//			if !bytes.Equal(entry.Bytes, key) {
//				break
//			}
//
//			if entry.Time.GreaterEq(&from) {
//				return true, false
//			}
//
//			//some legacy deletion entries may not be indexed since old TN maybe
//			//don't take pk in log tail when delete row , so check all rows for changes.
//			pivot.BlockID = entry.BlockID
//			pivot.RowID = entry.RowID
//			rowIter := p.rows.Iter()
//			seek := false
//			for {
//				if !seek {
//					seek = true
//					if !rowIter.Seek(pivot) {
//						break
//					}
//				} else {
//					if !rowIter.Next() {
//						break
//					}
//				}
//				row := rowIter.Item()
//				if row.BlockID.Compare(entry.BlockID) != 0 {
//					break
//				}
//				if !row.RowID.Equal(entry.RowID) {
//					break
//				}
//				if row.Time.GreaterEq(&from) {
//					rowIter.Release()
//					return true, false
//				}
//			}
//			rowIter.Release()
//		}
//
//		iter.First()
//	}
//
//	p.shared.Lock()
//	lastFlushTimestamp := p.shared.lastFlushTimestamp
//	p.shared.Unlock()
//	if lastFlushTimestamp.LessEq(&from) {
//		return false, false
//	}
//	return false, true
//}
