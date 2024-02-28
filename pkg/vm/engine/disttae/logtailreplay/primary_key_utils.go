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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func ForeachCommittedObjects(
	createObjs []objectio.ObjectNameShort,
	delObjs []objectio.ObjectNameShort,
	p *PartitionState,
	onObj func(ObjectInfo) error) (err error) {
	for _, obj := range createObjs {
		if objInfo, ok := p.GetObject(obj); ok {
			if err = onObj(objInfo); err != nil {
				return
			}
		}
	}
	for _, obj := range delObjs {
		if objInfo, ok := p.GetObject(obj); ok {
			if err = onObj(objInfo); err != nil {
				return
			}
		}
	}
	return nil

}

func (p *PartitionState) PrimaryKeysPersistedMayBeModified(
	from types.TS,
	to types.TS,
	keys *vector.Vector,
) bool {
	candidateBlks := make(map[types.Blockid]*objectio.BlockInfo)

	//only check data objects.
	delObjs, cObjs := p.GetChangedObjsBetween(from.Next(), types.MaxTs())
	if err := ForeachCommittedObjects(cObjs, delObjs, p, func(obj ObjectInfo) (err2 error) {
		var zmCkecked bool
		// if the object info contains a pk zonemap, fast-check with the zonemap
		if !obj.ZMIsEmpty() {
			if isVec {
				if !obj.SortKeyZoneMap().AnyIn(vec) {
					return
				}
			} else {
				if !obj.SortKeyZoneMap().ContainsKey(val) {
					return
				}
			}
			zmCkecked = true
		}

		var objMeta objectio.ObjectMeta
		location := obj.Location()

		// load object metadata
		v2.TxnRangesLoadedObjectMetaTotalCounter.Inc()
		if objMeta, err2 = objectio.FastLoadObjectMeta(
			tbl.proc.Load().Ctx, &location, false, fs,
		); err2 != nil {
			return
		}

		// reset bloom filter to nil for each object
		meta = objMeta.MustDataMeta()

		// check whether the object is skipped by zone map
		// If object zone map doesn't contains the pk value, we need to check bloom filter
		if !zmCkecked {
			if isVec {
				if !meta.MustGetColumn(uint16(tbl.primaryIdx)).ZoneMap().AnyIn(vec) {
					return
				}
			} else {
				if !meta.MustGetColumn(uint16(tbl.primaryIdx)).ZoneMap().ContainsKey(val) {
					return
				}
			}
		}

		bf = nil
		if bf, err2 = objectio.LoadBFWithMeta(
			tbl.proc.Load().Ctx, meta, location, fs,
		); err2 != nil {
			return
		}

		ForeachBlkInObjStatsList(false, meta, func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
			if isVec {
				if !blkMeta.IsEmpty() && !blkMeta.MustGetColumn(uint16(tbl.primaryIdx)).ZoneMap().AnyIn(vec) {
					return true
				}
			} else {
				if !blkMeta.IsEmpty() && !blkMeta.MustGetColumn(uint16(tbl.primaryIdx)).ZoneMap().ContainsKey(val) {
					return true
				}
			}

			blkBf := bf.GetBloomFilter(uint32(blk.BlockID.Sequence()))
			blkBfIdx := index.NewEmptyBinaryFuseFilter()
			if err2 = index.DecodeBloomFilter(blkBfIdx, blkBf); err2 != nil {
				return false
			}
			var exist bool
			if isVec {
				if exist = blkBfIdx.MayContainsAny(vec); !exist {
					return true
				}
			} else {
				if exist, err2 = blkBfIdx.MayContainsKey(val); err2 != nil {
					return false
				} else if !exist {
					return true
				}
			}

			blk.Sorted = obj.Sorted
			blk.EntryState = obj.EntryState
			blk.CommitTs = obj.CommitTS
			if obj.HasDeltaLoc {
				deltaLoc, commitTs, ok := snapshot.GetBockDeltaLoc(blk.BlockID)
				if ok {
					blk.DeltaLoc = deltaLoc
					blk.CommitTs = commitTs
				}
			}
			blk.PartitionNum = -1
			blocks.AppendBlockInfo(blk)
			return true
		}, obj.ObjectStats)
		return
	}); err != nil {
		return true
	}

	return true
}

func (p *PartitionState) PrimaryKeysInMemMayBeModified(
	from types.TS,
	to types.TS,
	keys [][]byte,
) bool {

	//p.shared.Lock()
	//lastFlushTimestamp := p.shared.lastFlushTimestamp
	//p.shared.Unlock()

	//if !lastFlushTimestamp.IsEmpty() {
	//	if from.LessEq(lastFlushTimestamp) {
	//		return true
	//	}
	//}

	iter := p.primaryIndex.Copy().Iter()
	pivot := RowEntry{
		Time: types.BuildTS(math.MaxInt64, math.MaxUint32),
	}
	idxEntry := &PrimaryIndexEntry{}
	defer iter.Release()

	for _, key := range keys {

		idxEntry.Bytes = key

		for ok := iter.Seek(idxEntry); ok; ok = iter.Next() {

			entry := iter.Item()

			if !bytes.Equal(entry.Bytes, key) {
				break
			}

			if entry.Time.GreaterEq(from) {
				return true
			}

			//some legacy deletion entries may not be indexed since old TN maybe
			//don't take pk in log tail when delete row , so check all rows for changes.
			pivot.BlockID = entry.BlockID
			pivot.RowID = entry.RowID
			rowIter := p.rows.Iter()
			seek := false
			for {
				if !seek {
					seek = true
					if !rowIter.Seek(pivot) {
						break
					}
				} else {
					if !rowIter.Next() {
						break
					}
				}
				row := rowIter.Item()
				if row.BlockID.Compare(entry.BlockID) != 0 {
					break
				}
				if !row.RowID.Equal(entry.RowID) {
					break
				}
				if row.Time.GreaterEq(from) {
					rowIter.Release()
					return true
				}
			}
			rowIter.Release()
		}

		iter.First()
	}
	return false
}
