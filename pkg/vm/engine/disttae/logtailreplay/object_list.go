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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/tidwall/btree"
)

const (
	ObjectListAttr_Stats       = "stats"
	ObjectListAttr_CreateAt    = "create_at"
	ObjectListAttr_DeleteAt    = "delete_at"
	ObjectListAttr_IsTombstone = "is_tombstone"
)

const (
	ObjectListAttr_Stats_Idx       = 0
	ObjectListAttr_CreateAt_Idx    = 1
	ObjectListAttr_DeleteAt_Idx    = 2
	ObjectListAttr_IsTombstone_Idx = 3
)

var ObjectListAttrs = []string{
	ObjectListAttr_Stats,
	ObjectListAttr_CreateAt,
	ObjectListAttr_DeleteAt,
	ObjectListAttr_IsTombstone,
}

var ObjectListTypes = []types.Type{
	types.T_char.ToType(), // objectio.ObjectStats as bytes
	types.T_TS.ToType(),   // create_at
	types.T_TS.ToType(),   // delete_at
	types.T_bool.ToType(), // is_tombstone
}

type ObjectList struct {
	stats       []objectio.ObjectStats
	createAt    []types.TS
	deleteAt    []types.TS
	isTombstone []bool
}

// CreateObjectListBatch creates a new batch for object list with proper schema
func CreateObjectListBatch() *batch.Batch {
	return batch.NewWithSchema(false, ObjectListAttrs, ObjectListTypes)
}

func tailCheckFn(objEntry objectio.ObjectEntry, start, end types.TS) bool {
	if objEntry.CreateTime.GE(&start) && objEntry.CreateTime.LE(&end) {
		return true
	}
	if !objEntry.DeleteTime.IsEmpty() &&
		objEntry.DeleteTime.GE(&start) &&
		objEntry.DeleteTime.LE(&end) {
		return true
	}
	return false
}

func snapshotCheckFn(objEntry objectio.ObjectEntry, snapshotTS types.TS) bool {
	if objEntry.CreateTime.GT(&snapshotTS) {
		return false
	}
	if !objEntry.DeleteTime.IsEmpty() && objEntry.DeleteTime.LE(&snapshotTS) {
		return false
	}
	return true
}

func CollectObjectList(
	ctx context.Context,
	state *PartitionState,
	start, end types.TS,
	bat **batch.Batch,
	mp *mpool.MPool,
) (objectList string, err error) {
	fillInObjectListFn := func(iter btree.IterG[objectio.ObjectEntry], bat **batch.Batch, isTombstone bool, mp *mpool.MPool) {
		for iter.Next() {
			objEntry := iter.Item()
			if tailCheckFn(objEntry, start, end) {
				// Append ObjectStats as bytes
				vector.AppendBytes((*bat).Vecs[ObjectListAttr_Stats_Idx], objEntry.ObjectStats[:], false, mp)
				// Append CreateTime
				vector.AppendFixed[types.TS]((*bat).Vecs[ObjectListAttr_CreateAt_Idx], objEntry.CreateTime, false, mp)
				// Append DeleteTime
				vector.AppendFixed[types.TS]((*bat).Vecs[ObjectListAttr_DeleteAt_Idx], objEntry.DeleteTime, false, mp)
				// Append isTombstone
				vector.AppendFixed[bool]((*bat).Vecs[ObjectListAttr_IsTombstone_Idx], isTombstone, false, mp)
			}
		}
	}
	fillInObjectListFn(state.dataObjectsNameIndex.Iter(), bat, false, mp)
	fillInObjectListFn(state.tombstoneObjectsNameIndex.Iter(), bat, true, mp)
	(*bat).SetRowCount((*bat).Vecs[0].Length())
	return
}

func CollectSnapshotObjectList(
	ctx context.Context,
	state *PartitionState,
	snapshotTS types.TS,
	bat **batch.Batch,
	mp *mpool.MPool,
) (objectList string, err error) {
	fillInObjectListFn := func(iter btree.IterG[objectio.ObjectEntry], bat **batch.Batch, isTombstone bool, mp *mpool.MPool) {
		for iter.Next() {
			objEntry := iter.Item()
			if snapshotCheckFn(objEntry, snapshotTS) {
				// Append ObjectStats as bytes
				vector.AppendBytes((*bat).Vecs[ObjectListAttr_Stats_Idx], objEntry.ObjectStats[:], false, mp)
				// Append CreateTime
				vector.AppendFixed[types.TS]((*bat).Vecs[ObjectListAttr_CreateAt_Idx], objEntry.CreateTime, false, mp)
				// Append DeleteTime
				vector.AppendFixed[types.TS]((*bat).Vecs[ObjectListAttr_DeleteAt_Idx], objEntry.DeleteTime, false, mp)
				// Append isTombstone
				vector.AppendFixed[bool]((*bat).Vecs[ObjectListAttr_IsTombstone_Idx], isTombstone, false, mp)
			}
		}
	}
	fillInObjectListFn(state.dataObjectsNameIndex.Iter(), bat, false, mp)
	fillInObjectListFn(state.tombstoneObjectsNameIndex.Iter(), bat, true, mp)
	(*bat).SetRowCount((*bat).Vecs[0].Length())
	return
}

// GetObjectListFromCKP reads object entries from checkpoint entries and collects them into a batch.
// It filters objects based on the time range [start, end] similar to CollectObjectList.
func GetObjectListFromCKP(
	ctx context.Context,
	tid uint64,
	sid string,
	start, end types.TS,
	checkpointEntries []*checkpoint.CheckpointEntry,
	bat **batch.Batch,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	// Initialize batch if needed
	if *bat == nil {
		*bat = CreateObjectListBatch()
	}

	// Fill function to append object entry to batch
	fillInObjectListFn := func(objEntry objectio.ObjectEntry, isTombstone bool, mp *mpool.MPool) {
		if tailCheckFn(objEntry, start, end) {
			// Append ObjectStats as bytes
			vector.AppendBytes((*bat).Vecs[ObjectListAttr_Stats_Idx], objEntry.ObjectStats[:], false, mp)
			// Append CreateTime
			vector.AppendFixed[types.TS]((*bat).Vecs[ObjectListAttr_CreateAt_Idx], objEntry.CreateTime, false, mp)
			// Append DeleteTime
			vector.AppendFixed[types.TS]((*bat).Vecs[ObjectListAttr_DeleteAt_Idx], objEntry.DeleteTime, false, mp)
			// Append isTombstone
			vector.AppendFixed[bool]((*bat).Vecs[ObjectListAttr_IsTombstone_Idx], isTombstone, false, mp)
		}
	}

	// Create checkpoint readers
	readers := make([]*logtail.CKPReader, 0)
	for _, entry := range checkpointEntries {
		reader := logtail.NewCKPReaderWithTableID_V2(entry.GetVersion(), entry.GetLocation(), tid, mp, fs)
		readers = append(readers, reader)
		if loc := entry.GetLocation(); !loc.IsEmpty() {
			ioutil.Prefetch(sid, fs, loc)
		}
	}

	// Read metadata for all readers
	for _, reader := range readers {
		if err = reader.ReadMeta(ctx); err != nil {
			return
		}
		reader.PrefetchData(sid)
	}

	// Consume checkpoint entries and fill batch
	for _, reader := range readers {
		if err = reader.ConsumeCheckpointWithTableID(
			ctx,
			func(ctx context.Context, fs fileservice.FileService, obj objectio.ObjectEntry, isTombstone bool) (err error) {
				fillInObjectListFn(obj, isTombstone, mp)
				return
			},
		); err != nil {
			return
		}
		(*bat).SetRowCount((*bat).Vecs[0].Length())
	}

	return
}
