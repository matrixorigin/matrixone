// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func HybridScanByBlock(
	ctx context.Context,
	tableEntry *catalog.TableEntry,
	txn txnif.TxnReader,
	bat **containers.Batch,
	readSchema *catalog.Schema,
	colIdxs []int,
	blkID *objectio.Blockid,
	mp *mpool.MPool,
) error {
	dataObject, err := tableEntry.GetObjectByID(blkID.Object(), false)
	if err != nil {
		return err
	}
	_, offset := blkID.Offsets()
	deleteStartOffset := 0
	if *bat != nil {
		deleteStartOffset = (*bat).Length()
	}
	err = dataObject.GetObjectData().Scan(ctx, bat, txn, readSchema, offset, colIdxs, mp)
	if err != nil {
		return err
	}
	if *bat == nil {
		return nil
	}
	it := tableEntry.MakeTombstoneObjectIt()
	for it.Next() {
		tombstone := it.Item()
		if !tombstone.IsVisible(txn) {
			continue
		}
		err := tombstone.GetObjectData().FillBlockTombstones(ctx, txn, blkID, &(*bat).Deletes, uint64(deleteStartOffset), mp)
		if err != nil {
			(*bat).Close()
			return err
		}
	}
	id := dataObject.AsCommonID()
	id.BlockID = *blkID
	err = txn.GetStore().FillInWorkspaceDeletes(id, &(*bat).Deletes, uint64(deleteStartOffset))
	if err != nil {
		(*bat).Close()
	}
	return err
}

func TombstoneRangeScanByObject(
	ctx context.Context,
	tableEntry *catalog.TableEntry,
	objectID objectio.ObjectId,
	start, end types.TS,
	mp *mpool.MPool,
	vpool *containers.VectorPool,
) (bat *containers.Batch, err error) {
	it := tableEntry.MakeTombstoneObjectIt()
	for it.Next() {
		tombstone := it.Item()
		if tombstone.IsAppendable() {
			needWait, txnToWait := tombstone.CreateNode.NeedWaitCommitting(end)
			if needWait {
				txnToWait.GetTxnState(true)
			}
		}
	}
	it = tableEntry.MakeTombstoneObjectIt()
	for it.Next() {
		tombstone := it.Item()
		if tombstone.IsCreatingOrAborted() {
			continue
		}
		if tombstone.IsAppendable() {
			if !tombstone.DeletedAt.IsEmpty() && tombstone.DeletedAt.LT(&start) {
				continue
			}
			if tombstone.CreatedAt.GT(&end) {
				continue
			}
		} else {
			if !tombstone.ObjectStats.GetCNCreated() {
				continue
			}
		}
		if tombstone.HasCommittedPersistedData() {
			zm := tombstone.SortKeyZoneMap()
			if !zm.RowidPrefixEq(objectID[:]) {
				continue
			}
			// TODO: Bloomfilter
		}
		err = tombstone.GetObjectData().CollectObjectTombstoneInRange(ctx, start, end, &objectID, &bat, mp, vpool)
		if err != nil {
			return nil, err
		}
	}
	return
}

func RangeScanInMemoryByObject(
	ctx context.Context,
	objEntry *catalog.ObjectEntry,
	batches map[uint32]*containers.BatchWithVersion,
	start, end types.TS,
	mp *mpool.MPool,
) (err error) {
	err = objEntry.GetObjectData().ScanInMemory(ctx, batches, start, end, mp)
	return
}

// TODO: ensure bat does not exceed the 1G limit
func ReadSysTableBatch(ctx context.Context, entry *catalog.TableEntry, readTxn txnif.AsyncTxn) *containers.Batch {
	if entry.ID > 3 {
		panic(fmt.Sprintf("unsupported sys table id %v", entry))
	}
	schema := entry.GetLastestSchema(false)
	it := entry.MakeObjectIt(false)
	defer it.Release()
	colIdxes := make([]int, 0, len(schema.ColDefs))
	for _, col := range schema.ColDefs {
		if col.IsPhyAddr() {
			continue
		}
		colIdxes = append(colIdxes, col.Idx)
	}
	var bat *containers.Batch
	prevLen := 0
	for it.Next() {
		obj := it.Item()
		if !obj.IsVisible(readTxn) {
			continue
		}
		for blkOffset := range obj.BlockCnt() {
			blkid := objectio.NewBlockidWithObjectID(obj.ID(), uint16(blkOffset))
			err := HybridScanByBlock(ctx, entry, readTxn, &bat, schema, colIdxes, blkid, common.CheckpointAllocator)
			if err != nil || bat == nil || bat.Length() == prevLen {
				panic(fmt.Sprintf("blkbat is nil, obj %v, blkid %v, err %v", obj.ID().String(), blkid, err))
			}
		}
	}
	if bat != nil {
		bat.Compact()
	}
	return bat
}
