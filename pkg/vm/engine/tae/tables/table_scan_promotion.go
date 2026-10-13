// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

// ReadMergeSettingsBatchForPromotion reads a complete settings snapshot. The
// caller owns the returned batch; this function owns and closes a partial batch
// on error. Unlike the normal startup reader, it never returns partial data.
func ReadMergeSettingsBatchForPromotion(
	ctx context.Context,
	entry *catalog.TableEntry,
	txn txnif.AsyncTxn,
) (bat *containers.Batch, err error) {
	defer func() {
		if err != nil && bat != nil {
			bat.Close()
			bat = nil
		}
	}()

	schema := entry.GetLastestSchema(false)
	colIdxes := make([]int, 0, len(schema.ColDefs))
	for _, col := range schema.ColDefs {
		if !col.IsPhyAddr() {
			colIdxes = append(colIdxes, col.Idx)
		}
	}
	it := entry.MakeDataVisibleObjectIt(txn)
	defer it.Release()
	for it.Next() {
		obj := it.Item()
		for blkOffset := range obj.BlockCnt() {
			if err = ctx.Err(); err != nil {
				return
			}
			blkID := objectio.NewBlockidWithObjectID(obj.ID(), uint16(blkOffset))
			if err = scanMergeSettingsBlockForPromotion(
				ctx, entry, txn, &bat, schema, colIdxes, &blkID,
			); err != nil {
				return
			}
		}
	}
	if bat != nil {
		bat.Compact()
	}
	return bat, nil
}

// This is the same block read as HybridScanByBlock without its inconsistent
// internal batch Close calls. The enclosing reader is the sole batch owner.
func scanMergeSettingsBlockForPromotion(
	ctx context.Context,
	entry *catalog.TableEntry,
	txn txnif.AsyncTxn,
	bat **containers.Batch,
	schema *catalog.Schema,
	colIdxes []int,
	blkID *objectio.Blockid,
) error {
	obj, err := entry.GetObjectByID(blkID.Object(), false)
	if err != nil {
		return err
	}
	deleteStartOffset := 0
	if *bat != nil {
		deleteStartOffset = (*bat).Length()
	}
	_, offset := blkID.Offsets()
	if err = obj.GetObjectData().Scan(
		ctx, bat, txn, schema, offset, colIdxes, common.MergeAllocator,
	); err != nil || *bat == nil {
		return err
	}
	it := entry.MakeTombstoneVisibleObjectIt(txn)
	defer it.Release()
	for it.Next() {
		tombstone := it.Item()
		if err = tombstone.GetObjectData().FillBlockTombstones(
			ctx, txn, blkID, &(*bat).Deletes, uint64(deleteStartOffset), common.MergeAllocator,
		); err != nil {
			return err
		}
	}
	id := obj.AsCommonID()
	id.BlockID = *blkID
	return txn.GetStore().FillInWorkspaceDeletes(id, &(*bat).Deletes, uint64(deleteStartOffset))
}
