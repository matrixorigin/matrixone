// Copyright 2021 Matrix Origin
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

package tables

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type object struct {
	*baseObject
}

func newObject(
	meta *catalog.ObjectEntry,
	rt *dbutils.Runtime,
) *object {
	obj := &object{}
	obj.baseObject = newBaseObject(obj, meta, rt)
	pnode := newPersistedNode(obj.baseObject)
	node := NewNode(pnode)
	node.Ref()
	obj.node.Store(node)
	return obj
}

func (obj *object) Init() (err error) {
	return
}

func (obj *object) OnApplyDelete(
	deleted uint64,
	ts types.TS) (err error) {
	obj.meta.GetTable().RemoveRows(deleted)
	return
}

func (obj *object) PrepareCompact() bool {
	return obj.meta.PrepareCompact()
}

func (obj *object) PrepareCompactInfo() (result bool, reason string) {
	return obj.meta.PrepareCompact(), ""
}

func (obj *object) FreezeAppend() {}

func (obj *object) Pin() *common.PinnedItem[*object] {
	obj.Ref()
	return &common.PinnedItem[*object]{
		Val: obj,
	}
}

func (obj *object) GetColumnDataByIds(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	blkID uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (view *containers.BlockView, err error) {
	node := obj.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	return obj.ResolvePersistedColumnDatas(
		ctx, txn, schema, blkID, colIdxes, false, mp,
	)
}

// GetColumnDataById Get the snapshot at txn's start timestamp of column data.
// Notice that for non-appendable object, if it is visible to txn,
// then all the object data pointed by meta location also be visible to txn;
func (obj *object) GetColumnDataById(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	blkID uint16,
	col int,
	mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	schema := readSchema.(*catalog.Schema)
	return obj.ResolvePersistedColumnData(
		ctx,
		txn,
		schema,
		blkID,
		col,
		false,
		mp,
	)
}
func (obj *object) CoarseCheckAllRowsCommittedBefore(ts types.TS) bool {
	obj.meta.RLock()
	defer obj.meta.RUnlock()
	creatTS := obj.meta.GetCreatedAtLocked()
	return creatTS.Less(&ts)
}

func (obj *object) BatchDedup(
	ctx context.Context,
	txn txnif.AsyncTxn,
	keys containers.Vector,
	keysZM index.ZM,
	rowmask *roaring.Bitmap,
	precommit bool,
	bf objectio.BloomFilter,
	mp *mpool.MPool,
) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Infof("BatchDedup %s (%v)obj-%s: %v",
				obj.meta.GetTable().GetLastestSchemaLocked().Name,
				obj.IsAppendable(),
				obj.meta.ID.String(),
				err)
		}
	}()
	return obj.PersistedBatchDedup(
		ctx,
		txn,
		precommit,
		keys,
		keysZM,
		rowmask,
		false,
		bf,
		mp,
	)
}

func (obj *object) GetValue(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	blkID uint16,
	row, col int,
	mp *mpool.MPool,
) (v any, isNull bool, err error) {
	node := obj.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	return obj.getPersistedValue(
		ctx, txn, schema, blkID, row, col, false, mp,
	)
}

func (obj *object) RunCalibration() (score int, err error) {
	score, _ = obj.estimateRawScore()
	return
}

func (obj *object) estimateRawScore() (score int, dropped bool) {
	if obj.meta.HasDropCommitted() && !obj.meta.InMemoryDeletesExisted() {
		dropped = true
		return
	}
	changeCnt := uint32(0)
	obj.RLock()
	objectMVCC := obj.tryGetMVCC()
	if objectMVCC != nil {
		changeCnt = objectMVCC.GetChangeIntentionCntLocked()
	}
	obj.RUnlock()
	if changeCnt == 0 {
		// No deletes found
		score = 0
	} else {
		// Any delete
		score = 1
	}

	// If any delete found and the table or database of the object had
	// been deleted. Force checkpoint the object
	if score > 0 {
		if _, terminated := obj.meta.GetTerminationTS(); terminated {
			score = 100
		}
	}
	return
}

func (obj *object) GetByFilter(
	ctx context.Context,
	txn txnif.AsyncTxn,
	filter *handle.Filter,
	mp *mpool.MPool,
) (blkID uint16, offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	if obj.meta.GetSchema().SortKey == nil {
		rid := filter.Val.(types.Rowid)
		offset = rid.GetRowOffset()
		return
	}

	node := obj.PinNode()
	defer node.Unref()
	return obj.getPersistedRowByFilter(ctx, node.MustPNode(), txn, filter, mp)
}

func (obj *object) getPersistedRowByFilter(
	ctx context.Context,
	pnode *persistedNode,
	txn txnif.TxnReader,
	filter *handle.Filter,
	mp *mpool.MPool,
) (blkID uint16, offset uint32, err error) {
	var sortKey containers.Vector
	schema := obj.meta.GetSchema()
	idx := schema.GetSingleSortKeyIdx()
	objMVCC := obj.tryGetMVCC()
	for blkID = uint16(0); blkID < uint16(obj.meta.BlockCnt()); blkID++ {
		var ok bool
		ok, err = pnode.ContainsKey(ctx, filter.Val, uint32(blkID))
		if err != nil {
			return
		}
		if !ok {
			continue
		}
		if sortKey, err = obj.LoadPersistedColumnData(ctx, schema, idx, mp, blkID); err != nil {
			continue
		}
		defer sortKey.Close()
		off, existed := compute.GetOffsetByVal(sortKey, filter.Val, nil)
		if !existed {
			continue
		}
		offset = uint32(off)

		if objMVCC == nil {
			return
		}
		objMVCC.RLock()
		defer objMVCC.RUnlock()
		var deleted bool
		deleted, err = objMVCC.IsDeletedLocked(offset, txn, blkID)
		if err != nil {
			return
		}
		if deleted {
			continue
		}
		var deletes *nulls.Nulls
		deletes, err = obj.persistedCollectDeleteMaskInRange(ctx, blkID, types.TS{}, txn.GetStartTS(), mp)
		if err != nil {
			return
		}
		if !deletes.Contains(uint64(offset)) {
			return
		}

	}
	err = moerr.NewNotFoundNoCtx()
	return
}

func (obj *object) EstimateMemSize() (int, int) {
	node := obj.PinNode()
	defer node.Unref()
	obj.RLock()
	defer obj.RUnlock()
	objMVCC := obj.tryGetMVCC()
	if objMVCC == nil {
		return 0, 0
	}
	dsize := objMVCC.EstimateMemSizeLocked()
	return 0, dsize
}

func (obj *object) GetRowsOnReplay() uint64 {
	fileRows := uint64(obj.meta.GetLatestCommittedNodeLocked().
		BaseNode.ObjectStats.Rows())
	return fileRows
}
