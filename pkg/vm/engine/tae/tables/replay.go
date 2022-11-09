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
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

func (blk *dataBlock) ReplayDelta() (err error) {
	if !blk.meta.IsAppendable() {
		return
	}
	an := updates.NewCommittedAppendNode(types.TS{}, 0, blk.node.rows, blk.mvcc)
	blk.mvcc.OnReplayAppendNode(an)
	deletes := &roaring.Bitmap{}
	if err != nil || deletes == nil {
		return
	}
	deleteNode := updates.NewMergedNode(types.TS{})
	deleteNode.SetDeletes(deletes)
	err = blk.OnReplayDelete(deleteNode)
	return
}

func (blk *dataBlock) ReplayIndex() (err error) {
	if blk.meta.IsAppendable() {
		return blk.replayMutIndex()
	}
	return blk.replayImmutIndex()
}

func (blk *dataBlock) ReplayImmutIndex() (err error) {
	blk.mvcc.Lock()
	defer blk.mvcc.Unlock()
	for _, index := range blk.indexes {
		if err = index.Destroy(); err != nil {
			return
		}
	}
	blk.dataFlushed = true
	return blk.replayImmutIndex()
}

// replayMutIndex load column data to memory to construct index
func (blk *dataBlock) replayMutIndex() error {
	schema := blk.meta.GetSchema()
	for _, colDef := range schema.ColDefs {
		if colDef.IsPhyAddr() {
			continue
		}
		keysCtx := new(index.KeysCtx)
		vec, err := blk.node.GetColumnData(0, blk.node.rows, colDef.Idx, nil)
		if err != nil {
			return err
		}
		// TODO: apply deletes
		keysCtx.Keys = vec
		keysCtx.Count = vec.Length()
		defer keysCtx.Keys.Close()
		blk.indexes[colDef.Idx].BatchUpsert(keysCtx, 0)
	}
	return nil
}

// replayImmutIndex load index meta to construct managed node
func (blk *dataBlock) replayImmutIndex() error {
	schema := blk.meta.GetSchema()
	pkIdx := -1024
	if schema.HasPK() {
		pkIdx = schema.GetSingleSortKeyIdx()
	}
	for i := range schema.ColDefs {
		index := indexwrapper.NewImmutableIndex()
		if err := index.ReadFrom(blk, schema.ColDefs[i], uint16(i)); err != nil {
			return err
		}
		blk.indexes[i] = index
		if i == pkIdx {
			blk.pkIndex = index
		}
	}
	return nil
}

func (blk *dataBlock) OnReplayDelete(node txnif.DeleteNode) (err error) {
	blk.mvcc.OnReplayDeleteNode(node)
	err = node.OnApply()
	return
}

func (blk *dataBlock) OnReplayAppend(node txnif.AppendNode) (err error) {
	an := node.(*updates.AppendNode)
	blk.node.block.mvcc.OnReplayAppendNode(an)
	return
}

func (blk *dataBlock) OnReplayAppendPayload(bat *containers.Batch) (err error) {
	appender, err := blk.MakeAppender()
	if err != nil {
		return
	}
	_, err = appender.ReplayAppend(bat, nil)
	return
}
