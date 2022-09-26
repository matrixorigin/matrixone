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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
	an := updates.NewCommittedAppendNode(blk.ckpTs.Load().(types.TS), 0, blk.node.rows, blk.mvcc)
	blk.mvcc.OnReplayAppendNode(an)
	masks, vals := blk.file.LoadUpdates()
	for colIdx, mask := range masks {
		logutil.Info("[Start]",
			common.TimestampField(blk.ckpTs.Load().(types.TS)),
			common.OperationField("install-update"),
			common.OperandNameSpace(),
			common.AnyField("rows", blk.node.rows),
			common.AnyField("col", colIdx),
			common.CountField(int(mask.GetCardinality())))
		un := updates.NewCommittedColumnUpdateNode(blk.ckpTs.Load().(types.TS), blk.ckpTs.Load().(types.TS), blk.meta.AsCommonID(), nil)
		un.SetMask(mask)
		un.SetValues(vals[colIdx])
		if err = blk.OnReplayUpdate(uint16(colIdx), un); err != nil {
			return
		}
	}
	deletes, err := blk.file.LoadDeletes()
	if err != nil || deletes == nil {
		return
	}
	logutil.Info("[Start]", common.TimestampField(blk.ckpTs.Load().(types.TS)),
		common.OperationField("install-del"),
		common.OperandNameSpace(),
		common.AnyField("rows", blk.node.rows),
		common.CountField(int(deletes.GetCardinality())))
	deleteNode := updates.NewMergedNode(blk.ckpTs.Load().(types.TS))
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

// replayMutIndex load column data to memory to construct index
func (blk *dataBlock) replayMutIndex() error {
	schema := blk.meta.GetSchema()
	for _, colDef := range schema.ColDefs {
		if colDef.IsPhyAddr() {
			continue
		}
		keysCtx := new(index.KeysCtx)
		vec, err := blk.node.GetColumnDataCopy(0, blk.node.rows, colDef.Idx, nil)
		if err != nil {
			return err
		}
		// TODO: apply deletes
		keysCtx.Keys = vec
		keysCtx.Count = vec.Length()
		defer keysCtx.Keys.Close()
		var zeroV types.TS
		blk.indexes[colDef.Idx].BatchUpsert(keysCtx, 0, zeroV)
	}
	return nil
}

// replayImmutIndex load index meta to construct managed node
func (blk *dataBlock) replayImmutIndex() error {
	file := blk.GetBlockFile()
	idxMeta, err := file.LoadIndexMeta()
	if err != nil {
		return err
	}
	metas := idxMeta.(*indexwrapper.IndicesMeta)

	pkIdx := -1024
	schema := blk.meta.GetSchema()
	if schema.HasPK() {
		pkIdx = schema.GetSingleSortKeyIdx()
	}
	var prevPkMeta indexwrapper.IndexMeta
	for _, meta := range metas.Metas {
		colIdx := int(meta.ColIdx)
		if colIdx == pkIdx {
			if prevPkMeta.IdxType == indexwrapper.InvalidIndexType {
				// found one meta, stash it for later use
				prevPkMeta = meta
			} else {
				// found two meta, construct the pk index
				index := indexwrapper.NewImmutableIndex()
				if err = index.ReadFrom(blk, schema.ColDefs[pkIdx], prevPkMeta, meta); err != nil {
					return err
				}
				blk.indexes[pkIdx] = index
				blk.pkIndex = index
			}
		} else {
			// non-pk column, construct index
			index := indexwrapper.NewImmutableIndex()
			if err = index.ReadFrom(blk, schema.ColDefs[meta.ColIdx], meta); err != nil {
				return err
			}
			blk.indexes[colIdx] = index
		}
	}
	return nil
}

func (blk *dataBlock) OnReplayDelete(node txnif.DeleteNode) (err error) {
	blk.mvcc.OnReplayDeleteNode(node)
	err = node.OnApply()
	return
}

func (blk *dataBlock) OnReplayUpdate(colIdx uint16, node txnif.UpdateNode) (err error) {
	chain := blk.mvcc.GetColumnChain(colIdx)
	chain.OnReplayUpdateNode(node)
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
	err = appender.ReplayAppend(bat)
	return
}
