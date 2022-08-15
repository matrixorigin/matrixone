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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
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
		if !blk.meta.GetSchema().HasPK() {
			return
		}
		keysCtx := new(index.KeysCtx)
		err = blk.node.DoWithPin(func() (err error) {
			var vec containers.Vector
			if blk.meta.GetSchema().IsSinglePK() {
				// TODO: use mempool
				vec, err = blk.node.GetColumnDataCopy(blk.node.rows, blk.meta.GetSchema().GetSingleSortKeyIdx(), nil)
				if err != nil {
					return
				}
				// TODO: apply deletes
				keysCtx.Keys = vec
			} else {
				sortKeys := blk.meta.GetSchema().SortKey
				vs := make([]containers.Vector, sortKeys.Size())
				for i := range vs {
					vec, err = blk.node.GetColumnDataCopy(blk.node.rows, sortKeys.Defs[i].Idx, nil)
					if err != nil {
						return
					}
					// TODO: apply deletes
					vs[i] = vec
					defer vs[i].Close()
				}
				keysCtx.Keys = model.EncodeCompoundColumn(vs...)
			}
			return
		})
		if err != nil {
			return
		}
		keysCtx.Start = 0
		keysCtx.Count = keysCtx.Keys.Length()
		defer keysCtx.Keys.Close()
		var zeroV types.TS
		_, err = blk.index.BatchUpsert(keysCtx, 0, zeroV)
		return
	}
	if blk.meta.GetSchema().HasSortKey() {
		err = blk.index.ReadFrom(blk)
	}
	return
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
