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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

var _ NodeT = (*persistedNode)(nil)

type persistedNode struct {
	common.RefHelper
	object *baseObject
}

func newPersistedNode(object *baseObject) *persistedNode {
	node := &persistedNode{
		object: object,
	}
	node.OnZeroCB = node.close
	return node
}

func (node *persistedNode) close() {}

func (node *persistedNode) Rows() (uint32, error) {
	return node.object.meta.Load().GetObjectStats().Rows(), nil
}

func (node *persistedNode) Contains(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM,
	txn txnif.TxnReader,
	isCommitting bool,
	mp *mpool.MPool,
) (err error) {
	panic("should not be called")
}
func (node *persistedNode) GetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	maxVisibleRow uint32,
	keys containers.Vector,
	keysZM index.ZM,
	rowIDs containers.Vector,
	isCommitting bool,
	_ bool,
	_ bool,
	mp *mpool.MPool,
) (err error) {
	panic("should not be balled")
}

func (node *persistedNode) GetDataWindow(
	readSchema *catalog.Schema, colIdxes []int, from, to uint32, mp *mpool.MPool,
) (bat *containers.Batch, err error) {
	panic("to be implemented")
}

func (node *persistedNode) IsPersisted() bool { return true }

func (node *persistedNode) Scan(
	ctx context.Context,
	bat **containers.Batch,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	blkID uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (err error) {
	id := node.object.meta.Load().AsCommonID()
	id.SetBlockOffset(uint16(blkID))
	location, err := node.object.buildMetalocation(uint16(blkID))
	if err != nil {
		return err
	}
	var tsForAppendable *types.TS
	if node.object.meta.Load().IsAppendable() {
		ts := txn.GetStartTS()
		tsForAppendable = &ts
	}
	vecs, deletes, err := LoadPersistedColumnDatas(
		ctx, readSchema, node.object.rt, id, colIdxes, location, mp, tsForAppendable,
	)
	if err != nil {
		return err
	}
	// TODO: check visibility
	if *bat == nil {
		*bat = containers.NewBatch()
		(*bat).Deletes = deletes
		for i, idx := range colIdxes {
			var attr string
			if idx == objectio.SEQNUM_COMMITTS {
				attr = objectio.TombstoneAttr_CommitTs_Attr
				if vecs[i].GetType().Oid != types.T_TS {
					vecs[i].Close()
					vecs[i] = node.object.rt.VectorPool.Transient.GetVector(&objectio.TSType)
					createTS := node.object.meta.Load().GetCreatedAt()
					vector.AppendMultiFixed(vecs[i].GetDownstreamVector(), createTS, false, vecs[0].Length(), mp)
				}
			} else {
				attr = readSchema.ColDefs[idx].Name
			}
			(*bat).AddVector(attr, vecs[i])
		}
	} else {
		if (*bat).Deletes == nil {
			(*bat).Deletes = nulls.NewWithSize(1024)
		}
		len := (*bat).Length()
		deletes.Foreach(func(i uint64) bool {
			(*bat).Deletes.Add(i + uint64(len))
			return true
		})
		for i, idx := range colIdxes {
			var attr string
			if idx == objectio.SEQNUM_COMMITTS {
				attr = objectio.TombstoneAttr_CommitTs_Attr
				if vecs[i].GetType().Oid != types.T_TS {
					vecs[i].Close()
					vecs[i] = node.object.rt.VectorPool.Transient.GetVector(&objectio.TSType)
					createTS := node.object.meta.Load().GetCreatedAt()
					vector.AppendMultiFixed(vecs[i].GetDownstreamVector(), createTS, false, vecs[0].Length(), mp)
				}
			} else {
				attr = readSchema.ColDefs[idx].Name
			}
			(*bat).GetVectorByName(attr).Extend(vecs[i])
		}
	}
	return
}

func (node *persistedNode) CollectObjectTombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	objID *types.Objectid,
	bat **containers.Batch,
	mp *mpool.MPool,
	vpool *containers.VectorPool,
) (err error) {
	if !node.object.meta.Load().IsTombstone {
		panic("not support")
	}
	colIdxes := objectio.TombstoneColumns_TN_Created
	readSchema := node.object.meta.Load().GetTable().GetLastestSchema(true)
	var startTS types.TS
	if !node.object.meta.Load().IsAppendable() {
		startTS = node.object.meta.Load().GetCreatedAt()
		if startTS.LT(&start) || startTS.Greater(&end) {
			return
		}
	} else {
		createAt := node.object.meta.Load().GetCreatedAt()
		deleteAt := node.object.meta.Load().GetDeleteAt()
		if deleteAt.LT(&start) || createAt.Greater(&end) {
			return
		}
	}
	id := node.object.meta.Load().AsCommonID()

	var bf objectio.BloomFilter
	if bf, err = objectio.FastLoadBF(
		ctx,
		node.object.meta.Load().GetLocation(),
		false,
		node.object.rt.Fs.Service,
	); err != nil {
		return
	}
	objLocation := node.object.meta.Load().GetLocation()
	objDataMeta, err := objectio.FastLoadObjectMeta(ctx, &objLocation, false, node.object.GetFs().Service)
	if err != nil {
		return err
	}
	colCount := objDataMeta.MustGetMeta(objectio.SchemaData).GetBlockMeta(0).GetColumnCount()
	persistedByCN := colCount == 2
	for blkID := 0; blkID < node.object.meta.Load().BlockCnt(); blkID++ {
		buf := bf.GetBloomFilter(uint32(blkID))
		bfIndex := index.NewEmptyBloomFilterWithType(index.HBF)
		if err = index.DecodeBloomFilter(bfIndex, buf); err != nil {
			return
		}
		containes, err := bfIndex.PrefixMayContainsKey(objID[:], index.PrefixFnID_Object, 1)
		if err != nil {
			return err
		}
		if !containes {
			continue
		}
		id.SetBlockOffset(uint16(blkID))
		location, err := node.object.buildMetalocation(uint16(blkID))
		if err != nil {
			return err
		}
		vecs, _, err := LoadPersistedColumnDatas(
			ctx, readSchema, node.object.rt, id, colIdxes, location, mp, nil,
		)
		if err != nil {
			return err
		}
		defer func() {
			for i := range vecs {
				vecs[i].Close()
			}
		}()
		var commitTSs []types.TS
		if !persistedByCN {
			commitTSs = vector.MustFixedColWithTypeCheck[types.TS](vecs[2].GetDownstreamVector())
			rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](vecs[0].GetDownstreamVector())
			for i := 0; i < len(commitTSs); i++ {
				commitTS := commitTSs[i]
				if commitTS.GreaterEq(&start) && commitTS.LessEq(&end) &&
					types.PrefixCompare(rowIDs[i][:], objID[:]) == 0 { // TODO
					if *bat == nil {
						pkIdx := readSchema.GetColIdx(objectio.TombstoneAttr_PK_Attr)
						pkType := readSchema.ColDefs[pkIdx].GetType()
						*bat = catalog.NewTombstoneBatchByPKType(pkType, mp)
					}
					(*bat).GetVectorByName(objectio.TombstoneAttr_Rowid_Attr).Append(rowIDs[i], false)
					(*bat).GetVectorByName(objectio.TombstoneAttr_PK_Attr).Append(vecs[1].Get(i), false)
					(*bat).GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).Append(commitTS, false)
				}
			}
		} else {
			rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](vecs[0].GetDownstreamVector())
			for i := 0; i < len(rowIDs); i++ {
				if types.PrefixCompare(rowIDs[i][:], objID[:]) == 0 { // TODO
					if *bat == nil {
						pkIdx := readSchema.GetColIdx(objectio.TombstoneAttr_PK_Attr)
						pkType := readSchema.ColDefs[pkIdx].GetType()
						*bat = catalog.NewTombstoneBatchByPKType(pkType, mp)
					}
					(*bat).GetVectorByName(objectio.TombstoneAttr_Rowid_Attr).Append(rowIDs[i], false)
					(*bat).GetVectorByName(objectio.TombstoneAttr_PK_Attr).Append(vecs[1].Get(i), false)
					(*bat).GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).Append(startTS, false)
				}
			}
		}
	}
	return
}

func (node *persistedNode) FillBlockTombstones(
	ctx context.Context,
	txn txnif.TxnReader,
	blkID *objectio.Blockid,
	deletes **nulls.Nulls,
	deleteStartOffset uint64,
	mp *mpool.MPool) error {
	startTS := txn.GetStartTS()
	if !node.object.meta.Load().IsAppendable() {
		node.object.RLock()
		createAt := node.object.meta.Load().GetCreatedAt()
		node.object.RUnlock()
		if createAt.Greater(&startTS) {
			return nil
		}
	}
	id := node.object.meta.Load().AsCommonID()
	readSchema := node.object.meta.Load().GetTable().GetLastestSchema(true)
	var bf objectio.BloomFilter
	var err error
	if bf, err = objectio.FastLoadBF(
		ctx,
		node.object.meta.Load().GetLocation(),
		false,
		node.object.rt.Fs.Service,
	); err != nil {
		return err
	}
	for tombstoneBlkID := 0; tombstoneBlkID < node.object.meta.Load().BlockCnt(); tombstoneBlkID++ {
		buf := bf.GetBloomFilter(uint32(tombstoneBlkID))
		bfIndex := index.NewEmptyBloomFilterWithType(index.HBF)
		if err := index.DecodeBloomFilter(bfIndex, buf); err != nil {
			return err
		}
		containes, err := bfIndex.PrefixMayContainsKey(blkID[:], index.PrefixFnID_Block, 2)
		if err != nil {
			return err
		}
		if !containes {
			continue
		}
		id.SetBlockOffset(uint16(tombstoneBlkID))
		location, err := node.object.buildMetalocation(uint16(tombstoneBlkID))
		if err != nil {
			return err
		}
		vecs, _, err := LoadPersistedColumnDatas(
			ctx, readSchema, node.object.rt, id, []int{0}, location, mp, nil,
		)
		if err != nil {
			return err
		}
		defer func() {
			for i := range vecs {
				vecs[i].Close()
			}
		}()
		var commitTSs []types.TS
		var commitTSVec containers.Vector
		if node.object.meta.Load().IsAppendable() {
			commitTSVec, err = node.object.LoadPersistedCommitTS(uint16(tombstoneBlkID))
			if err != nil {
				return err
			}
			commitTSs = vector.MustFixedColWithTypeCheck[types.TS](commitTSVec.GetDownstreamVector())
		}
		if commitTSVec != nil {
			defer commitTSVec.Close()
		}
		rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](vecs[0].GetDownstreamVector())
		// TODO: biselect, check visibility
		for i := 0; i < len(rowIDs); i++ {
			if node.object.meta.Load().IsAppendable() {
				if commitTSs[i].Greater(&startTS) {
					continue
				}
			}
			rowID := rowIDs[i]
			if types.PrefixCompare(rowID[:], blkID[:]) == 0 {
				if *deletes == nil {
					*deletes = &nulls.Nulls{}
				}
				offset := rowID.GetRowOffset()
				(*deletes).Add(uint64(offset) + deleteStartOffset)
			}
		}
	}
	return nil
}
