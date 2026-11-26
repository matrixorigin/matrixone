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
	"slices"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
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

type scanNoCopyKey struct{}

// WithScanNoCopy signals the scan chain to use zero-copy mode (needCopy=false).
// Vectors will wrap fileservice buffers directly; the caller must hold the
// returned DataRelease on the batch until the data is fully consumed.
func WithScanNoCopy(ctx context.Context) context.Context {
	return context.WithValue(ctx, scanNoCopyKey{}, true)
}

func isScanNoCopy(ctx context.Context) bool {
	v, _ := ctx.Value(scanNoCopyKey{}).(bool)
	return v
}

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
	mp *mpool.MPool,
) (err error) {
	panic("should not be called")
}
func (node *persistedNode) GetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	lower, to types.TS,
	keys containers.Vector,
	keysZM index.ZM,
	rowIDs containers.Vector,
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
	vecs, deletes, release, err := LoadPersistedColumnData(
		ctx, readSchema, node.object.rt, id, colIdxes, location, mp, tsForAppendable,
		!isScanNoCopy(ctx),
	)
	replaceCommitts := func(vecs []containers.Vector, i int) {
		createTS := node.object.meta.Load().GetCreatedAt()
		length := vecs[0].Length()
		containers.ReleaseVector(vecs[i])
		vecs[i] = node.object.rt.VectorPool.Transient.GetVector(&objectio.TSType)
		vector.AppendMultiFixed(vecs[i], createTS, false, length, vecs[i].MPool())
	}
	if err != nil {
		return err
	}
	// TODO: check visibility
	if *bat == nil {
		*bat = containers.NewBatch()
		(*bat).Deletes = deletes
		(*bat).DataRelease = release
		for i, idx := range colIdxes {
			var attr string
			if idx == objectio.SEQNUM_COMMITTS {
				attr = objectio.TombstoneAttr_CommitTs_Attr
				if vecs[i].IsConstNull() {
					replaceCommitts(vecs, i)
				}
				/// TODO: Read old version of nonappendable block?
			} else if idx == objectio.SEQNUM_ABORT {
				attr = objectio.TombstoneAttr_Abort_Attr
			} else {
				attr = readSchema.ColDefs[idx].Name
			}

			// RelLogicalID COMPAT
			if attr == pkgcatalog.SystemRelAttr_LogicalID && vecs[i].IsConstNull() {
				dup, err := (*bat).GetVectorByName(pkgcatalog.SystemRelAttr_ID).Dup(mp)
				if err != nil {
					// vecs[:i] are in *bat, which the caller closes.
					for _, v := range vecs[i:] {
						containers.ReleaseVector(v)
					}
					return err
				}
				containers.ReleaseVector(vecs[i])
				vecs[i] = dup
			}
			(*bat).AddVector(attr, vecs[i])
		}
	} else {
		len := (*bat).Length()
		if !deletes.IsEmpty() {
			// Reserve every row the merge can set, so the Adds below cannot
			// fail. Nothing loaded is in the batch yet: on failure close it.
			need := len + int(deletes.Len())
			if (*bat).Deletes == nil {
				(*bat).Deletes, err = nulls.NewWithSize(need, mp)
			} else {
				err = (*bat).Deletes.GetBitmap().TryExpandWithSize(need)
			}
			if err != nil {
				deletes.Free()
				for _, v := range vecs {
					containers.ReleaseVector(v)
				}
				if release != nil {
					release()
				}
				return err
			}
		}
		deletes.Foreach(func(i uint64) bool {
			(*bat).Deletes.Add(i + uint64(len))
			return true
		})
		// The rows are merged into the batch's own bitmap.
		deletes.Free()

		// RelLogicalID COMPAT
		var tidvec containers.Vector
		for i, idx := range colIdxes {
			var attr string
			if idx == objectio.SEQNUM_COMMITTS {
				attr = objectio.TombstoneAttr_CommitTs_Attr
				if vecs[i].IsConstNull() {
					replaceCommitts(vecs, i)
				}
			} else if idx == objectio.SEQNUM_ABORT {
				attr = objectio.TombstoneAttr_Abort_Attr
			} else {
				attr = readSchema.ColDefs[idx].Name
			}
			// RelLogicalID COMPAT
			if attr == pkgcatalog.SystemRelAttr_LogicalID && vecs[i].IsConstNull() {
				containers.ReleaseVector(vecs[i])
				vecs[i] = tidvec
			}

			// System table COMPAT: columns added in newer versions produce
			// const null vectors when reading old data blocks. The batch
			// vector from the first block may be const; convert it to a
			// regular vector so Extend (which panics on const dest) works.
			if destVec := (*bat).GetVectorByName(attr); destVec.IsConst() {
				destIdx := (*bat).Nameidx[attr]
				constInner := destVec
				length := constInner.Length()
				newInner := vector.NewVec(*constInner.GetType(), mp)
				if err = newInner.UnionBatch(constInner, 0, length, nil, mp); err != nil {
					newInner.Free(mp)
					// Close is idempotent, so closing every input vector
					// (tidvec included) is safe whatever was closed already.
					for _, v := range vecs {
						containers.ReleaseVector(v)
					}
					if tidvec != nil {
						containers.ReleaseVector(tidvec)
					}
					if release != nil {
						release()
					}
					return
				}
				containers.ReleaseVector(destVec)
				(*bat).Vecs[destIdx] = newInner
			}
			containers.Extend((*bat).GetVectorByName(attr), vecs[i])

			// RelLogicalID COMPAT
			if attr == pkgcatalog.SystemRelAttr_ID {
				tidvec = vecs[i]
				continue
			}
			containers.ReleaseVector(vecs[i])
		}
		// RelLogicalID COMPAT
		if tidvec != nil {
			containers.ReleaseVector(tidvec)
		}
		// In zero-copy mode (needCopy=false), release frees the IOVector backing.
		// Data has been Extend()ed (copied) into the existing batch, so the
		// IOVector is no longer needed.  In copy mode release is nil.
		if release != nil {
			release()
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
	colIdxes := append(
		slices.Clone(objectio.TombstoneColumns_TN_Created),
		objectio.SEQNUM_ABORT,
	)
	readSchema := node.object.meta.Load().GetTable().GetLastestSchema(true)
	var startTS types.TS
	if !node.object.meta.Load().IsAppendable() {
		startTS = node.object.meta.Load().GetCreatedAt()
		if startTS.LT(&start) || startTS.GT(&end) {
			return
		}
	} else {
		createAt := node.object.meta.Load().GetCreatedAt()
		deleteAt := node.object.meta.Load().GetDeleteAt()
		if deleteAt.LT(&start) || createAt.GT(&end) {
			return
		}
	}
	id := node.object.meta.Load().AsCommonID()

	var bf objectio.BloomFilter
	if bf, err = objectio.FastLoadBF(
		ctx,
		node.object.meta.Load().GetLocation(),
		false,
		node.object.rt.Fs,
	); err != nil {
		return
	}
	objLocation := node.object.meta.Load().GetLocation()
	objDataMeta, err := objectio.FastLoadObjectMeta(ctx, &objLocation, false, node.object.GetFs())
	if err != nil {
		return err
	}
	firstBlock := objDataMeta.MustGetMeta(objectio.SchemaData).GetBlockMeta(0)
	persistedByCN := objectio.ResolveSpecialColumnLayout(firstBlock).CommitTS ==
		objectio.InvalidSpecialColumnPosition
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
		vecs, _, _, err := LoadPersistedColumnData(
			ctx, readSchema, node.object.rt, id, colIdxes, location, mp, nil,
			true,
		)
		if err != nil {
			return err
		}
		var commitTSs []types.TS
		if !persistedByCN {
			commitTSs = vector.MustFixedColWithTypeCheck[types.TS](vecs[2])
			abortVec := vecs[3]
			var aborts []bool
			if !abortVec.IsConstNull() {
				aborts = vector.MustFixedColWithTypeCheck[bool](abortVec)
			}
			rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](vecs[0])
			for i := 0; i < len(commitTSs); i++ {
				if aborts != nil && aborts[i] {
					continue
				}
				commitTS := commitTSs[i]
				if commitTS.GE(&start) && commitTS.LE(&end) &&
					types.PrefixCompare(rowIDs[i][:], objID[:]) == 0 { // TODO
					if *bat == nil {
						pkIdx := readSchema.GetColIdx(objectio.TombstoneAttr_PK_Attr)
						pkType := readSchema.ColDefs[pkIdx].GetType()
						*bat = catalog.NewTombstoneBatchByPKType(pkType, mp)
					}
					containers.AppendValue((*bat).GetVectorByName(objectio.TombstoneAttr_Rowid_Attr), rowIDs[i], false)
					containers.AppendValue((*bat).GetVectorByName(objectio.TombstoneAttr_PK_Attr), containers.GetValue(vecs[1], i), false)
					containers.AppendValue((*bat).GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr), commitTS, false)
				}
			}
		} else {
			rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](vecs[0])
			for i := 0; i < len(rowIDs); i++ {
				if types.PrefixCompare(rowIDs[i][:], objID[:]) == 0 { // TODO
					if *bat == nil {
						pkIdx := readSchema.GetColIdx(objectio.TombstoneAttr_PK_Attr)
						pkType := readSchema.ColDefs[pkIdx].GetType()
						*bat = catalog.NewTombstoneBatchByPKType(pkType, mp)
					}
					containers.AppendValue((*bat).GetVectorByName(objectio.TombstoneAttr_Rowid_Attr), rowIDs[i], false)
					containers.AppendValue((*bat).GetVectorByName(objectio.TombstoneAttr_PK_Attr), containers.GetValue(vecs[1], i), false)
					containers.AppendValue((*bat).GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr), startTS, false)
				}
			}
		}
		for i := range vecs {
			containers.ReleaseVector(vecs[i])
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
		if createAt.GT(&startTS) {
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
		node.object.rt.Fs,
	); err != nil {
		return err
	}
	colIdxs := []int{0}
	var snapshotTS *types.TS
	if node.object.meta.Load().IsAppendable() {
		colIdxs = append(colIdxs, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT)
		snapshotTS = &startTS
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
		vecs, visibilityDeletes, _, err := LoadPersistedColumnData(
			ctx, readSchema, node.object.rt, id, colIdxs, location, mp, snapshotTS,
			true,
		)
		if err != nil {
			return err
		}
		rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](vecs[0])
		for i := 0; i < len(rowIDs); i++ {
			if visibilityDeletes != nil && visibilityDeletes.Contains(uint64(i)) {
				continue
			}
			rowID := rowIDs[i]
			if types.PrefixCompare(rowID[:], blkID[:]) == 0 {
				if *deletes == nil {
					*deletes = &nulls.Nulls{}
					(*deletes).Init(mp)
				}
				offset := rowID.GetRowOffset()
				(*deletes).Add(uint64(offset) + deleteStartOffset)
			}
		}
		visibilityDeletes.Free()
		for i := range vecs {
			containers.ReleaseVector(vecs[i])
		}
	}
	return nil
}
