// Copyright 2021-2024 Matrix Origin
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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/partitionprune"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	splan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ engine.Relation = (*partitionTxnTable)(nil)

type partitionTxnTable struct {
	primary  *txnTable
	ps       partitionservice.PartitionService
	metadata partition.PartitionMetadata
}

func newPartitionTxnTable(
	primary *txnTable,
	metadata partition.PartitionMetadata,
	ps partitionservice.PartitionService,
) *partitionTxnTable {
	return &partitionTxnTable{
		primary:  primary,
		metadata: metadata,
		ps:       ps,
	}
}

func (t *partitionTxnTable) getRelation(
	ctx context.Context,
	idx int,
) (engine.Relation, error) {
	return t.primary.db.relation(
		ctx,
		t.metadata.Partitions[idx].PartitionTableName,
		t.primary.proc.Load(),
	)
}

func (t *partitionTxnTable) Ranges(
	ctx context.Context,
	param engine.RangesParam,
) (engine.RelData, error) {
	targets, err := partitionprune.Filter(
		t.primary.proc.Load(),
		param.BlockFilters,
		t.metadata,
	)
	if err != nil {
		return nil, err
	}

	pd := newPartitionedRelData()
	for _, idx := range targets {
		rel, err := t.getRelation(ctx, idx)
		if err != nil {
			return nil, err
		}
		if err := pd.addPartition(ctx, rel, param, idx); err != nil {
			return nil, err
		}
	}
	return pd, nil
}

func (t *partitionTxnTable) BuildReaders(
	ctx context.Context,
	proc any,
	expr *plan.Expr,
	relData engine.RelData,
	num int,
	txnOffset int,
	orderBy bool,
	policy engine.TombstoneApplyPolicy,
	filterHint engine.FilterHint,
) ([]engine.Reader, error) {
	var readers []engine.Reader
	//m := make(map[int]engine.RelData, 2)
	//slice := relData.GetBlockInfoSlice()
	//n := slice.Len()
	//for i := 0; i < n; i++ {
	//	value := slice.Get(i)
	//	data, ok := m[int(value.PartitionIdx)]
	//	if !ok {
	//		data = relData.BuildEmptyRelData(n)
	//		data.AttachTombstones(data.GetTombstones())
	//		m[int(value.PartitionIdx)] = data
	//	}
	//	data.AppendBlockInfo(value)
	//}

	r := relData.(*PartitionedRelData)

	for idx, data := range r.partitions {
		rel, err := t.getRelation(ctx, int(idx))
		if err != nil {
			return nil, err
		}
		r, err := rel.BuildReaders(
			ctx,
			proc,
			expr,
			data,
			num,
			txnOffset,
			orderBy,
			policy,
			filterHint,
		)
		if err != nil {
			return nil, err
		}
		readers = append(readers, r...)
	}
	return readers, nil
}

func (t *partitionTxnTable) BuildShardingReaders(
	ctx context.Context,
	proc any,
	expr *plan.Expr,
	relData engine.RelData,
	num int,
	txnOffset int,
	orderBy bool,
	policy engine.TombstoneApplyPolicy,
) ([]engine.Reader, error) {
	panic("Not Support")
}

func (t *partitionTxnTable) Rows(
	ctx context.Context,
) (uint64, error) {
	rows := uint64(0)
	for idx := range t.metadata.Partitions {
		p, err := t.getRelation(ctx, idx)
		if err != nil {
			return 0, nil
		}

		v, err := p.Rows(ctx)
		if err != nil {
			return 0, err
		}

		rows += v
	}
	return rows, nil
}

func (t *partitionTxnTable) Stats(
	ctx context.Context,
	sync bool,
) (*statsinfo.StatsInfo, error) {
	value := splan.NewStatsInfo()
	for idx := range t.metadata.Partitions {
		p, err := t.getRelation(ctx, idx)
		if err != nil {
			return nil, nil
		}

		v, err := p.Stats(ctx, sync)
		if err != nil {
			return nil, err
		}

		value.Merge(v)
	}
	return value, nil
}

func (t *partitionTxnTable) Size(
	ctx context.Context,
	columnName string,
) (uint64, error) {
	value := uint64(0)
	for idx := range t.metadata.Partitions {
		p, err := t.getRelation(ctx, idx)
		if err != nil {
			return 0, nil
		}

		v, err := p.Size(ctx, columnName)
		if err != nil {
			return 0, err
		}

		value += v
	}
	return value, nil
}

func (t *partitionTxnTable) CollectTombstones(
	ctx context.Context,
	txnOffset int,
	policy engine.TombstoneCollectPolicy,
) (engine.Tombstoner, error) {
	var tombstone engine.Tombstoner
	for idx := range t.metadata.Partitions {
		p, err := t.getRelation(ctx, idx)
		if err != nil {
			return nil, err
		}

		t, err := p.CollectTombstones(ctx, txnOffset, policy)
		if err != nil {
			return nil, err
		}
		if tombstone == nil {
			tombstone = t
			continue
		}
		if err := tombstone.Merge(t); err != nil {
			return nil, err
		}
	}
	return tombstone, nil
}

func (t *partitionTxnTable) CollectChanges(
	ctx context.Context,
	from, to types.TS,
	mp *mpool.MPool,
) (engine.ChangesHandle, error) {
	panic("not implemented")
}

func (t *partitionTxnTable) ApproxObjectsNum(ctx context.Context) int {
	num := 0
	for idx := range t.metadata.Partitions {
		p, err := t.getRelation(ctx, idx)
		if err != nil {
			// TODO: fix , return error
			return 0
		}
		num += p.ApproxObjectsNum(ctx)
	}
	return num
}

func (t *partitionTxnTable) MergeObjects(
	ctx context.Context,
	objstats []objectio.ObjectStats,
	targetObjSize uint32,
) (*api.MergeCommitEntry, error) {
	panic("not implemented")
}

func (t *partitionTxnTable) GetNonAppendableObjectStats(ctx context.Context) ([]objectio.ObjectStats, error) {
	var stats []objectio.ObjectStats
	for idx := range t.metadata.Partitions {
		p, err := t.getRelation(ctx, idx)
		if err != nil {
			return nil, err
		}
		values, err := p.GetNonAppendableObjectStats(ctx)
		if err != nil {
			return nil, err
		}
		stats = append(stats, values...)
	}
	return stats, nil
}

func (t *partitionTxnTable) GetColumMetadataScanInfo(
	ctx context.Context,
	name string,
	visitTombstone bool,
) ([]*plan.MetadataScanInfo, error) {
	var values []*plan.MetadataScanInfo
	for idx := range t.metadata.Partitions {
		p, err := t.getRelation(ctx, idx)
		if err != nil {
			return nil, err
		}
		v, err := p.GetColumMetadataScanInfo(ctx, name, visitTombstone)
		if err != nil {
			return nil, err
		}
		values = append(values, v...)
	}
	return values, nil
}

func (t *partitionTxnTable) UpdateConstraint(context.Context, *engine.ConstraintDef) error {
	panic("not implemented")
}

func (t *partitionTxnTable) AlterTable(ctx context.Context, c *engine.ConstraintDef, reqs []*api.AlterTableReq) error {
	return t.primary.AlterTable(ctx, c, reqs)
}

func (t *partitionTxnTable) TableRenameInTxn(ctx context.Context, constraint [][]byte) error {
	panic("not implemented")
}

func (t *partitionTxnTable) MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error) {
	panic("not implemented")
}

func (t *partitionTxnTable) TableDefs(ctx context.Context) ([]engine.TableDef, error) {
	return t.primary.TableDefs(ctx)
}

func (t *partitionTxnTable) GetTableDef(ctx context.Context) *plan.TableDef {
	return t.primary.GetTableDef(ctx)
}

func (t *partitionTxnTable) CopyTableDef(ctx context.Context) *plan.TableDef {
	return t.primary.CopyTableDef(ctx)
}

func (t *partitionTxnTable) GetPrimaryKeys(ctx context.Context) ([]*engine.Attribute, error) {
	return t.primary.GetPrimaryKeys(ctx)
}

func (t *partitionTxnTable) AddTableDef(context.Context, engine.TableDef) error {
	return nil
}

func (t *partitionTxnTable) DelTableDef(context.Context, engine.TableDef) error {
	return nil
}

func (t *partitionTxnTable) GetTableID(ctx context.Context) uint64 {
	return t.primary.GetTableID(ctx)
}

func (t *partitionTxnTable) GetTableName() string {
	return t.primary.GetTableName()
}

func (t *partitionTxnTable) GetDBID(ctx context.Context) uint64 {
	return t.primary.GetDBID(ctx)
}

func (t *partitionTxnTable) TableColumns(ctx context.Context) ([]*engine.Attribute, error) {
	return t.primary.TableColumns(ctx)
}

func (t *partitionTxnTable) GetEngineType() engine.EngineType {
	return t.primary.GetEngineType()
}

func (t *partitionTxnTable) GetProcess() any {
	return t.primary.GetProcess()
}

func (t *partitionTxnTable) PrimaryKeysMayBeModified(
	ctx context.Context,
	from types.TS,
	to types.TS,
	bat *batch.Batch,
	pkIndex int32,
	partitionIndex int32,
) (bool, error) {
	res, err := partitionprune.Prune(t.primary.proc.Load(), bat, t.metadata, partitionIndex)
	if err != nil {
		return false, err
	}
	defer res.Close()

	changed := false
	res.Iter(
		func(p partition.Partition, bat *batch.Batch) bool {
			v, e := t.primary.db.relation(
				ctx,
				p.PartitionTableName,
				t.primary.proc.Load(),
			)
			if e != nil {
				err = e
				return false
			}
			changed, err = v.PrimaryKeysMayBeModified(
				ctx,
				from,
				to,
				bat,
				pkIndex,
				partitionIndex,
			)
			if err != nil || changed {
				return false
			}
			return true
		},
	)
	return changed, err
}

func (t *partitionTxnTable) Write(context.Context, *batch.Batch) error {
	panic("BUG: cannot write data to partition primary table")
}

func (t *partitionTxnTable) Delete(context.Context, *batch.Batch, string) error {
	panic("BUG: cannot delete data to partition primary table")
}

func (t *partitionTxnTable) PrimaryKeysMayBeUpserted(
	ctx context.Context,
	from types.TS,
	to types.TS,
	bat *batch.Batch,
	pkIndex int32,
) (bool, error) {
	panic("BUG: cannot upsert primary keys in partition primary table")
}

func (t *partitionTxnTable) Reset(op client.TxnOperator) error {
	return t.primary.Reset(op)
}

func (t *partitionTxnTable) GetExtraInfo() *api.SchemaExtra {
	return t.primary.extraInfo
}

type PartitionedRelData struct {
	cnt        int
	blocks     objectio.BlockInfoSlice
	partitions map[int]engine.RelData
	tables     map[uint64]engine.Relation
}

func newPartitionedRelData() *PartitionedRelData {
	return &PartitionedRelData{
		partitions: make(map[int]engine.RelData),
		tables:     make(map[uint64]engine.Relation),
	}
}

func (r *PartitionedRelData) addPartition(
	ctx context.Context,
	table engine.Relation,
	param engine.RangesParam,
	idx int,
) error {
	data, err := table.Ranges(
		ctx,
		param,
	)
	if err != nil {
		return err
	}

	id := table.GetTableID(ctx)
	r.tables[id] = table
	r.partitions[idx] = data
	r.cnt += data.DataCnt()
	r.blocks = append(r.blocks, data.GetBlockInfoSlice()...)
	return nil
}

func (r *PartitionedRelData) AttachTombstones(tombstones engine.Tombstoner) error {
	for _, p := range r.partitions {
		if err := p.AttachTombstones(tombstones); err != nil {
			return err
		}
	}
	return nil
}

func (r *PartitionedRelData) BuildEmptyRelData(preAllocSize int) engine.RelData {
	for _, p := range r.partitions {
		return p.BuildEmptyRelData(preAllocSize)
	}
	panic("BUG: no partitions")
}

func (r *PartitionedRelData) DataCnt() int {
	return r.cnt
}

func (r *PartitionedRelData) GetBlockInfoSlice() objectio.BlockInfoSlice {
	return r.blocks
}

func (r *PartitionedRelData) GetType() engine.RelDataType {
	panic("not implemented")
}

func (r *PartitionedRelData) String() string {
	return "PartitionedRelData"
}

func (r *PartitionedRelData) MarshalBinary() ([]byte, error) {
	panic("not implemented")
}

func (r *PartitionedRelData) UnmarshalBinary(buf []byte) error {
	panic("not implemented")
}

func (r *PartitionedRelData) GetTombstones() engine.Tombstoner {
	panic("not implemented")
}

func (r *PartitionedRelData) DataSlice(begin, end int) engine.RelData {
	panic("not implemented")
}

func (r *PartitionedRelData) GetShardIDList() []uint64 {
	panic("not implemented")
}

func (r *PartitionedRelData) GetShardID(i int) uint64 {
	panic("not implemented")
}

func (r *PartitionedRelData) SetShardID(i int, id uint64) {
	panic("not implemented")
}

func (r *PartitionedRelData) AppendShardID(id uint64) {
	panic("not implemented")
}

func (r *PartitionedRelData) SetBlockInfo(i int, blk *objectio.BlockInfo) {
	panic("not implemented")
}

func (r *PartitionedRelData) GetBlockInfo(i int) objectio.BlockInfo {
	panic("not implemented")
}

func (r *PartitionedRelData) AppendBlockInfo(blk *objectio.BlockInfo) {
	panic("not implemented")
}

func (r *PartitionedRelData) AppendBlockInfoSlice(objectio.BlockInfoSlice) {
	panic("not implemented")
}

func (r *PartitionedRelData) Split(i int) []engine.RelData {
	panic("not implemented")
}
