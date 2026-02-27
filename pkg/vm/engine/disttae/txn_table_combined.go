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
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	splan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ engine.Relation = (*combinedTxnTable)(nil)

type pruneFunc func(
	ctx context.Context,
	param engine.RangesParam,
) ([]engine.Relation, error)

type prunePKFunc func(
	bat *batch.Batch,
	partitionIndex int32,
) ([]engine.Relation, error)

type tablesFunc func() ([]engine.Relation, error)

type combinedTxnTable struct {
	primary     *txnTable
	pruneFunc   pruneFunc
	tablesFunc  tablesFunc
	prunePKFunc prunePKFunc
}

func newCombinedTxnTable(
	primary *txnTable,
	tablesFunc tablesFunc,
	pruneFunc pruneFunc,
	prunePKFunc prunePKFunc,
) *combinedTxnTable {
	return &combinedTxnTable{
		primary:     primary,
		pruneFunc:   pruneFunc,
		tablesFunc:  tablesFunc,
		prunePKFunc: prunePKFunc,
	}
}

func (t *combinedTxnTable) Ranges(
	ctx context.Context,
	param engine.RangesParam,
) (engine.RelData, error) {
	relations, err := t.pruneFunc(ctx, param)
	if err != nil {
		return nil, err
	}

	pd := newCombinedRelData()
	for _, rel := range relations {
		if err := pd.add(ctx, rel, param); err != nil {
			return nil, err
		}
	}
	return pd, nil
}

func (t *combinedTxnTable) BuildReaders(
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
	if relData == nil {
		tables, err := t.tablesFunc()
		if err != nil {
			return nil, err
		}
		for _, rel := range tables {
			r, err := rel.BuildReaders(
				ctx,
				proc,
				expr,
				nil,
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

	r := relData.(*CombinedRelData)
	for idx, data := range r.tables {
		rel := r.relations[idx]
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

func (t *combinedTxnTable) BuildShardingReaders(
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

func (t *combinedTxnTable) Rows(
	ctx context.Context,
) (uint64, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return 0, err
	}

	rows := uint64(0)
	for _, rel := range tables {
		v, err := rel.Rows(ctx)
		if err != nil {
			return 0, err
		}

		rows += v
	}
	return rows, nil
}

func (t *combinedTxnTable) Stats(
	ctx context.Context,
	sync bool,
) (*statsinfo.StatsInfo, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return nil, err
	}

	value := splan.NewStatsInfo()
	for _, rel := range tables {
		v, err := rel.Stats(ctx, sync)
		if err != nil {
			return nil, err
		}

		value.Merge(v)
	}
	return value, nil
}

func (t *combinedTxnTable) Size(
	ctx context.Context,
	columnName string,
) (uint64, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return 0, err
	}

	value := uint64(0)
	for _, rel := range tables {
		v, err := rel.Size(ctx, columnName)
		if err != nil {
			return 0, err
		}

		value += v
	}
	return value, nil
}

func (t *combinedTxnTable) CollectTombstones(
	ctx context.Context,
	txnOffset int,
	policy engine.TombstoneCollectPolicy,
) (engine.Tombstoner, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return nil, err
	}

	var tombstone engine.Tombstoner
	for _, rel := range tables {
		t, err := rel.CollectTombstones(ctx, txnOffset, policy)
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

func (t *combinedTxnTable) StarCount(ctx context.Context) (uint64, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return 0, err
	}

	var total uint64
	for _, rel := range tables {
		count, err := rel.StarCount(ctx)
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func (t *combinedTxnTable) EstimateCommittedTombstoneCount(ctx context.Context) (int, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return 0, err
	}

	var total int
	for _, rel := range tables {
		count, err := rel.EstimateCommittedTombstoneCount(ctx)
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func (t *combinedTxnTable) CollectChanges(
	ctx context.Context,
	from, to types.TS,
	_ bool,
	mp *mpool.MPool,
) (engine.ChangesHandle, error) {
	panic("not implemented")
}

func (t *combinedTxnTable) ApproxObjectsNum(ctx context.Context) int {
	tables, err := t.tablesFunc()
	if err != nil {
		return 0
	}

	num := 0
	for _, rel := range tables {
		num += rel.ApproxObjectsNum(ctx)
	}
	return num
}

func (t *combinedTxnTable) MergeObjects(
	ctx context.Context,
	objstats []objectio.ObjectStats,
	targetObjSize uint32,
) (*api.MergeCommitEntry, error) {
	panic("not implemented")
}

func (t *combinedTxnTable) GetNonAppendableObjectStats(ctx context.Context) ([]objectio.ObjectStats, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return nil, err
	}

	var stats []objectio.ObjectStats
	for _, rel := range tables {
		values, err := rel.GetNonAppendableObjectStats(ctx)
		if err != nil {
			return nil, err
		}
		stats = append(stats, values...)
	}
	return stats, nil
}

func (t *combinedTxnTable) GetColumMetadataScanInfo(
	ctx context.Context,
	name string,
	visitTombstone bool,
) ([]*plan.MetadataScanInfo, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return nil, err
	}

	var values []*plan.MetadataScanInfo
	for _, rel := range tables {
		v, err := rel.GetColumMetadataScanInfo(ctx, name, visitTombstone)
		if err != nil {
			return nil, err
		}
		values = append(values, v...)
	}
	return values, nil
}

func (t *combinedTxnTable) UpdateConstraint(context.Context, *engine.ConstraintDef) error {
	panic("not implemented")
}

func (t *combinedTxnTable) AlterTable(ctx context.Context, c *engine.ConstraintDef, reqs []*api.AlterTableReq) error {
	return t.primary.AlterTable(ctx, c, reqs)
}

func (t *combinedTxnTable) TableRenameInTxn(ctx context.Context, constraint [][]byte) error {
	panic("not implemented")
}

func (t *combinedTxnTable) MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error) {
	panic("not implemented")
}

func (t *combinedTxnTable) TableDefs(ctx context.Context) ([]engine.TableDef, error) {
	return t.primary.TableDefs(ctx)
}

func (t *combinedTxnTable) GetTableDef(ctx context.Context) *plan.TableDef {
	return t.primary.GetTableDef(ctx)
}

func (t *combinedTxnTable) CopyTableDef(ctx context.Context) *plan.TableDef {
	return t.primary.CopyTableDef(ctx)
}

func (t *combinedTxnTable) GetPrimaryKeys(ctx context.Context) ([]*engine.Attribute, error) {
	return t.primary.GetPrimaryKeys(ctx)
}

func (t *combinedTxnTable) AddTableDef(context.Context, engine.TableDef) error {
	return nil
}

func (t *combinedTxnTable) DelTableDef(context.Context, engine.TableDef) error {
	return nil
}

func (t *combinedTxnTable) GetTableID(ctx context.Context) uint64 {
	return t.primary.GetTableID(ctx)
}

func (t *combinedTxnTable) GetTableName() string {
	return t.primary.GetTableName()
}

func (t *combinedTxnTable) GetDBID(ctx context.Context) uint64 {
	return t.primary.GetDBID(ctx)
}

func (t *combinedTxnTable) TableColumns(ctx context.Context) ([]*engine.Attribute, error) {
	return t.primary.TableColumns(ctx)
}

func (t *combinedTxnTable) GetEngineType() engine.EngineType {
	return t.primary.GetEngineType()
}

func (t *combinedTxnTable) GetProcess() any {
	return t.primary.GetProcess()
}

func (t *combinedTxnTable) PrimaryKeysMayBeModified(
	ctx context.Context,
	from types.TS,
	to types.TS,
	bat *batch.Batch,
	pkIndex int32,
	partitionIndex int32,
) (bool, error) {
	relations, err := t.prunePKFunc(bat, partitionIndex)
	if err != nil {
		return false, err
	}

	changed := false
	for _, rel := range relations {
		v, e := rel.PrimaryKeysMayBeModified(
			ctx,
			from,
			to,
			bat,
			pkIndex,
			partitionIndex,
		)
		if e != nil {
			return false, e
		}
		if v {
			changed = true
			break
		}
	}
	return changed, err
}

func (t *combinedTxnTable) Write(context.Context, *batch.Batch) error {
	panic("BUG: cannot write data to partition primary table")
}

func (t *combinedTxnTable) Delete(context.Context, *batch.Batch, string) error {
	panic("BUG: cannot delete data to partition primary table")
}

func (t *combinedTxnTable) PrimaryKeysMayBeUpserted(
	ctx context.Context,
	from types.TS,
	to types.TS,
	bat *batch.Batch,
	pkIndex int32,
) (bool, error) {
	relations, err := t.prunePKFunc(bat, -1)
	if err != nil {
		return false, err
	}

	changed := false
	for _, rel := range relations {
		v, e := rel.PrimaryKeysMayBeUpserted(
			ctx,
			from,
			to,
			bat,
			pkIndex,
		)
		if e != nil {
			return false, e
		}
		if v {
			changed = true
			break
		}
	}
	return changed, err
}

func (t *combinedTxnTable) Reset(op client.TxnOperator) error {
	return t.primary.Reset(op)
}

func (t *combinedTxnTable) GetExtraInfo() *api.SchemaExtra {
	return t.primary.extraInfo
}

type CombinedRelData struct {
	cnt       int
	blocks    objectio.BlockInfoSlice
	tables    []engine.RelData
	relations []engine.Relation
}

func newCombinedRelData() *CombinedRelData {
	return &CombinedRelData{}
}

func (r *CombinedRelData) add(
	ctx context.Context,
	table engine.Relation,
	param engine.RangesParam,
) error {
	data, err := table.Ranges(
		ctx,
		param,
	)
	if err != nil {
		return err
	}

	r.relations = append(r.relations, table)
	r.tables = append(r.tables, data)
	r.cnt += data.DataCnt()
	r.blocks = append(r.blocks, data.GetBlockInfoSlice()...)
	return nil
}

func (r *CombinedRelData) AttachTombstones(tombstones engine.Tombstoner) error {
	for _, p := range r.tables {
		if err := p.AttachTombstones(tombstones); err != nil {
			return err
		}
	}
	return nil
}

func (r *CombinedRelData) BuildEmptyRelData(preAllocSize int) engine.RelData {
	for _, p := range r.tables {
		return p.BuildEmptyRelData(preAllocSize)
	}
	panic("BUG: no partitions")
}

func (r *CombinedRelData) DataCnt() int {
	return r.cnt
}

func (r *CombinedRelData) GetBlockInfoSlice() objectio.BlockInfoSlice {
	return r.blocks
}

func (r *CombinedRelData) GetType() engine.RelDataType {
	panic("not implemented")
}

func (r *CombinedRelData) String() string {
	return "PartitionedRelData"
}

func (r *CombinedRelData) MarshalBinary() ([]byte, error) {
	panic("not implemented")
}

func (r *CombinedRelData) UnmarshalBinary(buf []byte) error {
	panic("not implemented")
}

func (r *CombinedRelData) GetTombstones() engine.Tombstoner {
	panic("not implemented")
}

func (r *CombinedRelData) DataSlice(begin, end int) engine.RelData {
	panic("not implemented")
}

func (r *CombinedRelData) GetShardIDList() []uint64 {
	panic("not implemented")
}

func (r *CombinedRelData) GetShardID(i int) uint64 {
	panic("not implemented")
}

func (r *CombinedRelData) SetShardID(i int, id uint64) {
	panic("not implemented")
}

func (r *CombinedRelData) AppendShardID(id uint64) {
	panic("not implemented")
}

func (r *CombinedRelData) SetBlockInfo(i int, blk *objectio.BlockInfo) {
	panic("not implemented")
}

func (r *CombinedRelData) GetBlockInfo(i int) objectio.BlockInfo {
	panic("not implemented")
}

func (r *CombinedRelData) AppendBlockInfo(blk *objectio.BlockInfo) {
	panic("not implemented")
}

func (r *CombinedRelData) AppendBlockInfoSlice(objectio.BlockInfoSlice) {
	panic("not implemented")
}

func (r *CombinedRelData) Split(i int) []engine.RelData {
	panic("not implemented")
}
