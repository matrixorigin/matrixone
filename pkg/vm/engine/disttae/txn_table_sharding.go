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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func newTxnTableWithItem(
	db *txnDatabase,
	item cache.TableItem,
	process *process.Process,
) *txnTable {
	tbl := &txnTable{
		db:            db,
		accountId:     item.AccountId,
		tableId:       item.Id,
		version:       item.Version,
		tableName:     item.Name,
		defs:          item.Defs,
		tableDef:      item.TableDef,
		primaryIdx:    item.PrimaryIdx,
		primarySeqnum: item.PrimarySeqnum,
		clusterByIdx:  item.ClusterByIdx,
		relKind:       item.Kind,
		viewdef:       item.ViewDef,
		comment:       item.Comment,
		partitioned:   item.Partitioned,
		partition:     item.Partition,
		createSql:     item.CreateSql,
		constraint:    item.Constraint,
		rowid:         item.Rowid,
		rowids:        item.Rowids,
		lastTS:        db.op.SnapshotTS(),
	}
	tbl.proc.Store(process)
	return tbl
}

// TODO: resume enable sharding. Avoid sca not used.

// type txnTableDelegate struct {
// 	ctx context.Context
// 	raw *txnTable
// }

// func newTxnTable(
// 	ctx context.Context,
// 	key tableKey,
// 	item cache.TableItem,
// 	db *txnDatabase,
// 	process *process.Process,
// ) engine.Relation {
// 	tbl := newTxnTableWithItem(
// 		db,
// 		item,
// 		process,
// 	)
// 	db.getTxn().tableCache.tableMap.Store(key, tbl)
// 	return &txnTableDelegate{
// 		ctx: ctx,
// 		raw: tbl,
// 	}
// }

// func (tbl *txnTableDelegate) Stats(
// 	ctx context.Context,
// 	sync bool,
// ) (*pb.StatsInfo, error) {
// 	// TODO: forward
// 	return nil, nil
// }

// func (tbl *txnTableDelegate) Rows(
// 	ctx context.Context,
// ) (uint64, error) {
// 	// TODO: forward
// 	return 0, nil
// }

// func (tbl *txnTableDelegate) Size(
// 	ctx context.Context,
// 	columnName string,
// ) (uint64, error) {
// 	// TODO: forward
// 	return 0, nil
// }

// func (tbl *txnTableDelegate) Ranges(
// 	context.Context,
// 	[]*plan.Expr,
// 	int,
// ) (engine.Ranges, error) {
// 	// TODO: forward
// 	return nil, nil
// }

// func (tbl *txnTableDelegate) GetColumMetadataScanInfo(
// 	ctx context.Context,
// 	name string,
// ) ([]*plan.MetadataScanInfo, error) {
// 	// TODO: forward
// 	return nil, nil
// }

// func (tbl *txnTableDelegate) ApproxObjectsNum(
// 	ctx context.Context,
// ) int {
// 	// TODO: forward
// 	return 0
// }

// func (tbl *txnTableDelegate) NewReader(
// 	ctx context.Context,
// 	num int,
// 	expr *plan.Expr,
// 	ranges []byte,
// 	orderedScan bool,
// 	txnOffset int,
// ) ([]engine.Reader, error) {
// 	// forward
// 	return nil, nil
// }

// func (tbl *txnTableDelegate) PrimaryKeysMayBeModified(
// 	ctx context.Context,
// 	from types.TS,
// 	to types.TS,
// 	keyVector *vector.Vector,
// ) (bool, error) {
// 	// TODO: forward
// 	return false, nil
// }

// func (tbl *txnTableDelegate) MergeObjects(
// 	ctx context.Context,
// 	objstats []objectio.ObjectStats,
// 	policyName string,
// 	targetObjSize uint32,
// ) (*api.MergeCommitEntry, error) {
// 	// TODO: forward
// 	return nil, nil
// }

// func (tbl *txnTableDelegate) TableDefs(
// 	ctx context.Context,
// ) ([]engine.TableDef, error) {
// 	return tbl.raw.TableDefs(ctx)
// }

// func (tbl *txnTableDelegate) GetTableDef(
// 	ctx context.Context,
// ) *plan.TableDef {
// 	return tbl.raw.GetTableDef(ctx)
// }

// func (tbl *txnTableDelegate) CopyTableDef(
// 	ctx context.Context,
// ) *plan.TableDef {
// 	return tbl.raw.CopyTableDef(ctx)
// }

// func (tbl *txnTableDelegate) GetPrimaryKeys(
// 	ctx context.Context,
// ) ([]*engine.Attribute, error) {
// 	return tbl.raw.GetPrimaryKeys(ctx)
// }

// func (tbl *txnTableDelegate) GetHideKeys(
// 	ctx context.Context,
// ) ([]*engine.Attribute, error) {
// 	return tbl.raw.GetHideKeys(ctx)
// }

// func (tbl *txnTableDelegate) Write(
// 	ctx context.Context,
// 	bat *batch.Batch,
// ) error {
// 	return tbl.raw.Write(ctx, bat)
// }

// func (tbl *txnTableDelegate) Update(
// 	ctx context.Context,
// 	bat *batch.Batch,
// ) error {
// 	return tbl.raw.Update(ctx, bat)
// }

// func (tbl *txnTableDelegate) Delete(
// 	ctx context.Context,
// 	bat *batch.Batch,
// 	name string,
// ) error {
// 	return tbl.raw.Delete(ctx, bat, name)
// }

// func (tbl *txnTableDelegate) AddTableDef(
// 	ctx context.Context,
// 	def engine.TableDef,
// ) error {
// 	return tbl.raw.AddTableDef(ctx, def)
// }

// func (tbl *txnTableDelegate) DelTableDef(
// 	ctx context.Context,
// 	def engine.TableDef,
// ) error {
// 	return tbl.raw.DelTableDef(ctx, def)
// }

// func (tbl *txnTableDelegate) AlterTable(
// 	ctx context.Context,
// 	c *engine.ConstraintDef,
// 	constraint [][]byte,
// ) error {
// 	return tbl.raw.AlterTable(ctx, c, constraint)
// }

// func (tbl *txnTableDelegate) UpdateConstraint(
// 	ctx context.Context,
// 	c *engine.ConstraintDef,
// ) error {
// 	return tbl.raw.UpdateConstraint(ctx, c)
// }

// func (tbl *txnTableDelegate) TableRenameInTxn(
// 	ctx context.Context,
// 	constraint [][]byte,
// ) error {
// 	return tbl.raw.TableRenameInTxn(ctx, constraint)
// }

// func (tbl *txnTableDelegate) GetTableID(
// 	ctx context.Context,
// ) uint64 {
// 	return tbl.raw.GetTableID(ctx)
// }

// func (tbl *txnTableDelegate) GetTableName() string {
// 	return tbl.raw.GetTableName()
// }

// func (tbl *txnTableDelegate) GetDBID(
// 	ctx context.Context,
// ) uint64 {
// 	return tbl.raw.GetDBID(ctx)
// }

// func (tbl *txnTableDelegate) TableColumns(
// 	ctx context.Context,
// ) ([]*engine.Attribute, error) {
// 	return tbl.raw.TableColumns(ctx)
// }

// func (tbl *txnTableDelegate) MaxAndMinValues(
// 	ctx context.Context,
// ) ([][2]any, []uint8, error) {
// 	return tbl.raw.MaxAndMinValues(ctx)
// }

// func (tbl *txnTableDelegate) GetEngineType() engine.EngineType {
// 	return tbl.raw.GetEngineType()
// }

// func getTxnTable(
// 	ctx context.Context,
// 	param shard.ReadParam,
// 	engine engine.Engine,
// ) (*txnTable, error) {
// 	// TODO: reduce mem allocate
// 	proc, err := process.GetCodecService().Decode(
// 		ctx,
// 		param.Process,
// 	)
// 	if err != nil {
// 		return nil, err
// 	}

// 	db := &txnDatabase{
// 		op:           proc.TxnOperator,
// 		databaseName: param.TxnTable.DatabaseName,
// 		databaseId:   param.TxnTable.DatabaseID,
// 	}

// 	item, err := db.getTableItem(
// 		ctx,
// 		uint32(param.TxnTable.AccountID),
// 		param.TxnTable.TableName,
// 		engine.(*Engine),
// 	)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return newTxnTableWithItem(
// 		db,
// 		item,
// 		proc,
// 	), nil
// }
