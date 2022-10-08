// Copyright 2022 Matrix Origin
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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ engine.Relation = new(table)

func (tbl *table) Rows(ctx context.Context) (int64, error) {
	var rows int64

	for _, blks := range tbl.meta.blocks {
		for _, blk := range blks {
			rows += blockRows(blk)
		}
	}
	return rows, nil
}

func (tbl *table) Size(ctx context.Context, name string) (int64, error) {
	// TODO
	return 0, nil
}

// return all unmodified blocks
func (tbl *table) Ranges(ctx context.Context, expr *plan.Expr) ([][]byte, error) {
	var writes [][]Entry

	// consider halloween problem
	if int64(tbl.db.txn.statementId)-1 > 0 {
		writes = tbl.db.txn.writes[:tbl.db.txn.statementId-1]
	}
	ranges := make([][]byte, 0, len(tbl.meta.blocks))
	dnList := needSyncDnStores(expr, tbl.defs, tbl.db.txn.dnStores)
	tbl.dnList = dnList
	dnStores := make([]DNStore, len(dnList))
	for _, i := range dnList {
		dnStores = append(dnStores, tbl.db.txn.dnStores[i])
	}
	if err := tbl.db.txn.db.Update(ctx, dnStores, tbl.db.databaseId,
		tbl.tableId, tbl.db.txn.meta.SnapshotTS); err != nil {
		return nil, err
	}
	tbl.meta.modifedBlocks = make([][]BlockMeta, len(tbl.meta.blocks))
	for _, i := range dnList {
		blks := tbl.parts[i].BlockList(ctx, tbl.db.txn.meta.SnapshotTS,
			tbl.meta.blocks[i], writes)
		for _, blk := range blks {
			if needRead(expr, blk) {
				ranges = append(ranges, blockMarshal(blk))
			}
		}
		tbl.meta.modifedBlocks[i] = genModifedBlocks(tbl.meta.blocks[i], blks, expr)
	}
	return ranges, nil
}

func (tbl *table) TableDefs(ctx context.Context) ([]engine.TableDef, error) {
	return tbl.defs, nil
}

func (tbl *table) GetPrimaryKeys(ctx context.Context) ([]*engine.Attribute, error) {
	attrs := make([]*engine.Attribute, 0, 1)
	for _, def := range tbl.defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			if attr.Attr.Primary {
				attrs = append(attrs, &attr.Attr)
			}
		}
	}
	return attrs, nil
}

func (tbl *table) GetHideKeys(ctx context.Context) ([]*engine.Attribute, error) {
	attrs := make([]*engine.Attribute, 0, 1)
	for _, def := range tbl.defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			if attr.Attr.IsHidden {
				attrs = append(attrs, &attr.Attr)
			}
		}
	}
	return attrs, nil
}

func (tbl *table) Write(ctx context.Context, bat *batch.Batch) error {
	bats, err := partitionBatch(bat, tbl.insertExpr, tbl.db.txn.m, len(tbl.parts))
	if err != nil {
		return err
	}
	for i := range bats {
		if bats[i].GetVector(0).Length() == 0 {
			continue
		}
		if err := tbl.db.txn.WriteBatch(INSERT, tbl.db.databaseId, tbl.tableId,
			tbl.db.databaseName, tbl.tableName, bats[i], tbl.db.txn.dnStores[i]); err != nil {
			return err
		}
	}
	return nil
}

func (tbl *table) Update(ctx context.Context, bat *batch.Batch) error {
	return nil
}

func (tbl *table) Delete(ctx context.Context, vec *vector.Vector, name string) error {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vec
	bats, err := partitionBatch(bat, tbl.insertExpr, tbl.db.txn.m, len(tbl.parts))
	if err != nil {
		return err
	}
	for i := range bats {
		if bats[i].GetVector(0).Length() == 0 {
			continue
		}
		if err := tbl.db.txn.WriteBatch(INSERT, tbl.db.databaseId, tbl.tableId,
			tbl.db.databaseName, tbl.tableName, bats[i], tbl.db.txn.dnStores[i]); err != nil {
			return err
		}
	}
	return nil
}

func (tbl *table) Truncate(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (tbl *table) AddTableDef(ctx context.Context, def engine.TableDef) error {
	return nil
}

func (tbl *table) DelTableDef(ctx context.Context, def engine.TableDef) error {
	return nil
}

func (tbl *table) GetTableID(ctx context.Context) string {
	return strconv.FormatUint(tbl.tableId, 10)
}

func (tbl *table) NewReader(ctx context.Context, num int, expr *plan.Expr,
	ranges [][]byte) ([]engine.Reader, error) {
	rds := make([]engine.Reader, num)
	if len(ranges) == 0 {
		return tbl.newMergeReader(ctx, num, expr)
	}
	blks := make([]BlockMeta, len(ranges))
	for i := range ranges {
		blks[i] = blockUnmarshal(ranges[i])
	}
	if len(ranges) < num {
		for i := range ranges {
			rds[i] = &blockReader{
				ctx:  ctx,
				blks: []BlockMeta{blks[i]},
			}
		}
		for j := len(ranges); j < num; j++ {
			rds[j] = &blockReader{
				ctx: ctx,
			}
		}
		return rds, nil
	}
	step := len(ranges) / num
	if step < 1 {
		step = 1
	}
	for i := 0; i < num; i++ {
		if i == num-1 {
			rds[i] = &blockReader{
				ctx:  ctx,
				blks: blks[i*step:],
			}
		} else {
			rds[i] = &blockReader{
				ctx:  ctx,
				blks: blks[i*step : (i+1)*step],
			}
		}
	}
	return rds, nil
}

func (tbl *table) newMergeReader(ctx context.Context, num int,
	expr *plan.Expr) ([]engine.Reader, error) {
	var writes [][]Entry

	// consider halloween problem
	if int64(tbl.db.txn.statementId)-1 > 0 {
		writes = tbl.db.txn.writes[:tbl.db.txn.statementId-1]
	}
	rds := make([]engine.Reader, num)
	mrds := make([]mergeReader, num)
	for _, i := range tbl.dnList {
		rds0, err := tbl.parts[i].NewReader(ctx, num, expr,
			tbl.meta.modifedBlocks[i], tbl.db.txn.meta.SnapshotTS, writes)
		if err != nil {
			return nil, err
		}
		for i := range rds0 {
			mrds[i].rds = append(mrds[i].rds, rds0[i])
		}
	}
	for i := range rds {
		rds[i] = &mrds[i]
	}
	return rds, nil
}
