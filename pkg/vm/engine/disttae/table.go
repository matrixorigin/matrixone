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

	for _, part := range tbl.parts {
		rows += part.data.RowsCount(ctx, tbl.db.txn.meta.SnapshotTS)
	}
	return rows, nil
}

func (tbl *table) Size(ctx context.Context, name string) (int64, error) {
	return 0, nil
}

func (tbl *table) Ranges(ctx context.Context) ([][]byte, error) {
	return nil, nil
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
	return nil, nil
}
