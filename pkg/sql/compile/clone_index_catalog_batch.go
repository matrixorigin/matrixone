// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type cloneIndexCatalogBatchKey struct{}

// CloneIndexCatalogBatch combines independent index catalog inserts whose
// tables are invisible until the enclosing database clone commits.
type CloneIndexCatalogBatch struct {
	proc   *process.Process
	ctx    context.Context
	staged *batch.Batch
	rows   *batch.Batch
	engine engine.Engine
}

func NewCloneIndexCatalogBatch(ctx context.Context, proc *process.Process) *CloneIndexCatalogBatch {
	return &CloneIndexCatalogBatch{ctx: ctx, proc: proc, engine: proc.GetSessionInfo().StorageEngine}
}

func (b *CloneIndexCatalogBatch) Close() {
	if b.staged != nil {
		b.staged.Clean(b.proc.Mp())
		b.staged = nil
	}
	if b.rows != nil {
		b.rows.Clean(b.proc.Mp())
		b.rows = nil
	}
}

func (b *CloneIndexCatalogBatch) TableContext(ctx context.Context) context.Context {
	if b.staged != nil {
		b.staged.CleanOnlyData()
	}
	return context.WithValue(ctx, cloneIndexCatalogBatchKey{}, b)
}

// FinishTable admits staged metadata only after the table clone succeeds.
func (b *CloneIndexCatalogBatch) FinishTable() error {
	if b == nil || b.staged == nil || b.staged.RowCount() == 0 {
		return nil
	}
	defer func() {
		if b.staged != nil {
			b.staged.CleanOnlyData()
		}
	}()
	if b.rows == nil {
		b.rows = b.staged
		b.staged = nil
		return nil
	}
	combined, err := b.rows.Append(b.ctx, b.proc.Mp(), b.staged)
	if err != nil {
		return err
	}
	b.rows = combined
	if b.rows.Size() >= 128<<10 {
		return b.Flush()
	}
	return nil
}

func (b *CloneIndexCatalogBatch) Flush() error {
	if b == nil || b.rows == nil || b.rows.RowCount() == 0 {
		return nil
	}
	rel, err := colexec.GetNewRelation(b.engine, catalog.MO_CATALOG, catalog.MO_INDEXES, b.proc.GetTxnOperator(), b.ctx)
	if err != nil {
		return err
	}
	// Use the current catalog layout and physical primary-key expression.
	// This also covers a catalog whose nullable included_columns was added.
	def := rel.GetTableDef(b.ctx)
	prepared := batch.NewWithSize(0)
	var preparedAttrs []string
	for _, col := range def.Cols {
		if col.Name == catalog.Row_ID || col.Name == catalog.CPrimaryKeyColName {
			continue
		}
		preparedAttrs = append(preparedAttrs, col.Name)
		var value *vector.Vector
		for i, attr := range colexec.InsertIndexMetaAttrs {
			if attr == col.Name {
				value = b.rows.Vecs[i]
				break
			}
		}
		if value == nil {
			if col.Name != "included_columns" {
				return moerr.NewInternalErrorf(b.ctx, "unexpected index catalog column %s", col.Name)
			}
			value = vector.NewConstNull(types.MustTypeFromPlan(col.Typ), b.rows.RowCount(), b.proc.Mp())
			defer value.Free(b.proc.Mp())
		}
		typ := types.MustTypeFromPlan(col.Typ)
		value.SetType(typ)
		prepared.Vecs = append(prepared.Vecs, value)
	}
	prepared.SetRowCount(b.rows.RowCount())
	positions := make(map[string]int32, len(preparedAttrs))
	for i, name := range preparedAttrs {
		positions[name] = int32(i)
	}
	args := make([]*plan.Expr, 0, len(def.Pkey.Names))
	for _, name := range def.Pkey.Names {
		column := def.Cols[def.Name2ColIndex[name]]
		args = append(args, &plan.Expr{Typ: column.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: positions[name]}}})
	}
	fn := "serial"
	if def.KeyFormat == collation.KeyFormatV1 {
		fn = "physical_serial"
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(b.ctx, fn, args)
	if err != nil {
		return err
	}
	executor, err := colexec.NewExpressionExecutor(b.proc, expr)
	if err != nil {
		return err
	}
	defer executor.Free()
	key, err := executor.Eval(b.proc, []*batch.Batch{prepared}, nil)
	if err != nil {
		return err
	}
	preparedAttrs = append(preparedAttrs, catalog.CPrimaryKeyColName)
	prepared.Vecs = append(prepared.Vecs, key)
	if err = rel.Write(b.ctx, prepared, preparedAttrs); err != nil {
		return err
	}
	b.rows.CleanOnlyData()
	return nil
}

// CloneIndexCatalogCanBatch accepts only native indexes whose CREATE and
// restore steps have no plugin hooks that consume index catalog rows.
func CloneIndexCatalogCanBatch(stmt *tree.CreateTable) bool {
	if stmt == nil || stmt.Temporary || stmt.IsClusterTable {
		return false
	}
	for _, def := range stmt.Defs {
		switch idx := def.(type) {
		case *tree.FullTextIndex:
			return false
		case *tree.Index:
			if idx.KeyType != tree.INDEX_TYPE_INVALID && idx.KeyType != tree.INDEX_TYPE_BTREE {
				return false
			}
		}
	}
	return true
}

func (c *Compile) stageCloneIndexCatalogInsert(query *plan.CreateTable, database engine.Database, relation engine.Relation) (bool, error) {
	b, _ := c.proc.Ctx.Value(cloneIndexCatalogBatchKey{}).(*CloneIndexCatalogBatch)
	if b == nil || createTablePublishesForeignKeys(query) {
		return false, nil
	}
	for _, idx := range query.GetTableDef().GetIndexes() {
		if idx.IndexAlgo != "" && idx.IndexAlgo != "btree" {
			return false, nil
		}
	}
	id, err := strconv.ParseUint(database.GetDatabaseId(c.proc.Ctx), 10, 64)
	if err != nil {
		return true, err
	}
	constraint, err := GetConstraintDef(c.proc.Ctx, relation)
	if err != nil {
		return true, err
	}
	b.staged, err = colexec.BuildInsertIndexMetaBatch(relation.GetTableID(c.proc.Ctx), id, constraint, relation.GetTableDef(c.proc.Ctx), c.e, b.proc, b.staged)
	return true, err
}
