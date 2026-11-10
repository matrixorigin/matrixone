// Copyright 2026 Matrix Origin
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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

type alterCopySourceContext struct {
	plan2.CompilerContext
	matchingColumns bool
}

func TestAlterCopyPkPrecheckRequiresUnambiguousSource(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mutate      func(*plan.AlterTable)
		wantColumns []string
		wantNull    bool
	}{
		{"valid zero column ID", func(*plan.AlterTable) {}, []string{"a", "b"}, false},
		{"nullable source", func(q *plan.AlterTable) { q.TableDef.Cols[0].Typ.NotNullable = false }, []string{"a", "b"}, true},
		{"missing map", func(q *plan.AlterTable) { delete(q.ChangeTblColIdMap, 0) }, nil, false},
		{"different mapped column", func(q *plan.AlterTable) { q.ChangeTblColIdMap[0].Name = "b" }, nil, false},
		{"replacement ID", func(q *plan.AlterTable) { q.CopyTableDef.Cols[0].ColId = plan2.UnKnownColId }, nil, false},
		{"different sequence", func(q *plan.AlterTable) { q.CopyTableDef.Cols[0].Seqnum++ }, nil, false},
		{"generated source", func(q *plan.AlterTable) { q.TableDef.Cols[0].GeneratedCol = &plan.GeneratedCol{} }, nil, false},
		{"generated target", func(q *plan.AlterTable) { q.CopyTableDef.Cols[0].GeneratedCol = &plan.GeneratedCol{} }, nil, false},
		{"floating-point key", func(q *plan.AlterTable) {
			q.TableDef.Cols[0].Typ.Id = int32(types.T_float64)
			q.CopyTableDef.Cols[0].Typ.Id = int32(types.T_float64)
		}, nil, false},
		{"duplicate IDs", func(q *plan.AlterTable) { q.TableDef.Cols[1].ColId = 0 }, nil, false},
		{"duplicate source names", func(q *plan.AlterTable) { q.TableDef.Cols[1].Name = "A" }, nil, false},
		{"duplicate key parts", func(q *plan.AlterTable) { q.CopyTableDef.Pkey.Names = []string{"a", "A"} }, nil, false},
		{"empty key part", func(q *plan.AlterTable) { q.CopyTableDef.Pkey.Names = []string{"a", ""} }, nil, false},
		{"nullable auto increment", func(q *plan.AlterTable) {
			q.TableDef.Cols[0].Typ.AutoIncr = true
			q.TableDef.Cols[0].Typ.NotNullable = false
			q.CopyTableDef.Cols[0].Typ.AutoIncr = true
		}, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &plan.ColDef{Name: "a", ColId: 0, Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}}
			b := &plan.ColDef{Name: "b", ColId: 1, Seqnum: 2, Typ: a.Typ}
			old := &plan.TableDef{Cols: []*plan.ColDef{a, b}}
			updated := plan2.DeepCopyTableDef(old, true)
			updated.Pkey = &plan.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, Names: []string{"a", "b"}}
			qry := &plan.AlterTable{
				TableDef: old, CopyTableDef: updated, Options: &plan.AlterCopyOpt{},
				ChangeTblColIdMap: map[uint64]*plan.ColDef{0: {Name: "a"}, 1: {Name: "b"}},
			}
			tc.mutate(qry)
			cols, checkNull := getAlterCopyPkPrecheck(qry)
			require.Equal(t, tc.wantColumns, cols)
			require.Equal(t, tc.wantNull, checkNull)
		})
	}
}

func (c *alterCopySourceContext) Resolve(db, table string, snapshot *plan2.Snapshot) (*plan2.ObjectRef, *plan2.TableDef, error) {
	ref, def, err := c.CompilerContext.Resolve(db, table, snapshot)
	if def != nil {
		if col := plan2.FindColumn(def.Cols, "a"); col != nil {
			col.Typ.Width = 64
			col.Typ.Scale = -1
			if c.matchingColumns {
				if other := plan2.FindColumn(def.Cols, "b"); other != nil {
					other.Typ = col.Typ
				}
			}
		}
	}
	return ref, def, err
}

func TestAlterCopyPkPrecheckUsesSourceLineage(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sql      string
		wantCols []string
		swap     bool
	}{
		{
			name: "replacement uses default",
			sql: "ALTER TABLE t1 ALGORITHM=COPY, DROP PRIMARY KEY, DROP COLUMN a, " +
				"ADD COLUMN a BIGINT DEFAULT 8, MODIFY COLUMN a BIGINT DEFAULT 8, ADD PRIMARY KEY(a)",
		},
		{
			name:     "existing column is copied",
			sql:      "ALTER TABLE t1 ALGORITHM=COPY, DROP PRIMARY KEY, ADD PRIMARY KEY(b)",
			wantCols: []string{"b"},
		},
		{
			name: "same-name primary key reads another source",
			sql: "ALTER TABLE t1 ALGORITHM=COPY, RENAME COLUMN a TO tmp, " +
				"RENAME COLUMN b TO a, DROP PRIMARY KEY, ADD PRIMARY KEY(a)",
			swap: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &alterCopySourceContext{
				CompilerContext: plan2.NewMockOptimizer(false).CurrentContext(),
				matchingColumns: tc.swap,
			}
			stmts, err := mysql.Parse(ctx.GetContext(), tc.sql, 1)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			built, err := plan2.BuildPlan(ctx, stmts[0], false)
			require.NoError(t, err)
			alter := built.GetDdl().GetAlterTable()
			require.NotNil(t, alter)
			require.False(t, alter.GetOptions().GetSkipPkDedup())
			if tc.wantCols == nil {
				old := plan2.FindColumn(alter.TableDef.Cols, "a")
				updated := plan2.FindColumn(alter.CopyTableDef.Cols, "a")
				require.Equal(t, old.Typ.Id, updated.Typ.Id)
				require.Equal(t, old.Typ.Width, updated.Typ.Width)
				require.Equal(t, old.Typ.Scale, updated.Typ.Scale)
			}
			if tc.swap {
				oldB := plan2.FindColumn(alter.TableDef.Cols, "b")
				require.Equal(t, "a", alter.ChangeTblColIdMap[oldB.ColId].Name)
			}
			got, _ := getAlterCopyPkPrecheck(alter)
			require.Equal(t, tc.wantCols, got, "copy SQL: %s", alter.GetInsertTmpDataSql())
		})
	}
}
