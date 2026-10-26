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

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func parseForeignKeyDef(t *testing.T, sql string) *tree.ForeignKey {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	t.Cleanup(func() { stmt.Free() })
	for _, def := range stmt.(*tree.CreateTable).Defs {
		if fk, ok := def.(*tree.ForeignKey); ok {
			return fk
		}
	}
	t.Fatalf("no foreign key in %q", sql)
	return nil
}

// A foreign key that references a partitioned parent table, or that is added
// to an existing partitioned child table, must be rejected with MySQL's 1506
// instead of reaching the engine (where the partitioned relation panics with
// "not implemented" in UpdateConstraint).
func TestForeignKeyRejectsPartitionedTables(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	ctx.SetContext(context.Background())
	ctx.tables[catalog.MOForeignKeys] = &TableDef{
		Name: catalog.MOForeignKeys,
		Cols: []*ColDef{
			{Name: "referenced_index_name"},
			{Name: "on_delete_origin"},
			{Name: "on_update_origin"},
		},
	}
	intType := plan.Type{Id: int32(types.T_int32)}
	newParent := func(name string, partitioned bool) *TableDef {
		def := &TableDef{
			Name:  name,
			TblId: 42,
			Cols:  []*plan.ColDef{{ColId: 2, Name: "id", Typ: intType}},
			Pkey:  &plan.PrimaryKeyDef{Names: []string{"id"}},
		}
		if partitioned {
			def.Partition = &plan.Partition{}
		}
		return def
	}
	newChild := func(partitioned bool) *TableDef {
		def := &TableDef{
			Name: "child",
			Cols: []*plan.ColDef{{ColId: 1, Name: "parent_id", Typ: intType}},
		}
		if partitioned {
			def.Partition = &plan.Partition{}
		}
		return def
	}
	ctx.tables["pparent"] = newParent("pparent", true)
	ctx.tables["parent"] = newParent("parent", false)

	_, err := getForeignKeyData(ctx, "db", newChild(false), parseForeignKeyDef(t,
		"create table child (parent_id int, foreign key (parent_id) references pparent (id))"))
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrForeignKeyOnPartitioned), "%v", err)

	_, err = getForeignKeyData(ctx, "db", newChild(true), parseForeignKeyDef(t,
		"create table child (parent_id int, foreign key (parent_id) references parent (id))"))
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrForeignKeyOnPartitioned), "%v", err)

	// Control: an ordinary parent and child still build the foreign key.
	data, err := getForeignKeyData(ctx, "db", newChild(false), parseForeignKeyDef(t,
		"create table child (parent_id int, foreign key (parent_id) references parent (id))"))
	require.NoError(t, err)
	require.Equal(t, uint64(42), data.Def.ForeignTbl)
}
