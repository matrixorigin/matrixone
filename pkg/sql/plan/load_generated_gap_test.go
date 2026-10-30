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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Issue #31737: without a column list, LOAD maps file fields to the writable
// columns in order. The external reader emits one vector per non-hidden scan
// column, so the scan table must not keep a stored generated column between
// writable ones: it took an output slot and shifted every later field.
func TestImplicitLoadScanSkipsGeneratedColumns(t *testing.T) {
	for _, tc := range []struct {
		name, create string
		writable     []string
	}{
		{"middle", "create table gen_gap(id bigint primary key, base int not null, g int as (base+1) stored, qty int not null)", []string{"id", "base", "qty"}},
		{"first after pk", "create table gen_gap(id bigint primary key, g int as (id*10) stored, base int, qty int)", []string{"id", "base", "qty"}},
		{"no primary key", "create table gen_gap(a int, g int as (a*2) stored, b varchar(5), c int)", []string{"a", "b", "c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
			create, err := mysql.ParseOne(t.Context(), tc.create, 1)
			require.NoError(t, err)
			defer create.Free()
			ddl, err := BuildPlan(ctx, create, false)
			require.NoError(t, err)
			table := ddl.GetDdl().GetCreateTable().TableDef
			table.TblId = 990137
			table.Name2ColIndex = make(map[string]int32, len(table.Cols))
			for i, col := range table.Cols {
				table.Name2ColIndex[col.Name] = int32(i)
			}
			ctx.tables["gen_gap"] = table
			ctx.objects["gen_gap"] = &planpb.ObjectRef{Obj: int64(table.TblId), SchemaName: "tpch", ObjName: "gen_gap"}

			load, err := mysql.ParseOne(t.Context(),
				"load data inline format='csv', data='1,11,21' into table gen_gap fields terminated by ','", 1)
			require.NoError(t, err)
			defer load.Free()
			// Inspect the binder output: with the mock catalog the full BuildPlan
			// falls back to the legacy LOAD planner, which has its own mapping.
			builder := NewQueryBuilder(planpb.Query_INSERT, ctx, false, true)
			bindCtx := NewBindContext(builder, nil)
			_, err = builder.bindLoad(load.(*tree.Load), bindCtx)
			if err != nil {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnsupportedDML), err.Error())
			}

			var scan *planpb.Node
			for _, node := range builder.qry.Nodes {
				if node.NodeType == planpb.Node_EXTERNAL_SCAN {
					scan = node
				}
			}
			require.NotNil(t, scan)
			// Output slot of every non-hidden scan column == its file field.
			var slots []string
			for _, col := range scan.TableDef.Cols {
				if col.Hidden {
					continue
				}
				require.Nil(t, col.GeneratedCol, "generated column %s must not be read from the file", col.Name)
				field, ok := scan.ExternScan.TbColToDataCol[col.Name]
				require.True(t, ok, col.Name)
				require.Equal(t, int32(len(slots)), field, col.Name)
				slots = append(slots, col.Name)
			}
			require.Equal(t, tc.writable, slots)
		})
	}
}
