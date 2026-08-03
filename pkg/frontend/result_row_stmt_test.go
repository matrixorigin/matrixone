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

package frontend

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

var (
	benchmarkMysqlColumns  []interface{}
	benchmarkResultColumns []*plan.ColDef
)

func newResultColumnTestPlan(columnCount int) *plan.Plan {
	headings := make([]string, columnCount)
	projectList := make([]*plan.Expr, columnCount)
	for i := range columnCount {
		headings[i] = fmt.Sprintf("column_%d", i)
		projectList[i] = &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_int64), Width: 64},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				Name:    headings[i],
				TblName: "table_name",
				DbName:  "database_name",
			}},
		}
	}

	return &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeId:      0,
			NodeType:    plan.Node_PROJECT,
			ProjectList: projectList,
		}},
		Headings: headings,
	}}}
}

func TestGetSelectColumnsReusesResultColumns(t *testing.T) {
	ctx := context.Background()
	testPlan := newResultColumnTestPlan(1)
	cw := &TxnComputationWrapper{stmt: &tree.Select{}, plan: testPlan}

	legacyColumns, err := cw.GetColumns(ctx)
	require.NoError(t, err)
	legacyResultColumns := plan2.GetResultColumnsFromPlan(testPlan)

	columns, resultColumns, err := getSelectColumnsAndResultColumns(ctx, cw)
	require.NoError(t, err)
	require.Equal(t, legacyColumns, columns)
	require.Equal(t, legacyResultColumns, resultColumns)

	rs := &plan.ResultColDef{ResultCols: resultColumns}
	require.Equal(t, legacyResultColumns, rs.ResultCols)
	require.Equal(t, "column_0", columns[0].(*MysqlColumn).Name())
	require.Equal(t, "table_name", columns[0].(*MysqlColumn).Table())
	require.Equal(t, "database_name", columns[0].(*MysqlColumn).Schema())

	resultColumns[0].Name = "renamed_result_column"
	require.Equal(t, "column_0", columns[0].(*MysqlColumn).Name())
}

func TestGetSelectColumnsKeepsShowColumnsBehavior(t *testing.T) {
	for _, test := range []struct {
		name              string
		columnCount       int
		thirdMetadataName string
	}{
		{name: "regular", columnCount: 7, thirdMetadataName: "Null"},
		{name: "full", columnCount: 9, thirdMetadataName: "Collation"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			testPlan := newResultColumnTestPlan(test.columnCount)
			cw := &TxnComputationWrapper{stmt: &tree.ShowColumns{}, plan: testPlan}

			legacyColumns, err := cw.GetColumns(ctx)
			require.NoError(t, err)
			legacyResultColumns := plan2.GetResultColumnsFromPlan(testPlan)

			columns, resultColumns, err := getSelectColumnsAndResultColumns(ctx, cw)
			require.NoError(t, err)
			require.Equal(t, legacyColumns, columns)
			require.Equal(t, legacyResultColumns, resultColumns)
			require.Equal(t, "Field", columns[0].(*MysqlColumn).Name())
			require.Equal(t, test.thirdMetadataName, columns[2].(*MysqlColumn).Name())
			require.Equal(t, "column_0", resultColumns[0].Name)
		})
	}
}

func TestGetSelectColumnsAfterPreparedExecuteReuse(t *testing.T) {
	_, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 103)
	defer prepareStmt.Close()

	cached := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = cached

	ret, err := cw.Compile(execCtx, nil)
	require.NoError(t, err)
	require.NotNil(t, ret)
	require.Same(t, cached, ret)
	require.IsType(t, &tree.Select{}, cw.GetAst())

	columns, resultColumns, err := getSelectColumnsAndResultColumns(execCtx.reqCtx, cw)
	require.NoError(t, err)
	require.Len(t, columns, 1)
	require.Len(t, resultColumns, 1)
	require.Equal(t, resultColumns[0].Name, columns[0].(*MysqlColumn).Name())
}

func BenchmarkGetSelectColumns(b *testing.B) {
	ctx := context.Background()
	testPlan := newResultColumnTestPlan(1)
	cw := &TxnComputationWrapper{stmt: &tree.Select{}, plan: testPlan}

	b.Run("duplicate-result-columns", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			var err error
			benchmarkMysqlColumns, err = cw.GetColumns(ctx)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkResultColumns = plan2.GetResultColumnsFromPlan(testPlan)
		}
	})

	b.Run("reuse-result-columns", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			var err error
			benchmarkMysqlColumns, benchmarkResultColumns, err = getSelectColumnsAndResultColumns(ctx, cw)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
