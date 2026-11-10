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
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedGroupConcatDependentMainOutputMetadata(t *testing.T) {
	for _, dependent := range []bool{false, true} {
		for _, binary := range []bool{false, true} {
			t.Run(fmt.Sprintf("dependent=%t/binary=%t", dependent, binary), func(t *testing.T) {
				value := "t.k"
				if binary {
					value = "convert(t.k using binary)"
				}
				companion := "t.k"
				if dependent {
					companion = "(select (select t.k))"
				}
				sql := fmt.Sprintf("select group_concat(%s) as gc, %s as k from (select 1 as k union all select 2) t group by t.k", value, companion)
				proto, _, prepared := newBinaryPrepareProtocolTestCase(t, sql)
				defer prepared.Close()
				q := prepared.PreparePlan.GetDcl().GetPrepare().GetPlan().GetQuery()
				if dependent {
					require.NotNil(t, q.Correlation)
					require.NotEqual(t, int32(len(q.Steps)-1), q.MainOutputStep())
					require.Len(t, q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList, 1)
				}
				for _, floor := range []uint64{64, 512, 513} {
					prepared.groupConcatMaxLenFloor = floor
					columns := getPreparedResultColumns(prepared, false)
					require.Len(t, columns, 2)
					column, err := colDef2MysqlColumn(t.Context(), columns[0])
					require.NoError(t, err)
					want := defines.MYSQL_TYPE_VAR_STRING
					if floor > 512 {
						want = defines.MYSQL_TYPE_LONG_BLOB
						if binary {
							want = defines.MYSQL_TYPE_BLOB
						}
					}
					for _, command := range []int{int(COM_STMT_PREPARE), int(COM_STMT_EXECUTE)} {
						packet := proto.makeColumnDefinition41Payload(column, command)
						definition := parsePrepareColumnDefinition(t, packet[HeaderOffset:])
						require.Equal(t, want, definition.typ)
						if binary {
							require.Equal(t, uint16(charsetBinary), definition.charset)
							require.NotZero(t, definition.flags&uint16(defines.BINARY_FLAG))
						}
					}
					// Result metadata must not change the execution aggregate type.
					for _, node := range q.Nodes {
						for _, expr := range node.AggList {
							if preparedExprContainsGroupConcat(expr) {
								wantType := types.T_text
								if binary {
									wantType = types.T_blob
								}
								require.Equal(t, int32(wantType), expr.Typ.Id)
							}
						}
					}
				}
			})
		}
	}
}

func TestPreparedGroupConcatMetadataOutputSelection(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		correlated, returning bool
		main, returningStep   int32
		want                  types.T
	}{
		{"main_not_last", true, false, 0, 0, types.T_text},
		{"returning_override", true, true, 0, 1, types.T_varchar},
		{"invalid_returning", true, true, 0, -1, types.T_text},
		{"ordinary_last", false, false, 0, 0, types.T_varchar},
		{"invalid_main_negative", true, false, -1, 0, types.T_text},
		{"invalid_main_large", true, false, 2, 0, types.T_text},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := &plan.Query{Steps: []int32{0, 1}, HasReturning: tc.returning, ReturningStep: tc.returningStep,
				Nodes: []*plan.Node{
					{ProjectList: []*plan.Expr{{}}},
					{ProjectList: []*plan.Expr{{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "group_concat"}}}}}},
				}}
			if tc.correlated {
				q.Correlation = &plan.CorrelationSpec{MainOutputStep: tc.main}
			}
			column := &plan.ColDef{Typ: types.T_text.ToType().PlanType()}
			overlayPreparedGroupConcatResultMetadata(q, []*plan.ColDef{column}, 64)
			require.Equal(t, int32(tc.want), column.Typ.Id)
		})
	}
}

func TestPreparedGroupConcatResultMetadataFollowsMonotonicFloor(t *testing.T) {
	_, _, prepareStmt := newBinaryPrepareProtocolTestCase(t, "select group_concat(?) as gc")
	defer prepareStmt.Close()

	for _, test := range []struct {
		name      string
		requested uint64
		effective uint64
		mysqlType defines.MysqlType
		length    uint32
	}{
		{name: "prepare floor", requested: 64, effective: 64, mysqlType: defines.MYSQL_TYPE_VAR_STRING, length: 256},
		{name: "execution widened floor", requested: 1_000_000, effective: 1_000_000, mysqlType: defines.MYSQL_TYPE_LONG_BLOB, length: types.MaxLongTextLen},
		{name: "later lower execution keeps widened floor", requested: 64, effective: 1_000_000, mysqlType: defines.MYSQL_TYPE_LONG_BLOB, length: types.MaxLongTextLen},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.requested > prepareStmt.groupConcatMaxLenFloor {
				prepareStmt.groupConcatMaxLenFloor = test.requested
			}
			require.Equal(t, test.effective, prepareStmt.groupConcatMaxLenFloor)
			columns := getPreparedResultColumns(prepareStmt, false)
			require.Len(t, columns, 1)
			column, err := colDef2MysqlColumn(t.Context(), columns[0])
			require.NoError(t, err)
			require.Equal(t, test.mysqlType, column.ColumnType())
			require.Equal(t, test.length, column.Length())
		})
	}

	query := prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan().GetQuery()
	for _, node := range query.Nodes {
		for _, expr := range node.AggList {
			if fn := expr.GetF(); fn != nil && fn.GetFunc() != nil && fn.GetFunc().GetObjName() == "group_concat" {
				require.Equal(t, int32(types.T_text), expr.Typ.Id)
			}
		}
	}
}

func TestPreparedGroupConcatExecutePublishesMetadataAfterSuccess(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 28676, "select group_concat('abcdef') as gc")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	prepareStmt.groupConcatMaxLenFloor = 64
	oldColDefData := [][]byte{[]byte("prepare-time")}
	prepareStmt.ColDefData = oldColDefData
	execCtx.prepareColDef = oldColDefData

	writer := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	var metadataCalls int
	var metadataColumn *MysqlColumn
	writer.makeColumnDefDataFunc = func(ctx context.Context, columns []*plan.ColDef) ([][]byte, error) {
		metadataCalls++
		require.Len(t, columns, 1)
		var err error
		metadataColumn, err = colDef2MysqlColumn(ctx, columns[0])
		require.NoError(t, err)
		return [][]byte{[]byte(fmt.Sprintf("metadata-%d", metadataCalls))}, nil
	}

	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "group_concat_max_len", int64(1_000_000)))
	_, _, executionStmt, _, owned, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	if owned && executionStmt != nil {
		executionStmt.Free()
	}
	require.Equal(t, 1, metadataCalls)
	require.Equal(t, defines.MYSQL_TYPE_LONG_BLOB, metadataColumn.ColumnType())
	require.Equal(t, uint32(types.MaxLongTextLen), metadataColumn.Length())
	require.Equal(t, uint64(1_000_000), prepareStmt.groupConcatMaxLenFloor)
	require.Equal(t, [][]byte{[]byte("metadata-1")}, prepareStmt.ColDefData)
	require.Equal(t, prepareStmt.ColDefData, execCtx.prepareColDef)

	columns, colDefs, err := getSelectColumnsAndResultColumns(execCtx.reqCtx, cw)
	require.NoError(t, err)
	require.Len(t, columns, 1)
	require.Len(t, colDefs, 1)
	require.Equal(t, defines.MYSQL_TYPE_LONG_BLOB, columns[0].(*MysqlColumn).ColumnType())
	require.Equal(t, int32(types.T_text), colDefs[0].Typ.Id)
	require.Equal(t, int32(types.MaxLongTextLen), colDefs[0].Typ.Width)

	// A lower value in the same prepared lifetime must neither rebuild the
	// protocol metadata nor narrow the result columns.
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "group_concat_max_len", int64(64)))
	_, _, executionStmt, _, owned, err = initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	if owned && executionStmt != nil {
		executionStmt.Free()
	}
	require.Equal(t, 1, metadataCalls)
	require.Equal(t, uint64(1_000_000), prepareStmt.groupConcatMaxLenFloor)
	require.Equal(t, [][]byte{[]byte("metadata-1")}, prepareStmt.ColDefData)
	columns, colDefs, err = getSelectColumnsAndResultColumns(execCtx.reqCtx, cw)
	require.NoError(t, err)
	require.Equal(t, defines.MYSQL_TYPE_LONG_BLOB, columns[0].(*MysqlColumn).ColumnType())
	require.Equal(t, int32(types.T_text), colDefs[0].Typ.Id)
}

func TestPreparedGroupConcatRejectedExecuteDoesNotPublishFloor(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 28677, "select group_concat(?) as gc")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	prepareStmt.groupConcatMaxLenFloor = 64
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "group_concat_max_len", int64(1_000_000)))

	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.Error(t, err)
	require.Equal(t, uint64(64), prepareStmt.groupConcatMaxLenFloor)
}

func TestPreparedGroupConcatMetadataFailureDoesNotPublishFloor(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 28678, "select group_concat('abcdef') as gc")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	prepareStmt.groupConcatMaxLenFloor = 64
	oldColDefData := [][]byte{[]byte("old")}
	prepareStmt.ColDefData = oldColDefData
	execCtx.prepareColDef = oldColDefData
	writer := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	writer.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
		return nil, errors.New("injected metadata failure")
	}
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "group_concat_max_len", int64(1_000_000)))

	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.ErrorContains(t, err, "injected metadata failure")
	require.Equal(t, uint64(64), prepareStmt.groupConcatMaxLenFloor)
	require.Equal(t, oldColDefData, prepareStmt.ColDefData)
	require.Equal(t, oldColDefData, execCtx.prepareColDef)
}

func TestPreparedGroupConcatRebuildRejectedExecuteRefreshesWireMetadata(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 28680, "select group_concat(?) as gc")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	prepareStmt.groupConcatMaxLenFloor = 64
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "group_concat_max_len", int64(64)))
	oldColDefData := [][]byte{[]byte("stale-int-column")}
	prepareStmt.ColDefData = oldColDefData
	execCtx.prepareColDef = oldColDefData

	proto := &MysqlProtocolImpl{io: NewIOPackage(true)}
	var generatedColumn *MysqlColumn
	writer := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	writer.makeColumnDefDataFunc = func(ctx context.Context, columns []*plan.ColDef) ([][]byte, error) {
		require.Len(t, columns, 1)
		var err error
		generatedColumn, err = colDef2MysqlColumn(ctx, columns[0])
		require.NoError(t, err)
		return [][]byte{proto.makeColumnDefinition41Payload(generatedColumn, int(COM_STMT_EXECUTE))}, nil
	}

	// An unrelated temporary-table change forces the prepared plan through the
	// same rebuild path as a schema refresh. Reject the execute after the rebuild
	// has produced a new plan, but before its parameters are valid.
	ses.AddTempTable("db1", "unrelated", "temp-unrelated")
	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.Error(t, err)
	require.True(t, prepareStmt.needsRebuild)
	require.True(t, prepareStmt.compileNeedsRebuild)
	require.Equal(t, oldColDefData, prepareStmt.ColDefData)
	require.Equal(t, oldColDefData, execCtx.prepareColDef)

	// The next execution has the same metadata floor. It must rebuild and
	// publish the wire metadata that belongs to that plan generation instead of
	// reusing the stale same-count definition left by the rejected execution.
	require.NoError(t, proto.ParseExecuteData(
		execCtx.reqCtx, cw.proc, prepareStmt,
		buildStringExecutePacket(proto, defines.MYSQL_TYPE_VAR_STRING, "abcdef"), 0))
	_, _, _, _, _, err = initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.False(t, prepareStmt.needsRebuild)
	require.False(t, prepareStmt.compileNeedsRebuild)
	require.NotNil(t, generatedColumn)
	require.Equal(t, defines.MYSQL_TYPE_VAR_STRING, generatedColumn.ColumnType())
	require.NotEqual(t, oldColDefData, prepareStmt.ColDefData)
	definition := parsePrepareColumnDefinition(t, prepareStmt.ColDefData[0][HeaderOffset:])
	require.Equal(t, defines.MYSQL_TYPE_VAR_STRING, definition.typ)
}

func TestPreparedGroupConcatBinaryMetadataPreservesBinaryFlag(t *testing.T) {
	_, _, prepareStmt := newBinaryPrepareProtocolTestCase(t,
		"select group_concat(convert(? using binary)) as gc")
	defer prepareStmt.Close()
	proto := &MysqlProtocolImpl{io: NewIOPackage(true)}

	for _, test := range []struct {
		name      string
		floor     uint64
		mysqlType defines.MysqlType
	}{
		{name: "varchar boundary", floor: 64, mysqlType: defines.MYSQL_TYPE_VAR_STRING},
		{name: "binary blob after boundary", floor: 1_000_000, mysqlType: defines.MYSQL_TYPE_BLOB},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepareStmt.groupConcatMaxLenFloor = test.floor
			columns := getPreparedResultColumns(prepareStmt, false)
			require.Len(t, columns, 1)
			column, err := colDef2MysqlColumn(t.Context(), columns[0])
			require.NoError(t, err)
			require.Equal(t, test.mysqlType, column.ColumnType())
			require.Equal(t, uint16(charsetBinary), column.Charset())
			require.NotZero(t, column.Flag()&uint16(defines.BINARY_FLAG))

			packet := proto.makeColumnDefinition41Payload(column, int(COM_STMT_PREPARE))
			definition := parsePrepareColumnDefinition(t, packet[HeaderOffset:])
			require.Equal(t, test.mysqlType, definition.typ)
			require.Equal(t, uint16(charsetBinary), definition.charset)
			require.NotZero(t, definition.flags&uint16(defines.BINARY_FLAG))
		})
	}
}

func TestPreparedGroupConcatMetadataStopsAtSetOperation(t *testing.T) {
	for index, sql := range []string{
		"select group_concat(?) union all select repeat('x', 1000)",
		"select repeat('x', 1000) union all select group_concat(?)",
	} {
		t.Run(fmt.Sprintf("branch-%d", index), func(t *testing.T) {
			_, _, prepareStmt := newBinaryPrepareProtocolTestCase(t, sql)
			defer prepareStmt.Close()
			prepareStmt.groupConcatMaxLenFloor = 64
			columns := getPreparedResultColumns(prepareStmt, false)
			require.Len(t, columns, 1)
			column, err := colDef2MysqlColumn(t.Context(), columns[0])
			require.NoError(t, err)
			require.NotEqual(t, defines.MYSQL_TYPE_VAR_STRING, column.ColumnType())
		})
	}
}

func TestPreparedGroupConcatFloorAdvancesAtExecute(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 28679, "select group_concat('abcdef') as gc")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	prepareStmt.groupConcatMaxLenFloor = 64

	for _, test := range []struct {
		name      string
		requested int64
		effective uint64
	}{
		{name: "widen", requested: 1_000_000, effective: 1_000_000},
		{name: "lower", requested: 64, effective: 1_000_000},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, ses.SetSessionSysVar(
				execCtx.reqCtx, "group_concat_max_len", test.requested))
			_, _, clonedStmt, _, owned, err := initExecuteStmtParam(
				execCtx, ses, cw, nil, prepareStmt.Name)
			require.NoError(t, err)
			if owned && clonedStmt != nil {
				clonedStmt.Free()
			}
			require.Equal(t, test.effective, prepareStmt.groupConcatMaxLenFloor)
		})
	}
}
