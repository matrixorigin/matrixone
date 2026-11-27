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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestReplaceValuesSQLModeLookupDoesNotScaleWithRows(t *testing.T) {
	for _, mode := range []string{"STRICT_TRANS_TABLES", ""} {
		for _, verb := range []string{"replace", "insert"} {
			lookups := func(rowCount int) int {
				proc := newPlanTestProcess(t)
				count := 0
				proc.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
					if name == "sql_mode" {
						count++
						return mode, nil
					}
					return nil, nil
				})
				mock := NewMockOptimizer(false, proc)
				mock.ctxt.SetSqlModeOverride(mode)
				created, err := runOneStmt(mock, t, "create table values_mode_t (id int primary key, amount decimal(6,2), delivery datetime, info char(24))")
				require.NoError(t, err)
				table := created.GetDdl().GetCreateTable().TableDef
				table.TblId = 33282
				table.TableType = catalog.SystemOrdinaryRel
				table.Cols = append(table.Cols, &planpb.ColDef{
					Name: catalog.Row_ID, Typ: types.T_Rowid.ToType().PlanType(), Hidden: true,
				})
				mock.ctxt.tables["values_mode_t"] = table
				mock.ctxt.objects["values_mode_t"] = &planpb.ObjectRef{
					SchemaName: "tpch", ObjName: "values_mode_t", Obj: 33282,
				}
				count = 0
				rows := make([]string, rowCount)
				for i := range rows {
					rows[i] = fmt.Sprintf("(%d,12.34,'2026-11-26 01:00:00','ABCDEFGHIJKLMNOPQRSTUVWX')", i+1)
				}
				_, err = runOneStmt(mock, t, verb+" into values_mode_t values "+strings.Join(rows, ","))
				require.NoError(t, err)
				return count
			}
			single, many := lookups(1), lookups(32)
			require.LessOrEqual(t, many-single, 2, "%s SQL mode resolution should be bounded per VALUES plan", verb)
		}
	}
}

type issue32002WarningSink struct{ codes []uint16 }

func (s *issue32002WarningSink) AppendWarningDiagnostic(code uint16, _ string) {
	s.codes = append(s.codes, code)
}

func TestCTASDegreesFromNotNullColumnIsNullable(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	created, err := runOneStmt(mock, t, "create table degrees_source(v double not null)")
	require.NoError(t, err)
	table := created.GetDdl().GetCreateTable().TableDef
	table.TblId = 32002
	mock.ctxt.tables["degrees_source"] = table
	mock.ctxt.objects["degrees_source"] = &planpb.ObjectRef{
		SchemaName: "tpch", ObjName: "degrees_source", Obj: 32002,
	}

	ctas, err := runOneStmt(mock, t,
		"create table degrees_result as select degrees(v) as d from degrees_source")
	require.NoError(t, err)
	for _, col := range ctas.GetDdl().GetCreateTable().TableDef.Cols {
		if col.Name == "d" {
			require.True(t, col.Default.NullAbility,
				"DEGREES can return NULL on overflow even for a NOT NULL input")
			return
		}
	}
	t.Fatal("CTAS result column d was not built")
}

func TestStrictDateValuesKeepsClockTextForAssignment(t *testing.T) {
	proc := newPlanTestProcess(t)
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		if name == "sql_mode" {
			return "STRICT_TRANS_TABLES", nil
		}
		return nil, nil
	})
	mock := NewMockOptimizer(false, proc)
	mock.ctxt.SetSqlModeOverride("STRICT_TRANS_TABLES")
	created, err := runOneStmt(mock, t, "create table date_target(d date)")
	require.NoError(t, err)
	table := created.GetDdl().GetCreateTable().TableDef
	table.TblId = 32003
	mock.ctxt.tables["date_target"] = table
	mock.ctxt.objects["date_target"] = &planpb.ObjectRef{
		SchemaName: "tpch", ObjName: "date_target", Obj: 32003,
	}

	for _, tc := range []struct {
		text       string
		wantReject bool
	}{
		{"2026-10-21", false},
		{"2026-10-21 10:20:30", true},
	} {
		t.Run(tc.text, func(t *testing.T) {
			insert, err := runOneStmt(mock, t,
				fmt.Sprintf("insert into date_target values ('%s')", tc.text))
			require.NoError(t, err)
			casts := queryFunctionsNamed(insert.GetQuery(), "cast_assign")
			require.Len(t, casts, 1, "plain VALUES must retain the assignment cast")
			require.Equal(t, tc.text, casts[0].GetF().Args[0].GetLit().GetSval())
			_, free, err := colexec.GetReadonlyResultFromExpression(proc, casts[0],
				[]*batch.Batch{batch.EmptyForConstFoldBatch})
			if free != nil {
				free()
			}
			if tc.wantReject {
				require.Error(t, err, "strict assignment must reject DATE text with clock fields")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDecimalValuesUsesIntegerAssignmentPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, mode, sql, cast, columnType, wantErrorText string
		wantError                                        bool
	}{
		{"strict unsigned", "STRICT_TRANS_TABLES", "insert into decimal_target values (300.0)", "cast_assign", "tinyint unsigned", "data type TINYINT UNSIGNED, value 300", true},
		{"strict signed", "STRICT_TRANS_TABLES", "insert into decimal_target values (-9223372036854775809)", "cast_assign", "bigint", "data type int64, value '-9223372036854775809'", true},
		{"nonstrict", "", "insert into decimal_target values (300.0)", "cast_assign", "tinyint unsigned", "", false},
		{"ignore", "STRICT_TRANS_TABLES", "insert ignore into decimal_target values (300.0)", "cast_ignore", "tinyint unsigned", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			proc.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
				if name == "sql_mode" {
					return tc.mode, nil
				}
				return nil, nil
			})
			sink := &issue32002WarningSink{}
			proc.WarningSink = sink
			mock := NewMockOptimizer(false, proc)
			mock.ctxt.SetSqlModeOverride(tc.mode)
			created, err := runOneStmt(mock, t, "create table decimal_target(u "+tc.columnType+")")
			require.NoError(t, err)
			table := created.GetDdl().GetCreateTable().TableDef
			table.TblId = 32004
			table.TableType = catalog.SystemOrdinaryRel
			table.Cols = append(table.Cols, &planpb.ColDef{
				Name: catalog.Row_ID, Typ: types.T_Rowid.ToType().PlanType(), Hidden: true,
			})
			mock.ctxt.tables["decimal_target"] = table
			mock.ctxt.objects["decimal_target"] = &planpb.ObjectRef{
				SchemaName: "tpch", ObjName: "decimal_target", Obj: 32004,
			}
			insert, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			casts := queryFunctionsNamed(insert.GetQuery(), tc.cast)
			require.Len(t, casts, 1)
			require.True(t, types.T(casts[0].GetF().Args[0].Typ.Id).IsDecimal())
			got, free, err := colexec.GetReadonlyResultFromExpression(proc, casts[0],
				[]*batch.Batch{batch.EmptyForConstFoldBatch})
			if free != nil {
				defer free()
			}
			if tc.wantError {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.wantErrorText)
				require.Empty(t, sink.codes)
				return
			}
			require.NoError(t, err)
			require.Equal(t, uint8(255), vector.GetFixedAtNoTypeCheck[uint8](got, 0))
			require.Equal(t, []uint16{moerr.ER_WARN_DATA_OUT_OF_RANGE}, sink.codes)
		})
	}
}
