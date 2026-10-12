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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedExecutionBindingsOptionalNullSourceTypes(t *testing.T) {
	null := plan2.ParamValue{IsBinaryProtocol: true}
	number := plan2.ParamValue{IsBinaryProtocol: true, Value: "42"}
	for _, tc := range []struct {
		name          string
		values        []any
		protocolTypes []byte
		bitCountTypes []types.Type
		wantTypes     []types.Type
		wantError     string
	}{
		{name: "empty"},
		{name: "first NULL", values: []any{null}, wantTypes: []types.Type{{}}},
		{name: "all NULL with hints", values: []any{null, null},
			bitCountTypes: []types.Type{types.T_int64.ToType(), types.T_uint64.ToType()},
			wantTypes:     []types.Type{{}, {}}},
		{name: "incomplete NULL metadata", values: []any{null},
			protocolTypes: []byte{byte(defines.MYSQL_TYPE_BLOB)}, wantTypes: []types.Type{{}}},
		{name: "known binary NULL", values: []any{null},
			protocolTypes: []byte{byte(defines.MYSQL_TYPE_BLOB), 0}, wantTypes: []types.Type{types.T_blob.ToType()}},
		{name: "known numeric NULL", values: []any{null},
			protocolTypes: []byte{byte(defines.MYSQL_TYPE_LONG), 0}, wantTypes: []types.Type{{}}},
		{name: "unsupported type on NULL preserves existing semantics", values: []any{null},
			protocolTypes: []byte{0x7f, 0}, wantTypes: []types.Type{{}}},
		{name: "typed value then NULL without metadata", values: []any{number, null},
			protocolTypes: []byte{byte(defines.MYSQL_TYPE_LONGLONG), 0},
			wantTypes:     []types.Type{types.T_int64.ToType(), {}}},
		{name: "missing non-NULL metadata", values: []any{number},
			wantError: "prepared parameter has no protocol source type"},
		{name: "incomplete non-NULL metadata", values: []any{number},
			protocolTypes: []byte{byte(defines.MYSQL_TYPE_LONGLONG)},
			wantError:     "prepared parameter has no protocol source type"},
		{name: "NULL cannot exempt neighboring value", values: []any{null, number},
			protocolTypes: []byte{byte(defines.MYSQL_TYPE_NULL), 0},
			wantError:     "prepared parameter has no protocol source type"},
		{name: "unsupported non-NULL metadata", values: []any{number},
			protocolTypes: []byte{0x7f, 0}, wantError: "unsupported prepared parameter source type"},
		{name: "SQL typed NULL", values: []any{plan2.ParamValue{SourceType: types.T_blob.ToType(), HasSourceType: true}},
			wantTypes: []types.Type{types.T_blob.ToType()}},
		{name: "ordinary nil", values: []any{nil}, wantTypes: []types.Type{{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bindings, err := preparedExecutionBindings(context.Background(), tc.values, tc.protocolTypes, tc.bitCountTypes)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				require.Nil(t, bindings)
				return
			}
			require.NoError(t, err)
			require.Len(t, bindings, len(tc.wantTypes))
			for i, binding := range bindings {
				want := plan2.PreparedSourceBinding{Position: int32(i), Type: tc.wantTypes[i]}
				if i < len(tc.bitCountTypes) {
					want.BitCountType = tc.bitCountTypes[i]
				}
				require.Equal(t, want, binding)
			}
		})
	}
}

func TestCOMStmtPreparedNullWithoutSourceTypes(t *testing.T) {
	for _, query := range []string{"select cast(? as decimal(12,3))", "select ? is null"} {
		t.Run(query, func(t *testing.T) {
			ses, stmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 29964, query)
			t.Cleanup(func() {
				cw.proc.SetPrepareParams(nil)
				stmt.Close()
			})
			proto, _, scratch := newBinaryPrepareProtocolTestCase(t, query)
			t.Cleanup(scratch.Close)
			originalPlan, err := stmt.PreparePlan.Marshal()
			require.NoError(t, err)
			for _, tc := range []struct {
				name   string
				packet []byte
				null   bool
			}{
				{name: "first NULL without types", packet: []byte{0, 1, 0, 0, 0, 1, 0}, null: true},
				{name: "decimal supplies types", packet: buildStringExecutePacket(proto, defines.MYSQL_TYPE_NEWDECIMAL, "1.500")},
				{name: "NULL reuses types", packet: []byte{0, 1, 0, 0, 0, 1, 0}, null: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					require.NoError(t, proto.ParseExecuteData(execCtx.reqCtx, cw.proc, stmt, tc.packet, 0))
					if tc.name == "first NULL without types" {
						require.Empty(t, stmt.ParamTypes)
					} else {
						require.Equal(t, []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0}, stmt.ParamTypes)
					}
					_, runtimePlan, executionStmt, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, nil, stmt.Name)
					require.NoError(t, err)
					t.Cleanup(func() {
						if owned && executionStmt != nil {
							executionStmt.Free()
						}
						stmt.clearBinaryParamState(cw.proc)
					})
					plan := runtimePlan.GetQuery()
					project := plan.Nodes[plan.Steps[len(plan.Steps)-1]].ProjectList[0]
					executor, err := colexec.NewExpressionExecutor(cw.proc, project)
					require.NoError(t, err)
					t.Cleanup(executor.Free)
					result, err := executor.Eval(cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
					require.NoError(t, err)
					require.Equal(t, 1, result.Length())
					if query == "select ? is null" {
						require.False(t, result.IsNull(0))
						require.Equal(t, tc.null, vector.GetFixedAtNoTypeCheck[bool](result, 0))
					} else {
						require.Equal(t, tc.null, result.IsNull(0))
						if !tc.null {
							require.Equal(t, "1.500", formatPreparedTimeDecimalResult(t, result))
						}
					}
					after, err := stmt.PreparePlan.Marshal()
					require.NoError(t, err)
					require.Equal(t, originalPlan, after, "execution must not mutate the cached prepare plan")
				})
			}
		})
	}
}
