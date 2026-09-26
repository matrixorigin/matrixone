// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestPreparedPublicationPatternBindings(t *testing.T) {
	for _, binaryExecute := range []bool{false, true} {
		t.Run(fmt.Sprintf("binary=%t", binaryExecute), func(t *testing.T) {
			ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQL(t, 121, "show publications like ?")
			t.Cleanup(prepared.Close)
			t.Cleanup(func() { cw.proc.SetPrepareParams(nil) })
			ses.tenant = &TenantInfo{Tenant: sysAccountName, TenantID: sysAccountID}
			execCtx.input.isBinaryProtExecute = binaryExecute
			execCtx.cw = cw
			show := prepared.PrepareStmt.(*tree.ShowPublications)
			marker := show.Like.Right
			execPlan := &plan.Execute{Name: prepared.Name}
			_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
			require.Error(t, err, "missing bindings must not execute")

			for _, tc := range []struct {
				name     string
				value    any
				wireType defines.MysqlType
				invalid  bool
			}{
				{"first", "team_%", defines.MYSQL_TYPE_VAR_STRING, false},
				{"integer", int64(42), defines.MYSQL_TYPE_LONGLONG, true},
				{"null", nil, defines.MYSQL_TYPE_NULL, true},
				{"after_errors", "other_%", defines.MYSQL_TYPE_VAR_STRING, false},
				{"empty_literal_compatibility", "", defines.MYSQL_TYPE_VAR_STRING, false},
				{"escaped", "x' OR 1=1 -- \\_%\x00", defines.MYSQL_TYPE_BLOB, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if binaryExecute {
						cw.proc.SetPrepareParams(nil)
						if prepared.params != nil {
							prepared.params.Free(cw.proc.Mp())
						}
						prepared.params = vector.NewVec(types.T_text.ToType())
						value := ""
						if tc.value != nil {
							value = fmt.Sprint(tc.value)
						}
						require.NoError(t, vector.AppendBytes(prepared.params, []byte(value), tc.value == nil, cw.proc.Mp()))
						prepared.ParamTypes = []byte{byte(tc.wireType), 0}
					} else {
						require.NoError(t, ses.SetUserDefinedVar("pattern", tc.value, ""))
						execPlan.Args = []*plan.Expr{{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "pattern"}}}}
					}
					_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
					require.NoError(t, err)
					// The actual Compile path marks the resolved EXECUTE only after
					// binding and authorization. Exercise that handler boundary too.
					cw.ifIsExeccute = true
					if tc.invalid {
						err := handleShowPublications(ses, execCtx, show)
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
						return
					}
					pattern, err := showPublicationsLike(execCtx.reqCtx, show, cw.proc)
					require.NoError(t, err)
					require.Equal(t, tc.value, pattern)
					require.Same(t, marker, show.Like.Right)
					cw.ifIsExeccute = false
					require.ErrorContains(t, handleShowPublications(ses, execCtx, show), "requires prepared execution")
					cw.ifIsExeccute = true

					ctrl := gomock.NewController(t)
					bh := mock_frontend.NewMockBackgroundExec(ctrl)
					stub := gostub.StubFunc(&NewBackgroundExec, bh)
					t.Cleanup(stub.Reset)
					bh.EXPECT().Close()
					bh.EXPECT().ClearExecResultSet().Times(2)
					gomock.InOrder(
						bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ any, sql string) error {
							parsed, err := mysql.Parse(execCtx.reqCtx, sql, 1)
							require.NoError(t, err)
							require.Len(t, parsed, 1, "pattern must not introduce another statement")
							defer parsed[0].Free()
							where := parsed[0].(*tree.Select).Select.(*tree.SelectClause).Where.Expr
							if pattern != "" {
								and := where.(*tree.AndExpr)
								like := and.Right.(*tree.ComparisonExpr)
								require.Equal(t, tree.LIKE, like.Op)
								require.Equal(t, pattern, like.Right.(*tree.NumVal).String())
							} else {
								require.IsType(t, &tree.ComparisonExpr{}, where)
							}
							return nil
						}),
						bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil),
					)
					result := mock_frontend.NewMockExecResult(ctrl)
					result.EXPECT().GetRowCount().Return(uint64(0)).AnyTimes()
					bh.EXPECT().GetExecResultSet().Return([]interface{}{result}).Times(2)
					require.NoError(t, handleShowPublications(ses, execCtx, show))
					require.Len(t, ses.mrs.Columns, 8)
					require.Empty(t, ses.mrs.Data)
				})
			}
		})
	}
}

func TestShowPublicationsPatternValidation(t *testing.T) {
	// The literal path and malformed frontend inputs must not need a process.
	for _, tc := range []struct {
		sql     string
		invalid bool
	}{
		{"show publications", false},
		{"show publications like 'team_%'", false},
		{"show publications like ''", false},
		{"show publications like 1", true},
		{"show publications like concat('a', 'b')", true},
		{"show publications like ?", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmts, err := mysql.Parse(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			_, err = showPublicationsLike(t.Context(), stmts[0].(*tree.ShowPublications), nil)
			if tc.invalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
