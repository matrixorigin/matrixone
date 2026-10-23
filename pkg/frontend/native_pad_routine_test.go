// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type nativePADRoutineResult struct{ *evalCondResult }

func (r *nativePADRoutineResult) GetString(context.Context, uint64, uint64) (string, error) {
	return strconv.FormatInt(r.value, 10), nil
}

type nativePADRoutineBackgroundExec struct {
	*evalCondBackgroundExec
	ses *Session
}

func (b *nativePADRoutineBackgroundExec) Exec(ctx context.Context, sql string) error {
	if strings.HasPrefix(strings.ToLower(sql), "select ") {
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
		if err != nil {
			return err
		}
		defer stmt.Free()
		compiler := b.ses.GetTxnCompileCtx()
		parent := compiler.GetContext()
		compiler.SetContext(ctx)
		defer compiler.SetContext(parent)
		value, err := GetSimpleExprValue(ctx, stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr, b.ses)
		if err != nil {
			return err
		}
		var result int64
		if value.(bool) {
			result = 1
		}
		b.execResult = []interface{}{&nativePADRoutineResult{evalCondResult: &evalCondResult{value: result}}}
	}
	return b.evalCondBackgroundExec.Exec(ctx, sql)
}

func TestNativePADLegacyProcedureBodyAndCallerArguments(t *testing.T) {
	for _, language := range []string{"sql", "starlark"} {
		t.Run(language, func(t *testing.T) {
			parent := plan2.WithCollationBindingRevision(t.Context(), 1)
			ses := newTestSession(t, gomock.NewController(t))
			t.Cleanup(ses.Close)
			proc := testutil.NewProcess(t)
			setProtocolVersionForTest(t, proc.GetService(), defines.MORPCVersion130)
			ses.GetTxnCompileCtx().execCtx = &ExecCtx{reqCtx: parent, proc: proc, ses: ses}
			proc.SetResolveVariableFunc(ses.GetTxnCompileCtx().ResolveVariable)
			proc.SetResolveVariableIsBinFunc(ses.GetTxnCompileCtx().ResolveVariableIsBin)
			proc.SetResolveVariableBinaryStringFunc(ses.GetTxnCompileCtx().ResolveVariableBinaryString)
			ses.GetTxnCompileCtx().SetContext(parent)

			call, err := parsers.ParseOne(parent, dialect.MYSQL,
				"call p('A' collate utf8mb4_general_ci = 'a', @result)", 1)
			require.NoError(t, err)
			defer call.Free()
			args := call.(*tree.CallStmt).Args
			values := []map[string]interface{}{}
			typeScopes := []map[string]pb.Type{}
			boolean := pb.Type{Id: int32(types.T_bool)}
			back := &nativePADRoutineBackgroundExec{evalCondBackgroundExec: &evalCondBackgroundExec{}, ses: ses}
			interp := &Interpreter{
				ctx: parent, ses: ses, bh: back, varScope: &values, varTypeScope: &typeScopes,
				fmtctx:      tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true)),
				argsMap:     map[string]tree.Expr{"input": args[0], "result": args[1]},
				argsAttr:    map[string]tree.InOutArgType{"input": tree.TYPE_IN, "result": tree.TYPE_OUT},
				argsType:    map[string]pb.Type{"input": boolean, "result": boolean},
				outParamMap: make(map[string]interface{}),
			}
			if language == "sql" {
				body, err := parsers.ParseOne(parent, dialect.MYSQL,
					"begin set result = ('A' collate utf8mb4_general_ci = 'a'); end", 1)
				require.NoError(t, err)
				defer body.Free()
				require.NoError(t, interp.ExecuteSp(body, "db", false))
				require.Equal(t, true, values[0]["input"], "CALL argument retains the caller's native semantics")
			} else {
				require.NoError(t, interp.ExecuteStarlark("if not input: fail('caller argument lost native semantics')\nout_result = mo.sql(\"select 'A' collate utf8mb4_general_ci = 'a'\")[0][0][0] == '1'", "db", false))
			}
			result, err := ses.GetUserDefinedVar("result")
			require.NoError(t, err)
			require.Equal(t, false, result.Value, "an unversioned stored body retains legacy binding")
			require.Same(t, parent, ses.GetTxnCompileCtx().GetContext())
		})
	}
}
