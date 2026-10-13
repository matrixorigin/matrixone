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
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func starlarkOutputTestSession(t *testing.T) *Session {
	t.Helper()
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	proc := testutil.NewProcess(t)
	ses.GetTxnCompileCtx().execCtx = &ExecCtx{reqCtx: context.Background(), proc: proc, ses: ses}
	return ses
}

func starlarkOutputTestInterpreter(t *testing.T, ses *Session, declaration string) (*Interpreter, *backgroundExecTest) {
	t.Helper()
	ctx := context.Background()
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "create procedure p("+declaration+") language 'starlark' $$pass$$", 1)
	require.NoError(t, err)
	defer stmt.Free()
	back := &backgroundExecTest{}
	back.init()
	interp := &Interpreter{ctx: ctx, ses: ses, bh: back, argsAttr: make(map[string]tree.InOutArgType), argsMap: make(map[string]tree.Expr), argsType: make(map[string]plan.Type), outParamMap: make(map[string]interface{})}
	for _, arg := range stmt.(*tree.CreateProcedure).Args {
		decl := arg.(*tree.ProcedureArgDecl)
		name := decl.Name.ColName()
		typ, err := plan2.GetTypeFromAst(ctx, decl.Type)
		require.NoError(t, err)
		interp.argsAttr[name] = decl.InOutType
		interp.argsMap[name] = &tree.VarExpr{Name: name}
		interp.argsType[name] = typ
	}
	return interp, back
}

func TestStarlarkOutputDeclaredTypes(t *testing.T) {
	ses := starlarkOutputTestSession(t)
	for _, tc := range []struct {
		name, typ, value string
		want             any
		bad              bool
	}{
		{"int8 min", "tinyint", "-128", int8(-128), false},
		{"int8 max", "tinyint", "127", int8(127), false},
		{"int8 overflow", "tinyint", "128", nil, true},
		{"uint8 max", "tinyint unsigned", "255", uint8(255), false},
		{"uint8 negative", "tinyint unsigned", "-1", nil, true},
		{"int16 min", "smallint", "-32768", int16(-32768), false},
		{"int16 max", "smallint", "32767", int16(32767), false},
		{"int16 overflow", "smallint", "32768", nil, true},
		{"uint16 max", "smallint unsigned", "65535", uint16(65535), false},
		{"uint16 negative", "smallint unsigned", "-1", nil, true},
		{"mediumint min", "mediumint", "-8388608", int32(-8388608), false},
		{"mediumint max", "mediumint", "8388607", int32(8388607), false},
		{"mediumint overflow", "mediumint", "8388608", nil, true},
		{"mediumuint max", "mediumint unsigned", "16777215", uint32(16777215), false},
		{"mediumuint overflow", "mediumint unsigned", "16777216", nil, true},
		{"int32 min", "int", "-2147483648", int32(-2147483648), false},
		{"int32 max", "int", "2147483647", int32(2147483647), false},
		{"int32 overflow", "int", "2147483648", nil, true},
		{"uint32 max", "int unsigned", "4294967295", uint32(4294967295), false},
		{"uint32 negative", "int unsigned", "-1", nil, true},
		{"int64 min", "bigint", "-9223372036854775808", int64(-9223372036854775808), false},
		{"int64 max", "bigint", "9223372036854775807", int64(9223372036854775807), false},
		{"int64 overflow", "bigint", "9223372036854775808", nil, true},
		{"uint64 max", "bigint unsigned", "18446744073709551615", uint64(18446744073709551615), false},
		{"uint64 overflow", "bigint unsigned", "1 << 64", nil, true},
		{"uint64 negative", "bigint unsigned", "-1", nil, true},
		{"decimal uint64", "decimal(30,0)", "18446744073709551615", "18446744073709551615", false},
		{"decimal beyond uint64", "decimal(30,0)", "1 << 64", "18446744073709551616", false},
		{"decimal negative beyond int64", "decimal(30,0)", "-(1 << 64)", "-18446744073709551616", false},
		{"decimal precision", "decimal(30,0)", "1000000000000000000000000000000", nil, true},
		{"decimal rounding", "decimal(5,2)", `"12.345"`, "12.35", false},
		{"float", "float", "1.25", float32(1.25), false},
		{"double", "double", "1.25", float64(1.25), false},
		{"bool", "bool", "True", true, false},
		{"int string", "int", `"12"`, int32(12), false},
		{"int bad", "int", `"not-an-int"`, nil, true},
		{"varchar width", "varchar(3)", `"abc"`, "abc", false},
		{"varchar overflow", "varchar(3)", `"too-long"`, nil, true},
		{"quoted string", "varchar(20)", `"a'b\\c"`, "a'b\\c", false},
		{"date", "date", `"2026-02-28"`, "2026-02-28", false},
		{"date bad", "date", `"bad"`, nil, true},
		{"time scale", "time(3)", `"12:34:56.1234"`, "12:34:56.123", false},
		{"time bad", "time", `"bad"`, nil, true},
		{"datetime scale", "datetime(3)", `"2026-02-28 12:34:56.1234"`, "2026-02-28 12:34:56.123", false},
		{"timestamp scale", "timestamp(3)", `"2026-02-28 12:34:56.1234"`, "2026-02-28 12:34:56.123", false},
		{"binary padding", "binary(3)", `"a"`, "a\x00\x00", false},
		{"binary width", "binary(3)", `"abcd"`, nil, true},
		{"varbinary", "varbinary(3)", `"abc"`, "abc", false},
		{"varbinary width", "varbinary(3)", `"abcd"`, nil, true},
		{"vector32", "vecf32(2)", `"[1,2]"`, []float32{1, 2}, false},
		{"vector64", "vecf64(2)", `"[1,2]"`, []float64{1, 2}, false},
		{"vector dimension", "vecf32(2)", `"[1,2,3]"`, nil, true},
		{"vector malformed", "vecf64(2)", `"bad"`, nil, true},
		{"null", "int", "None", nil, false},
		{"list unsupported", "int", "[1,2]", nil, true},
		{"dict unsupported", "varchar(20)", `{"a":1}`, nil, true},
	} {
		for _, direction := range []string{"out", "inout"} {
			t.Run(direction+" "+tc.name, func(t *testing.T) {
				interp, back := starlarkOutputTestInterpreter(t, ses, direction+" v "+tc.typ)
				require.NoError(t, ses.SetUserDefinedVar("v", int64(99), ""))
				previousCtx := ses.GetTxnCompileCtx().GetContext()
				err := interp.ExecuteStarlark("out_v = "+tc.value, "test", false)
				require.Equal(t, previousCtx, ses.GetTxnCompileCtx().GetContext())
				got, getErr := ses.GetUserDefinedVar("v")
				require.NoError(t, getErr)
				if tc.bad {
					require.Error(t, err)
					var sqlErr *moerr.Error
					require.ErrorAs(t, err, &sqlErr)
					require.Equal(t, int64(99), got.Value)
					require.Equal(t, "rollback;", back.executedSQLs[len(back.executedSQLs)-1])
				} else {
					require.NoError(t, err)
					require.Equal(t, tc.want, got.Value)
					require.Equal(t, "commit;", back.executedSQLs[len(back.executedSQLs)-1])
				}
			})
		}
	}
}

func TestStarlarkOutputAtomicityAndReuse(t *testing.T) {
	ses := starlarkOutputTestSession(t)
	for _, aliases := range []bool{false, true} {
		t.Run(map[bool]string{false: "distinct variables", true: "aliased variable"}[aliases], func(t *testing.T) {
			interp, back := starlarkOutputTestInterpreter(t, ses, "out a int, out b varchar(3), inout c int")
			for _, name := range []string{"a", "b", "c"} {
				require.NoError(t, ses.SetUserDefinedVar(name, int64(7), ""))
			}
			if aliases {
				interp.argsMap["b"] = &tree.VarExpr{Name: "a"}
				interp.argsMap["c"] = &tree.VarExpr{Name: "a"}
			}
			require.Error(t, interp.ExecuteStarlark("out_a = 12\nout_b = 'too-long'\nout_c = 13", "test", false))
			for _, name := range []string{"a", "b", "c"} {
				got, err := ses.GetUserDefinedVar(name)
				require.NoError(t, err)
				require.Equal(t, int64(7), got.Value)
			}
			require.Equal(t, "rollback;", back.executedSQLs[len(back.executedSQLs)-1])
			require.NoError(t, interp.ExecuteStarlark("out_a = 12\nout_b = 'ok'\nout_c = 13", "test", false))
			got, err := ses.GetUserDefinedVar("a")
			require.NoError(t, err)
			if aliases {
				require.Contains(t, []any{int32(12), "ok", int32(13)}, got.Value)
			} else {
				require.Equal(t, int32(12), got.Value)
			}
		})
	}
	interp, back := starlarkOutputTestInterpreter(t, ses, "out a int, inout c int")
	require.NoError(t, ses.SetUserDefinedVar("a", int64(3), ""))
	require.NoError(t, ses.SetUserDefinedVar("c", int64(4), ""))
	require.NoError(t, interp.ExecuteStarlark("pass", "test", false))
	for name, want := range map[string]any{"a": int64(3), "c": int64(4)} {
		got, err := ses.GetUserDefinedVar(name)
		require.NoError(t, err)
		require.Equal(t, want, got.Value)
	}
	require.Error(t, interp.ExecuteStarlark("fail('body error')", "test", false))
	require.Equal(t, "rollback;", back.executedSQLs[len(back.executedSQLs)-1])
	back.sql2err["commit;"] = errors.New("commit failed")
	require.ErrorContains(t, interp.ExecuteStarlark("pass", "test", false), "commit failed")
	require.Equal(t, "rollback;", back.executedSQLs[len(back.executedSQLs)-1])
}

// Values crossing the evaluator boundary must survive reuse of vector storage.
func TestGetValueFromVectorOwnsArrays(t *testing.T) {
	testOwnedArrayValue(t, types.T_array_float32, []float32{1, 2})
	testOwnedArrayValue(t, types.T_array_float64, []float64{1, 2})
	testOwnedArrayValue(t, types.T_array_bf16, []types.BF16{1, 2})
	testOwnedArrayValue(t, types.T_array_float16, []types.Float16{1, 2})
	testOwnedArrayValue(t, types.T_array_int8, []int8{1, 2})
	testOwnedArrayValue(t, types.T_array_uint8, []uint8{1, 2})
}
func testOwnedArrayValue[T types.ArrayElement](t *testing.T, oid types.T, want []T) {
	t.Helper()
	t.Run(oid.String(), func(t *testing.T) {
		proc := testutil.NewProcess(t)
		vec := vector.NewVec(oid.ToType())
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		require.NoError(t, vector.AppendArray(vec, want, false, proc.Mp()))
		got, err := getValueFromVector(context.Background(), vec, nil, nil)
		require.NoError(t, err)
		borrowed := vector.GetArrayAt[T](vec, 0)
		clear(borrowed)
		require.Equal(t, want, got)
	})
}
