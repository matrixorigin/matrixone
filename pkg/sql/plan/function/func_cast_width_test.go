// Copyright 2021 Matrix Origin
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

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func runStrToStrWidth(t *testing.T, mp *mpool.MPool, input string, toType types.Type, strict, allowTrim bool) (string, bool, error) {
	t.Helper()
	src := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(src, []byte(input), false, mp))
	defer src.Free(mp)

	from := vector.GenerateFunctionStrParameter(src)
	rsw := vector.NewFunctionResultWrapper(toType, mp)
	to := rsw.(*vector.FunctionResult[types.Varlena])
	defer to.Free()
	require.NoError(t, to.PreExtendAndReset(1))

	if err := strToStr(context.Background(), from, to, 1, toType, strict, allowTrim); err != nil {
		return "", false, err
	}
	got, null := vector.GenerateFunctionStrParameter(to.GetResultVector()).GetStrValue(0)
	return string(got), null, nil
}

// TestStrToStrWidthEnforcement covers the CHAR/VARCHAR over-length matrix:
// strict vs non-strict, and the trailing-space exemption (allowTrailingSpaceTrim).
func TestStrToStrWidthEnforcement(t *testing.T) {
	mp := mpool.MustNewZero()
	vc3 := types.New(types.T_varchar, 3, 0)

	cases := []struct {
		name      string
		input     string
		strict    bool
		allowTrim bool
		want      string
		wantErr   bool
	}{
		{"fits", "abc", true, true, "abc", false},
		{"strict_overlen_reject", "abcd", true, false, "", true},
		{"strict_overlen_reject_even_with_trimflag", "abcd", true, true, "", true},
		{"nonstrict_truncate", "abcd", false, true, "abc", false},
		{"nonstrict_truncate_no_trimflag", "abcd", false, false, "abc", false},
		{"trailing_space_exempt_under_strict", "abc   ", true, true, "abc", false},
		{"trailing_space_not_exempt_for_ddl", "abc   ", true, false, "", true},
		{"trailing_space_exempt_multibyte", "你好世   ", true, true, "你好世", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _, err := runStrToStrWidth(t, mp, c.input, vc3, c.strict, c.allowTrim)
			if c.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func TestOverLenIsAllTrailingSpaces(t *testing.T) {
	require.True(t, overLenIsAllTrailingSpaces("abc ", 3))
	require.True(t, overLenIsAllTrailingSpaces("abc   ", 3))
	require.True(t, overLenIsAllTrailingSpaces("你好世 ", 3))
	require.False(t, overLenIsAllTrailingSpaces("abcd", 3))
	require.False(t, overLenIsAllTrailingSpaces("ab cd", 3)) // excess runes 'c','d' are not spaces
	require.False(t, overLenIsAllTrailingSpaces("abc", 3))   // not over-length
	require.False(t, overLenIsAllTrailingSpaces("ab", 3))    // shorter
}

func TestIsStrictSqlMode(t *testing.T) {
	require.True(t, isStrictSqlMode(nil))

	proc := testutil.NewProcess(t)
	set := func(v interface{}, err error) {
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return v, err })
	}

	set("STRICT_TRANS_TABLES", nil)
	require.True(t, isStrictSqlMode(proc))
	set("STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION", nil)
	require.True(t, isStrictSqlMode(proc))
	set("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES", nil)
	require.True(t, isStrictSqlMode(proc))

	set("", nil)
	require.False(t, isStrictSqlMode(proc))
	set("NO_ENGINE_SUBSTITUTION", nil)
	require.False(t, isStrictSqlMode(proc))

	// error / nil / non-string default to strict (safe fallback)
	set(nil, moerr.NewInternalErrorNoCtx("boom"))
	require.True(t, isStrictSqlMode(proc))
	set(nil, nil)
	require.True(t, isStrictSqlMode(proc))
	set(123, nil)
	require.True(t, isStrictSqlMode(proc))
}

// TestResolveAssignStringWidth covers the constant-folded INSERT VALUES path:
// strict vs non-strict gating, trailing-space exemption, and INSERT IGNORE.
func TestResolveAssignStringWidth(t *testing.T) {
	strict := testutil.NewProcess(t)
	strict.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return "STRICT_TRANS_TABLES", nil })
	lenient := testutil.NewProcess(t)
	lenient.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return "", nil })

	// fits: returned unchanged, not rejected.
	got, reject := ResolveAssignStringWidth(strict, "abc", 3, false)
	require.False(t, reject)
	require.Equal(t, "abc", got)

	// strict + over-length non-space: rejected.
	_, reject = ResolveAssignStringWidth(strict, "abcd", 3, false)
	require.True(t, reject)

	// strict + trailing-space-only overflow: truncated, not rejected.
	got, reject = ResolveAssignStringWidth(strict, "abc   ", 3, false)
	require.False(t, reject)
	require.Equal(t, "abc", got)

	// non-strict: truncated, not rejected.
	got, reject = ResolveAssignStringWidth(lenient, "abcd", 3, false)
	require.False(t, reject)
	require.Equal(t, "abc", got)

	// INSERT IGNORE under strict: truncated, not rejected.
	got, reject = ResolveAssignStringWidth(strict, "abcd", 3, true)
	require.False(t, reject)
	require.Equal(t, "abc", got)

	// destLen <= 0 guard: unchanged.
	got, reject = ResolveAssignStringWidth(strict, "abcd", 0, false)
	require.False(t, reject)
	require.Equal(t, "abcd", got)
}

func castTextToVarchar3(t *testing.T, proc *process.Process, input string) (string, error) {
	t.Helper()
	vc3 := types.New(types.T_varchar, 3, 0)
	src := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(src, []byte(input), false, proc.Mp()))
	dst := vector.NewVec(vc3)
	rs := vector.NewFunctionResultWrapper(vc3, proc.Mp())
	defer rs.Free()
	require.NoError(t, rs.PreExtendAndReset(1))
	if err := NewAssignCast([]*vector.Vector{src, dst}, rs, proc, 1, nil); err != nil {
		return "", err
	}
	got, _ := vector.GenerateFunctionStrParameter(rs.GetResultVector()).GetStrValue(0)
	return string(got), nil
}

// TestNewAssignCastHonorsSqlMode verifies the DML assignment cast (cast_assign)
// gates the width check on sql_mode at runtime, applies the trailing-space
// exemption under strict mode, and that the DDL cast (cast_strict) does not.
func TestNewAssignCastHonorsSqlMode(t *testing.T) {
	newProc := func(sqlMode string) *process.Process {
		proc := testutil.NewProcess(t)
		proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
			if name == "sql_mode" {
				return sqlMode, nil
			}
			return nil, moerr.NewInternalError(proc.Ctx, "unexpected variable "+name)
		})
		return proc
	}

	// strict mode: over-length non-space value is rejected.
	_, err := castTextToVarchar3(t, newProc("STRICT_TRANS_TABLES"), "abcd")
	require.Error(t, err)

	// non-strict mode: over-length value is truncated, no error.
	got, err := castTextToVarchar3(t, newProc(""), "abcd")
	require.NoError(t, err)
	require.Equal(t, "abc", got)

	// strict mode: trailing-space-only overflow is accepted (truncated).
	got, err = castTextToVarchar3(t, newProc("STRICT_TRANS_TABLES"), "abc   ")
	require.NoError(t, err)
	require.Equal(t, "abc", got)

	// DDL cast (NewStrictCast) never applies the trailing-space exemption.
	proc := testutil.NewProcess(t)
	src := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(src, []byte("abc   "), false, proc.Mp()))
	dst := vector.NewVec(types.New(types.T_varchar, 3, 0))
	rs := vector.NewFunctionResultWrapper(types.New(types.T_varchar, 3, 0), proc.Mp())
	defer rs.Free()
	require.NoError(t, rs.PreExtendAndReset(1))
	require.Error(t, NewStrictCast([]*vector.Vector{src, dst}, rs, proc, 1, nil))
}
