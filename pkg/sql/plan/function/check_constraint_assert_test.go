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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCheckConstraintAssert(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	resolve := func() FuncGetResult {
		fn, err := GetFunctionByName(
			proc.Ctx,
			"_check_constraint_assert",
			[]types.Type{types.T_bool.ToType(), types.T_varchar.ToType()},
		)
		require.NoError(t, err)
		return fn
	}

	t.Run("success", func(t *testing.T) {
		flag := newVectorByType(mp, types.T_bool.ToType(), []bool{true}, &nulls.Nulls{})
		message := newVectorByType(mp, types.T_varchar.ToType(), []string{"unused"}, &nulls.Nulls{})
		defer flag.Free(mp)
		defer message.Free(mp)

		fn := resolve()
		result, err := RunFunctionDirectly(
			proc,
			fn.GetEncodedOverloadID(),
			[]*vector.Vector{flag, message},
			1,
		)
		require.NoError(t, err)
		defer result.Free(mp)
		require.Equal(t, []bool{true}, vector.MustFixedColWithTypeCheck[bool](result))
	})

	t.Run("constraint violation", func(t *testing.T) {
		flag := newVectorByType(mp, types.T_bool.ToType(), []bool{false}, &nulls.Nulls{})
		message := newVectorByType(
			mp,
			types.T_varchar.ToType(),
			[]string{"Check constraint 't_chk_1' is violated"},
			&nulls.Nulls{},
		)
		defer flag.Free(mp)
		defer message.Free(mp)

		fn := resolve()
		result, err := RunFunctionDirectly(
			proc,
			fn.GetEncodedOverloadID(),
			[]*vector.Vector{flag, message},
			1,
		)
		if result != nil {
			result.Free(mp)
		}
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation), err)
		require.Contains(t, err.Error(), "t_chk_1")
	})
}

func TestCheckConstraintAssertIgnoreAddsBoundedWarningsAndFiltersRows(t *testing.T) {
	session := &numericWarningSession{}
	proc := testutil.NewProcess(t)
	proc.Session = session
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
	mp := proc.Mp()

	flag := newVectorByType(mp, types.T_bool.ToType(), []bool{true, false, false}, &nulls.Nulls{})
	message := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{"Check constraint 'ck_pos' is violated."},
		&nulls.Nulls{},
	)
	defer flag.Free(mp)
	defer message.Free(mp)

	fn, err := GetFunctionByName(
		proc.Ctx,
		"_check_constraint_assert",
		[]types.Type{types.T_bool.ToType(), types.T_varchar.ToType()},
	)
	require.NoError(t, err)
	result, err := RunFunctionDirectly(
		proc,
		fn.GetEncodedOverloadID(),
		[]*vector.Vector{flag, message},
		3,
	)
	require.NoError(t, err)
	defer result.Free(mp)

	require.Equal(t, []bool{true, false, false}, vector.MustFixedColWithTypeCheck[bool](result))
	require.Len(t, session.warnings, 2)
	for _, warning := range session.warnings {
		require.Equal(t, moerr.ER_CHECK_CONSTRAINT_VIOLATED, warning.code)
		require.Equal(t, "Check constraint 'ck_pos' is violated.", warning.msg)
	}
}

func TestCheckConstraintAssertIgnoreKeepsConstantRowsAndOwnsMessage(t *testing.T) {
	session := &numericWarningSession{}
	proc := testutil.NewProcess(t)
	proc.Session = session
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
	mp := proc.Mp()

	flag, err := vector.NewConstFixed(types.T_bool.ToType(), false, 3, mp)
	require.NoError(t, err)
	message, err := vector.NewConstBytes(
		types.T_varchar.ToType(),
		[]byte("Check constraint 'ck_const' is violated."),
		3,
		mp,
	)
	require.NoError(t, err)
	defer flag.Free(mp)
	defer message.Free(mp)

	fn, err := GetFunctionByName(
		proc.Ctx,
		"_check_constraint_assert",
		[]types.Type{types.T_bool.ToType(), types.T_varchar.ToType()},
	)
	require.NoError(t, err)
	// A constant result is still evaluated once per input row because the
	// assertion owns a row-level warning side effect.
	overload, ok := GetFunctionByIdWithoutError(fn.GetEncodedOverloadID())
	require.True(t, ok)
	require.True(t, overload.CannotFold())
	result, err := RunFunctionDirectly(
		proc,
		fn.GetEncodedOverloadID(),
		[]*vector.Vector{flag, message},
		3,
	)
	require.NoError(t, err)
	defer result.Free(mp)

	// Mutating the input backing bytes after evaluation must not alter retained
	// diagnostics; warning messages are owned strings, not vector views.
	message.GetBytesAt(0)[0] = 'X'
	require.Equal(t, []bool{false, false, false}, vector.MustFixedColWithTypeCheck[bool](result))
	require.Len(t, session.warnings, 3)
	for _, warning := range session.warnings {
		require.Equal(t, moerr.ER_CHECK_CONSTRAINT_VIOLATED, warning.code)
		require.Equal(t, "Check constraint 'ck_const' is violated.", warning.msg)
	}
}
