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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Explicit CAST(text AS SIGNED/UNSIGNED) describes the truncated conversion
// as INTEGER, matching MySQL (issue #31443).
func TestExplicitCastTextToIntegerWarnsInteger(t *testing.T) {
	for _, target := range []types.T{types.T_int64, types.T_uint64} {
		proc := testutil.NewProcess(t)
		session := &numericWarningSession{}
		proc.Session = session

		var expect FunctionTestResult
		if target == types.T_int64 {
			expect = NewFunctionTestResult(target.ToType(), false, []int64{42}, nil)
		} else {
			expect = NewFunctionTestResult(target.ToType(), false, []uint64{42}, nil)
		}
		var targetInput FunctionTestInput
		if target == types.T_int64 {
			targetInput = NewFunctionTestInput(target.ToType(), []int64{}, nil)
		} else {
			targetInput = NewFunctionTestInput(target.ToType(), []uint64{}, nil)
		}
		testCase := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"42tail"}, nil),
				targetInput,
			},
			expect, NewExplicitCast)
		succeed, info := testCase.RunAndFree()
		require.True(t, succeed, info)

		require.NotEmpty(t, session.warnings, target.String())
		for _, w := range session.warnings {
			require.Equal(t, moerr.ER_TRUNCATED_WRONG_VALUE, w.code)
			require.Equal(t, "Truncated incorrect INTEGER value: '42tail'", w.msg, target.String())
		}
	}
}
