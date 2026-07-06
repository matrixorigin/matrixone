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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestOperatorInNullableListUsesThreeValuedLogic(t *testing.T) {
	proc := testutil.NewProcess(t)

	tc := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"keep", "key", ""}, []bool{false, false, true}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"keep", ""}, []bool{false, true}),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true, false, false}, []bool{false, true, true}),
		newOpOperatorStrIn().operatorIn,
	)

	ok, errInfo := tc.Run()
	require.True(t, ok, errInfo)
}

func TestOperatorNotInNullableListUsesThreeValuedLogic(t *testing.T) {
	proc := testutil.NewProcess(t)

	tc := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"keep", "key", ""}, []bool{false, false, true}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"keep", ""}, []bool{false, true}),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false, false, false}, []bool{false, true, true}),
		newOpOperatorStrIn().operatorNotIn,
	)

	ok, errInfo := tc.Run()
	require.True(t, ok, errInfo)
}
