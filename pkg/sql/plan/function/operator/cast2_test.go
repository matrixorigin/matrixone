// Copyright 2022 Matrix Origin
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

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewCastDemo(t *testing.T) {
	proc := testutil.NewProcess()
	// test bool to string
	inputs := []testutil.FunctionTestInput{
		// parameter1
		testutil.NewFunctionTestInput(
			types.T_bool.ToType(), []bool{true, true, false}, []bool{false, true, false}),
		// parameter2
		testutil.NewFunctionTestInput(
			types.T_varchar.ToType(), []string{}, []bool{}),
	}
	expected := testutil.NewFunctionTestResult(
		types.T_varchar.ToType(), false,
		[]string{"1", "", "0"}, []bool{false, true, false})
	tc := testutil.NewFunctionTestCase(proc, inputs, expected, NewCast)
	succeed, info := tc.Run()
	require.True(t, succeed, info)
}
