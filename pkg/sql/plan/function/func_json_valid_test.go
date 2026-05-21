// Copyright 2024 Matrix Origin
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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func initJsonValidTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "json_valid with varchar input",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1}`, `[1,2,3]`, `"hello"`, `hello`, `true`, `null`, `42`, ``},
					[]bool{false, false, false, false, false, false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, true, false, true, true, true, false},
				[]bool{false, false, false, false, false, false, false, true}),
		},
	}
}

func TestJsonValid(t *testing.T) {
	testCases := initJsonValidTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, JsonValid)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
