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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestCastToDatalinkValidatesValues(t *testing.T) {
	proc := testutil.NewProcess(t)
	datalinkType := types.T_datalink.ToType()

	for _, test := range []struct {
		name       string
		cast       fEvalFn
		values     []string
		nulls      []bool
		selectList *FunctionSelectList
		want       []string
		wantNulls  []bool
		wantErr    bool
	}{
		{
			name:    "assignment rejects unsupported scheme",
			cast:    NewAssignCast,
			values:  []string{"not-a-datalink"},
			wantErr: true,
		},
		{
			name:    "explicit cast rejects unsupported scheme",
			cast:    NewCast,
			values:  []string{"not-a-datalink"},
			wantErr: true,
		},
		{
			name:      "valid value and null are preserved",
			cast:      NewAssignCast,
			values:    []string{"file:///tmp/object.txt", ""},
			nulls:     []bool{false, true},
			want:      []string{"file:///tmp/object.txt", ""},
			wantNulls: []bool{false, true},
		},
		{
			name:       "inactive invalid value is not evaluated",
			cast:       NewAssignCast,
			values:     []string{"file:///tmp/object.txt", "not-a-datalink"},
			selectList: &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}},
			want:       []string{"file:///tmp/object.txt", ""},
			wantNulls:  []bool{false, true},
		},
		{
			name:    "active invalid value after valid row is rejected",
			cast:    NewAssignCast,
			values:  []string{"file:///tmp/object.txt", "not-a-datalink"},
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			testCase := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_text.ToType(), test.values, test.nulls),
					NewFunctionTestInput(datalinkType, []string{}, nil),
				},
				NewFunctionTestResult(datalinkType, test.wantErr, test.want, test.wantNulls),
				test.cast,
			).WithSelectList(test.selectList)

			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}
}
