// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestLoadFKIgnoreCheck(t *testing.T) {
	for _, limit := range []int{0, 1, 3} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			sink := &arithmeticWarningSink{limit: limit}
			proc.WarningSink = sink
			fn, err := GetFunctionByName(proc.Ctx, "_load_fk_ignore_check", []types.Type{types.T_bool.ToType()})
			require.NoError(t, err)
			overload, ok := GetFunctionByIdWithoutError(fn.GetEncodedOverloadID())
			require.True(t, ok)
			require.True(t, overload.CannotFold())
			ns := nulls.NewWithSize(3)
			ns.Add(2)
			flags := newVectorByType(proc.Mp(), types.T_bool.ToType(), []bool{true, false, false}, ns)
			defer flags.Free(proc.Mp())
			result, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{flags}, 3)
			require.NoError(t, err)
			defer result.Free(proc.Mp())
			require.Equal(t, []bool{true, false, false}, vector.MustFixedColWithTypeCheck[bool](result))
			require.Equal(t, uint64(2), sink.total)
			require.Len(t, sink.records, min(limit, 2))
			for _, record := range sink.records {
				require.Equal(t, moerr.ER_NO_REFERENCED_ROW_2, record.code)
			}
			// A second call must not flush the previous invocation's warnings again.
			good, err := vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp())
			require.NoError(t, err)
			defer good.Free(proc.Mp())
			second, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{good}, 1)
			require.NoError(t, err)
			defer second.Free(proc.Mp())
			require.Equal(t, uint64(2), sink.total)
		})
	}
}

func TestLoadFKIgnoreCheckSelectedConstantRows(t *testing.T) {
	for _, tc := range []struct {
		name      string
		length    int
		selection *FunctionSelectList
		count     uint64
	}{
		{"constant", 3, nil, 3},
		{"selected", 3, &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}, 2},
		{"none", 3, &FunctionSelectList{AnyNull: true, AllNull: true}, 0},
		{"empty", 0, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			sink := &arithmeticWarningSink{limit: 3}
			proc.WarningSink = sink
			flags, err := vector.NewConstFixed(types.T_bool.ToType(), false, tc.length, proc.Mp())
			require.NoError(t, err)
			defer flags.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(tc.length))
			require.NoError(t, loadFKIgnoreCheck([]*vector.Vector{flags}, result, proc, tc.length, tc.selection))
			require.Equal(t, tc.count, sink.total)
			require.Equal(t, tc.length, result.GetResultVector().Length())
		})
	}
}
