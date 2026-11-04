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

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestRegexpExecutorResetUsesFreshState(t *testing.T) {
	for _, tc := range []struct {
		name, function, firstPattern, nextPattern, firstValue, nextValue string
	}{
		{"like", "like", "a%", "z%", "abc", "zoo"},
		{"re2", "regexp_like", "^ab$", "^ac$", "ab", "ac"},
		{"icu", "regexp_like", "a(?=b)", "a(?=c)", "ab", "ac"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			checkExpressionStorageAfterCleanup(t, proc)
			column := func(pos int32) *plan.Expr {
				return &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
			}
			expr := bindTestFunction(t, proc, tc.function, column(0), column(1))
			executor, err := NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			for generation, input := range []struct {
				value, pattern string
				want           bool
			}{
				{tc.firstValue, tc.firstPattern, true},
				{tc.firstValue, tc.nextPattern, false},
				{tc.nextValue, tc.nextPattern, true},
			} {
				if generation > 0 {
					executor.ResetForNextQuery()
				}
				// 输入与 fresh 控制的资源在每轮结束即释放，不依赖最后一次断言成功。
				func() {
					bat := batch.NewWithSize(2)
					defer bat.Clean(proc.Mp())
					for i, value := range []string{input.value, input.pattern} {
						bat.Vecs[i] = vector.NewVec(types.T_varchar.ToType())
						require.NoError(t, vector.AppendBytes(bat.Vecs[i], []byte(value), false, proc.Mp()))
					}
					bat.SetRowCount(1)
					result, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
					require.NoError(t, err)
					require.Equal(t, 1, result.Length())
					require.False(t, result.IsNull(0))
					require.Equal(t, input.want, vector.GetFixedAtNoTypeCheck[bool](result, 0))
					fresh, err := NewExpressionExecutor(proc, expr)
					require.NoError(t, err)
					defer fresh.Free()
					control, err := fresh.Eval(proc, []*batch.Batch{bat}, nil)
					require.NoError(t, err)
					require.False(t, control.IsNull(0))
					require.Equal(t, input.want, vector.GetFixedAtNoTypeCheck[bool](control, 0))
				}()
			}
		})
	}
}
