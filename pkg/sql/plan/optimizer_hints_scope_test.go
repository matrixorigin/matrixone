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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/stretchr/testify/require"
)

func TestOptimizerHintsUseSessionInsteadOfCNRuntime(t *testing.T) {
	rt := runtime.ServiceRuntime("")
	old, present := rt.GetGlobalVariables("optimizer_hints")
	t.Cleanup(func() {
		if present {
			rt.SetGlobalVariables("optimizer_hints", old)
		} else {
			rt.CompareAndDeleteGlobalVariables("optimizer_hints", "applyIndices=1")
		}
	})
	rt.SetGlobalVariables("optimizer_hints", "applyIndices=1")

	for _, tc := range []struct {
		name, hint string
		want       int
	}{
		{"session without hint", "", 0},
		{"session with hint", "applyIndices=1", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			ctx.ResolveVariableFunc = func(name string, system, global bool) (interface{}, error) {
				require.Equal(t, "optimizer_hints", name)
				require.True(t, system)
				require.False(t, global)
				return tc.hint, nil
			}
			builder := &QueryBuilder{compCtx: ctx}
			builder.parseOptimizeHints()
			if tc.want == 0 {
				require.Nil(t, builder.optimizerHints)
			} else {
				require.NotNil(t, builder.optimizerHints)
				require.Equal(t, tc.want, builder.optimizerHints.applyIndices)
			}
		})
	}
}
