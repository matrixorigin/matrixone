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

package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
)

// TestIndexRewritesDisabled: the view guard infers "no index matched" from the absence of a
// rewrite, which is only sound while rewrites can run. The global optimizer_hints knob can
// switch applyIndices off wholesale, and every fulltext view would then be refused --
// including ones with a valid index -- cluster-wide until someone spotted the hint. Parsing
// must therefore recognise the switch in exactly the forms parseOptimizeHints accepts.
func TestIndexRewritesDisabled(t *testing.T) {
	rt := runtime.ServiceRuntime("")
	require.NotNil(t, rt, "test runtime must exist")

	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))

	for _, tc := range []struct {
		name string
		hint string
		want bool
	}{
		{"unset", "", false},
		{"applyIndices on", "applyIndices=1", true},
		{"applyIndices explicitly zero", "applyIndices=0", false},
		{"last value enables rewrites", "applyIndices=1,applyIndices=0", false},
		{"last value disables rewrites", "applyIndices=0,applyIndices=1", true},
		{"among other hints", "aggPushDown=1,applyIndices=1,joinOrdering=0", true},
		{"other hints only", "aggPushDown=1,joinOrdering=1", false},
		// Whitespace is NOT trimmed, because handleOptimizerHints does not trim either: the
		// key here is " applyIndices ", which matches no case, so the optimizer ignores the
		// hint and the rewrites really do run. An earlier version trimmed and answered true,
		// which switched the #27027 view guard off while the rewrites it reasons about were
		// still running -- the two must agree, so both use splitOptimizerHint.
		{"spaces around the pair", " applyIndices = 2 ", false},
		{"space after a comma", "aggPushDown=1, applyIndices=1", false},
		{"not a number", "applyIndices=yes", false},
		{"malformed", "applyIndices", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setPlanTestGlobalVariable(t, "", "optimizer_hints", tc.hint)

			require.Equal(t, tc.want, indexRewritesDisabled(ctx))

			// Parity with the optimizer itself: whatever the hint string, the guard's answer
			// must equal what handleOptimizerHints actually did to applyIndices.
			builder := &QueryBuilder{}
			for _, kv := range strings.Split(tc.hint, ",") {
				handleOptimizerHints(kv, builder)
			}
			applied := builder.optimizerHints != nil && builder.optimizerHints.applyIndices != 0
			require.Equal(t, applied, indexRewritesDisabled(ctx),
				"the view guard and handleOptimizerHints must agree about %q", tc.hint)
		})
	}

	require.False(t, indexRewritesDisabled(nil), "a nil context must not panic")
}

func TestIndexRewritesDisabledStatementOverride(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	setPlanTestGlobalVariable(t, ctx.GetProcess().GetService(), "optimizer_hints", "applyIndices=1")
	ctx.SetContext(context.WithValue(context.Background(), defines.OptimizerHints{}, "applyIndices=0"))
	require.False(t, indexRewritesDisabled(ctx))
}
