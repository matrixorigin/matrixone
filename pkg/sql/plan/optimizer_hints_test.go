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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// builderWithHintCtx builds a QueryBuilder whose compiler context carries the given per-statement
// optimizer_hints string on defines.OptimizerHints{} (the value the internal SQL executor bridges
// onto the execution context). An empty string carries no value at all.
func builderWithHintCtx(hints string) *QueryBuilder {
	mock := NewMockCompilerContext(true)
	ctx := context.Background()
	if hints != "" {
		ctx = context.WithValue(ctx, defines.OptimizerHints{}, hints)
	}
	mock.SetContext(ctx)
	return NewQueryBuilder(plan.Query_SELECT, mock, false, true)
}

// The per-statement optimizer_hints path (StatementOption.WithOptimizerHints, applied by
// parseOptimizeHints) is what the fulltext2 json probe relies on to pass applyIndices=1 to its
// fallback/tail SQL without touching the process-wide global variable.
func TestParseOptimizeHintsPerStatement(t *testing.T) {
	t.Run("applyIndices=1 sets the gate", func(t *testing.T) {
		b := builderWithHintCtx("applyIndices=1")
		b.parseOptimizeHints()
		require.NotNil(t, b.optimizerHints)
		require.Equal(t, 1, b.optimizerHints.applyIndices)
	})

	t.Run("absent context value leaves hints unset", func(t *testing.T) {
		b := builderWithHintCtx("")
		b.parseOptimizeHints()
		require.Nil(t, b.optimizerHints)
	})

	t.Run("multiple comma-separated hints all apply", func(t *testing.T) {
		b := builderWithHintCtx("applyIndices=1,aggPushDown=1")
		b.parseOptimizeHints()
		require.NotNil(t, b.optimizerHints)
		require.Equal(t, 1, b.optimizerHints.applyIndices)
		require.Equal(t, 1, b.optimizerHints.aggPushDown)
	})

	t.Run("non-integer value is ignored, not applied", func(t *testing.T) {
		b := builderWithHintCtx("applyIndices=on")
		b.parseOptimizeHints()
		require.Nil(t, b.optimizerHints)
	})

	t.Run("bare key (no =value) is ignored", func(t *testing.T) {
		b := builderWithHintCtx("applyIndices")
		b.parseOptimizeHints()
		require.Nil(t, b.optimizerHints)
	})

	t.Run("unknown key is ignored but does not corrupt others", func(t *testing.T) {
		b := builderWithHintCtx("noSuchHint=1,applyIndices=1")
		b.parseOptimizeHints()
		require.NotNil(t, b.optimizerHints)
		require.Equal(t, 1, b.optimizerHints.applyIndices)
	})

	t.Run("whitespace after comma is NOT trimmed, so the entry does not match", func(t *testing.T) {
		// Documented quirk: " applyIndices" (leading space) matches no hint. The gate must stay off.
		b := builderWithHintCtx("aggPushDown=1, applyIndices=1")
		b.parseOptimizeHints()
		require.NotNil(t, b.optimizerHints)
		require.Equal(t, 1, b.optimizerHints.aggPushDown)
		require.Equal(t, 0, b.optimizerHints.applyIndices)
	})

	t.Run("non-string context value is ignored", func(t *testing.T) {
		mock := NewMockCompilerContext(true)
		mock.SetContext(context.WithValue(context.Background(), defines.OptimizerHints{}, 42))
		b := NewQueryBuilder(plan.Query_SELECT, mock, false, true)
		b.parseOptimizeHints()
		require.Nil(t, b.optimizerHints)
	})
}

// The global optimizer_hints variable and the per-statement context hints compose: the global is
// applied first, the per-statement second, so a statement can add to (or override) the global.
func TestParseOptimizeHintsGlobalAndPerStatement(t *testing.T) {
	b := builderWithHintCtx("applyIndices=1")
	svc := b.compCtx.GetProcess().GetService()
	runtime.ServiceRuntime(svc).SetGlobalVariables("optimizer_hints", "aggPushDown=1")
	defer runtime.ServiceRuntime(svc).SetGlobalVariables("optimizer_hints", "")

	b.parseOptimizeHints()
	require.NotNil(t, b.optimizerHints)
	require.Equal(t, 1, b.optimizerHints.aggPushDown, "global hint applied")
	require.Equal(t, 1, b.optimizerHints.applyIndices, "per-statement hint applied on top")
}
