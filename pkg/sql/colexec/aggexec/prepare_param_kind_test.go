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

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPrepareParamKindStatesLifecycle(t *testing.T) {
	oneArg := []*plan.Expr{{}}
	threeArgs := []*plan.Expr{{}, {}, {}}

	for _, tc := range []struct {
		name string
		id   int64
		args []*plan.Expr
		want bool
	}{
		{name: "min", id: AggIdOfMin, args: oneArg, want: true},
		{name: "max-by", id: AggIdOfMaxBy, args: threeArgs, want: true},
		{name: "first-value", id: WinIdOfFirstValue, args: oneArg, want: true},
		{name: "lag-without-default", id: WinIdOfLag, args: oneArg, want: true},
		{name: "lag-with-default", id: WinIdOfLag, args: threeArgs, want: false},
		{name: "sum", id: AggIdOfSum, args: oneArg, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := MakeAggFunctionExpression(tc.id, false, tc.args, nil)
			require.Equal(t, tc.want, expr.PreservesFirstArgPrepareParamKind())
		})
	}

	preserving := MakeAggFunctionExpression(AggIdOfMin, false, oneArg, nil)
	converting := MakeAggFunctionExpression(AggIdOfSum, false, oneArg, nil)
	var states PrepareParamKindStates
	require.Equal(t, vector.PrepareParamNone, states.Get(0))
	kind, seen := states.GetState(0)
	require.Equal(t, vector.PrepareParamNone, kind)
	require.False(t, seen)

	states.Reset([]AggFuncExecExpression{preserving, converting})

	states.Observe(0, vector.PrepareParamFloat)
	states.Observe(0, vector.PrepareParamFloat)
	require.Equal(t, vector.PrepareParamFloat, states.Get(0))

	states.Observe(0, vector.PrepareParamDecimal)
	require.Equal(t, vector.PrepareParamNone, states.Get(0))

	states.Observe(1, vector.PrepareParamInteger)
	kind, seen = states.GetState(1)
	require.Equal(t, vector.PrepareParamNone, kind)
	require.False(t, seen)

	states.Reset([]AggFuncExecExpression{preserving})
	require.Equal(t, vector.PrepareParamNone, states.Get(0))
	kind, seen = states.GetState(0)
	require.Equal(t, vector.PrepareParamNone, kind)
	require.False(t, seen)

	states.ObserveState(0, vector.PrepareParamFloat, false)
	kind, seen = states.GetState(0)
	require.Equal(t, vector.PrepareParamNone, kind)
	require.False(t, seen)

	states.ObserveState(0, vector.PrepareParamNone, true)
	kind, seen = states.GetState(0)
	require.Equal(t, vector.PrepareParamNone, kind)
	require.True(t, seen)

	states.Reset([]AggFuncExecExpression{preserving})
	states.Observe(0, vector.PrepareParamInteger)
	require.Equal(t, vector.PrepareParamInteger, states.Get(0))
}

func TestPrepareParamKindStatesReuseDoesNotAllocate(t *testing.T) {
	aggs := []AggFuncExecExpression{
		MakeAggFunctionExpression(AggIdOfMin, false, []*plan.Expr{{}}, nil),
		MakeAggFunctionExpression(AggIdOfSum, false, []*plan.Expr{{}}, nil),
	}
	var states PrepareParamKindStates
	states.Reset(aggs)

	allocs := testing.AllocsPerRun(100, func() {
		states.Reset(aggs)
		states.Observe(0, vector.PrepareParamInteger)
		states.Observe(1, vector.PrepareParamFloat)
	})
	require.Zero(t, allocs)
}
