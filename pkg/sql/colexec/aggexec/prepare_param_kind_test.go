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

func TestAggPrepareParamKindLifecycle(t *testing.T) {
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

	expr := MakeAggFunctionExpression(AggIdOfMin, false, oneArg, nil)
	expr.ObservePrepareParamKind(vector.PrepareParamFloat)
	expr.ObservePrepareParamKind(vector.PrepareParamFloat)
	require.Equal(t, vector.PrepareParamFloat, expr.GetPrepareParamKind())

	expr.ObservePrepareParamKind(vector.PrepareParamDecimal)
	require.Equal(t, vector.PrepareParamNone, expr.GetPrepareParamKind())

	expr.ResetPrepareParamKind()
	require.Equal(t, vector.PrepareParamNone, expr.GetPrepareParamKind())
	expr.ObservePrepareParamKind(vector.PrepareParamInteger)
	require.Equal(t, vector.PrepareParamInteger, expr.GetPrepareParamKind())
}
