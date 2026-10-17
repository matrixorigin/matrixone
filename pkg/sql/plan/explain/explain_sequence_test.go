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

package explain

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// Issue #30366: the sequence functions register with the
// UNKNOW_KIND_FUNCTION layout, and the explain formatter used to reject
// that layout outright. DISTINCT and GROUP BY expose them as aggregate
// group keys, so EXPLAIN failed for valid queries while the queries
// themselves executed. They must format as ordinary function calls.
func TestExplainSequenceFunctionAsGroupKey(t *testing.T) {
	varchar := types.T_varchar.ToType()
	strType := plan.Type{Id: int32(types.T_varchar)}

	seqCol := func(name string) *plan.Expr {
		return &plan.Expr{Typ: strType, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: name}}}
	}

	cases := []struct {
		name  string
		args  []types.Type
		exprs []*plan.Expr
		want  string
	}{
		{name: "nextval", args: []types.Type{varchar}, exprs: []*plan.Expr{seqCol("seq_name")}, want: "nextval("},
		{name: "currval", args: []types.Type{varchar}, exprs: []*plan.Expr{seqCol("seq_name")}, want: "currval("},
		{name: "setval", args: []types.Type{varchar, varchar}, exprs: []*plan.Expr{seqCol("seq_name"), seqCol("value")}, want: "setval("},
		{name: "lastval", args: []types.Type{}, exprs: nil, want: "lastval"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			registered, err := function.GetFunctionByName(context.Background(), tc.name, tc.args)
			require.NoError(t, err)

			expr := &plan.Expr{
				Typ: strType,
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{Obj: registered.GetEncodedOverloadID(), ObjName: tc.name},
					Args: tc.exprs,
				}},
			}

			var buf bytes.Buffer
			require.NoError(t, describeExpr(context.Background(), expr, &ExplainOptions{}, &buf))
			require.Contains(t, buf.String(), tc.want)
		})
	}
}
