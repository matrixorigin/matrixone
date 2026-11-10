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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// TestMakeWindowFrameConstValueRejectsMoCtrl pins #28985: a window frame bound is EVALUATED during
// binding -- before the frontend's mo_ctl sys-admin gate scans the built plan, and the evaluated
// bound is then replaced by a constant so the scan can never see the call. makeWindowFrameConstValue
// must therefore refuse a control function (mo_ctl / fault_inject) in a frame bound BEFORE it is
// evaluated, so the cluster-wide side effect never fires unauthenticated.
func TestMakeWindowFrameConstValueRejectsMoCtrl(t *testing.T) {
	ctrlExpr := func(name string) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: name},
			Args: []*planpb.Expr{
				{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "cn"}}}},
			},
		}}}
	}

	for _, name := range []string{"mo_ctl", "fault_inject"} {
		// baseBindExpr yields an expr that already carries the control call; the guard runs before
		// any evaluation, so a nil proc is never reached for the rejected path.
		bind := func(tree.Expr, int32, bool) (*Expr, error) { return ctrlExpr(name), nil }
		_, err := makeWindowFrameConstValue(bind, nil, context.Background(), nil, nil)
		require.Error(t, err, name)
		require.Contains(t, err.Error(), "window frame bound", name)
	}

	// A control call nested inside an otherwise ordinary bound expression is caught too.
	nested := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "+"},
		Args: []*planpb.Expr{
			{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 1}}}},
			ctrlExpr("mo_ctl"),
		},
	}}}
	bind := func(tree.Expr, int32, bool) (*Expr, error) { return nested, nil }
	_, err := makeWindowFrameConstValue(bind, nil, context.Background(), nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "window frame bound")
}
