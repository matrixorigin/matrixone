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
	"testing"

	"github.com/stretchr/testify/require"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func TestPreparedParamCommonTypeDependencies(t *testing.T) {
	param := func(pos int32, dependent bool) *Expr {
		expr := &Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
		if dependent {
			expr.Typ.Enumvalues = "mo_decimal_common_type_dependency"
		}
		return expr
	}
	call := func(functionID int32, args ...*Expr) *Expr {
		return &Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(functionID, 0)},
			Args: args,
		}}}
	}
	cast := func(arg *Expr, internal bool) *Expr {
		charset := uint32(0)
		if internal {
			charset = 255
		}
		target := &Expr{Typ: planpb.Type{Charset: charset}, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}}
		return call(function.CAST, arg, target)
	}
	query := &Query{
		Steps: []int32{0},
		Nodes: []*Node{{
			NodeType: planpb.Node_PROJECT,
			ProjectList: []*Expr{
				call(function.COALESCE, cast(param(0, true), true), cast(param(1, false), false), call(function.PLUS, param(2, false), param(3, false))),
				call(function.SUM, param(4, false)),
				call(function.GREATEST, param(5, true), param(6, true)),
			},
		}},
	}
	dependencies := PreparedParamCommonTypeDependencies(
		&Plan{Plan: &planpb.Plan_Query{Query: query}}, 7)
	require.Equal(t, []bool{true, false, false, false, false, true, true}, dependencies)
}
