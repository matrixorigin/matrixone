// Copyright 2021 - 2026 Matrix Origin
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func regexpTestExpr(oid types.T, value string) *Expr {
	return &Expr{
		Typ: planpb.Type{Id: int32(oid)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Sval{Sval: value},
		}},
	}
}

func TestBindRegexpRejectsStaticBinaryOperands(t *testing.T) {
	text := regexpTestExpr(types.T_varchar, "abc")
	binary := regexpTestExpr(types.T_binary, "abc")

	for _, tc := range []struct {
		name string
		args []*Expr
	}{
		{name: "reg_match", args: []*Expr{binary, text}},
		{name: "not_reg_match", args: []*Expr{text, binary}},
		{name: "regexp_like", args: []*Expr{binary, text}},
		{name: "regexp_instr", args: []*Expr{text, binary}},
		{name: "regexp_substr", args: []*Expr{binary, text}},
		{name: "regexp_replace", args: []*Expr{text, text, binary}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BindFuncExprImplByPlanExpr(context.Background(), tc.name, tc.args)
			require.Error(t, err)
			var moErr *moerr.Error
			require.ErrorAs(t, err, &moErr)
			require.Equal(t, uint16(moerr.ER_CHARACTER_SET_MISMATCH), moErr.MySQLCode())
			require.Equal(t, "HY000", moErr.SqlState())
		})
	}
}

func TestBindRegexpAllowsParamsAndBinaryNull(t *testing.T) {
	text := regexpTestExpr(types.T_varchar, "a")
	param := &Expr{
		Typ:  planpb.Type{Id: int32(types.T_blob)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	binaryNull := &Expr{
		Typ: planpb.Type{Id: int32(types.T_binary)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Isnull: true,
		}},
	}

	_, err := BindFuncExprImplByPlanExpr(context.Background(), "regexp_like", []*Expr{param, text})
	require.NoError(t, err)
	_, err = BindFuncExprImplByPlanExpr(context.Background(), "regexp_like", []*Expr{binaryNull, text})
	require.NoError(t, err)
}
