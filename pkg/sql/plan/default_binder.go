// Copyright 2022 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewDefaultBinder(sysCtx context.Context, builder *QueryBuilder, ctx *BindContext, typ Type, cols []string) *DefaultBinder {
	b := &DefaultBinder{typ: typ, cols: cols}
	b.sysCtx = sysCtx
	b.builder = builder
	b.ctx = ctx
	b.impl = b

	return b
}

func (b *DefaultBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	if value, ok := astExpr.(*tree.StoredEnumValue); ok {
		return b.bindStoredEnumValue(value)
	}
	return b.baseBindExpr(astExpr, depth, isRoot)
}

// A VALUES column reuses one DefaultBinder. Validate a source domain once,
// without retaining row values or introducing a cache outside this binder.
type storedEnumDomain struct {
	values   string
	encoding uint32
	count    int
}

func (b *DefaultBinder) bindStoredEnumValue(value *tree.StoredEnumValue) (*plan.Expr, error) {
	if b.typ.Id != int32(types.T_enum) {
		return nil, moerr.NewInvalidInput(b.GetContext(), "stored ENUM value requires an ENUM destination")
	}
	domain := b.storedEnumDomain
	if domain == nil || domain.values != value.EnumValues || domain.encoding != value.EnumEncoding {
		members, err := types.DecodeEnumMembers(value.EnumValues, value.EnumEncoding)
		if err != nil {
			return nil, err
		}
		equal, err := types.EqualEnumMembers(value.EnumValues, value.EnumEncoding, b.typ.Enumvalues, b.typ.EnumEncoding)
		if err != nil {
			return nil, err
		}
		if !equal {
			return nil, moerr.NewInvalidInput(b.GetContext(), "stored ENUM source and destination domains differ")
		}
		domain = &storedEnumDomain{values: value.EnumValues, encoding: value.EnumEncoding, count: len(members)}
		b.storedEnumDomain = domain
	}
	if int(value.Ordinal) > domain.count {
		return nil, moerr.NewInvalidInputf(b.GetContext(), "stored ENUM ordinal %d exceeds member count %d", value.Ordinal, domain.count)
	}
	return &plan.Expr{
		Typ: b.typ,
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_EnumVal{EnumVal: uint32(value.Ordinal)},
		}},
	}, nil
}

func (b *DefaultBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	if b.cols != nil {
		return b.bindColRef(astExpr, depth, isRoot)
	}
	return b.baseBindColRef(astExpr, depth, isRoot)
}

func (b *DefaultBinder) bindColRef(astExpr *tree.UnresolvedName, _ int32, _ bool) (expr *plan.Expr, err error) {
	col := astExpr.ColName()
	idx := -1
	for i, c := range b.cols {
		if c == col {
			idx = i
			break
		}
	}
	if idx == -1 {
		err = moerr.NewInvalidInputf(b.GetContext(), "column '%s' does not exist", astExpr.ColNameOrigin())
		return
	}
	expr = &plan.Expr{
		Typ: b.typ,
	}
	expr.Expr = &plan.Expr_Col{
		Col: &plan.ColRef{
			RelPos: 0,
			ColPos: int32(idx),
		},
	}
	return
}

func (b *DefaultBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind agregate functions '%s'", funcName)
}

func (b *DefaultBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind window functions '%s'", funcName)
}

func (b *DefaultBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewNYI(b.GetContext(), "subquery in JOIN condition")
}

func (b *DefaultBinder) BindTimeWindowFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind time window functions '%s'", funcName)
}
