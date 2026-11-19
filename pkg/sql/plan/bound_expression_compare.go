// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

type boundExpressionProjection uint8

const (
	boundExpressionNormalized boundExpressionProjection = iota
	boundExpressionHistorical
	boundExpressionLegacyOrigin
)

// sameProjectedBoundExpression compares read-only views instead of cloning whole
// speculative trees. Protobuf equality owns every field except the three edges
// removed from the views below; those edges retain their original order and
// projection rules. In particular, other expression edges are never normalized.
func sameProjectedBoundExpression(a, b *planpb.Expr, projection boundExpressionProjection) bool {
	if a == nil || b == nil {
		return a == b
	}
	// Most literal leaves are already canonical. Keep protobuf's original
	// allocation-free comparison when neither policy would change this node.
	if !boundExpressionHasProjectedChildren(a) && !boundExpressionHasProjectedChildren(b) &&
		!boundExpressionTypeNeedsNormalization(a.Typ) && !boundExpressionTypeNeedsNormalization(b.Typ) &&
		!boundExpressionNeedsHistoricalProjection(a, b, projection) {
		return proto.Equal(a, b)
	}
	left, right := *a, *b
	if projection != boundExpressionNormalized {
		omitHistoricalFloatCoercibility(&left, &right)
	}
	normalizeBoundExpressionType(&left.Typ)
	normalizeBoundExpressionType(&right.Typ)
	var leftChildren, rightChildren []*planpb.Expr
	childProjection := projection
	switch x := a.Expr.(type) {
	case *planpb.Expr_F:
		y, ok := b.Expr.(*planpb.Expr_F)
		if ok && x != nil && y != nil && x.F != nil && y.F != nil {
			lf, rf := *x.F, *y.F
			leftChildren, rightChildren = lf.Args, rf.Args
			lf.Args, rf.Args = nil, nil
			if projection == boundExpressionLegacyOrigin {
				// The old marker recovery did not descend through a missing Func.
				if rf.Func == nil {
					childProjection = boundExpressionNormalized
				} else {
					id, overload := function.DecodeOverloadID(rf.Func.Obj)
					if id == function.CAST && overload == 0 && !lf.SyntaxExplicitCast {
						rf.SyntaxExplicitCast = false
					}
				}
			}
			left.Expr, right.Expr = &planpb.Expr_F{F: &lf}, &planpb.Expr_F{F: &rf}
		}
	case *planpb.Expr_List:
		y, ok := b.Expr.(*planpb.Expr_List)
		if ok && x != nil && y != nil && x.List != nil && y.List != nil {
			ll, rl := *x.List, *y.List
			leftChildren, rightChildren = ll.List, rl.List
			ll.List, rl.List = nil, nil
			left.Expr, right.Expr = &planpb.Expr_List{List: &ll}, &planpb.Expr_List{List: &rl}
		}
	case *planpb.Expr_Lit:
		y, ok := b.Expr.(*planpb.Expr_Lit)
		if ok && x != nil && y != nil && x.Lit != nil && y.Lit != nil && (x.Lit.Src != nil || y.Lit.Src != nil) {
			ll, rl := *x.Lit, *y.Lit
			// Single-element local slices preserve nil source presence without writing
			// either input's payload or any shared argument/list backing array.
			leftChildren, rightChildren = []*planpb.Expr{ll.Src}, []*planpb.Expr{rl.Src}
			ll.Src, rl.Src = nil, nil
			left.Expr, right.Expr = &planpb.Expr_Lit{Lit: &ll}, &planpb.Expr_Lit{Lit: &rl}
		}
	}
	if len(leftChildren) != len(rightChildren) || !proto.Equal(&left, &right) {
		return false
	}
	for i := range leftChildren {
		if !sameProjectedBoundExpression(leftChildren[i], rightChildren[i], childProjection) {
			return false
		}
	}
	return true
}

func normalizeBoundExpressionType(typ *planpb.Type) {
	// Table is catalog lineage; TIME Scale remains the execution precision.
	typ.Table = ""
	if typ.Id == int32(types.T_time) && typ.Width == typ.Scale {
		typ.Width = 0
	}
}

func boundExpressionHasProjectedChildren(e *planpb.Expr) bool {
	if f := e.GetF(); f != nil && len(f.Args) != 0 {
		return true
	}
	if l := e.GetList(); l != nil && len(l.List) != 0 {
		return true
	}
	return e.GetLit() != nil && e.GetLit().Src != nil
}

func boundExpressionTypeNeedsNormalization(typ planpb.Type) bool {
	return typ.Table != "" || typ.Id == int32(types.T_time) && typ.Width != 0 && typ.Width == typ.Scale
}

func boundExpressionNeedsHistoricalProjection(a, b *planpb.Expr, projection boundExpressionProjection) bool {
	if projection == boundExpressionNormalized {
		return false
	}
	candidate := *b
	omitHistoricalFloatCoercibility(a, &candidate)
	if candidate.Typ.CollationCoercibility != b.Typ.CollationCoercibility || candidate.Typ.CollationCoercibilitySet != b.Typ.CollationCoercibilitySet {
		return true
	}
	if projection == boundExpressionLegacyOrigin {
		if x, y := a.GetF(), b.GetF(); x != nil && y != nil && y.Func != nil {
			id, overload := function.DecodeOverloadID(y.Func.Obj)
			return id == function.CAST && overload == 0 && !x.SyntaxExplicitCast && y.SyntaxExplicitCast
		}
	}
	return false
}
