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

import planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"

// Clone consumers before replacing bound output slots. Unknown expression variants
// fail closed; the optional guard adds the caller's scope and work limits.
func cloneAndRewriteBoundSlotRefs(
	exprs []*planpb.Expr,
	tag int32,
	replacements map[int32]*planpb.Expr,
	guard func(*planpb.Expr, int) bool,
) ([]*planpb.Expr, bool) {
	rewritten := make([]*planpb.Expr, len(exprs))
	for i, expr := range exprs {
		var ok bool
		rewritten[i], ok = cloneAndRewriteBoundSlotRef(expr, tag, replacements, guard)
		if !ok {
			return nil, false
		}
	}
	return rewritten, true
}

func cloneAndRewriteBoundSlotRef(
	expr *planpb.Expr,
	tag int32,
	replacements map[int32]*planpb.Expr,
	guard func(*planpb.Expr, int) bool,
) (*planpb.Expr, bool) {
	hasRef, safe := boundExprHasSlotRef(expr, tag, replacements, guard, 0)
	if !safe {
		return nil, false
	}
	if !hasRef {
		return expr, true
	}
	return rewriteBoundSlotRef(DeepCopyExpr(expr), tag, replacements)
}

func boundExprHasSlotRef(
	expr *planpb.Expr,
	tag int32,
	replacements map[int32]*planpb.Expr,
	guard func(*planpb.Expr, int) bool,
	depth int,
) (hasRef, safe bool) {
	if guard != nil && !guard(expr, depth) {
		return false, false
	}
	if expr == nil {
		return false, true
	}
	switch impl := expr.Expr.(type) {
	case *planpb.Expr_Lit:
		// These are executable leaves. Literal.Src is provenance rather than
		// an evaluated child and must not be remapped.
		return false, impl.Lit != nil
	case *planpb.Expr_P:
		return false, impl.P != nil
	case *planpb.Expr_V:
		return false, impl.V != nil
	case *planpb.Expr_Raw:
		return false, impl.Raw != nil
	case *planpb.Expr_T:
		return false, impl.T != nil
	case *planpb.Expr_Max:
		return false, impl.Max != nil
	case *planpb.Expr_Vec:
		return false, impl.Vec != nil
	case *planpb.Expr_Fold:
		return false, impl.Fold != nil
	case *planpb.Expr_Col:
		if impl.Col == nil {
			return false, false
		}
		if impl.Col.RelPos != tag {
			return false, true
		}
		_, ok := replacements[impl.Col.ColPos]
		return true, ok
	case *planpb.Expr_Corr:
		if impl.Corr == nil {
			return false, false
		}
		if impl.Corr.RelPos == tag {
			return true, false
		}
		return false, true
	case *planpb.Expr_F:
		if impl.F == nil || impl.F.Func == nil {
			return false, false
		}
		return boundExprListHasSlotRef(impl.F.Args, tag, replacements, guard, depth+1)
	case *planpb.Expr_List:
		if impl.List == nil {
			return false, false
		}
		return boundExprListHasSlotRef(impl.List.List, tag, replacements, guard, depth+1)
	case *planpb.Expr_Sub:
		if impl.Sub == nil {
			return false, false
		}
		return boundExprHasSlotRef(impl.Sub.Child, tag, replacements, guard, depth+1)
	case *planpb.Expr_W:
		if impl.W == nil {
			return false, false
		}
		hasRef, safe = boundExprHasSlotRef(impl.W.WindowFunc, tag, replacements, guard, depth+1)
		if !safe {
			return hasRef, false
		}
		for _, list := range [][]*planpb.Expr{impl.W.PartitionBy} {
			listHasRef, listSafe := boundExprListHasSlotRef(list, tag, replacements, guard, depth+1)
			hasRef = hasRef || listHasRef
			if !listSafe {
				return hasRef, false
			}
		}
		for _, orderBy := range impl.W.OrderBy {
			if orderBy == nil {
				continue
			}
			itemHasRef, itemSafe := boundExprHasSlotRef(orderBy.Expr, tag, replacements, guard, depth+1)
			hasRef = hasRef || itemHasRef
			if !itemSafe {
				return hasRef, false
			}
		}
		// DeepCopyExpr requires a complete frame for window expressions. Keep the
		// preflight traversal at least as strict as the clone it guards, otherwise
		// a malformed consumer could pass validation and panic before the atomic
		// rewrite is installed.
		if impl.W.Frame == nil || impl.W.Frame.Start == nil || impl.W.Frame.End == nil {
			return hasRef, false
		}
		for _, bound := range []*planpb.FrameBound{impl.W.Frame.Start, impl.W.Frame.End} {
			itemHasRef, itemSafe := boundExprHasSlotRef(bound.Val, tag, replacements, guard, depth+1)
			hasRef = hasRef || itemHasRef
			if !itemSafe {
				return hasRef, false
			}
		}
		return hasRef, true
	default:
		// A newly added expression variant may own executable children. Until
		// this traversal explicitly understands it, abort the whole rewrite
		// rather than leaving a stale aggregate slot hidden inside it.
		return false, false
	}
}

func boundExprListHasSlotRef(
	exprs []*planpb.Expr,
	tag int32,
	replacements map[int32]*planpb.Expr,
	guard func(*planpb.Expr, int) bool,
	depth int,
) (hasRef, safe bool) {
	for _, expr := range exprs {
		itemHasRef, itemSafe := boundExprHasSlotRef(expr, tag, replacements, guard, depth)
		hasRef = hasRef || itemHasRef
		if !itemSafe {
			return hasRef, false
		}
	}
	return hasRef, true
}

func rewriteBoundSlotRef(
	expr *planpb.Expr,
	tag int32,
	replacements map[int32]*planpb.Expr,
) (*planpb.Expr, bool) {
	if expr == nil {
		return nil, true
	}
	switch impl := expr.Expr.(type) {
	case *planpb.Expr_Lit, *planpb.Expr_P, *planpb.Expr_V,
		*planpb.Expr_Raw, *planpb.Expr_T, *planpb.Expr_Max,
		*planpb.Expr_Vec, *planpb.Expr_Fold, *planpb.Expr_Corr:
		return expr, true
	case *planpb.Expr_Col:
		if impl.Col != nil && impl.Col.RelPos == tag {
			replacement, ok := replacements[impl.Col.ColPos]
			if !ok {
				return nil, false
			}
			return DeepCopyExpr(replacement), true
		}
	case *planpb.Expr_F:
		if impl.F == nil || impl.F.Func == nil {
			return nil, false
		}
		for i := range impl.F.Args {
			var ok bool
			impl.F.Args[i], ok = rewriteBoundSlotRef(impl.F.Args[i], tag, replacements)
			if !ok {
				return nil, false
			}
		}
	case *planpb.Expr_List:
		if impl.List == nil {
			return expr, true
		}
		for i := range impl.List.List {
			var ok bool
			impl.List.List[i], ok = rewriteBoundSlotRef(impl.List.List[i], tag, replacements)
			if !ok {
				return nil, false
			}
		}
	case *planpb.Expr_Sub:
		if impl.Sub != nil {
			var ok bool
			impl.Sub.Child, ok = rewriteBoundSlotRef(impl.Sub.Child, tag, replacements)
			if !ok {
				return nil, false
			}
		}
	case *planpb.Expr_W:
		if impl.W == nil {
			return expr, true
		}
		var ok bool
		impl.W.WindowFunc, ok = rewriteBoundSlotRef(impl.W.WindowFunc, tag, replacements)
		if !ok {
			return nil, false
		}
		for i := range impl.W.PartitionBy {
			impl.W.PartitionBy[i], ok = rewriteBoundSlotRef(impl.W.PartitionBy[i], tag, replacements)
			if !ok {
				return nil, false
			}
		}
		for _, orderBy := range impl.W.OrderBy {
			if orderBy == nil {
				continue
			}
			orderBy.Expr, ok = rewriteBoundSlotRef(orderBy.Expr, tag, replacements)
			if !ok {
				return nil, false
			}
		}
		if impl.W.Frame != nil {
			for _, bound := range []*planpb.FrameBound{impl.W.Frame.Start, impl.W.Frame.End} {
				if bound == nil {
					continue
				}
				bound.Val, ok = rewriteBoundSlotRef(bound.Val, tag, replacements)
				if !ok {
					return nil, false
				}
			}
		}
	default:
		return nil, false
	}
	return expr, true
}
