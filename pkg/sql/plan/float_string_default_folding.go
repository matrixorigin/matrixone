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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// freezeVerifiedFloatDefault reconstructs a current SQL tree with the exact
// typed constant boundaries saved by historical DEFAULT folding. All three
// trees come from independent SQL binding, not from untrusted literal payloads.
// The caller still compares this complete witness to the separately widened
// authenticated catalog tree. Nothing changes ordinary expression equality.
func freezeVerifiedFloatDefault(ctx context.Context, folded, old, current *planpb.Expr, proc *process.Process, budget *tableDumpBindingBudget) (*planpb.Expr, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if folded == nil || old == nil || current == nil {
		return nil, false, errLegacyFloatStringRebind
	}
	if err := chargeFloatStringRebind(folded, budget); err != nil {
		return nil, false, err
	}
	if folded.GetLit() != nil || folded.GetVec() != nil {
		// A constant boundary must be demonstrably foldable on both sides. In
		// particular, a volatile/real-time subtree cannot be frozen just because
		// it has no row references. Charge both complete folding traversals.
		for _, candidate := range []*planpb.Expr{old, current} {
			if err := chargeFloatStringRebind(candidate, budget); err != nil {
				return nil, false, err
			}
		}
		previous, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(old), proc, false, true)
		if err != nil {
			return nil, false, err
		}
		if !sameTableDumpBoundExpression(folded, previous) {
			return nil, false, errLegacyFloatStringRebind
		}
		next, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(current), proc, false, true)
		if err != nil {
			return nil, false, err
		}
		if next.GetLit() == nil && next.GetVec() == nil {
			return nil, false, errLegacyFloatStringRebind
		}
		// The old value AND its width/provenance are immutable, even if a current
		// implicit conversion could produce more bytes or a wider literal type.
		return DeepCopyExpr(folded), !proto.Equal(folded, current), nil
	}
	f, o, n := folded.GetF(), old.GetF(), current.GetF()
	if f != nil || o != nil || n != nil {
		if f == nil || o == nil || n == nil || f.Func == nil || o.Func == nil || n.Func == nil ||
			f.Func.Obj != o.Func.Obj || f.Func.Obj != n.Func.Obj || f.SyntaxExplicitCast != o.SyntaxExplicitCast || f.SyntaxExplicitCast != n.SyntaxExplicitCast ||
			len(f.Args) != len(o.Args) || len(f.Args) != len(n.Args) {
			return nil, false, errLegacyFloatStringRebind
		}
		args := make([]*planpb.Expr, len(n.Args))
		changed := false
		for i := range args {
			if i == 1 && isLegacyFloatStringCast(folded) && !f.SyntaxExplicitCast {
				// This is precisely the live conversion authorized by the migration.
				// Its target Type node has no literal/AST child to freeze.
				target := n.Args[i]
				if target == nil || target.GetT() == nil || target.Typ.Id != int32(types.T_varchar) || target.Typ.Width != 34 || !proto.Equal(&target.Typ, &current.Typ) {
					return nil, false, errLegacyFloatStringRebind
				}
				args[i] = DeepCopyExpr(target)
				continue
			}
			arg, frozen, err := freezeVerifiedFloatDefault(ctx, f.Args[i], o.Args[i], n.Args[i], proc, budget)
			if err != nil {
				return nil, false, err
			}
			args[i] = arg
			changed = changed || frozen
		}
		if !changed {
			return DeepCopyExpr(current), false, nil
		}
		switch n.Func.ObjName {
		case "cast", "cast_assign", "cast_strict", "cast_ignore":
			// Declared/assignment casts own their target, truncation policy
			// and explicit-syntax marker. Only their verified child changes.
			result := DeepCopyExpr(current)
			result.GetF().Args = args
			return result, true, nil
		}
		if err := chargeFloatStringRebind(current, budget); err != nil {
			return nil, false, err
		}
		rebound, err := rebindFloatStringFunction(ctx, current, args)
		if err != nil {
			return nil, false, err
		}
		if rebound.GetF() == nil || rebound.GetF().Func == nil || rebound.GetF().Func.Obj != n.Func.Obj {
			return nil, false, errLegacyFloatStringRebind
		}
		return rebound, true, nil
	}
	if f, o, n := folded.GetList(), old.GetList(), current.GetList(); f != nil || o != nil || n != nil {
		if f == nil || o == nil || n == nil || len(f.List) != len(o.List) || len(f.List) != len(n.List) {
			return nil, false, errLegacyFloatStringRebind
		}
		result := DeepCopyExpr(current)
		changed := false
		for i := range f.List {
			arg, frozen, err := freezeVerifiedFloatDefault(ctx, f.List[i], o.List[i], n.List[i], proc, budget)
			if err != nil {
				return nil, false, err
			}
			result.GetList().List[i] = arg
			changed = changed || frozen
		}
		return result, changed, nil
	}
	if !sameTableDumpBoundExpression(folded, old) || !sameTableDumpBoundExpression(old, current) {
		return nil, false, errLegacyFloatStringRebind
	}
	return DeepCopyExpr(current), false, nil
}

// Observe resolved types before folding removes their rendering dependency.
// JSON text producers can consume JSON directly without an intermediate CAST.
func hasFloatDefaultFormatting(expr *planpb.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.Typ.Id == int32(types.T_json) {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil {
			id, _ := function.DecodeOverloadID(fn.Func.Obj)
			switch id {
			case function.JSON_UNQUOTE, function.JSON_PRETTY, function.JSON_VALUE:
				// These also parse VARCHAR documents internally.
				return true
			}
		}
		for _, arg := range fn.Args {
			if arg != nil && types.T(expr.Typ.Id).IsMySQLString() && (arg.Typ.Id == int32(types.T_float64) || arg.Typ.Id == int32(types.T_float32)) {
				return true
			}
			if hasFloatDefaultFormatting(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, arg := range list.List {
			if hasFloatDefaultFormatting(arg) {
				return true
			}
		}
	}
	return false
}
