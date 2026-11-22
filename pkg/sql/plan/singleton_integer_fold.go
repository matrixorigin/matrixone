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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Limit the additional executor work spent discovering string sizes.
// Literal sizes use the existing structural proof.
const maxSingletonIntegerFoldWork = 4096

type singletonIntegerFold struct {
	eval     *process.Process
	budget   *speculativeConstantFoldBudget
	workLeft int
	declined map[*plan.Expr]struct{}
}

// resolve operates only on the singleton's private proof tree. A successful
// fold replaces the operand there so subsequent sizing and folding can reuse
// it. An unsuccessful fold must leave the original operand intact.
func (f *singletonIntegerFold) resolve(expr *plan.Expr, depth int, spend func(int) bool) (int64, bool) {
	if value, ok := speculativeConstantInt64Within(expr, depth, spend); ok {
		return value, true
	}
	if f == nil || expr == nil || expr.GetF() == nil || !isSpeculativeIntegerType(types.T(expr.Typ.Id)) {
		return 0, false
	}
	if _, declined := f.declined[expr]; declined || !f.admitTree(expr, depth, spend) {
		return 0, false
	}

	candidate := DeepCopyExpr(expr)
	if isSpeculativeConstantFoldSafe(candidate, f.budget) && f.checkIntegerCasts(candidate, depth) {
		folded, err := constantFoldForSingletonProof(candidate, f.eval)
		if err == nil && f.eval.Ctx.Err() == nil {
			if value, ok := speculativeConstantInt64Within(folded, 1, nil); ok {
				*expr = *folded
				return value, true
			}
		}
	}
	if f.declined == nil {
		f.declined = make(map[*plan.Expr]struct{})
	}
	f.declined[expr] = struct{}{}
	return 0, false
}

func (f *singletonIntegerFold) admitTree(expr *plan.Expr, depth int, spend func(int) bool) bool {
	if expr == nil || depth == 0 || f.workLeft == 0 || f.eval.Ctx.Err() != nil || spend != nil && !spend(1) {
		return false
	}
	f.workLeft--
	if lit := expr.GetLit(); lit != nil {
		return lit.Src == nil && lit.GetU64Val() <= 1<<63-1
	}
	fn := expr.GetF()
	if fn == nil {
		return expr.GetT() != nil
	}
	if fn.Func == nil {
		return false
	}
	for _, arg := range fn.Args {
		if !f.admitTree(arg, depth-1, spend) {
			return false
		}
	}
	return true
}

func (f *singletonIntegerFold) checkIntegerCasts(expr *plan.Expr, depth int) bool {
	fn := expr.GetF()
	if fn == nil {
		return true
	}
	for _, arg := range fn.Args {
		if !f.checkIntegerCasts(arg, depth-1) {
			return false
		}
	}
	id, _ := function.DecodeOverloadID(fn.Func.GetObj())
	if id != function.CAST {
		return true
	}
	if len(fn.Args) != 2 {
		return false
	}
	source := fn.Args[0]
	src, dst := types.T(source.Typ.Id), types.T(expr.Typ.Id)
	if src == dst || !isSpeculativeIntegerType(src) || !isSpeculativeIntegerType(dst) {
		return true
	}
	if domain, ok := affineIntegerTypeRange(src); ok &&
		speculativeIntegerCastPreservesValue(dst, domain.min) &&
		speculativeIntegerCastPreservesValue(dst, domain.max) {
		return true
	}
	// Check narrowing and signedness changes before normal evaluation can
	// replace a lossy conversion with only its wrapped result.
	value, ok := f.resolve(source, depth-1, nil)
	return ok && speculativeIntegerCastPreservesValue(dst, value)
}
