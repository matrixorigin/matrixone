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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// annotateStringLengthSource keeps execution-invariant length values at the
// string-function owner. The existing evaluator supplies numeric semantics;
// only the metadata skeleton receives the folded value, never execution args.
func annotateStringLengthSource(expr *Expr, proc *process.Process) {
	if expr == nil || proc == nil {
		return
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return
	}
	index := -1
	switch strings.ToLower(fn.Func.ObjName) {
	case "left", "right", "repeat", "lpad", "rpad":
		if len(fn.Args) >= 2 {
			index = 1
		}
	case "substring", "substr", "sub_str", "mid":
		if len(fn.Args) == 3 {
			index = 2
		} else if len(fn.Args) == 2 {
			index = 1
		}
	}
	if index < 0 || fn.Args[index].GetLit() != nil || !stringLengthConstantCandidate(fn.Args[index]) {
		return
	}
	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(fn.Args[index]), proc, false, true)
	if err != nil || folded == nil || folded.GetLit() == nil || folded.GetLit().Isnull {
		return
	}
	// A successful closed evaluation is the authoritative constant. Keeping Src
	// would send declaration consumers back through the unevaluated expression.
	folded.GetLit().Src = nil
	folded.PreparedNumeric = nil
	folded.GetLit().StringSource = uint32(types.StringSourceExpression)
	witness := &Expr{Typ: expr.Typ, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: fn.Func, Args: append([]*Expr(nil), fn.Args...),
	}}}
	witness.GetF().Args[index] = folded
	if expr.PreparedNumeric == nil {
		expr.PreparedNumeric = &planpb.PreparedNumericMetadata{}
	}
	expr.PreparedNumeric.StringDomainSource = witness
}

// This deliberately closed set excludes arbitrary user functions, real-time
// values, parameters and variable payloads. Failure to prove/evaluate a value
// retains the unknown declaration and its original execution-time error.
func stringLengthConstantCandidate(expr *Expr) bool {
	if expr == nil || expr.GetPreparedNumeric().GetStringDomainSource() != nil {
		return false
	}
	if lit := expr.GetLit(); lit != nil {
		if lit.Isnull || lit.Src != nil {
			return false
		}
		source := types.StringSource(lit.StringSource)
		return source == types.StringSourceExpression || source == types.StringSourceLiteral
	}
	if expr.GetT() != nil {
		return true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	switch strings.ToLower(fn.Func.ObjName) {
	case "cast":
		if len(fn.Args) < 1 {
			return false
		}
		if makeTypeByPlan2Expr(fn.Args[0]).IsUInt() && types.T(expr.Typ.Id) == types.T_int64 {
			if _, _, known := regexpConstantInteger(expr); !known {
				return false
			}
		}
	case "abs", "ceil", "ceiling", "floor", "greatest", "least":
	default:
		return false
	}
	for _, arg := range fn.Args {
		if !stringLengthConstantCandidate(arg) {
			return false
		}
	}
	return true
}
