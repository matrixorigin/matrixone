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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
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
	if index < 0 || !stringLengthConstantCandidate(fn.Args[index]) {
		return
	}
	vec, free, warned, err := rule.EvaluateConstantExpression(proc, DeepCopyExpr(fn.Args[index]), batch.EmptyForConstFoldBatch)
	if err != nil {
		return
	}
	defer free()
	if warned || vec == nil {
		return
	}
	lit := rule.GetConstantValue(vec, false, 0)
	if lit == nil || lit.Isnull {
		return
	}
	folded := &Expr{Typ: makePlan2Type(vec.GetType()), Expr: &planpb.Expr_Lit{Lit: lit}}
	// A successful closed evaluation is the authoritative constant. Keeping Src
	// would send declaration consumers back through the unevaluated expression.
	folded.GetLit().Src = nil
	folded.PreparedNumeric = nil
	folded.GetLit().StringSource = uint32(types.StringSourceExpression)
	// This owner stores only local declaration facts. Runtime dependencies stay
	// in the real function args, and are summarized only at missing-input
	// boundaries (folded values/columns/subqueries). No ancestor copies a chain.
	args := make([]*Expr, len(fn.Args))
	for i, arg := range fn.Args {
		if i == index {
			args[i] = folded
		} else {
			args[i] = stringDeclarationWitnessArg(arg)
		}
	}
	witness := &Expr{Typ: expr.Typ, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: fn.Func.Obj, ObjName: fn.Func.ObjName}, Args: args,
	}}}
	if expr.PreparedNumeric == nil {
		expr.PreparedNumeric = &planpb.PreparedNumericMetadata{}
	}
	expr.PreparedNumeric.StringDomainSource = witness
}

// stringDeclarationWitnessArg preserves the logical declaration, not the
// execution envelope or value tree. NULL denotes an unknown value, not a zero
// length. The non-NULL BINARY tag retains unknown CAST ownership too: a NULL
// BINARY(-1) is the separate unassigned-variable zero-bound convention.
func stringDeclarationWitnessArg(arg *Expr) *Expr {
	if arg == nil {
		return nil
	}
	declared := regexpDeclaredStringType(arg)
	witness := &Expr{Typ: makePlan2Type(&declared), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
		Isnull: true, StringSource: uint32(types.StringSourceExpression),
	}}}
	// Non-length positions need types, not SQL integer-prefix values. In
	// particular '123' must not become an INT64 literal with a VARCHAR type.
	if regexpOwnsBinaryCast(arg) && (declared.Oid == types.T_varbinary || declared.Oid == types.T_blob) {
		witness.Typ.Id = int32(types.T_binary)
		if declared.Oid == types.T_blob {
			witness.Typ.Width = -1
			witness.GetLit().Isnull = false
			witness.GetLit().Value = &planpb.Literal_Sval{Sval: ""}
		}
	}
	return witness
}

// Registered foldable builtins exclude arbitrary user functions, real-time
// values, parameters and variable payloads. Failure to prove/evaluate a value
// retains the unknown declaration and its original execution-time error.
func stringLengthConstantCandidate(expr *Expr) bool {
	if expr == nil || (expr.GetF() == nil && expr.GetPreparedNumeric().GetStringDomainSource() != nil) {
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
	implementation, registered := function.GetFunctionByIdWithoutError(fn.Func.Obj)
	argTypes := make([]types.Type, len(fn.Args))
	for i, arg := range fn.Args {
		argTypes[i] = makeTypeByPlan2Expr(arg)
	}
	_, named := function.GetFunctionByNameWithoutError(fn.Func.ObjName, argTypes)
	if !registered || !named || implementation.IsRealTimeRelated() ||
		(implementation.CannotFold() && len(controlFlowValueIndexes(strings.ToLower(fn.Func.ObjName), len(fn.Args))) == 0) {
		return false
	}
	if strings.EqualFold(fn.Func.ObjName, "cast") {
		if len(fn.Args) < 1 {
			return false
		}
		if makeTypeByPlan2Expr(fn.Args[0]).IsUInt() && types.T(expr.Typ.Id) == types.T_int64 {
			if _, _, known := regexpConstantInteger(expr); !known {
				return false
			}
		}
	}
	for _, arg := range fn.Args {
		if lit := arg.GetLit(); lit != nil && lit.Isnull && lit.Src == nil && arg.PreparedNumeric == nil &&
			(types.StringSource(lit.StringSource) == types.StringSourceExpression ||
				types.StringSource(lit.StringSource) == types.StringSourceLiteral) {
			continue // A closed conditional can select its other, non-NULL arm.
		}
		if !stringLengthConstantCandidate(arg) {
			return false
		}
	}
	return true
}
