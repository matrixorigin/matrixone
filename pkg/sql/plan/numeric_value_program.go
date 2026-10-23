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
	"math"
	"strings"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

var nextNumericValueProgramScope atomic.Uint64

// appendNumericValueCastBeforeExpr installs two references to one lexical
// program. The raw output is the original value tree; the BOOL output is read
// from that same executor's branch decisions at runtime.
func appendNumericValueCastBeforeExpr(
	ctx context.Context, source *Expr, target Type, policy int32,
) (*Expr, error) {
	if source != nil {
		if literal := source.GetLit(); literal != nil {
			kind := makePlan2BoolConstExprWithType(!literal.Isnull && literal.GetIsBin())
			if isFalseBoolLiteral(kind) {
				// Preserve the existing conversion identity for ordinary text
				// and NULL. Only a selected binary-literal kind needs the new
				// explicit kind-aware cast contract.
				return appendCastBeforeExprWithOverload(ctx, source, target, policy)
			}
			return appendNumericValueCastWithKind(ctx, source, kind, target, policy)
		}
		if source.GetCol() != nil {
			// The plan-level lowerer can resolve whether this column already
			// carries a demanded companion. Leave the old cast in place here;
			// it will be upgraded only when the resolved kind is not false.
			return appendCastBeforeExprWithOverload(ctx, source, target, policy)
		}
	}
	value, kind, err := makeNumericValueProgramOutputs(ctx, source)
	if err != nil {
		return nil, err
	}
	if isFalseBoolLiteral(kind) {
		return appendCastBeforeExprWithOverload(ctx, source, target, policy)
	}
	return appendNumericValueCastWithKind(ctx, value, kind, target, policy)
}

// makeNumericValueProgramOutputs creates the producer-side raw value and
// ordinary BOOL passenger for one expression. Both references name the same
// lexical scope; the value expression owns the definition.
func makeNumericValueProgramOutputs(ctx context.Context, source *Expr) (*Expr, *Expr, error) {
	return makeNumericValueProgramOutputsWithKind(ctx, source, nil)
}

func makeNumericValueProgramOutputsWithKind(ctx context.Context, source, sourceKind *Expr) (*Expr, *Expr, error) {
	if source == nil {
		return nil, nil, moerr.NewInvalidInput(ctx, "numeric value source is nil")
	}
	if sourceKind != nil && types.T(sourceKind.Typ.Id) != types.T_bool {
		return nil, nil, moerr.NewInvalidInput(ctx, "numeric value kind source must be BOOL")
	}
	if sourceKind == nil && !numericValueMayCarrySelectedKind(source) {
		return DeepCopyExpr(source), makePlan2BoolConstExprWithType(false), nil
	}
	scopeID := nextNumericValueProgramScope.Add(1)
	if scopeID == 0 || scopeID > math.MaxInt64 {
		return nil, nil, moerr.NewInternalError(ctx, "numeric value program scope exhausted")
	}
	scope := int64(scopeID)
	raw := DeepCopyExpr(source)
	definition := numericValueProgramDefinition(scope, raw, sourceKind)
	valueRef := scalarNumericProgramRef(scope, 0, raw.Typ)
	valueRef.Expr.(*planpb.Expr_F).F.Args = append(
		[]*Expr{definition},
		valueRef.Expr.(*planpb.Expr_F).F.Args...,
	)
	kindRef := scalarNumericProgramRef(scope, 1, makeSimplePlan2Type(types.T_bool))
	return valueRef, kindRef, nil
}

// numericValueMayCarrySelectedKind follows the owner of a value's bytes. A
// selector can forward the kind of the branch it returns, and implicit string
// casts are transparent. Ordinary functions allocate their result and start
// a new string domain, so their output has the legacy text kind. Unknown or
// unresolved nodes stay conservative rather than being mistaken for text.
func numericValueMayCarrySelectedKind(expr *Expr) bool {
	if expr == nil {
		return true
	}
	if literal := expr.GetLit(); literal != nil {
		return !literal.Isnull && literal.GetIsBin()
	}
	if expr.GetCol() != nil || expr.GetP() != nil || expr.GetV() != nil {
		return types.T(expr.Typ.Id).IsMySQLString() || types.T(expr.Typ.Id) == types.T_any
	}
	if expr.GetList() != nil {
		return true
	}
	f := expr.GetF()
	if f == nil || f.Func == nil {
		return true
	}
	fid, overload := function.DecodeOverloadID(f.Func.GetObj())
	name := strings.ToLower(f.Func.GetObjName())
	if fid == function.CAST {
		if isInferredStringDeclarationCast(expr) {
			return numericValueMayCarrySelectedKind(f.Args[0])
		}
		if overload != 0 || f.SyntaxExplicitCast || len(f.Args) != 2 || f.Args[0] == nil ||
			!types.T(expr.Typ.Id).IsMySQLString() || !types.T(f.Args[0].Typ.Id).IsMySQLString() {
			return false
		}
		return numericValueMayCarrySelectedKind(f.Args[0])
	}

	if fid == function.SCALAR_PROGRAM_REF {
		_, slot, ok := numericProgramRefIdentity(expr)
		if !ok {
			return true
		}
		if slot == 1 {
			return true
		}
		if raw, ok := numericProgramRawValue(expr); ok {
			definition := f.Args[0].GetF()
			if definition != nil && len(definition.Args) == 3 {
				return !isFalseBoolLiteral(definition.Args[2])
			}
			return numericValueMayCarrySelectedKind(raw)
		}
		return true
	}
	if fid == function.SCALAR_PROGRAM {
		if len(f.Args) == 3 {
			return !isFalseBoolLiteral(f.Args[2])
		}
		if len(f.Args) == 2 {
			return numericValueMayCarrySelectedKind(f.Args[1])
		}
		return true
	}

	valueIndexes := controlFlowValueIndexes(name, len(f.Args))
	switch name {
	case "ifnull":
		if len(f.Args) == 2 {
			valueIndexes = []int{0, 1}
		}
	case "nullif":
		if len(f.Args) >= 1 {
			valueIndexes = []int{0}
		}
	}
	if len(valueIndexes) > 0 {
		for _, index := range valueIndexes {
			if index < 0 || index >= len(f.Args) || numericValueMayCarrySelectedKind(f.Args[index]) {
				return true
			}
		}
		return false
	}

	if _, exists := function.GetFunctionByIdWithoutError(f.Func.GetObj()); !exists {
		return true
	}
	return false
}

// numericValueProgramSourceOwner resolves only validated lexical owners. A
// slot-zero REF owns the source definition; the slot-one REF is a BOOL
// passenger and never contributes numeric value metadata or parameter roles.
func numericValueProgramSourceOwner(expr *Expr) (source *Expr, privateNode bool) {
	if expr == nil || expr.GetF() == nil || expr.GetF().Func == nil {
		return expr, false
	}
	fid, _ := function.DecodeOverloadID(expr.GetF().Func.GetObj())
	switch fid {
	case function.SCALAR_PROGRAM:
		fn := expr.GetF()
		if _, ok := numericProgramDefinitionScope(expr); !ok || len(fn.Args) < 2 || fn.Args[1] == nil {
			return nil, true
		}
		if len(fn.Args) == 3 && (fn.Args[2] == nil || types.T(fn.Args[2].Typ.Id) != types.T_bool) {
			return nil, true
		}
		return fn.Args[1], true
	case function.SCALAR_PROGRAM_REF:
		_, slot, ok := numericProgramRefIdentity(expr)
		if !ok || slot != 0 {
			return nil, true
		}
		source, ok := numericProgramRawValue(expr)
		if !ok {
			return nil, true
		}
		return source, true
	default:
		return expr, false
	}
}

// isStatementConstantNumericValueExpression admits private numeric-value
// nodes only through a validated value owner. Its source must still satisfy
// the ordinary statement-constant rules; the BOOL slot can never stand in for
// a value or make a row-dependent/volatile owner eligible.
func isStatementConstantNumericValueExpression(expr *Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func == nil {
			return false
		}
		fid, overload := function.DecodeOverloadID(fn.Func.GetObj())
		switch fid {
		case function.SCALAR_PROGRAM_REF:
			source, private := numericValueProgramSourceOwner(expr)
			if !private || source == nil || !isStatementConstantNumericValueExpression(source) {
				return false
			}
			definition := fn.Args[0].GetF()
			return definition != nil && (len(definition.Args) != 3 ||
				isStatementConstantNumericValueExpression(definition.Args[2]))
		case function.SCALAR_PROGRAM:
			source, private := numericValueProgramSourceOwner(expr)
			if !private || source == nil || !isStatementConstantNumericValueExpression(source) {
				return false
			}
			return len(fn.Args) != 3 || isStatementConstantNumericValueExpression(fn.Args[2])
		case function.CAST:
			if overload == function.NumericValueCastOverload {
				source, _, ok := numericValueCastAnalysisSource(expr)
				if !ok || !isStatementConstantNumericValueExpression(source) {
					return false
				}
				if _, private := numericValueProgramSourceOwner(fn.Args[0]); private {
					valueScope, valueSlot, valueOK := numericProgramRefIdentity(fn.Args[0])
					kindScope, kindSlot, kindOK := numericProgramRefIdentity(fn.Args[2])
					return valueOK && kindOK && valueSlot == 0 && kindSlot == 1 && valueScope == kindScope
				}
				return isStatementConstantNumericValueExpression(fn.Args[2])
			}
		}
		registered, ok := function.GetFunctionByIdWithoutError(fn.Func.GetObj())
		if !ok || registered.CannotFold() || registered.IsRealTimeRelated() ||
			registered.IsAgg() || registered.IsWin() {
			return false
		}
		for _, arg := range fn.Args {
			if !isStatementConstantNumericValueExpression(arg) {
				return false
			}
		}
		return true
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if !isStatementConstantNumericValueExpression(item) {
				return false
			}
		}
		return true
	}
	return function.IsStatementConstantInput(expr)
}

// isStatementConstantNumericCastInput limits prepared comparison rewriting to
// a complete numeric CAST chain. Non-cast scalar functions can still carry a
// statement-constant program for diagnostics, but scan predicates keep their
// original executable function tree and zone-map eligibility.
func isStatementConstantNumericCastInput(expr *Expr) bool {
	if function.IsStatementConstantInput(expr) {
		return true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) < 1 {
		return false
	}
	fid, overload := function.DecodeOverloadID(fn.Func.GetObj())
	if fid != function.CAST {
		return false
	}
	if overload == function.NumericValueCastOverload {
		source, _, ok := numericValueCastAnalysisSource(expr)
		if !ok || !isStatementConstantNumericValueExpression(source) {
			return false
		}
		if _, private := numericValueProgramSourceOwner(fn.Args[0]); private {
			valueScope, valueSlot, valueOK := numericProgramRefIdentity(fn.Args[0])
			kindScope, kindSlot, kindOK := numericProgramRefIdentity(fn.Args[2])
			return valueOK && kindOK && valueSlot == 0 && kindSlot == 1 && valueScope == kindScope
		}
		return isStatementConstantNumericValueExpression(fn.Args[2])
	}
	if overload < function.NumericValueCastPolicyOrdinary || overload > function.NumericValueCastPolicySetOperation ||
		len(fn.Args) != 2 {
		return false
	}
	return isStatementConstantNumericCastInput(fn.Args[0])
}

func numericValueProgramScope(expr *Expr) (int64, bool) {
	if expr == nil || expr.GetF() == nil || expr.GetF().Func == nil {
		return 0, false
	}
	fid, _ := function.DecodeOverloadID(expr.GetF().Func.GetObj())
	switch fid {
	case function.SCALAR_PROGRAM:
		return numericProgramDefinitionScope(expr)
	case function.SCALAR_PROGRAM_REF:
		scope, _, ok := numericProgramRefIdentity(expr)
		return scope, ok
	default:
		return 0, false
	}
}

func sameNumericValueProgramScope(left, right *Expr) bool {
	leftScope, leftOK := numericValueProgramScope(left)
	rightScope, rightOK := numericValueProgramScope(right)
	return leftOK && rightOK && leftScope == rightScope
}

func replaceNumericValueProgramSource(owner, source *Expr) (*Expr, bool) {
	if owner == nil || source == nil || owner.GetF() == nil || owner.GetF().Func == nil {
		return nil, false
	}
	fn := owner.GetF()
	fid, _ := function.DecodeOverloadID(fn.Func.GetObj())
	switch fid {
	case function.SCALAR_PROGRAM:
		if _, ok := numericProgramDefinitionScope(owner); !ok || len(fn.Args) != 2 && len(fn.Args) != 3 {
			return nil, false
		}
		copy := DeepCopyExpr(owner)
		copy.GetF().Args[1] = DeepCopyExpr(source)
		copy.Typ = makeSimplePlan2Type(types.T_any)
		return copy, true
	case function.SCALAR_PROGRAM_REF:
		_, slot, ok := numericProgramRefIdentity(owner)
		if !ok || slot != 0 || len(fn.Args) != 3 || !numericProgramDefinitionMatchesRef(owner) {
			return nil, false
		}
		copy := DeepCopyExpr(owner)
		definition := DeepCopyExpr(fn.Args[0])
		definition.GetF().Args[1] = DeepCopyExpr(source)
		definition.Typ = makeSimplePlan2Type(types.T_any)
		copy.GetF().Args[0] = definition
		copy.Typ = source.Typ
		return copy, true
	default:
		return nil, false
	}
}

func numericProgramDefinitionMatchesRef(value *Expr) bool {
	fn := value.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 3 {
		return false
	}
	refScope, slot, refOK := numericProgramRefIdentity(value)
	definitionScope, defOK := numericProgramDefinitionScope(fn.Args[0])
	return refOK && defOK && slot == 0 && refScope == definitionScope
}

// numericValueProgramDefinition pairs a projected value with its selected
// branch kind. An optional third expression is an already-materialized BOOL
// passenger consumed at a downstream projection boundary.
func numericValueProgramDefinition(scope int64, raw, sourceKind *Expr) *Expr {
	args := []*Expr{makePlan2Int64ConstExprWithType(scope), raw}
	if sourceKind != nil {
		args = append(args, DeepCopyExpr(sourceKind))
	}
	return &Expr{
		Typ: makeSimplePlan2Type(types.T_any),
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: getFunctionObjRef(int64(function.SCALAR_PROGRAM)<<32, "_scalar_numeric_program"),
			Args: args,
		}},
	}
}

// stripInferredStringDeclarationCastProofs removes binder-only ownership
// proofs once the final demand pass has either consumed them or established
// that the query does not need a numeric kind companion. The cast itself
// remains unchanged in the published plan.
func stripInferredStringDeclarationCastProofs(owner any) error {
	seen := make(map[*Expr]struct{})
	var stripTree func(*Expr) error
	stripTree = func(root *Expr) error {
		return planpb.VisitExprTree(root, func(expr *Expr) error {
			if expr == nil {
				return nil
			}
			if _, ok := seen[expr]; ok {
				return nil
			}
			seen[expr] = struct{}{}
			metadata := expr.GetPreparedNumeric()
			if metadata == nil {
				return nil
			}
			metadata.InferredStringDeclarationCast = false
			// StringDomainSource is a proof witness, not an executable child
			// of the expression tree. Visit it only for transient metadata
			// cleanup, and guard shared/cyclic witnesses explicitly.
			return stripTree(metadata.StringDomainSource)
		})
	}
	return planpb.VisitExpressionsInOwner(owner, func(root *Expr) error {
		return stripTree(root)
	})
}

// appendNumericValueCastWithKind consumes a raw value and its paired semantic
// kind. It is used after relational operators carry the kind as an ordinary
// BOOL column, so it does not introduce a second producer program.
func appendNumericValueCastWithKind(
	ctx context.Context, value, kind *Expr, target Type, policy int32,
) (*Expr, error) {
	if value == nil || kind == nil {
		return nil, moerr.NewInvalidInput(ctx, "numeric value and kind sources are required")
	}
	if types.T(kind.Typ.Id) != types.T_bool {
		return nil, moerr.NewInvalidInput(ctx, "numeric value kind source must be BOOL")
	}
	targetArg := &Expr{Typ: target, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}}
	policyArg := makePlan2Int32ConstExprWithType(policy)
	args := []*Expr{DeepCopyExpr(value), targetArg, DeepCopyExpr(kind), policyArg}
	argTypes := make([]types.Type, len(args))
	for i := range args {
		argTypes[i] = types.MustTypeFromPlan(args[i].Typ)
	}
	found, err := function.GetFunctionByNameWithOverload(
		ctx, "cast", argTypes, function.NumericValueCastOverload,
	)
	if err != nil {
		return nil, err
	}
	target.NotNullable = function.DeduceNotNullableForBinding(ctx, found.GetEncodedOverloadID(), args)
	targetArg.Typ.NotNullable = target.NotNullable
	return &Expr{
		Typ: target,
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: getFunctionObjRef(found.GetEncodedOverloadID(), "cast"),
			Args: args,
		}},
	}, nil
}

// numericValueCastAnalysisSource exposes the value input and conversion policy
// for prepared-plan analysis without changing the executable CAST9 expression
// or its paired kind reference. Ordinary ColRefs remain intact for node-aware
// lineage; private program refs are transparent only when their lexical owner
// is structurally valid.
func numericValueCastAnalysisSource(expr *Expr) (*Expr, int32, bool) {
	if expr == nil || expr.GetF() == nil || expr.GetF().Func == nil {
		return nil, 0, false
	}
	fn := expr.GetF()
	fid, overload := function.DecodeOverloadID(fn.Func.GetObj())
	if fid != function.CAST || overload != function.NumericValueCastOverload || len(fn.Args) != 4 ||
		fn.Args[0] == nil || fn.Args[1] == nil || fn.Args[2] == nil || fn.Args[3] == nil ||
		types.T(fn.Args[2].Typ.Id) != types.T_bool || fn.Args[3].GetLit() == nil ||
		fn.Args[3].GetLit().Isnull {
		return nil, 0, false
	}
	policy := fn.Args[3].GetLit().GetI32Val()
	if policy < function.NumericValueCastPolicyOrdinary ||
		policy > function.NumericValueCastPolicySetOperation {
		return nil, 0, false
	}
	source, privateNode := numericValueProgramSourceOwner(fn.Args[0])
	if privateNode {
		ownedSource, _, owned := function.NumericValueProgramCastOwner(expr)
		if source == nil || !owned || ownedSource == nil {
			return nil, 0, false
		}
		return ownedSource, policy, true
	}
	// Most lowered consumers already read ordinary producer columns directly:
	// CAST9(valueCol, target, kindCol, policy). Those values do not need a
	// lexical program owner; keep the value ColRef intact so node-aware lineage
	// can resolve it without confusing it with the BOOL passenger.
	return fn.Args[0], policy, true
}

// numericValueExpressionHasPrivateProgram reports when an expression needs
// its paired lexical owner to preserve the selected value's semantics.
func numericValueExpressionHasPrivateProgram(expr *Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil {
		fid, overload := function.DecodeOverloadID(fn.Func.GetObj())
		if fid == function.SCALAR_PROGRAM || fid == function.SCALAR_PROGRAM_REF ||
			(fid == function.CAST && overload == function.NumericValueCastOverload) {
			return true
		}
		for _, arg := range fn.Args {
			if numericValueExpressionHasPrivateProgram(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if numericValueExpressionHasPrivateProgram(item) {
				return true
			}
		}
	}
	return false
}

func scalarNumericProgramRef(scope int64, slot int64, typ Type) *Expr {
	return &Expr{
		Typ: typ,
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: getFunctionObjRef(int64(function.SCALAR_PROGRAM_REF)<<32, "_scalar_numeric_program_ref"),
			Args: []*Expr{
				makePlan2Int64ConstExprWithType(scope),
				makePlan2Int64ConstExprWithType(slot),
			},
		}},
	}
}
