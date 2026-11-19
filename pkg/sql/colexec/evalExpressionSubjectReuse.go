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

package colexec

import (
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

const (
	orLikeMaxTerms          = 8
	orLikeMaxSubjectNodes   = 64
	orLikeMaxSubjectDepth   = 16
	orLikeMaxLiteralBytes   = 4096
	orLikeMaxIdentityBytes  = 16 << 10
	orLikeDiscoveryBudget   = 512
	orLikeMinFirstBatchRows = 1024
)

// ReuseFilterExpressionExecutor may replace one fresh, unevaluated static filter
// root. Call only in the operator's first nonempty invocation, immediately before
// this root's first Eval, and update both its owning and borrowed slots.
func ReuseFilterExpressionExecutor(expression *plan.Expr, executor ExpressionExecutor, rowCount int) ExpressionExecutor {
	if rowCount < orLikeMinFirstBatchRows {
		return executor
	}
	return reuseORLikeSubjects(expression, executor)
}

type orLikeSubjectGroup struct {
	owner    *ExpressionExecutor
	original ExpressionExecutor
	subjects []*ExpressionExecutor
	first    *plan.Expr
	wrapper  *memoRootExpressionExecutor
	refs     []*memoExpressionExecutor
}

type orLikeSubjectDiscovery struct {
	remaining int
	exhausted bool
	groups    []orLikeSubjectGroup
}

// reuseORLikeSubjects shares one strictly admitted subject inside an eager OR.
// A root-local memo alone is insufficient: an OR below CASE can be invoked with
// a different mask within that root. Each OR therefore owns its own reset.
func reuseORLikeSubjects(expression *plan.Expr, executor ExpressionExecutor) ExpressionExecutor {
	discovery := orLikeSubjectDiscovery{remaining: orLikeDiscoveryBudget}
	discovery.visit(expression, executor, nil)
	if discovery.exhausted || len(discovery.groups) == 0 {
		return executor
	}
	// Prepare every replacement before mutating or freeing any original node.
	for i := range discovery.groups {
		group := &discovery.groups[i]
		state := &memoExpressionState{executor: *group.subjects[0], refs: len(group.subjects)}
		group.wrapper = &memoRootExpressionExecutor{
			executor: group.original,
			states:   []*memoExpressionState{state},
		}
		group.refs = make([]*memoExpressionExecutor, len(group.subjects))
		for j := range group.refs {
			group.refs[j] = &memoExpressionExecutor{state: state}
		}
	}
	for i := range discovery.groups {
		group := &discovery.groups[i]
		for j, slot := range group.subjects {
			original := *slot
			*slot = group.refs[j]
			if j != 0 {
				original.Free()
			}
		}
		if group.owner == nil {
			executor = group.wrapper
		} else {
			*group.owner = group.wrapper
		}
	}
	return executor
}

func (d *orLikeSubjectDiscovery) takeNode() bool {
	if d.remaining == 0 {
		d.exhausted = true
		return false
	}
	d.remaining--
	return true
}

func (d *orLikeSubjectDiscovery) visit(expression *plan.Expr, executor ExpressionExecutor, owner *ExpressionExecutor) {
	if d.exhausted || !d.takeNode() || expression == nil || expression.AuxId < 0 || expression.PreparedNumeric != nil {
		return
	}
	fn, ok := pairedExpressionFunction(expression, executor)
	if !ok {
		return // Existing memo/deferred executors are deliberately opaque.
	}
	if fn.fid == function.OR {
		group := orLikeSubjectGroup{owner: owner, original: executor}
		literalBytes := 0
		if d.collect(expression, executor, &group, &literalBytes, false) && len(group.subjects) >= 2 {
			d.groups = append(d.groups, group)
			return // Do not create overlapping owners inside an eligible OR.
		}
	}
	for i, argument := range expression.GetF().Args {
		d.visit(argument, fn.parameterExecutor[i], &fn.parameterExecutor[i])
		if d.exhausted {
			return
		}
	}
}

// collect validates a whole OR candidate before recording any replacement.
func (d *orLikeSubjectDiscovery) collect(expression *plan.Expr, executor ExpressionExecutor, group *orLikeSubjectGroup, literalBytes *int, count bool) bool {
	if (count && !d.takeNode()) || expression == nil || expression.AuxId < 0 || expression.PreparedNumeric != nil {
		return false
	}
	fn, ok := pairedExpressionFunction(expression, executor)
	if !ok || !plainExpressionFunction(fn) || expression.Typ.Id != int32(types.T_bool) {
		return false
	}
	arguments := expression.GetF().Args
	if fn.fid == function.OR {
		if int32(fn.overloadID) != 0 || len(arguments) < 2 || len(arguments) > orLikeMaxTerms {
			return false
		}
		for i, argument := range arguments {
			if !d.collect(argument, fn.parameterExecutor[i], group, literalBytes, true) {
				return false
			}
		}
		return true
	}
	if fn.fid != function.LIKE || len(arguments) != 2 || len(group.subjects) == orLikeMaxTerms {
		return false
	}
	_, overload := function.DecodeOverloadID(fn.overloadID)
	if overload < 0 || overload > 2 {
		return false
	}
	// The binder preserves legal string domains independently: mixed TEXT and
	// VARCHAR arguments legitimately select overload 0 without inserted casts.
	if !orLikeStringType(arguments[0]) || !orLikeStringType(arguments[1]) {
		return false
	}
	if !d.takeNode() || !orLikeStringLiteral(arguments[1], fn.parameterExecutor[1], false, literalBytes) {
		return false
	}
	nodes, rowDependent := 0, false
	if !d.subject(arguments[0], fn.parameterExecutor[0], 1, &nodes, literalBytes, &rowDependent, group.first) || !rowDependent || arguments[0].GetF() == nil {
		return false
	}
	// Size is safe only after the executable tree and hidden literal sources
	// were checked. Also bound strings/unknown protobuf fields in identity.
	if arguments[0].ProtoSize() > orLikeMaxIdentityBytes {
		return false
	}
	if group.first == nil {
		group.first = arguments[0]
	} else if !proto.Equal(group.first, arguments[0]) {
		return false
	}
	for _, previous := range group.subjects {
		if *previous == fn.parameterExecutor[0] {
			return false
		}
	}
	group.subjects = append(group.subjects, &fn.parameterExecutor[0])
	return true
}

func orLikeStringType(expression *plan.Expr) bool {
	if expression == nil {
		return false
	}
	switch types.T(expression.Typ.Id) {
	case types.T_char, types.T_varchar, types.T_text:
		return true
	default:
		return false
	}
}

func orLikeStringLiteral(expression *plan.Expr, executor ExpressionExecutor, nullable bool, literalBytes *int) bool {
	if !orLikeStringType(expression) || expression.AuxId < 0 || expression.PreparedNumeric != nil {
		return false
	}
	if _, ok := executor.(*FixedVectorExpressionExecutor); !ok {
		return false
	}
	literal := expression.GetLit()
	if literal == nil || literal.Src != nil {
		return false
	}
	var value *plan.Literal_Sval
	if literal.Value != nil {
		var ok bool
		value, ok = literal.Value.(*plan.Literal_Sval)
		if !ok || value == nil || len(value.Sval) > orLikeMaxLiteralBytes-*literalBytes {
			return false
		}
		*literalBytes += len(value.Sval)
	}
	if literal.Isnull {
		return nullable
	}
	return value != nil
}

func pairedExpressionFunction(expression *plan.Expr, executor ExpressionExecutor) (*FunctionExpressionExecutor, bool) {
	if expression == nil || expression.GetF() == nil || expression.GetF().Func == nil {
		return nil, false
	}
	fn, ok := executor.(*FunctionExpressionExecutor)
	if !ok || fn.overloadID != expression.GetF().Func.Obj || len(fn.parameterExecutor) != len(expression.GetF().Args) {
		return nil, false
	}
	return fn, true
}

func plainExpressionFunction(fn *FunctionExpressionExecutor) bool {
	if fn.volatile || fn.timeDependent || fn.rowScopedDiagnostics || fn.stringToNumericCast || fn.rowScopedDecimalCast {
		return false
	}
	overload, ok := function.GetFunctionByIdWithoutError(fn.overloadID)
	return ok && !overload.CannotFold() && !overload.IsRealTimeRelated() && !overload.HasRowScopedDiagnostics()
}

func orLikeNullSubstitutionMetadata(expression *plan.Expr) bool {
	metadata := expression.PreparedNumeric
	if metadata == nil {
		return true
	}
	fn := expression.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	fid, _ := function.DecodeOverloadID(fn.Func.Obj)
	if fid != function.CASE && fid != function.IFF {
		return false
	}
	// Ordinary IFNULL carries this planner-only marker. Reject hidden expression
	// provenance before Size/Equal, and bound unknown bytes before comparison.
	return metadata.StringDomainSource == nil && metadata.ProtoSize() == 2 &&
		proto.Equal(metadata, &plan.PreparedNumericMetadata{IfnullCommonValue: true})
}

func (d *orLikeSubjectDiscovery) subject(expression *plan.Expr, executor ExpressionExecutor, depth int, nodes, literalBytes *int, rowDependent *bool, expected *plan.Expr) bool {
	if !d.takeNode() || depth > orLikeMaxSubjectDepth || *nodes == orLikeMaxSubjectNodes ||
		!orLikeStringType(expression) || expression.AuxId < 0 || !orLikeNullSubstitutionMetadata(expression) {
		return false
	}
	*nodes++
	if reference := expression.GetCol(); reference != nil {
		_, ok := executor.(*ColumnExpressionExecutor)
		*rowDependent = true
		if !ok || reference.RelPos < 0 || reference.ColPos < 0 {
			return false
		}
		if expected != nil {
			previous := expected.GetCol()
			return previous != nil && previous.RelPos == reference.RelPos && previous.ColPos == reference.ColPos
		}
		return true
	}
	if expression.GetLit() != nil {
		return orLikeStringLiteral(expression, executor, true, literalBytes)
	}
	fn, ok := pairedExpressionFunction(expression, executor)
	if !ok || !plainExpressionFunction(fn) {
		return false
	}
	arguments := expression.GetF().Args
	if expected != nil {
		previous := expected.GetF()
		if previous == nil || previous.Func == nil || len(previous.Args) != len(arguments) {
			return false
		}
		previousID, _ := function.DecodeOverloadID(previous.Func.Obj)
		if previousID != fn.fid {
			return false
		}
	}
	_, overload := function.DecodeOverloadID(fn.overloadID)
	switch fn.fid {
	case function.LOWER:
		if overload != 0 || len(arguments) != 1 {
			return false
		}
	case function.CONCAT:
		if overload != 0 || len(arguments) == 0 || len(arguments) > orLikeMaxSubjectNodes {
			return false
		}
	case function.COALESCE:
		if len(arguments) < 2 || len(arguments) > orLikeMaxTerms {
			return false
		}
		// Encoded overload IDs use registry positions, not the overloadId field.
		// Read the actual signature, including retained noncanonical overloads.
		registered, ok := function.GetFunctionByIdWithoutError(fn.overloadID)
		if !ok {
			return false
		}
		argumentType, ok := registered.GetArgumentType(0)
		if !ok || (argumentType != types.T_char && argumentType != types.T_varchar && argumentType != types.T_text) {
			return false
		}
	case function.CASE, function.IFF:
		if overload != 0 || len(arguments) != 3 || arguments[2].GetCol() == nil || arguments[1].GetLit() == nil {
			return false
		}
		condition, ok := pairedExpressionFunction(arguments[0], fn.parameterExecutor[0])
		if !ok || !plainExpressionFunction(condition) || condition.fid != function.ISNULL || int32(condition.overloadID) != 0 ||
			arguments[0].AuxId < 0 || arguments[0].PreparedNumeric != nil || arguments[0].Typ.Id != int32(types.T_bool) || len(arguments[0].GetF().Args) != 1 {
			return false
		}
		if !d.takeNode() || depth+1 > orLikeMaxSubjectDepth || *nodes == orLikeMaxSubjectNodes {
			return false
		}
		*nodes++
		conditionColumn := arguments[0].GetF().Args[0]
		previousColumn := orLikeExpectedArgument(orLikeExpectedArgument(expected, 0), 0)
		if conditionColumn.GetCol() == nil || !d.subject(conditionColumn, condition.parameterExecutor[0], depth+2, nodes, literalBytes, rowDependent, previousColumn) {
			return false
		}
		for i := 1; i < 3; i++ {
			if !d.subject(arguments[i], fn.parameterExecutor[i], depth+1, nodes, literalBytes, rowDependent, orLikeExpectedArgument(expected, i)) {
				return false
			}
		}
		return conditionColumn.ProtoSize() <= orLikeMaxIdentityBytes && arguments[2].ProtoSize() <= orLikeMaxIdentityBytes &&
			proto.Equal(conditionColumn, arguments[2])
	default:
		return false
	}
	coalesceValueSeen := false
	for i, argument := range arguments {
		argumentRowDependent := false
		if !d.subject(argument, fn.parameterExecutor[i], depth+1, nodes, literalBytes, &argumentRowDependent, orLikeExpectedArgument(expected, i)) {
			return false
		}
		// Generic LOWER/CONCAT folding requires every argument to fold. COALESCE
		// can stop before a later column, so only its first value proves that the
		// existing folder must reach a row-dependent argument. Skip explicit NULL
		// literals only; admission never evaluates a constant function for NULL.
		if fn.fid != function.COALESCE || !coalesceValueSeen {
			*rowDependent = *rowDependent || argumentRowDependent
			coalesceValueSeen = argument.GetLit() == nil || !argument.GetLit().Isnull
		}
	}
	return true
}

// The model subject was fully validated before it can be used for cheap
// necessary-mismatch checks. Full bounded proto.Equal still decides identity.
func orLikeExpectedArgument(expression *plan.Expr, index int) *plan.Expr {
	if expression == nil || expression.GetF() == nil || index >= len(expression.GetF().Args) {
		return nil
	}
	return expression.GetF().Args[index]
}
