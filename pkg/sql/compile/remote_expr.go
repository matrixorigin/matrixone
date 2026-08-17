// Copyright 2021 Matrix Origin
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

package compile

import (
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	planExprPtrType   = reflect.TypeOf((*plan.Expr)(nil))
	operatorBaseType  = reflect.TypeOf(vm.OperatorBase{})
	operatorBasePType = reflect.TypeOf((*vm.OperatorBase)(nil))
)

type argExpressionsGetter interface {
	GetArgExpressions() []*plan.Expr
}

type argExpressionsRewriter interface {
	RewriteArgExpressions(func(*plan.Expr) (*plan.Expr, bool, error)) (bool, error)
}

type lockRowsExpressionsGetter interface {
	GetLockRowsExpressions() []*plan.Expr
}

type lockRowsExpressionsRewriter interface {
	RewriteLockRowsExpressions(func(*plan.Expr) (*plan.Expr, bool, error)) (bool, error)
}

func scopeContainsVarExpr(s *Scope) bool {
	if s == nil {
		return false
	}
	if sourceContainsVarExpr(s.DataSource) {
		return true
	}

	found := false
	if s.RootOp != nil {
		_ = vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
			if operatorContainsVarExpr(op) {
				found = true
			}
			return nil
		})
		if found {
			return true
		}
	}

	for _, pre := range s.PreScopes {
		if scopeContainsVarExpr(pre) {
			return true
		}
	}
	return false
}

func foldVarExprsInScope(s *Scope, proc *process.Process) (bool, error) {
	if s == nil {
		return false, nil
	}

	folded, err := foldVarExprsInValue(reflect.ValueOf(s.DataSource), nil, proc)
	if err != nil {
		return false, err
	}

	if s.RootOp != nil {
		var opErr error
		_ = vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
			var opFolded bool
			opFolded, opErr = foldVarExprsInValue(reflect.ValueOf(op), nil, proc)
			folded = folded || opFolded
			return opErr
		})
		if opErr != nil {
			return false, opErr
		}
	}

	for _, pre := range s.PreScopes {
		preFolded, err := foldVarExprsInScope(pre, proc)
		if err != nil {
			return false, err
		}
		folded = folded || preFolded
	}
	return folded, nil
}

func foldVarExprsInRemoteRunScope(s *Scope, proc *process.Process) (*Scope, bool, error) {
	if !scopeContainsVarExpr(s) {
		return s, false, nil
	}
	copied := copyScopeForVarExprFold(s)
	folded, err := foldVarExprsInScope(copied, proc)
	return copied, folded, err
}

func copyBlockFiltersForRemoteRun(s *Scope) *Scope {
	if s == nil {
		return nil
	}
	var copied *Scope
	preScopesCopied := false
	var blockFilters []*plan.Expr
	if s.DataSource != nil {
		// DataSource.BlockFilterList contains coordinator-owned Fold IDs after
		// InitAllDataSource. Send the original expressions from the plan node so
		// the remote Compile can create and evaluate its own Fold executors. This
		// also preserves empty scalar literals across protobuf serialization.
		if s.DataSource.node != nil && len(s.DataSource.node.BlockFilterList) > 0 {
			blockFilters = s.DataSource.node.BlockFilterList
		} else {
			blockFilters = s.DataSource.BlockFilterList
		}
	}
	if len(blockFilters) > 0 {
		value := *s
		dataSource := *s.DataSource
		dataSource.BlockFilterList = plan2.DeepCopyExprList(blockFilters)
		value.DataSource = &dataSource
		copied = &value
	}
	for i := range s.PreScopes {
		pre := copyBlockFiltersForRemoteRun(s.PreScopes[i])
		if pre != s.PreScopes[i] {
			if copied == nil {
				value := *s
				copied = &value
			}
			if !preScopesCopied {
				copied.PreScopes = append([]*Scope(nil), s.PreScopes...)
				preScopesCopied = true
			}
			copied.PreScopes[i] = pre
		}
	}
	if copied == nil {
		return s
	}
	return copied
}

func copyScopeForVarExprFold(s *Scope) *Scope {
	if s == nil {
		return nil
	}
	copied := *s
	if s.DataSource != nil {
		dataSource := *s.DataSource
		copied.DataSource = &dataSource
	}
	if s.RootOp != nil {
		copied.RootOp = copyOperatorTreeForVarExprFold(s.RootOp)
	}
	if len(s.PreScopes) > 0 {
		copied.PreScopes = make([]*Scope, len(s.PreScopes))
		for i := range s.PreScopes {
			copied.PreScopes[i] = copyScopeForVarExprFold(s.PreScopes[i])
		}
	}
	return &copied
}

func copyOperatorTreeForVarExprFold(op vm.Operator) vm.Operator {
	if op == nil {
		return nil
	}
	value := reflect.ValueOf(op)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return op
	}
	copiedValue := reflect.New(value.Elem().Type())
	copiedValue.Elem().Set(value.Elem())
	copied := copiedValue.Interface().(vm.Operator)
	copied.GetOperatorBase().SetChildren(nil)
	for i := 0; i < op.GetOperatorBase().NumChildren(); i++ {
		copied.GetOperatorBase().AppendChild(copyOperatorTreeForVarExprFold(op.GetOperatorBase().GetChildren(i)))
	}
	return copied
}

func sourceContainsVarExpr(source *Source) bool {
	if source == nil {
		return false
	}
	return containsVarExpr(source.FilterExpr) ||
		containsVarExprInValue(reflect.ValueOf(source.FilterList), nil) ||
		containsVarExprInValue(reflect.ValueOf(source.BlockFilterList), nil) ||
		containsVarExprInValue(reflect.ValueOf(source.RuntimeFilterSpecs), nil) ||
		containsVarExprInValue(reflect.ValueOf(source.OrderBy), nil) ||
		containsVarExprInValue(reflect.ValueOf(source.IndexReaderParam), nil)
}

func operatorContainsVarExpr(op vm.Operator) bool {
	return containsVarExprInValue(reflect.ValueOf(op), nil)
}

func containsVarExpr(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if _, ok := expr.Expr.(*plan.Expr_V); ok {
		return true
	}
	return containsVarExprInValue(reflect.ValueOf(expr.Expr), nil)
}

func containsVarExprInValue(v reflect.Value, seen map[uintptr]struct{}) bool {
	if !v.IsValid() {
		return false
	}

	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return false
		}
		return containsVarExprInValue(v.Elem(), seen)
	}

	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return false
		}
		if v.Type() == planExprPtrType {
			return containsVarExpr(v.Interface().(*plan.Expr))
		}
		if containsVarExprInHiddenExpressions(v) {
			return true
		}
		if seen == nil {
			seen = make(map[uintptr]struct{})
		}
		ptr := v.Pointer()
		if _, ok := seen[ptr]; ok {
			return false
		}
		seen[ptr] = struct{}{}
		return containsVarExprInValue(v.Elem(), seen)
	}

	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if containsVarExprInValue(v.Index(i), seen) {
				return true
			}
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			if containsVarExprInValue(iter.Key(), seen) || containsVarExprInValue(iter.Value(), seen) {
				return true
			}
		}
	case reflect.Struct:
		if v.Type() == operatorBaseType || v.Type() == operatorBasePType {
			return false
		}
		if containsVarExprInHiddenExpressions(v) {
			return true
		}
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			if !field.IsExported() || field.Name == "OperatorBase" {
				continue
			}
			if containsVarExprInValue(v.Field(i), seen) {
				return true
			}
		}
	}

	return false
}

func foldVarExprsInExpr(expr *plan.Expr, proc *process.Process) (*plan.Expr, bool, error) {
	if expr == nil {
		return nil, false, nil
	}
	if !containsVarExpr(expr) {
		return expr, false, nil
	}
	expr = plan2.DeepCopyExpr(expr)
	folded, err := foldVarExprsInExprInPlace(expr, proc)
	return expr, folded, err
}

func foldVarExprsInExprInPlace(expr *plan.Expr, proc *process.Process) (bool, error) {
	if expr == nil {
		return false, nil
	}
	// A user-variable reference is folded on the initiating CN before the
	// scope is sent to a remote CN.  The remote process deliberately has no
	// frontend session, so a numeric cast there cannot append its MySQL warning
	// (1292) to SHOW WARNINGS.  Evaluate the complete foldable expression here
	// while the original session is still attached, solely to publish that
	// diagnostic.  Evaluating the complete expression (instead of every nested
	// cast) preserves flow-control semantics: an unselected IF/CASE branch must
	// not produce a warning.  The expression is still folded and executed
	// remotely below; this does not change its value or execution ownership.
	appendRemoteNumericCoercionDiagnostic(expr, proc)
	return foldVarExprsInExprInPlaceNoDiagnostic(expr, proc)
}

func foldVarExprsInExprInPlaceNoDiagnostic(expr *plan.Expr, proc *process.Process) (bool, error) {
	if expr == nil {
		return false, nil
	}
	if _, ok := expr.Expr.(*plan.Expr_V); ok {
		vec, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
		if err != nil {
			return false, err
		}
		defer free()

		lit := rule.GetConstantValue(vec, false, 0)
		if lit == nil {
			return false, nil
		}
		expr.Expr = &plan.Expr_Lit{Lit: lit}
		return true, nil
	}
	return foldVarExprsInValueWithDiagnostics(reflect.ValueOf(expr.Expr), nil, proc, false)
}

func isRemoteNumericCast(f *plan.Function, expr *plan.Expr) bool {
	if f == nil || f.Func == nil || expr == nil {
		return false
	}
	if f.Func.GetObjName() != "cast" {
		return false
	}
	return types.T(expr.Typ.Id).ToType().IsNumeric()
}

// remoteExprHasOnlyFoldableInputs prevents the diagnostic-only evaluation from
// trying to read columns or other row-dependent expressions from the empty
// const-fold batch. The actual expression is still folded normally below.
func remoteExprHasOnlyFoldableInputs(expr *plan.Expr) bool {
	if expr == nil {
		return true
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Lit, *plan.Expr_V, *plan.Expr_T, *plan.Expr_Vec:
		return true
	case *plan.Expr_F:
		if e.F == nil {
			return true
		}
		for _, arg := range e.F.Args {
			if !remoteExprHasOnlyFoldableInputs(arg) {
				return false
			}
		}
		return true
	case *plan.Expr_List:
		if e.List == nil {
			return true
		}
		for _, item := range e.List.List {
			if !remoteExprHasOnlyFoldableInputs(item) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func remoteExprContainsNumericCast(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if f := expr.GetF(); f != nil {
		if isRemoteNumericCast(f, expr) {
			return true
		}
		for _, arg := range f.Args {
			if remoteExprContainsNumericCast(arg) {
				return true
			}
		}
		return false
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if remoteExprContainsNumericCast(item) {
				return true
			}
		}
	}
	return false
}

func remoteExprFunctionsCanFold(expr *plan.Expr, proc *process.Process) bool {
	if expr == nil {
		return true
	}
	if f := expr.GetF(); f != nil {
		if f.Func == nil || proc == nil {
			return false
		}
		overload, err := function.GetFunctionById(proc.Ctx, f.Func.Obj)
		if err != nil || overload.CannotFold() || overload.IsRealTimeRelated() {
			return false
		}
		for _, arg := range f.Args {
			if !remoteExprFunctionsCanFold(arg, proc) {
				return false
			}
		}
		return true
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if !remoteExprFunctionsCanFold(item, proc) {
				return false
			}
		}
	}
	return true
}

func appendRemoteNumericCoercionDiagnostic(expr *plan.Expr, proc *process.Process) {
	if proc == nil || proc.GetSession() == nil ||
		!remoteExprContainsNumericCast(expr) ||
		!remoteExprHasOnlyFoldableInputs(expr) ||
		!remoteExprFunctionsCanFold(expr, proc) {
		return
	}
	if _, free, err := colexec.GetReadonlyResultFromExpression(
		proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch}); err == nil {
		free()
	}
}

func foldVarExprInSettableValue(v reflect.Value, proc *process.Process) (bool, error) {
	if !v.IsValid() || v.Type() != planExprPtrType {
		return false, nil
	}
	expr, _ := v.Interface().(*plan.Expr)
	foldedExpr, folded, err := foldVarExprsInExpr(expr, proc)
	if err != nil || !folded {
		return false, err
	}
	if !v.CanSet() {
		return false, nil
	}
	v.Set(reflect.ValueOf(foldedExpr))
	return true, nil
}

func foldVarExprsInValue(v reflect.Value, seen map[uintptr]struct{}, proc *process.Process) (bool, error) {
	return foldVarExprsInValueWithDiagnostics(v, seen, proc, true)
}

func foldVarExprsInValueWithDiagnostics(
	v reflect.Value,
	seen map[uintptr]struct{},
	proc *process.Process,
	diagnostics bool,
) (bool, error) {
	if !v.IsValid() {
		return false, nil
	}

	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return false, nil
		}
		return foldVarExprsInValueWithDiagnostics(v.Elem(), seen, proc, diagnostics)
	}

	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return false, nil
		}
		if v.Type() == planExprPtrType {
			if diagnostics {
				return foldVarExprInSettableValue(v, proc)
			}
			expr, _ := v.Interface().(*plan.Expr)
			folded, err := foldVarExprsInExprInPlaceNoDiagnostic(expr, proc)
			foldedExpr := expr
			if err != nil || !folded || !v.CanSet() {
				return false, err
			}
			v.Set(reflect.ValueOf(foldedExpr))
			return true, nil
		}
		if v.CanSet() && containsVarExprInValue(v, nil) {
			copied := reflect.New(v.Type().Elem())
			copied.Elem().Set(v.Elem())
			v.Set(copied)
			v = copied
		}
		hiddenFolded, err := foldVarExprsInHiddenExpressions(v, proc)
		if err != nil {
			return false, err
		}
		if seen == nil {
			seen = make(map[uintptr]struct{})
		}
		ptr := v.Pointer()
		if _, ok := seen[ptr]; ok {
			return hiddenFolded, nil
		}
		seen[ptr] = struct{}{}
		folded, err := foldVarExprsInValueWithDiagnostics(v.Elem(), seen, proc, diagnostics)
		return hiddenFolded || folded, err
	}

	folded := false
	switch v.Kind() {
	case reflect.Slice:
		if !containsVarExprInValue(v, nil) || !v.CanSet() {
			return false, nil
		}
		copied := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		reflect.Copy(copied, v)
		for i := 0; i < copied.Len(); i++ {
			itemFolded, err := foldVarExprsInValueWithDiagnostics(copied.Index(i), seen, proc, diagnostics)
			if err != nil {
				return false, err
			}
			folded = folded || itemFolded
		}
		if folded {
			v.Set(copied)
		}
	case reflect.Array:
		for i := 0; i < v.Len(); i++ {
			itemFolded, err := foldVarExprsInValueWithDiagnostics(v.Index(i), seen, proc, diagnostics)
			if err != nil {
				return false, err
			}
			folded = folded || itemFolded
		}
	case reflect.Map:
		if !containsVarExprInValue(v, nil) || !v.CanSet() {
			return false, nil
		}
		copied := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			value := iter.Value()
			newValue := value
			valueFolded := false
			if value.Type() == planExprPtrType {
				expr, _ := value.Interface().(*plan.Expr)
				var foldedExpr *plan.Expr
				var exprFolded bool
				var err error
				if diagnostics {
					foldedExpr, exprFolded, err = foldVarExprsInExpr(expr, proc)
				} else {
					foldedExpr = plan2.DeepCopyExpr(expr)
					exprFolded, err = foldVarExprsInExprInPlaceNoDiagnostic(foldedExpr, proc)
				}
				if err != nil {
					return false, err
				}
				if exprFolded {
					newValue = reflect.ValueOf(foldedExpr)
					valueFolded = true
				}
			}
			copied.SetMapIndex(iter.Key(), newValue)
			folded = folded || valueFolded
		}
		if folded {
			v.Set(copied)
		}
	case reflect.Struct:
		if v.Type() == operatorBaseType || v.Type() == operatorBasePType {
			return false, nil
		}
		hiddenFolded, err := foldVarExprsInHiddenExpressions(v, proc)
		if err != nil {
			return false, err
		}
		folded = folded || hiddenFolded
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			if !field.IsExported() || field.Name == "OperatorBase" {
				continue
			}
			fieldFolded, err := foldVarExprsInValueWithDiagnostics(v.Field(i), seen, proc, diagnostics)
			if err != nil {
				return false, err
			}
			folded = folded || fieldFolded
		}
	}
	return folded, nil
}

func foldVarExprsInHiddenExpressions(v reflect.Value, proc *process.Process) (bool, error) {
	if !v.IsValid() {
		return false, nil
	}
	if v.CanInterface() {
		folded, err := foldVarExprsInExpressionGetters(v.Interface(), proc)
		if err != nil || folded {
			return folded, err
		}
	}
	if v.Kind() != reflect.Pointer && v.CanAddr() && v.Addr().CanInterface() {
		return foldVarExprsInExpressionGetters(v.Addr().Interface(), proc)
	}
	return false, nil
}

func foldVarExprsInExpressionGetters(value any, proc *process.Process) (bool, error) {
	folded := false
	if rewriter, ok := value.(argExpressionsRewriter); ok {
		rewritten, err := rewriter.RewriteArgExpressions(func(expr *plan.Expr) (*plan.Expr, bool, error) {
			return foldVarExprsInExpr(expr, proc)
		})
		if err != nil {
			return false, err
		}
		folded = folded || rewritten
	}
	if rewriter, ok := value.(lockRowsExpressionsRewriter); ok {
		rewritten, err := rewriter.RewriteLockRowsExpressions(func(expr *plan.Expr) (*plan.Expr, bool, error) {
			return foldVarExprsInExpr(expr, proc)
		})
		if err != nil {
			return false, err
		}
		folded = folded || rewritten
	}
	return folded, nil
}

func containsVarExprInHiddenExpressions(v reflect.Value) bool {
	if !v.IsValid() {
		return false
	}
	if v.CanInterface() {
		if containsVarExprInExpressionGetters(v.Interface()) {
			return true
		}
	}
	if v.Kind() != reflect.Pointer && v.CanAddr() && v.Addr().CanInterface() {
		if containsVarExprInExpressionGetters(v.Addr().Interface()) {
			return true
		}
	}
	return false
}

func containsVarExprInExpressionGetters(value any) bool {
	if getter, ok := value.(argExpressionsGetter); ok {
		if containsVarExprInValue(reflect.ValueOf(getter.GetArgExpressions()), nil) {
			return true
		}
	}
	if getter, ok := value.(lockRowsExpressionsGetter); ok {
		if containsVarExprInValue(reflect.ValueOf(getter.GetLockRowsExpressions()), nil) {
			return true
		}
	}
	return false
}
