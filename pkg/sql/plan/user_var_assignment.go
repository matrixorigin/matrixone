package plan

import (
	"context"
	"reflect"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func containsUserVarAssignment(expr tree.Expr) bool {
	if expr == nil {
		return false
	}
	return containsUserVarAssignmentValue(reflect.ValueOf(expr), make(map[assignmentVisit]struct{}))
}

// containsUserVarAssignmentValue deliberately does not use tree.Expr.Accept:
// several legacy AST nodes (notably Subquery) still panic from Accept because
// they were never needed by the old visitors. Scope validation runs for every
// SELECT, so using Accept here would make unrelated queries panic. Reflection
// is limited to the AST's exported fields and is only used during binding.
type assignmentVisit struct {
	typ reflect.Type
	ptr uintptr
}

var userVarAssignmentExprType = reflect.TypeOf((*tree.UserVarAssignmentExpr)(nil))
var userVarExprType = reflect.TypeOf((*tree.VarExpr)(nil))
var funcExprType = reflect.TypeOf((*tree.FuncExpr)(nil))

func containsUserVarAssignmentValue(v reflect.Value, seen map[assignmentVisit]struct{}) bool {
	if !v.IsValid() {
		return false
	}
	for v.Kind() == reflect.Interface {
		if v.IsNil() {
			return false
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return false
		}
		if v.Type() == userVarAssignmentExprType {
			return true
		}
		visit := assignmentVisit{typ: v.Type(), ptr: v.Pointer()}
		if _, ok := seen[visit]; ok {
			return false
		}
		seen[visit] = struct{}{}
		return containsUserVarAssignmentValue(v.Elem(), seen)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			if !field.CanInterface() {
				continue
			}
			if containsUserVarAssignmentValue(field, seen) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if containsUserVarAssignmentValue(v.Index(i), seen) {
				return true
			}
		}
	}
	return false
}

func isUserVarAssignmentRoot(expr tree.Expr) bool {
	return userVarAssignmentRoot(expr) != nil
}

func userVarAssignmentRoot(expr tree.Expr) *tree.UserVarAssignmentExpr {
	for {
		paren, ok := expr.(*tree.ParenExpr)
		if !ok {
			break
		}
		expr = paren.Expr
	}
	assignment, _ := expr.(*tree.UserVarAssignmentExpr)
	return assignment
}

// containsUserVarAssignmentUnderAggregate identifies the one composition that
// cannot be made row-wise: an assignment nested in an aggregate/window input.
// The outer projection assignment remains supported, as do conditional
// assignments such as @a := IF(..., @b := 1, 2). Reflection keeps this check
// aligned with the full AST without relying on visitors that do not support
// every legacy expression node.
func containsUserVarAssignmentUnderAggregate(expr tree.Expr) bool {
	return containsUserVarAssignmentUnderAggregateValue(reflect.ValueOf(expr), false, make(map[assignmentVisit]struct{}))
}

func containsUserVarAssignmentUnderAggregateValue(
	v reflect.Value, aggregate bool, seen map[assignmentVisit]struct{},
) bool {
	if !v.IsValid() {
		return false
	}
	for v.Kind() == reflect.Interface {
		if v.IsNil() {
			return false
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return false
		}
		if v.Type() == userVarAssignmentExprType {
			if aggregate {
				return true
			}
		}
		childAggregate := aggregate
		if v.Type() == funcExprType {
			fn := v.Interface().(*tree.FuncExpr)
			childAggregate = childAggregate || fn.WindowSpec != nil
			name := ""
			if fn.FuncName != nil {
				name = fn.FuncName.Compare()
			} else if fn.Func != nil {
				if ref, ok := fn.Func.FunctionReference.(*tree.UnresolvedName); ok && ref != nil {
					name = ref.ColName()
				}
			}
			childAggregate = childAggregate || function.GetFunctionIsAggregateByName(name)
		}
		visit := assignmentVisit{typ: v.Type(), ptr: v.Pointer()}
		if _, ok := seen[visit]; ok {
			return false
		}
		seen[visit] = struct{}{}
		return containsUserVarAssignmentUnderAggregateValue(v.Elem(), childAggregate, seen)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			if field.CanInterface() && containsUserVarAssignmentUnderAggregateValue(field, aggregate, seen) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if containsUserVarAssignmentUnderAggregateValue(v.Index(i), aggregate, seen) {
				return true
			}
		}
	}
	return false
}

func userVariableReadsBeforeAssignment(expr tree.Expr) map[string]struct{} {
	return userVariableReadsBeforeAssignmentValue(reflect.ValueOf(expr))
}

func userVariableReadsBeforeAssignmentValue(v reflect.Value) map[string]struct{} {
	names := make(map[string]struct{})
	collectUserVariableReads(v, make(map[assignmentVisit]struct{}), names)
	return names
}

func userVarAssignmentNames(expr tree.Expr) map[string]struct{} {
	names := make(map[string]struct{})
	collectUserVarAssignmentNames(reflect.ValueOf(expr), make(map[assignmentVisit]struct{}), names)
	return names
}

func collectUserVarAssignmentNames(
	v reflect.Value, seen map[assignmentVisit]struct{}, names map[string]struct{},
) {
	if !v.IsValid() {
		return
	}
	for v.Kind() == reflect.Interface {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return
		}
		if v.Type() == userVarAssignmentExprType {
			assignment := v.Interface().(*tree.UserVarAssignmentExpr)
			names[strings.ToLower(assignment.Name)] = struct{}{}
		}
		visit := assignmentVisit{typ: v.Type(), ptr: v.Pointer()}
		if _, ok := seen[visit]; ok {
			return
		}
		seen[visit] = struct{}{}
		collectUserVarAssignmentNames(v.Elem(), seen, names)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			if !field.CanInterface() {
				continue
			}
			collectUserVarAssignmentNames(field, seen, names)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			collectUserVarAssignmentNames(v.Index(i), seen, names)
		}
	}
}

func collectUserVariableReads(
	v reflect.Value, seen map[assignmentVisit]struct{}, names map[string]struct{},
) {
	if !v.IsValid() {
		return
	}
	for v.Kind() == reflect.Interface {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return
		}
		if v.Type() == userVarExprType {
			varExpr := v.Interface().(*tree.VarExpr)
			if !varExpr.System {
				names[strings.ToLower(varExpr.Name)] = struct{}{}
			}
		}
		visit := assignmentVisit{typ: v.Type(), ptr: v.Pointer()}
		if _, ok := seen[visit]; ok {
			return
		}
		seen[visit] = struct{}{}
		collectUserVariableReads(v.Elem(), seen, names)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			if !field.CanInterface() {
				continue
			}
			collectUserVariableReads(field, seen, names)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			collectUserVariableReads(v.Index(i), seen, names)
		}
	}
}

func userVarAssignmentNotSupported(ctx context.Context, scope string) error {
	return moerr.NewNotSupported(ctx,
		"user-variable assignment is supported only as a top-level SELECT projection; "+scope+" is not supported")
}

// rejectUserVarAssignmentInDML is the statement-level admission fence for the
// side-effecting expression.  INSERT VALUES (and the other DML binders) do not
// pass through bindSelect's SELECT-clause scope validator: they bind value
// expressions directly with DefaultBinder/UpdateBinder.  Keep the fence at
// the public BuildPlan boundary as well, so a later DML fallback cannot turn a
// rejected assignment back into an executable plan or bypass retry
// classification.
func rejectUserVarAssignmentInDML(ctx context.Context, stmt tree.Statement, scope string) error {
	if stmt == nil || !containsUserVarAssignmentValue(reflect.ValueOf(stmt), make(map[assignmentVisit]struct{})) {
		return nil
	}
	return userVarAssignmentNotSupported(ctx, scope)
}

func rejectUserVarAssignmentInView(ctx context.Context, stmt tree.Statement) error {
	return rejectUserVarAssignmentInDML(ctx, stmt, "view definitions")
}

func validateUserVarAssignmentScope(
	ctx context.Context,
	clause *tree.SelectClause,
	astOrderBy tree.OrderBy,
	astLimit *tree.Limit,
) error {
	if clause == nil {
		return nil
	}
	hasAssignment := false
	assignmentProjections := 0
	priorUserVariableReads := make(map[string]struct{})
	assignedUserVariables := make(map[string]struct{})
	for _, selectExpr := range clause.Exprs {
		if containsUserVarAssignment(selectExpr.Expr) {
			hasAssignment = true
			if !isUserVarAssignmentRoot(selectExpr.Expr) {
				return userVarAssignmentNotSupported(ctx, "a nested projection expression")
			}
			if containsUserVarAssignmentUnderAggregate(selectExpr.Expr) {
				return userVarAssignmentNotSupported(ctx, "an aggregate or window argument")
			}
			for assignedName := range userVarAssignmentNames(selectExpr.Expr) {
				assignedUserVariables[assignedName] = struct{}{}
				if _, readBefore := priorUserVariableReads[assignedName]; readBefore {
					return userVarAssignmentNotSupported(ctx,
						"reading a user variable before its assignment projection")
				}
			}
			assignmentProjections++
		} else if !hasAssignment {
			for name := range userVariableReadsBeforeAssignment(selectExpr.Expr) {
				priorUserVariableReads[name] = struct{}{}
			}
		}
	}
	if clause.Where != nil && containsUserVarAssignment(clause.Where.Expr) {
		return userVarAssignmentNotSupported(ctx, "WHERE")
	}
	if hasAssignment && clause.Where != nil {
		for name := range userVariableReadsBeforeAssignment(clause.Where.Expr) {
			if _, assigned := assignedUserVariables[name]; assigned {
				return userVarAssignmentNotSupported(ctx,
					"reading a user variable in WHERE before its assignment projection")
			}
		}
	}
	if clause.From != nil {
		if containsUserVarAssignmentValue(reflect.ValueOf(clause.From), make(map[assignmentVisit]struct{})) {
			return userVarAssignmentNotSupported(ctx, "FROM/JOIN")
		}
		if hasAssignment {
			for name := range userVariableReadsBeforeAssignmentValue(reflect.ValueOf(clause.From)) {
				if _, assigned := assignedUserVariables[name]; assigned {
					return userVarAssignmentNotSupported(ctx,
						"reading a user variable in JOIN ON before its assignment projection")
				}
			}
		}
	}
	if clause.GroupBy != nil {
		for _, exprs := range clause.GroupBy.GroupByExprsList {
			for _, expr := range exprs {
				if containsUserVarAssignment(expr) {
					return userVarAssignmentNotSupported(ctx, "GROUP BY")
				}
			}
		}
		for _, expr := range clause.GroupBy.GroupingSet {
			if containsUserVarAssignment(expr) {
				return userVarAssignmentNotSupported(ctx, "GROUP BY")
			}
		}
	}
	if clause.Having != nil && containsUserVarAssignment(clause.Having.Expr) {
		return userVarAssignmentNotSupported(ctx, "HAVING")
	}
	for _, definition := range clause.Windows {
		if definition == nil || definition.Spec == nil {
			continue
		}
		for _, expr := range definition.Spec.PartitionBy {
			if containsUserVarAssignment(expr) {
				return userVarAssignmentNotSupported(ctx, "WINDOW PARTITION BY")
			}
		}
		for _, order := range definition.Spec.OrderBy {
			if order != nil && containsUserVarAssignment(order.Expr) {
				return userVarAssignmentNotSupported(ctx, "WINDOW ORDER BY")
			}
		}
	}
	for _, order := range astOrderBy {
		if order != nil && containsUserVarAssignment(order.Expr) {
			return userVarAssignmentNotSupported(ctx, "ORDER BY")
		}
	}
	if !hasAssignment {
		return nil
	}
	// Assignment executors evaluate one projection row at a time, while the
	// ordinary projection operator evaluates sibling expressions over a whole
	// batch. Until those two boundaries share an explicit ordering contract,
	// accepting multiple assignment projections would make results depend on
	// the executor's batch partitioning. Reject the ambiguous form rather than
	// silently changing the value history when the physical plan is repartitioned.
	if assignmentProjections > 1 {
		return userVarAssignmentNotSupported(ctx, "multiple user-variable assignment projections")
	}
	if clause.Distinct {
		return userVarAssignmentNotSupported(ctx, "DISTINCT")
	}
	if len(astOrderBy) > 0 {
		return userVarAssignmentNotSupported(ctx, "ORDER BY with user-variable assignment")
	}
	if astLimit != nil {
		return userVarAssignmentNotSupported(ctx, "LIMIT with user-variable assignment")
	}
	return nil
}

func selectContainsUserVarAssignment(selectStmt *tree.Select) bool {
	if selectStmt == nil {
		return false
	}
	if containsUserVarAssignmentInSelectStatement(selectStmt.Select) {
		return true
	}
	for _, order := range selectStmt.OrderBy {
		if order != nil && containsUserVarAssignment(order.Expr) {
			return true
		}
	}
	return false
}

func containsUserVarAssignmentInSelectStatement(stmt tree.SelectStatement) bool {
	switch selectStmt := stmt.(type) {
	case *tree.SelectClause:
		for _, expr := range selectStmt.Exprs {
			if containsUserVarAssignment(expr.Expr) {
				return true
			}
		}
		if selectStmt.Where != nil && containsUserVarAssignment(selectStmt.Where.Expr) {
			return true
		}
		if selectStmt.Having != nil && containsUserVarAssignment(selectStmt.Having.Expr) {
			return true
		}
	case *tree.UnionClause:
		return containsUserVarAssignmentInSelectStatement(selectStmt.Left) ||
			containsUserVarAssignmentInSelectStatement(selectStmt.Right)
	case *tree.ValuesClause:
		for _, row := range selectStmt.Rows {
			for _, expr := range row {
				if containsUserVarAssignment(expr) {
					return true
				}
			}
		}
	}
	return false
}
