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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// A user variable assigned from date-like text must keep its source domain
// through `+/- INTERVAL n UNIT` so the operator spelling reaches the existing
// date_add/date_sub rewrite, exactly like the literal and DATE_ADD forms.
// All queries below go through the real parser and full plan construction.

// issue31708Optimizer resolves the user variables as untyped runtime values,
// the state `SET @dt = '2026-01-15'` leaves in a variable.
func issue31708Optimizer(t *testing.T) Optimizer {
	optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
	optimizer.ctxt.ResolveVariableTypeFunc = func(varName string, isSystemVar, isGlobalVar bool) (Type, error) {
		if !isSystemVar && (varName == "dt" || varName == "n") {
			return makeSimplePlan2Type(types.T_any), nil
		}
		return (&MockCompilerContext{ctx: optimizer.ctxt.ctx}).ResolveVariableType(varName, isSystemVar, isGlobalVar)
	}
	return optimizer
}

func requireRootProjectionExpr(t *testing.T, logicPlan *Plan) *planpb.Expr {
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.NotEmpty(t, query.Steps)
	projection := query.Nodes[query.Steps[0]].ProjectList
	require.NotEmpty(t, projection)
	return projection[0]
}

func collectObjectNames(expr *planpb.Expr, names map[string]int) {
	if expr == nil {
		return
	}
	if function := expr.GetF(); function != nil {
		if function.Func != nil {
			names[function.Func.ObjName]++
		}
		for _, arg := range function.Args {
			collectObjectNames(arg, names)
		}
		return
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			collectObjectNames(item, names)
		}
	}
}

// requireProjectionFunction pins the projection on exactly one call of the
// given object so a numeric fallback cannot satisfy the assertions.
func requireProjectionFunction(t *testing.T, logicPlan *Plan, objectName string) *planpb.Expr {
	expr := requireRootProjectionExpr(t, logicPlan)
	names := make(map[string]int)
	collectObjectNames(expr, names)
	require.Equalf(t, 1, names[objectName],
		"projection must contain exactly one %s call, found %v", objectName, names)
	return expr
}

// findPlanFunction returns the first function node with the given object
// name, searching arguments and interval lists recursively.
func findPlanFunction(expr *planpb.Expr, objectName string) *planpb.Expr {
	if expr == nil {
		return nil
	}
	if function := expr.GetF(); function != nil {
		if function.Func != nil && function.Func.ObjName == objectName {
			return expr
		}
		for _, arg := range function.Args {
			if found := findPlanFunction(arg, objectName); found != nil {
				return found
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if found := findPlanFunction(item, objectName); found != nil {
				return found
			}
		}
	}
	return nil
}

// requireTemporalSourceVariable pins where the temporal function consumes its
// date-like input: the raw user variable with its own string text domain.
// Outer arithmetic result types are not a source oracle here, because the
// numeric var-type walkers propagate them to every descendant.
func requireTemporalSourceVariable(t *testing.T, expr *planpb.Expr, temporalFunc, varName string) {
	temporal := findPlanFunction(expr, temporalFunc)
	require.NotNilf(t, temporal, "%s must be present in the projection", temporalFunc)
	require.NotEmpty(t, temporal.GetF().Args)
	source := temporal.GetF().Args[0]
	variable := source.GetV()
	require.NotNilf(t, variable,
		"%s first argument must be the raw %s variable", temporalFunc, varName)
	require.Equal(t, varName, variable.Name)
	require.Truef(t, types.T(source.Typ.Id).IsMySQLString(),
		"%s first argument must keep the variable's text domain, got %s",
		temporalFunc, types.T(source.Typ.Id).ToType())
}

// requireUserVariableCastArgument asserts the operator argument is a numeric
// consumer of the named user variable through a real cast chain, so the
// enclosing numeric context keeps its own binding path.
func requireUserVariableCastArgument(t *testing.T, expr *planpb.Expr, op string, argIndex int, varName string) {
	node := findPlanFunction(expr, op)
	require.NotNilf(t, node, "%s must be present in the projection", op)
	operands := node.GetF().Args
	require.Greaterf(t, len(operands), argIndex, "%s must have argument %d", op, argIndex)
	current := operands[argIndex]
	require.Truef(t, types.T(current.Typ.Id).ToType().IsNumeric(),
		"%s argument %d must be a numeric consumer, got %s",
		op, argIndex, types.T(current.Typ.Id).ToType())
	for depth := 0; depth < 4; depth++ {
		if variable := current.GetV(); variable != nil {
			require.Equalf(t, varName, variable.Name,
				"%s argument %d must reach user variable %s", op, argIndex, varName)
			return
		}
		castFn := current.GetF()
		require.NotNilf(t, castFn,
			"%s argument %d must reach %s through casts only", op, argIndex, varName)
		require.Equal(t, "cast", castFn.Func.ObjName)
		require.NotEmpty(t, castFn.Args)
		current = castFn.Args[0]
	}
	t.Fatalf("%s argument %d never reaches %s within a bounded cast chain", op, argIndex, varName)
}

// The reported defect: binding `@dt + INTERVAL 1 DAY` established a pure
// numeric context over the whole subtree, prefix-converted the date text
// variable to DOUBLE, and then rejected the +/- with
// `invalid argument operator +, bad value [DOUBLE INTERVAL]` before the
// date rewrite could see the source text. The operator spelling must bind
// identically to DATE_ADD.
func TestUserVariablePlusIntervalBindsLikeDateAdd(t *testing.T) {
	logicPlan, err := runOneStmt(issue31708Optimizer(t), t, "select @dt + interval 1 day")
	require.NoError(t, err)
	operatorExpr := requireProjectionFunction(t, logicPlan, "date_add")

	dateAddPlan, err := runOneStmt(issue31708Optimizer(t), t, "select date_add(@dt, interval 1 day)")
	require.NoError(t, err)
	dateAddExpr := requireProjectionFunction(t, dateAddPlan, "date_add")
	require.Equal(t, dateAddExpr, operatorExpr,
		"@dt + INTERVAL must bind identically to DATE_ADD(@dt, INTERVAL)")

	varTypes := userVariableEffectiveTypes(logicPlan)
	require.Contains(t, varTypes, "dt")
	require.NotEqual(t, int32(types.T_float64), varTypes["dt"].Id,
		"time-domain user variable must not be prefix-converted to DOUBLE")
}

// Subtraction and the DATE_SUB spelling must stay interchangeable.
func TestUserVariableMinusIntervalBindsLikeDateSub(t *testing.T) {
	logicPlan, err := runOneStmt(issue31708Optimizer(t), t, "select @dt - interval 1 day")
	require.NoError(t, err)
	operatorExpr := requireProjectionFunction(t, logicPlan, "date_sub")

	dateSubPlan, err := runOneStmt(issue31708Optimizer(t), t, "select date_sub(@dt, interval 1 day)")
	require.NoError(t, err)
	dateSubExpr := requireProjectionFunction(t, dateSubPlan, "date_sub")
	require.Equal(t, dateSubExpr, operatorExpr,
		"@dt - INTERVAL must bind identically to DATE_SUB(@dt, INTERVAL)")
}

// `INTERVAL n unit + <date source>` is the legal reversed addition and must
// keep bound operand order of DATE_ADD.
func TestIntervalPlusUserVariableBindsLikeDateAdd(t *testing.T) {
	logicPlan, err := runOneStmt(issue31708Optimizer(t), t, "select interval 1 day + @dt")
	require.NoError(t, err)
	operatorExpr := requireProjectionFunction(t, logicPlan, "date_add")

	dateAddPlan, err := runOneStmt(issue31708Optimizer(t), t, "select date_add(@dt, interval 1 day)")
	require.NoError(t, err)
	dateAddExpr := requireProjectionFunction(t, dateAddPlan, "date_add")
	require.Equal(t, dateAddExpr, operatorExpr,
		"INTERVAL + @dt must bind identically to DATE_ADD(@dt, INTERVAL)")
}

// Parentheses around either operand must not change the recognition.
func TestParenthesizedIntervalArithmeticBindsLikeBareForm(t *testing.T) {
	barePlan, err := runOneStmt(issue31708Optimizer(t), t, "select @dt + interval 1 day")
	require.NoError(t, err)
	bareExpr := requireProjectionFunction(t, barePlan, "date_add")

	parenthesizedPlan, err := runOneStmt(issue31708Optimizer(t), t, "select (@dt) + (interval 1 day)")
	require.NoError(t, err)
	parenthesizedExpr := requireProjectionFunction(t, parenthesizedPlan, "date_add")
	require.Equal(t, bareExpr, parenthesizedExpr,
		"parenthesized operands must bind identically to the bare form")
}

// The interval quantity itself may be a user variable and must keep its
// text domain instead of being coerced numerically.
func TestUserVariableIntervalQuantityBindsLikeDateAdd(t *testing.T) {
	logicPlan, err := runOneStmt(issue31708Optimizer(t), t, "select @dt + interval @n day")
	require.NoError(t, err)
	operatorExpr := requireProjectionFunction(t, logicPlan, "date_add")

	dateAddPlan, err := runOneStmt(issue31708Optimizer(t), t, "select date_add(@dt, interval @n day)")
	require.NoError(t, err)
	dateAddExpr := requireProjectionFunction(t, dateAddPlan, "date_add")
	require.Equal(t, dateAddExpr, operatorExpr,
		"@dt + INTERVAL @n DAY must bind identically to DATE_ADD(@dt, INTERVAL @n DAY)")

	varTypes := userVariableEffectiveTypes(logicPlan)
	require.Contains(t, varTypes, "n")
	require.NotEqual(t, int32(types.T_float64), varTypes["n"].Id,
		"interval quantity user variable must not be prefix-converted to DOUBLE")
}

// Outside INTERVAL arithmetic the existing numeric prefix-conversion
// contract for user variables must be preserved untouched.
func TestUserVariableArithmeticOutsideIntervalKeepsNumericContext(t *testing.T) {
	logicPlan, err := runOneStmt(issue31708Optimizer(t), t, "select @dt + 0")
	require.NoError(t, err)

	varTypes := userVariableEffectiveTypes(logicPlan)
	require.Contains(t, varTypes, "dt")
	require.True(t, types.T(varTypes["dt"].Id).ToType().IsNumeric(),
		"@dt + 0 must preserve the numeric prefix context")

	names := make(map[string]int)
	collectObjectNames(requireRootProjectionExpr(t, logicPlan), names)
	require.Equal(t, 0, names["date_add"],
		"@dt + 0 is numeric arithmetic and must not become date_add")
}

// The literal and CAST spellings already bind temporally today and must not
// drift when the user-variable form stops entering the numeric context.
func TestLiteralAndCastIntervalSpellingsKeepCurrentBindings(t *testing.T) {
	literalPlan, err := runOneStmt(issue31708Optimizer(t), t, "select '2026-01-15' + interval 1 day")
	require.NoError(t, err)
	requireProjectionFunction(t, literalPlan, "date_add")

	castPlan, err := runOneStmt(issue31708Optimizer(t), t, "select cast(@dt as date) + interval 1 day")
	require.NoError(t, err)
	requireProjectionFunction(t, castPlan, "date_add")

	varTypes := userVariableEffectiveTypes(castPlan)
	require.Contains(t, varTypes, "dt")
	require.Equal(t, int32(types.T_date), varTypes["dt"].Id,
		"CAST(@dt AS DATE) must keep binding the variable to DATE")
}

// `INTERVAL n unit - <date source>` is not a temporal subtraction spelling;
// neither the user-variable nor the literal form may become date_sub.
func TestIntervalMinusDateSourceStaysInvalid(t *testing.T) {
	_, err := runOneStmt(issue31708Optimizer(t), t, "select interval 1 day - @dt")
	require.Error(t, err, "reversed subtraction with a user variable must stay invalid")
	_, err = runOneStmt(issue31708Optimizer(t), t, "select interval 1 day - '2026-01-15'")
	require.Error(t, err, "reversed subtraction with a literal must stay invalid")
}

// The recognition must track the parser's own INTERVAL production: a
// two-argument function named interval whose second argument is the time
// unit, optionally parenthesized. An ordinary function call that merely
// shares the name, or the same name with another arity or argument shape,
// must not be treated as syntactic INTERVAL arithmetic.
func TestSyntacticIntervalOperandMatchesParserProduction(t *testing.T) {
	newIntervalFunc := func(name string, args tree.Exprs) *tree.FuncExpr {
		return &tree.FuncExpr{
			Func:     tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(name)),
			FuncName: tree.NewCStr(name, 1),
			Exprs:    args,
		}
	}
	value := tree.NewUnresolvedColName("n")
	unit := tree.NewTimeUnitExpr("day")

	require.True(t, isSyntacticIntervalOperand(newIntervalFunc("INTERVAL", tree.Exprs{value, unit})),
		"the uppercase grammar spelling must be recognized")
	require.True(t, isSyntacticIntervalOperand(newIntervalFunc("interval", tree.Exprs{value, unit})),
		"the lowercase spelling is the same function name")
	require.True(t, isSyntacticIntervalOperand(&tree.ParenExpr{Expr: newIntervalFunc("INTERVAL", tree.Exprs{value, unit})}),
		"a parenthesized INTERVAL must still be recognized")

	require.False(t, isSyntacticIntervalOperand(newIntervalFunc("INTERVAL", tree.Exprs{unit})),
		"a one-argument interval call is not the parser production")
	require.False(t, isSyntacticIntervalOperand(newIntervalFunc("INTERVAL", tree.Exprs{value, unit, unit})),
		"a three-argument interval call is not the parser production")
	require.False(t, isSyntacticIntervalOperand(newIntervalFunc("INTERVAL", tree.Exprs{value, value})),
		"an interval call without a time unit is an ordinary function")
	require.False(t, isSyntacticIntervalOperand(newIntervalFunc("coalesce", tree.Exprs{value, unit})),
		"another function carrying a time unit is not INTERVAL")
	require.False(t, isSyntacticIntervalOperand(value),
		"a plain operand is not INTERVAL")
}

// The interval +/- subtree must also escape a numeric context INHERITED from
// an enclosing numeric expression, exactly like the DATE_ADD function
// boundary does: `(@dt + INTERVAL 1 DAY) + @n` vs
// `DATE_ADD(@dt, INTERVAL 1 DAY) + @n`.
func TestUserVariableIntervalPlusUnderInheritedNumericContextBindsLikeDateAdd(t *testing.T) {
	controlPlan, controlErr := runOneStmt(issue31708Optimizer(t), t, "select date_add(@dt, interval 1 day) + @n")
	require.NoError(t, controlErr, "the DATE_ADD control must bind")
	operatorPlan, operatorErr := runOneStmt(issue31708Optimizer(t), t, "select (@dt + interval 1 day) + @n")
	require.NoError(t, operatorErr,
		"the operator spelling must bind like the DATE_ADD control even under an inherited numeric context")

	controlExpr := requireProjectionFunction(t, controlPlan, "+")
	operatorExpr := requireProjectionFunction(t, operatorPlan, "+")
	require.Equal(t, operatorExpr, controlExpr,
		"(@dt + INTERVAL 1 DAY) + @n must bind identically to DATE_ADD(@dt, INTERVAL 1 DAY) + @n")

	// Source-domain oracle on both plans: the temporal function consumes the
	// raw date text variable; outer arithmetic result types are not a valid
	// source oracle here.
	requireTemporalSourceVariable(t, controlExpr, "date_add", "dt")
	requireTemporalSourceVariable(t, operatorExpr, "date_add", "dt")
	// The enclosing numeric consumer keeps binding @n through a real cast.
	requireUserVariableCastArgument(t, controlExpr, "+", 1, "n")
	requireUserVariableCastArgument(t, operatorExpr, "+", 1, "n")
}

// The subtraction spelling inherits the same temporal-subtree boundary.
func TestUserVariableIntervalMinusUnderInheritedNumericContextBindsLikeDateSub(t *testing.T) {
	controlPlan, controlErr := runOneStmt(issue31708Optimizer(t), t, "select date_sub(@dt, interval 1 day) + @n")
	require.NoError(t, controlErr, "the DATE_SUB control must bind")
	operatorPlan, operatorErr := runOneStmt(issue31708Optimizer(t), t, "select (@dt - interval 1 day) + @n")
	require.NoError(t, operatorErr,
		"the subtraction operator spelling must bind like the DATE_SUB control even under an inherited numeric context")

	controlExpr := requireProjectionFunction(t, controlPlan, "+")
	operatorExpr := requireProjectionFunction(t, operatorPlan, "+")
	require.Equal(t, operatorExpr, controlExpr,
		"(@dt - INTERVAL 1 DAY) + @n must bind identically to DATE_SUB(@dt, INTERVAL 1 DAY) + @n")

	requireTemporalSourceVariable(t, controlExpr, "date_sub", "dt")
	requireTemporalSourceVariable(t, operatorExpr, "date_sub", "dt")
	requireUserVariableCastArgument(t, controlExpr, "+", 1, "n")
	requireUserVariableCastArgument(t, operatorExpr, "+", 1, "n")
}
