// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type SpStatus int

const (
	SpOk        SpStatus = 0
	SpNotOk     SpStatus = 1
	SpBranchHit SpStatus = 2
	SpLeaveLoop SpStatus = 3
	SpIterLoop  SpStatus = 4
)

type Interpreter struct {
	ctx          context.Context
	ses          FeSession
	bh           BackgroundExec
	varScope     *[]map[string]interface{}
	varTypeScope *[]map[string]plan.Type
	fmtctx       *tree.FmtCtx
	result       []ExecResult
	argsAttr     map[string]tree.InOutArgType // used for IN, OUT, IN/OUT check
	argsMap      map[string]tree.Expr         // used for argument to parameter mapping
	argsType     map[string]plan.Type         // declared SQL type for every parameter
	outParamMap  map[string]interface{}       // used for storing and updating OUT type arg

	lastAffectedRows    int64
	initialAffectedRows int64
}

func (interpreter *Interpreter) ensureVariableScopes() {
	if interpreter.varScope == nil {
		var scopes []map[string]interface{}
		interpreter.varScope = &scopes
	}
	if interpreter.varTypeScope == nil {
		var scopes []map[string]plan.Type
		interpreter.varTypeScope = &scopes
	}
	for len(*interpreter.varTypeScope) < len(*interpreter.varScope) {
		*interpreter.varTypeScope = append(*interpreter.varTypeScope, make(map[string]plan.Type))
	}
	if len(*interpreter.varTypeScope) > len(*interpreter.varScope) {
		*interpreter.varTypeScope = (*interpreter.varTypeScope)[:len(*interpreter.varScope)]
	}
	for i := range *interpreter.varTypeScope {
		if (*interpreter.varTypeScope)[i] == nil {
			(*interpreter.varTypeScope)[i] = make(map[string]plan.Type)
		}
	}
}

func (interpreter *Interpreter) storedProcedureContext() context.Context {
	interpreter.ensureVariableScopes()
	if interpreter.ctx == nil {
		interpreter.ctx = context.Background()
	}
	if scopes, ok := interpreter.ctx.Value(defines.VarScopeKey{}).(*[]map[string]interface{}); ok && scopes == interpreter.varScope {
		if typeScopes, ok := interpreter.ctx.Value(defines.VarScopeTypeKey{}).(*[]map[string]plan.Type); ok && typeScopes == interpreter.varTypeScope {
			if inSp, _ := interpreter.ctx.Value(defines.InSp{}).(bool); inSp {
				return interpreter.ctx
			}
		}
	}
	ctx := context.WithValue(interpreter.ctx, defines.VarScopeKey{}, interpreter.varScope)
	ctx = context.WithValue(ctx, defines.VarScopeTypeKey{}, interpreter.varTypeScope)
	return context.WithValue(ctx, defines.InSp{}, true)
}

func (interpreter *Interpreter) evaluateStoredProcedureExpr(e tree.Expr, targetType *plan.Type) (interface{}, error) {
	// Bound ownership: keep only the base procedure-scoped context on the
	// interpreter. storedProcedureContext returns interpreter.ctx while the
	// variable-scope pointers are unchanged, so persisting a wrapped context
	// here would retain one more wrapper on every repeated SET in a loop;
	// the marker rides a temporary context handed to the evaluation alone
	// (the same statement-local shape EvalCond uses), and the compile
	// context is still restored by evaluateExprInContext's deferred
	// cleanup.
	interpreter.ctx = interpreter.storedProcedureContext()
	savedBodyCtx := context.WithValue(interpreter.ctx, defines.SavedSpBodyKey{}, true)
	// Static saved-body expressions - DECLARE defaults and both SET arms,
	// including the @-variable arm routed through
	// GetSimpleExprValueWithSpVar - are release-authored text, so a
	// syntax-owned radix DECIMAL literal keeps the historical
	// persisted-coefficient binding here.
	return interpreter.evaluateExprInContext(savedBodyCtx, e, targetType)
}

func (interpreter *Interpreter) evaluateExprInContext(ctx context.Context, e tree.Expr, targetType *plan.Type) (interface{}, error) {
	txnCompileCtx := interpreter.ses.GetTxnCompileCtx()
	previousCtx := txnCompileCtx.GetContext()
	txnCompileCtx.SetContext(ctx)
	defer txnCompileCtx.SetContext(previousCtx)
	if targetType != nil {
		return GetSimpleExprValueWithType(ctx, e, interpreter.ses, *targetType)
	}
	return GetSimpleExprValue(ctx, e, interpreter.ses)
}

func (interpreter *Interpreter) recordAffectedRows() {
	if provider, ok := interpreter.bh.(backgroundExecRowCount); ok {
		interpreter.lastAffectedRows = provider.GetLastAffectedRows()
	}
}

func (interpreter *Interpreter) setAffectedRows(rows int64) {
	interpreter.lastAffectedRows = rows
	if provider, ok := interpreter.bh.(backgroundExecRowCount); ok {
		provider.SetLastAffectedRows(rows)
	}
}

func (interpreter *Interpreter) GetResult() []ExecResult {
	return interpreter.result
}

func (interpreter *Interpreter) GetExprString(input tree.Expr) string {
	interpreter.fmtctx.Reset()
	input.Format(interpreter.fmtctx)
	return interpreter.fmtctx.String()
}

func (interpreter *Interpreter) GetStatementString(input tree.Statement) string {
	interpreter.fmtctx.Reset()
	input.Format(interpreter.fmtctx)
	return interpreter.fmtctx.String()
}

func (interpreter *Interpreter) executeSQL(sql string) (SpStatus, error) {
	interpreter.bh.ClearExecResultSet()
	interpreter.ctx = interpreter.storedProcedureContext()
	// executeSQL's only caller executes the procedure's persisted body text,
	// so mark this compilation as a saved-body statement: a syntax-owned
	// HEX/BIT literal inside it must keep the historical persisted-coefficient
	// binding, while the CALL-site argument statements bind unchanged.
	savedBodyCtx := context.WithValue(interpreter.ctx, defines.SavedSpBodyKey{}, true)
	if err := interpreter.bh.Exec(savedBodyCtx, sql); err != nil {
		return SpNotOk, err
	}
	interpreter.recordAffectedRows()
	erArray, err := getResultSet(savedBodyCtx, interpreter.bh)
	if err != nil {
		return SpNotOk, err
	}
	if execResultArrayHasData(erArray) {
		interpreter.result = append(interpreter.result, erArray...)
	}
	return SpOk, nil
}

func (interpreter *Interpreter) GetSpVar(varName string) (interface{}, error) {
	interpreter.ensureVariableScopes()
	varName = strings.ToLower(varName)
	for i := len(*interpreter.varScope) - 1; i >= 0; i-- {
		curScope := (*interpreter.varScope)[i]
		val, ok := curScope[varName]
		if ok {
			return val, nil
		}
	}
	return "", nil
}

func (interpreter *Interpreter) GetSpVarType(varName string) (plan.Type, bool) {
	interpreter.ensureVariableScopes()
	varName = strings.ToLower(varName)
	for i := len(*interpreter.varScope) - 1; i >= 0; i-- {
		if _, ok := (*interpreter.varScope)[i][varName]; ok {
			typ, typeOK := (*interpreter.varTypeScope)[i][varName]
			return typ, typeOK
		}
	}
	return plan.Type{}, false
}

// Return error if variable is not declared yet. PARAM is an exception!
func (interpreter *Interpreter) SetSpVar(name string, value interface{}) error {
	interpreter.ensureVariableScopes()
	name = strings.ToLower(name)
	for i := len(*interpreter.varScope) - 1; i >= 0; i-- {
		curScope := (*interpreter.varScope)[i]
		if _, ok := curScope[name]; ok {
			curScope[name] = value
			return nil
		}
	}
	// loop up OUT param and SET in-place
	if _, ok := interpreter.outParamMap[name]; ok {
		// save at local
		interpreter.outParamMap[name] = value
		return nil
	}
	return moerr.NewNotSupported(interpreter.ctx, fmt.Sprintf("variable %s has to be declared using DECLARE.", name))
}

func (interpreter *Interpreter) FlushParam() error {
	for k, v := range (*interpreter.varScope)[0] {
		if _, ok := interpreter.argsMap[k]; ok && (interpreter.argsAttr[k] == tree.TYPE_INOUT || interpreter.argsAttr[k] == tree.TYPE_OUT) {
			// save INOUT at session
			interpreter.bh.ClearExecResultSet()
			// system setvar execution
			err := interpreter.ses.SetUserDefinedVar(interpreter.argsMap[k].(*tree.VarExpr).Name, v, "")
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (interpreter *Interpreter) GetSimpleExprValueWithSpVar(e tree.Expr) (interface{}, error) {
	return interpreter.evaluateStoredProcedureExpr(e, nil)
}

// Currently we support only binary, unary and comparison expression.
func (interpreter *Interpreter) MatchExpr(expr tree.Expr) (tree.Expr, error) {
	switch e := expr.(type) {
	case *tree.BinaryExpr:
		leftExpr, err := interpreter.MatchExpr(e.Left)
		if err != nil {
			return nil, err
		}
		rightExpr, err := interpreter.MatchExpr(e.Right)
		if err != nil {
			return nil, err
		}
		return &tree.BinaryExpr{
			Op:    e.Op,
			Left:  leftExpr,
			Right: rightExpr,
		}, nil
	case *tree.UnaryExpr:
	case *tree.ComparisonExpr:
		leftExpr, err := interpreter.MatchExpr(e.Left)
		if err != nil {
			return nil, err
		}
		rightExpr, err := interpreter.MatchExpr(e.Right)
		if err != nil {
			return nil, err
		}
		return &tree.ComparisonExpr{
			Op:     e.Op,
			SubOp:  e.SubOp,
			Left:   leftExpr,
			Right:  rightExpr,
			Escape: e.Escape,
		}, nil
	case *tree.AndExpr:
	case *tree.XorExpr:
	case *tree.OrExpr:
	case *tree.NotExpr:
	case *tree.IsNullExpr:
	case *tree.IsNotNullExpr:
	case *tree.IsUnknownExpr:
	case *tree.IsNotUnknownExpr:
	case *tree.IsTrueExpr:
	case *tree.IsNotTrueExpr:
	case *tree.IsFalseExpr:
	case *tree.IsNotFalseExpr:
	case *tree.FuncExpr:
	case *tree.UnresolvedName:
		// change column name to var name
		val, err := interpreter.GetSpVar(e.ColName())
		if err != nil {
			return nil, err
		}
		retName := &tree.UnresolvedName{
			NumParts:  e.NumParts,
			Star:      e.Star,
			CStrParts: e.CStrParts,
		}
		retName.CStrParts[0] = tree.NewCStr(fmt.Sprintf("%v", val), 1)
		return retName, nil
	default:
		return e, nil
	}
	return nil, nil
}

// Evaluate condition by sending it to bh with a select
func (interpreter *Interpreter) EvalCond(cond string) (int, error) {
	savedAffectedRows := interpreter.lastAffectedRows
	defer interpreter.setAffectedRows(savedAffectedRows)

	interpreter.bh.ClearExecResultSet()
	interpreter.ctx = interpreter.storedProcedureContext()
	// IF/ELSEIF/WHILE/REPEAT/CASE conditions are the saved body's own
	// release-authored text: keep the historical persisted-coefficient
	// binding for a syntax-owned radix literal in the reparsed condition,
	// through a statement-local context that ends with this evaluation.
	savedBodyCtx := context.WithValue(interpreter.ctx, defines.SavedSpBodyKey{}, true)
	err := interpreter.bh.Exec(savedBodyCtx, "select "+cond)
	if err != nil {
		return 0, err
	}
	erArray, err := getResultSet(savedBodyCtx, interpreter.bh)
	if err != nil {
		return 0, err
	}

	if execResultArrayHasData(erArray) {
		cond, err := erArray[0].GetInt64(interpreter.ctx, 0, 0)
		if err != nil {
			return 0, err
		}
		return int(cond), nil
	}
	return 0, nil
}

// EvalCaseWhenCond evaluates a simple-CASE WHEN equality condition the same
// way EvalCond does, and additionally reports whether the result is SQL NULL.
// SQL NULL is a valid result of the generated equality expression (for
// example CASE 1 WHEN NULL); the simple CASE statement keeps its original
// NULL disposition of matching no branch while raising no error.
func (interpreter *Interpreter) EvalCaseWhenCond(cond string) (int, bool, error) {
	savedAffectedRows := interpreter.lastAffectedRows
	defer interpreter.setAffectedRows(savedAffectedRows)

	interpreter.bh.ClearExecResultSet()
	interpreter.ctx = interpreter.storedProcedureContext()
	if err := interpreter.bh.Exec(interpreter.ctx, "select "+cond); err != nil {
		return 0, false, err
	}
	erArray, err := getResultSet(interpreter.ctx, interpreter.bh)
	if err != nil {
		return 0, false, err
	}

	if execResultArrayHasData(erArray) {
		// Decide SQL NULL from the typed result state before the numeric
		// conversion, so a NULL comparison value is not mistaken for an
		// evaluation error.
		if isNull, nullErr := erArray[0].ColumnIsNull(interpreter.ctx, 0, 0); nullErr == nil && isNull {
			return 0, true, nil
		}
		condVal, err := erArray[0].GetInt64(interpreter.ctx, 0, 0)
		if err != nil {
			return 0, false, err
		}
		return int(condVal), false, nil
	}
	return 0, false, nil
}

func (interpreter *Interpreter) ExecuteSp(stmt tree.Statement, dbName string, bg bool) (err error) {
	curScope := make(map[string]interface{})
	curTypeScope := make(map[string]plan.Type)
	interpreter.ensureVariableScopes()
	argumentCtx := interpreter.ctx
	interpreter.bh.ClearExecResultSet()

	// use current database as default
	err = interpreter.bh.Exec(interpreter.ctx, "use "+dbName)
	if err != nil {
		return err
	}

	// A top-level procedure owns its transaction. A nested CALL already runs
	// through a shared-transaction background executor and must reuse it.
	if !bg {
		err = interpreter.bh.Exec(interpreter.ctx, "begin;")
		defer func() {
			err = finishTxn(interpreter.ctx, interpreter.bh, err)
		}()
		if err != nil {
			return err
		}
	}

	// save parameters as local variables
	*interpreter.varScope = append(*interpreter.varScope, curScope)
	*interpreter.varTypeScope = append(*interpreter.varTypeScope, curTypeScope)
	interpreter.ctx = interpreter.storedProcedureContext()
	for k, v := range interpreter.argsMap {
		name := strings.ToLower(k)
		argType, hasArgType := interpreter.argsType[name]
		if hasArgType {
			curTypeScope[name] = argType
		}
		var value interface{}
		if varParam, ok := v.(*tree.VarExpr); ok {
			if interpreter.argsAttr[name] == tree.TYPE_OUT {
				curScope[name] = nil
			} else { // For INOUT and IN type, fetch store its previous value
				interpreter.bh.ClearExecResultSet()
				userVar, getErr := interpreter.ses.GetUserDefinedVar(varParam.Name)
				if getErr != nil {
					return getErr
				}
				if userVar == nil {
					// raise an error as INOUT / IN type param has to have a value
					return moerr.NewNotSupported(interpreter.ctx, fmt.Sprintf("parameter %s with type INOUT or IN has to have a specified value.", name))
				}
				if hasArgType {
					value, err = interpreter.evaluateExprInContext(argumentCtx, v, &argType)
				} else {
					value, err = interpreter.evaluateExprInContext(argumentCtx, v, nil)
				}
				if err != nil {
					return err
				}
				curScope[name] = value
			}
		} else {
			// if param type is INOUT or OUT and the param is not provided with variable expr, raise an error
			if interpreter.argsAttr[name] == tree.TYPE_INOUT || interpreter.argsAttr[name] == tree.TYPE_OUT {
				return moerr.NewNotSupported(interpreter.ctx, fmt.Sprintf("parameter %s with type INOUT or OUT has to be passed in using @.", name))
			}
			// evaluate the param
			if hasArgType {
				value, err = interpreter.evaluateExprInContext(argumentCtx, v, &argType)
			} else {
				value, err = interpreter.evaluateExprInContext(argumentCtx, v, nil)
			}
			if err != nil {
				return err
			}
			curScope[name] = value
		}
	}

	interpreter.setAffectedRows(interpreter.initialAffectedRows)
	_, err = interpreter.interpret(stmt)

	if err != nil {
		return err
	}

	err = interpreter.FlushParam()
	if err != nil {
		return err
	}

	// err = interpreter.bh.Exec(interpreter.ctx, "commit;")
	// if err != nil {
	// 	return err
	// }

	return nil
}

func (interpreter *Interpreter) interpret(stmt tree.Statement) (SpStatus, error) {
	if stmt == nil {
		return SpOk, nil
	}
	switch st := stmt.(type) {
	case *tree.CompoundStmt:
		// create new variable scope and push it
		curScope := make(map[string]interface{})
		curTypeScope := make(map[string]plan.Type)
		interpreter.ensureVariableScopes()
		*interpreter.varScope = append(*interpreter.varScope, curScope)
		*interpreter.varTypeScope = append(*interpreter.varTypeScope, curTypeScope)
		defer func() {
			*interpreter.varScope = (*interpreter.varScope)[:len(*interpreter.varScope)-1]
			*interpreter.varTypeScope = (*interpreter.varTypeScope)[:len(*interpreter.varTypeScope)-1]
		}()
		interpreter.ses.Info(interpreter.ctx, "current scope level: "+strconv.Itoa(len(*interpreter.varScope)))
		// recursively execute
		for _, innerSt := range st.Stmts {
			_, err := interpreter.interpret(innerSt)
			if err != nil {
				return SpNotOk, err
			}
		}
		return SpOk, nil
	case *tree.RepeatStmt:
		for {
			// first execute body
			for _, stmt := range st.Body {
				_, err := interpreter.interpret(stmt)
				if err != nil {
					return SpNotOk, err
				}
			}
			// then evaluate condition
			condStr := interpreter.GetExprString(st.Cond)
			condVal, err := interpreter.EvalCond(condStr)
			if err != nil {
				return SpNotOk, err
			}
			if condVal == 1 {
				break
			}
		}
	case *tree.WhileStmt:
		for {
			// first evaluate
			condStr := interpreter.GetExprString(st.Cond)
			condVal, err := interpreter.EvalCond(condStr)
			if err != nil {
				return SpNotOk, err
			}
			if condVal == 0 {
				break
			}
			// then execute body
			for _, stmt := range st.Body {
				_, err := interpreter.interpret(stmt)
				if err != nil {
					return SpNotOk, err
				}
			}
		}
	case *tree.LoopStmt:
	start:
		for {
			for _, stmt := range st.Body {
				status, err := interpreter.interpret(stmt)
				if err != nil {
					return SpNotOk, err
				}
				if status == SpLeaveLoop {
					// check label here using stmt
					goto exit
				}
				if status == SpIterLoop {
					// check label here using stmt
					goto start
				}
			}
		}
	exit:
		return SpOk, nil
	case *tree.IterateStmt:
		return SpIterLoop, nil
	case *tree.LeaveStmt:
		return SpLeaveLoop, nil
	case *tree.ElseIfStmt:
		// evaluate condition
		condStr := interpreter.GetExprString(st.Cond)
		condVal, err := interpreter.EvalCond(condStr)
		if err != nil {
			return SpNotOk, err
		}
		if condVal == 1 {
			// execute current else-if branch, remember to terminate other else-if
			for _, bodyStmt := range st.Body {
				status, err := interpreter.interpret(bodyStmt)
				if err != nil {
					return SpNotOk, err
				}
				if status == SpBranchHit || status == SpIterLoop || status == SpLeaveLoop {
					return status, nil
				}
			}
			return SpBranchHit, nil
		} else {
			return SpOk, nil
		}
	case *tree.IfStmt:
		// evaluate condition
		condStr := interpreter.GetExprString(st.Cond)
		condVal, err := interpreter.EvalCond(condStr)
		if err != nil {
			return SpNotOk, err
		}
		if condVal == 1 {
			// execute current branch
			for _, bodyStmt := range st.Body {
				status, err := interpreter.interpret(bodyStmt)
				if err != nil {
					return SpNotOk, err
				}
				if status == SpBranchHit || status == SpIterLoop || status == SpLeaveLoop {
					return status, nil
				}
			}
		} else {
			if len(st.Elifs) != 0 {
				// bunch of elif branch
				for _, elifStmt := range st.Elifs {
					status, err := interpreter.interpret(elifStmt)
					if err != nil {
						return SpNotOk, err
					}
					if status == SpBranchHit {
						// this means this else-if branch gets executed, no need to execute the rest elseif and else.
						goto end
					}
					if status == SpIterLoop || status == SpLeaveLoop {
						return status, nil
					}
				}
			}
			// else branch
			for _, elseStmt := range st.Else {
				status, err := interpreter.interpret(elseStmt)
				if err != nil {
					return SpNotOk, err
				}
				if status == SpBranchHit || status == SpIterLoop || status == SpLeaveLoop {
					return status, nil
				}
			}
		end:
			break
		}
	case *tree.WhenStmt:
		// any whenstmt that comes here will get executed, as we've already evaluated the condition in casestmt
		for _, stmt := range st.Body {
			// we use this branch
			_, err := interpreter.interpret(stmt)
			if err != nil {
				return SpNotOk, err
			}
		}
	case *tree.CaseStmt:
		// match case expression with all of its whens
		for _, whenStmt := range st.Whens {
			// build equality checker
			equalityExpr := &tree.ComparisonExpr{
				Op:    tree.EQUAL,
				Left:  st.Expr,
				Right: whenStmt.Cond,
			}
			condVal, isNull, err := interpreter.EvalCaseWhenCond(interpreter.GetExprString(equalityExpr))
			if err != nil {
				return SpNotOk, err
			}
			if isNull {
				// SQL NULL equality (e.g. CASE 1 WHEN NULL) is not an
				// evaluation error and never matches under the documented
				// CASE contract: keep checking the remaining WHEN clauses
				// in order, so a later matching WHEN, the ELSE clause, and
				// any later WHEN evaluation error are all reached.
				continue
			}
			if condVal == 1 {
				// we use this branch
				_, err := interpreter.interpret(whenStmt)
				if err != nil {
					return SpNotOk, err
				}
				return SpOk, nil
			}
		}

		// none of the WHEN branch hit, we execute ELSE
		for _, stmt := range st.Else {
			_, err := interpreter.interpret(stmt)
			if err != nil {
				return SpNotOk, err
			}
		}
		return SpOk, nil
	case *tree.Declare:
		interpreter.ensureVariableScopes()
		declaredType, err := plan2.GetTypeFromAst(interpreter.ctx, st.ColumnType)
		if err != nil {
			return SpNotOk, err
		}
		var value interface{}
		// store variables into current scope
		if st.DefaultVal != nil {
			value, err = interpreter.evaluateStoredProcedureExpr(st.DefaultVal, &declaredType)
			if err != nil {
				return SpNotOk, err
			}
		}
		for _, v := range st.Variables {
			name := strings.ToLower(v)
			(*interpreter.varScope)[len(*interpreter.varScope)-1][name] = value
			(*interpreter.varTypeScope)[len(*interpreter.varTypeScope)-1][name] = declaredType
		}
		return SpOk, nil
	case *tree.SetVar:
		for _, assign := range st.Assignments {
			name := assign.Name

			if !assign.System {
				value, err := interpreter.GetSimpleExprValueWithSpVar(assign.Value)
				if err != nil {
					return SpNotOk, err
				}
				setSQL := "set @" + name + " = " + interpreter.GetExprString(assign.Value)
				if err = interpreter.ses.SetUserDefinedVar(name, value, setSQL); err != nil {
					return SpNotOk, err
				}
				interpreter.setAffectedRows(0)
			} else {
				// custom defined variable
				declaredType, ok := interpreter.GetSpVarType(name)
				if !ok {
					return SpNotOk, moerr.NewNotSupported(interpreter.ctx, fmt.Sprintf("variable %s has to be declared using DECLARE.", name))
				}
				// get updated value
				value, err := interpreter.evaluateStoredProcedureExpr(assign.Value, &declaredType)
				if err != nil {
					return SpNotOk, err
				}

				// update local value
				err = interpreter.SetSpVar(name, value)
				if err != nil {
					return SpNotOk, err
				}
			}
		}
	default: // normal sql. Since we don't support SELECT INTO for now, we don't have to worry about updating variables
		return interpreter.executeSQL(interpreter.GetStatementString(st))
	}
	return SpOk, nil
}
