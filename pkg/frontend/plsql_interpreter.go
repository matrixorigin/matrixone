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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
	ctx         context.Context
	ses         *Session
	bh          BackgroundExec
	varScope    *[]map[string]interface{}
	fmtctx      *tree.FmtCtx
	result      []ExecResult
	argsAttr    map[string]tree.InOutArgType // used for IN, OUT, IN/OUT check
	argsMap     map[string]tree.Expr         // used for argument to parameter mapping
	outParamMap map[string]interface{}       // used for storing and updating OUT type arg
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

func (interpreter *Interpreter) GetSpVar(varName string) (interface{}, error) {
	for i := len(*interpreter.varScope) - 1; i >= 0; i-- {
		curScope := (*interpreter.varScope)[i]
		val, ok := curScope[strings.ToLower(varName)]
		if ok {
			return val, nil
		}
	}
	return "", nil
}

// Return error if variable is not declared yet. PARAM is an exception!
func (interpreter *Interpreter) SetSpVar(name string, value interface{}) error {
	for i := len(*interpreter.varScope) - 1; i >= 0; i-- {
		curScope := (*interpreter.varScope)[i]
		if _, ok := curScope[strings.ToLower(name)]; ok {
			curScope[strings.ToLower(name)] = value
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
		if _, ok := interpreter.argsMap[k]; ok && interpreter.argsAttr[k] == tree.TYPE_INOUT {
			// save INOUT at session
			interpreter.bh.ClearExecResultSet()
			// system setvar execution
			err := interpreter.ses.SetUserDefinedVar(interpreter.argsMap[k].(*tree.VarExpr).Name, v, "")
			if err != nil {
				return err
			}
		}
	}

	for k, v := range interpreter.outParamMap {
		// save at session
		interpreter.bh.ClearExecResultSet()
		// system setvar execution
		err := interpreter.ses.SetUserDefinedVar(interpreter.argsMap[k].(*tree.VarExpr).Name, v, "")
		if err != nil {
			return err
		}
	}

	return nil
}

func (interpreter *Interpreter) GetSimpleExprValueWithSpVar(e tree.Expr) (interface{}, error) {
	newExpr, err := interpreter.MatchExpr(e)
	if err != nil {
		return nil, err
	}
	retStmt, err := parsers.ParseOne(interpreter.ctx, dialect.MYSQL, "select "+interpreter.GetExprString(newExpr), 1, 0)
	if err != nil {
		return nil, err
	}
	retExpr := retStmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	if err != nil {
		return nil, err
	}
	return GetSimpleExprValue(interpreter.ctx, retExpr, interpreter.ses)
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
		val, err := interpreter.GetSpVar(e.Parts[0])
		if err != nil {
			return nil, err
		}
		retName := &tree.UnresolvedName{
			NumParts: e.NumParts,
			Star:     e.Star,
			Parts:    e.Parts,
		}
		retName.Parts[0] = fmt.Sprintf("%v", val)
		return retName, nil
	default:
		return e, nil
	}
	return nil, nil
}

// Evaluate condition by sending it to bh with a select
func (interpreter *Interpreter) EvalCond(cond string) (int, error) {
	interpreter.bh.ClearExecResultSet()
	interpreter.ctx = context.WithValue(interpreter.ctx, defines.VarScopeKey{}, interpreter.varScope)
	interpreter.ctx = context.WithValue(interpreter.ctx, defines.InSp{}, true)
	err := interpreter.bh.Exec(interpreter.ctx, "select "+cond)
	if err != nil {
		return 0, err
	}
	erArray, err := getResultSet(interpreter.ctx, interpreter.bh)
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

func (interpreter *Interpreter) ExecuteSp(stmt tree.Statement, dbName string) (err error) {
	curScope := make(map[string]interface{})
	interpreter.bh.ClearExecResultSet()

	// use current database as default
	err = interpreter.bh.Exec(interpreter.ctx, "use "+dbName)
	if err != nil {
		return err
	}

	// make sure the entire sp is in a single transaction
	err = interpreter.bh.Exec(interpreter.ctx, "begin;")
	defer func() {
		err = finishTxn(interpreter.ctx, interpreter.bh, err)
	}()
	if err != nil {
		return err
	}

	// save parameters as local variables
	*interpreter.varScope = append(*interpreter.varScope, curScope)
	for k, v := range interpreter.argsMap {
		var value interface{}
		if varParam, ok := v.(*tree.VarExpr); ok {
			// For OUT type, store it in a separate map only for SET to update it and flush at the end
			if interpreter.argsAttr[k] == tree.TYPE_OUT {
				interpreter.outParamMap[k] = 0
			} else { // For INOUT and IN type, fetch store its previous value
				interpreter.bh.ClearExecResultSet()
				_, value, _ := interpreter.ses.GetUserDefinedVar(varParam.Name)
				if value == nil {
					// raise an error as INOUT / IN type param has to have a value
					return moerr.NewNotSupported(interpreter.ctx, fmt.Sprintf("parameter %s with type INOUT or IN has to have a specified value.", k))
				}
				// save param to local var scope
				(*interpreter.varScope)[len(*interpreter.varScope)-1][strings.ToLower(k)] = value.Value
			}
		} else {
			// if param type is INOUT or OUT and the param is not provided with variable expr, raise an error
			if interpreter.argsAttr[k] == tree.TYPE_INOUT || interpreter.argsAttr[k] == tree.TYPE_OUT {
				return moerr.NewNotSupported(interpreter.ctx, fmt.Sprintf("parameter %s with type INOUT or OUT has to be passed in using @.", k))
			}
			// evaluate the param
			value, err = interpreter.GetSimpleExprValueWithSpVar(v)
			if err != nil {
				return err
			}
			// save param to local var scope
			(*interpreter.varScope)[len(*interpreter.varScope)-1][strings.ToLower(k)] = value
		}
	}

	_, err = interpreter.interpret(stmt)

	if err != nil {
		return err
	}

	// // commit the param flush part of sp
	// err = interpreter.bh.Exec(interpreter.ctx, "begin;")
	// if err != nil {
	// 	return err
	// }

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
		*interpreter.varScope = append(*interpreter.varScope, curScope)
		logutil.Info("current scope level: " + strconv.Itoa(len(*interpreter.varScope)))
		// recursively execute
		for _, innerSt := range st.Stmts {
			_, err := interpreter.interpret(innerSt)
			if err != nil {
				return SpNotOk, err
			}
		}
		// pop current scope
		*interpreter.varScope = (*interpreter.varScope)[:len(*interpreter.varScope)-1]
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
			condVal, err := interpreter.EvalCond(interpreter.GetExprString(equalityExpr))
			if err != nil {
				return SpNotOk, nil
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
		var err error
		var value interface{}
		// store variables into current scope
		if st.DefaultVal != nil {
			value, err = GetSimpleExprValue(interpreter.ctx, st.DefaultVal, interpreter.ses)
			if err != nil {
				return SpNotOk, nil
			}
		}
		for _, v := range st.Variables {
			(*interpreter.varScope)[len(*interpreter.varScope)-1][v] = value
		}
		return SpOk, nil
	case *tree.SetVar:
		for _, assign := range st.Assignments {
			name := assign.Name

			// if this is a system set, ignore if it's not a INOUT/OUT arg
			if strings.Contains(interpreter.GetExprString(st), "@") {
				str := interpreter.GetExprString(st)
				interpreter.bh.ClearExecResultSet()
				// system setvar execution
				err := interpreter.bh.Exec(interpreter.ctx, str)
				if err != nil {
					return SpNotOk, err
				}
			} else {
				// custom defined variable
				var value interface{}
				// get updated value
				value, err := interpreter.GetSimpleExprValueWithSpVar(assign.Value)
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
		str := interpreter.GetStatementString(st)
		interpreter.bh.ClearExecResultSet()
		// For sp variable replacement
		interpreter.ctx = context.WithValue(interpreter.ctx, defines.VarScopeKey{}, interpreter.varScope)
		interpreter.ctx = context.WithValue(interpreter.ctx, defines.InSp{}, true)
		err := interpreter.bh.Exec(interpreter.ctx, str)
		if err != nil {
			return SpNotOk, err
		}
		erArray, err := getResultSet(interpreter.ctx, interpreter.bh)
		if err != nil {
			return SpNotOk, err
		}
		if execResultArrayHasData(erArray) {
			interpreter.result = append(interpreter.result, erArray[0])
		}
		return SpOk, nil
	}
	return SpOk, nil
}
