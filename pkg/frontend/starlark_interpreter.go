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
	"errors"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/monlp/llm"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	ujson "github.com/matrixorigin/matrixone/pkg/util/json"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
	"go.starlark.net/syntax"
	"go.uber.org/zap"
)

type starlarkInterpreter struct {
	interp      *Interpreter
	thread      *starlark.Thread
	predeclared starlark.StringDict

	llm llm.LLMClient

	// lastErr is the codes of the most recent failing mo.* call; see
	// lastFailure.
	lastErr lastFailure
}

func convertToStarlarkValue(ctx context.Context, v any) (starlark.Value, error) {
	val := reflect.ValueOf(v)
	kind := val.Kind()
	if kind == reflect.Ptr {
		kind = val.Elem().Kind()
	}
	switch kind {
	case reflect.Bool:
		return starlark.Bool(val.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return starlark.MakeInt64(val.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return starlark.MakeUint64(val.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return starlark.Float(val.Float()), nil
	case reflect.String:
		return starlark.String(val.String()), nil
	}
	return nil, moerr.NewInvalidInputf(ctx, "type %s is not a supported starlark type", val.Kind().String())
}

func convertFromStarlarkValue(ctx context.Context, v starlark.Value) (any, error) {
	var err error
	switch v := v.(type) {
	case starlark.NoneType:
		return nil, nil
	case starlark.Bool:
		return bool(v), nil
	case starlark.Int:
		iv, _ := v.Int64()
		return iv, nil
	case starlark.Float:
		return float64(v), nil
	case starlark.String:
		return string(v), nil
	case *starlark.List:
		ls := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			ls[i], err = convertFromStarlarkValue(ctx, v.Index(i))
			if err != nil {
				return nil, err
			}
		}
		return ls, nil
	case *starlark.Tuple:
		ls := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			ls[i], err = convertFromStarlarkValue(ctx, v.Index(i))
			if err != nil {
				return nil, err
			}
		}
		return ls, nil
	}
	return nil, moerr.NewInvalidInputf(ctx, "type %s is not a supported starlark type", v.Type())
}

func (interpreter *Interpreter) ExecuteStarlark(spBody string, dbName string, bg bool) error {
	// preamble
	interpreter.bh.ClearExecResultSet()
	// use current database
	err := interpreter.bh.Exec(interpreter.ctx, "use "+dbName)
	if err != nil {
		return err
	}

	// make sure the entire sp is in a single transaction
	if !bg {
		err = interpreter.bh.Exec(interpreter.ctx, "begin;")
		defer func() {
			err = finishTxn(interpreter.ctx, interpreter.bh, err)
		}()
		if err != nil {
			return err
		}
	}

	// build the starlark interpreter
	var si starlarkInterpreter
	si.interp = interpreter
	si.thread = &starlark.Thread{
		Print: func(thread *starlark.Thread, msg string) {
			logutil.Info("starlark", zap.String("msg", msg))
		},
	}

	si.predeclared = starlark.StringDict{
		"mo": si.buildModule(),
	}

	// populate the variables
	for k, v := range interpreter.argsMap {
		if varParam, ok := v.(*tree.VarExpr); ok {
			if interpreter.argsAttr[k] == tree.TYPE_OUT || interpreter.argsAttr[k] == tree.TYPE_INOUT {
				// out type, just record the name
				interpreter.outParamMap["out_"+k] = varParam.Name
			}

			if interpreter.argsAttr[k] == tree.TYPE_IN || interpreter.argsAttr[k] == tree.TYPE_INOUT {
				value, err := interpreter.ses.GetUserDefinedVar(varParam.Name)
				if value == nil || err != nil {
					return moerr.NewInvalidInputf(interpreter.ctx, "parameter %s with type INOUT or IN has to have a specified value.", k)
				}
				starlarkValue, err := convertToStarlarkValue(interpreter.ctx, value.Value)
				if err != nil {
					return err
				}
				si.predeclared[k] = starlarkValue
			}
		} else {
			if interpreter.argsAttr[k] == tree.TYPE_OUT || interpreter.argsAttr[k] == tree.TYPE_INOUT {
				return moerr.NewInvalidInput(interpreter.ctx, "OUT or INOUT parameter must be passed in as an @variable")
			}
			value, err := interpreter.GetSimpleExprValueWithSpVar(v)
			if err != nil {
				return err
			}
			starlarkValue, err := convertToStarlarkValue(interpreter.ctx, value)
			if err != nil {
				return err
			}
			si.predeclared[k] = starlarkValue
		}
	}

	interpreter.setAffectedRows(interpreter.initialAffectedRows)
	globals, err := starlark.ExecFileOptions(
		&syntax.FileOptions{
			While:           true,
			TopLevelControl: true,
			GlobalReassign:  true,
			Recursion:       true,
		},
		si.thread,
		"",
		spBody,
		si.predeclared,
	)
	if err != nil {
		return err
	}

	// bind out variables
	for k := range interpreter.outParamMap {
		gv := globals[k]
		if gv != nil {
			av, err := convertFromStarlarkValue(interpreter.ctx, gv)
			if err != nil {
				return err
			}

			varName := interpreter.outParamMap[k].(string)
			err = interpreter.ses.SetUserDefinedVar(varName, av, "")
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// lastFailure records the codes of the most recent mo.* builtin failure, so a
// procedure can branch on an error CLASS instead of matching message text:
//
//	rs, err = mo.sql("insert into t values (1)")
//	if err != None and mo.errno() == 1062:
//	    ...                      # duplicate key, whatever the message says
//
// The failure itself is still handed back in the `ok` slot as a plain string,
// exactly as it always was.  Carrying the codes ON that value was tried and
// abandoned: a Go value cannot be a drop-in for starlark.String, because the
// interpreter type-asserts the concrete type for equality, hashing, ordering
// and dict membership -- and claiming Type() == "string" makes sorted() panic.
// Keeping the string untouched keeps every existing procedure working.
type lastFailure struct {
	code     uint16
	sqlstate string
	set      bool
}

// beginCall resets the recorded failure.  Every builtin that can fail calls it
// first, so mo.errno() answers for the CALL THAT JUST HAPPENED and never for a
// stale one.  mo.errno and mo.sqlstate deliberately do not call it: reporting
// a value must not consume it.
func (si *starlarkInterpreter) beginCall() {
	si.lastErr = lastFailure{}
}

// failed records a builtin failure and renders it for the `ok` slot.  The
// rendering is a plain starlark.String, which is what mo.* always returned.
func (si *starlarkInterpreter) failed(err error) starlark.Value {
	si.lastErr = lastFailure{set: true}
	var me *moerr.Error
	if errors.As(err, &me) {
		si.lastErr.code = me.MySQLCode()
		si.lastErr.sqlstate = me.SqlState()
	}
	return starlark.String(err.Error())
}

// moErrno reports the MySQL error number of the most recent mo.* failure: 0
// when the last call succeeded, and 0 when it failed with something that is
// not a moerr (argument unpacking, say) -- there is no error number to borrow.
func (si *starlarkInterpreter) moErrno(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if err := starlark.UnpackPositionalArgs("errno", args, kwargs, 0); err != nil {
		return nil, err
	}
	return starlark.MakeInt(int(si.lastErr.code)), nil
}

// moSqlstate reports the SQLSTATE of the most recent mo.* failure, "" when
// there is none.
func (si *starlarkInterpreter) moSqlstate(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if err := starlark.UnpackPositionalArgs("sqlstate", args, kwargs, 0); err != nil {
		return nil, err
	}
	return starlark.String(si.lastErr.sqlstate), nil
}

func (si *starlarkInterpreter) buildModule() starlark.Value {
	return &starlarkstruct.Module{
		Name: "mo",
		Members: starlark.StringDict{
			"sql":         starlark.NewBuiltin("mo.sql", si.moSql),
			"jq":          starlark.NewBuiltin("mo.jq", si.moJq),
			"quote":       starlark.NewBuiltin("mo.quote", si.moQuote),
			"getvar":      starlark.NewBuiltin("mo.getvar", si.moGetVar),
			"setvar":      starlark.NewBuiltin("mo.getvar", si.moSetVar),
			"llm_connect": starlark.NewBuiltin("mo.llm_connect", si.moLlmConnect),
			"llm_chat":    starlark.NewBuiltin("mo.llm_chat", si.moLlmChat),
			"errno":       starlark.NewBuiltin("mo.errno", si.moErrno),
			"sqlstate":    starlark.NewBuiltin("mo.sqlstate", si.moSqlstate),
		},
	}
}

func (si *starlarkInterpreter) moSql(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	si.beginCall()
	var sql string
	// ret is the list of [result, ok]
	var ret = []starlark.Value{starlark.None, starlark.None}

	if err := starlark.UnpackPositionalArgs("sql", args, kwargs, 1, &sql); err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}

	si.interp.bh.ClearExecResultSet()
	err := si.interp.bh.Exec(si.interp.ctx, sql)
	si.interp.recordAffectedRows()
	if err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}

	erArray, err := getResultSet(si.interp.ctx, si.interp.bh)
	if err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}

	if len(erArray) == 0 {
		return starlark.NewList(ret), nil
	} else if len(erArray) > 1 {
		ret[1] = si.failed(moerr.NewInvalidInput(si.interp.ctx, "sql must return a single result set"))
		return starlark.NewList(ret), nil
	}

	er := erArray[0]
	rows := make([]starlark.Value, er.GetRowCount())
	for i := range rows {
		rowsi := make(starlark.Tuple, er.GetColumnCount())
		for j := range rowsi {
			v, err := er.GetString(si.interp.ctx, uint64(i), uint64(j))
			if err != nil {
				ret[1] = si.failed(err)
				return starlark.NewList(ret), nil
			}
			rowsi[j] = starlark.String(v)
		}
		rows[i] = rowsi
	}

	ret[0] = starlark.NewList(rows)
	return starlark.NewList(ret), nil
}

func (si *starlarkInterpreter) moJq(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	si.beginCall()
	var jq string
	var data string
	var ret = []starlark.Value{starlark.None, starlark.None}
	if err := starlark.UnpackPositionalArgs("jq", args, kwargs, 2, &jq, &data); err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}

	res, err := ujson.RunJQOnString(jq, data)
	if err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}

	bs := ujson.MustMarshal(res)
	ret[0] = starlark.String(string(bs))
	return starlark.NewList(ret), nil
}

// mo.quote(s) SQL quote a string.  Each single quote in the string is doubled.
func (si *starlarkInterpreter) moQuote(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	si.beginCall()
	var s string
	var ret = []starlark.Value{starlark.None, starlark.None}
	if err := starlark.UnpackPositionalArgs("quote", args, kwargs, 1, &s); err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}

	ret[0] = starlark.String(plan.EscapeFormat(s))
	return starlark.NewList(ret), nil
}

func (si *starlarkInterpreter) moGetVar(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	si.beginCall()
	var name string
	var ret = []starlark.Value{starlark.None, starlark.None}
	if err := starlark.UnpackPositionalArgs("getvar", args, kwargs, 1, &name); err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}

	value, err := si.interp.ses.GetUserDefinedVar(name)
	if err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}

	starlarkValue, err := convertToStarlarkValue(si.interp.ctx, value.Value)
	if err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}

	ret[0] = starlarkValue
	return starlark.NewList(ret), nil
}

func (si *starlarkInterpreter) moSetVar(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	si.beginCall()
	var name string
	var value starlark.Value
	var ret = []starlark.Value{starlark.None, starlark.None}
	if err := starlark.UnpackPositionalArgs("setvar", args, kwargs, 2, &name, &value); err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}

	sqlValue, err := convertFromStarlarkValue(si.interp.ctx, value)
	if err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}

	err = si.interp.ses.SetUserDefinedVar(name, sqlValue, "")
	if err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}
	return starlark.NewList(ret), nil
}

func (si *starlarkInterpreter) llmGetVar(name, orig string) string {
	if orig == "" {
		value, err := si.interp.ses.GetUserDefinedVar(name)
		if err != nil {
			return ""
		}
		return value.Value.(string)
	}
	return orig
}

func (si *starlarkInterpreter) llmConnect(server, addr, model, options string) (llm.LLMClient, error) {
	server = si.llmGetVar("llm_server", server)
	addr = si.llmGetVar("llm_addr", addr)
	model = si.llmGetVar("llm_model", model)
	options = si.llmGetVar("llm_options", options)
	return llm.NewLLMClient(server, addr, model, options)
}

func (si *starlarkInterpreter) moLlmConnect(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	si.beginCall()
	var err error
	var server, addr, model, options string
	var ret = []starlark.Value{starlark.None, starlark.None}
	if err := starlark.UnpackPositionalArgs("llm_connect", args, kwargs, 3, &server, &addr, &model, &options); err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}

	si.llm, err = si.llmConnect(server, addr, model, options)
	if err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}
	return starlark.NewList(ret), nil
}

func (si *starlarkInterpreter) moLlmChat(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	si.beginCall()
	var err error
	var prompt string
	var ret = []starlark.Value{starlark.None, starlark.None}
	if err := starlark.UnpackPositionalArgs("llm_chat", args, kwargs, 1, &prompt); err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}

	if si.llm == nil {
		si.llm, err = si.llmConnect("", "", "", "")
		if err != nil {
			ret[1] = si.failed(err)
			return starlark.NewList(ret), nil
		}
	}

	reply, err := si.llm.Chat(si.interp.ctx, prompt)
	if err != nil {
		ret[1] = si.failed(err)
		return starlark.NewList(ret), nil
	}

	ret[0] = starlark.String(reply)
	return starlark.NewList(ret), nil
}
