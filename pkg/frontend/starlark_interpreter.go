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
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
	switch v := v.(type) {
	case starlark.Bool:
		return bool(v), nil
	case starlark.Int:
		iv, _ := v.Int64()
		return iv, nil
	case starlark.Float:
		return float64(v), nil
	case starlark.String:
		return string(v), nil
	}
	return nil, moerr.NewInvalidInputf(ctx, "type %s is not a supported starlark type", v.Type())
}

func (si *starlarkInterpreter) buildModule() starlark.Value {
	return &starlarkstruct.Module{
		Name: "json",
		Members: starlark.StringDict{
			"sql":   starlark.NewBuiltin("mo.sql", si.moSql),
			"jq":    starlark.NewBuiltin("mo.jq", si.moJq),
			"quote": starlark.NewBuiltin("mo.quote", si.moQuote),
		},
	}
}

func (si *starlarkInterpreter) moSql(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var sql string
	if err := starlark.UnpackPositionalArgs("sql", args, kwargs, 1, &sql); err != nil {
		return nil, err
	}

	err := si.interp.bh.Exec(si.interp.ctx, sql)
	if err != nil {
		return starlark.String(err.Error()), nil
	}

	erArray, err := getResultSet(si.interp.ctx, si.interp.bh)
	if err != nil {
		return starlark.String(err.Error()), nil
	}

	if len(erArray) == 0 {
		return starlark.None, nil
	}

	rows := make([]starlark.Value, len(erArray))
	for i, er := range erArray {
		rowsi := make(starlark.Tuple, er.GetColumnCount())
		for j := range rowsi {
			v, err := er.GetString(si.interp.ctx, uint64(i), uint64(j))
			if err != nil {
				return nil, err
			}
			rowsi[j] = starlark.String(v)
		}
		rows[i] = rowsi
	}
	return starlark.NewList(rows), nil
}

func (si *starlarkInterpreter) moJq(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var jq string
	var data string
	if err := starlark.UnpackPositionalArgs("jq", args, kwargs, 2, &jq, &data); err != nil {
		return nil, err
	}

	res, err := ujson.RunJQOnString(jq, data)
	if err != nil {
		return nil, err
	}

	bs := ujson.MustMarshal(res)
	return starlark.String(string(bs)), nil
}

// mo.quote(s) SQL quote a string.  Each single quote in the string is doubled.
func (si *starlarkInterpreter) moQuote(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var s string
	if err := starlark.UnpackPositionalArgs("quote", args, kwargs, 1, &s); err != nil {
		return nil, err
	}

	qs := plan.EscapeFormat(s)
	return starlark.String(qs), nil
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
