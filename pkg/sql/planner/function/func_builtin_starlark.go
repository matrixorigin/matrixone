// Copyright 2021 - 2025 Matrix Origin
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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	ujson "github.com/matrixorigin/matrixone/pkg/util/json"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
	"go.uber.org/zap"
)

//
// starlark function, see https://github.com/starlark-lang/starlark-go
//
// starlark(code, args) runs the starlark code with the given args.
// the starlark code should have a main function, that takes a string as argument.
// the main function should return a string.  For other types (and multiple args),
// user must econde the args into string -- usually using json.
//
// we expose a few more functions to the starlark runtime --
// 	1. mo_sql that runs a sql statement, returns result or resultset as string (json)
// 	2. mo_jq that runs a jq query, returns result string.
// Note that with mo_sql, we have an effective stored procedure system.   Just run
// `select starlark('storedproc_code', 'arg')`
// If necessary, we can have syntax `create procedure ... language starlark ...`
// but that seems to be cosmetic.
//
// try_starlark is the same as startlark, but it will error if there is error
// when running starlark.  Instead, it will just return NULL.
//

type opBuiltInStarlark struct {
	th          *starlark.Thread
	predeclared starlark.StringDict
	globals     starlark.StringDict
	fn          starlark.Value
}

func newOpBuiltInStarlark() *opBuiltInStarlark {
	return &opBuiltInStarlark{}
}

func (op *opBuiltInStarlark) buildFn(proc *process.Process, program string) error {
	var err error

	if op.fn != nil {
		// already built
		return nil
	}

	op.th = &starlark.Thread{
		Print: func(thread *starlark.Thread, msg string) {
			logutil.Info("starlark", zap.String("msg", msg))
		},
	}

	moJq := func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
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

	op.predeclared = starlark.StringDict{
		// "sql": starlark.NewBuiltin("sql",moSql),
		"jq": starlark.NewBuiltin("jq", moJq),
	}

	op.globals, err = starlark.ExecFileOptions(
		&syntax.FileOptions{
			While:           true,
			TopLevelControl: true,
			GlobalReassign:  true,
			Recursion:       true,
		},
		op.th,
		"",
		program,
		op.predeclared,
	)
	if err != nil {
		return err
	}

	if op.globals == nil || op.globals["main"] == nil {
		return moerr.NewInvalidInput(proc.Ctx, "failed to parse starlark program")
	}

	op.fn = op.globals["main"]
	return nil
}

func (op *opBuiltInStarlark) starlark(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList) error {
	return op.tryStarlarkImpl(params, result, proc, length, selectList, false)
}

func (op *opBuiltInStarlark) tryStarlark(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList) error {
	return op.tryStarlarkImpl(params, result, proc, length, selectList, true)
}

func (op *opBuiltInStarlark) tryStarlarkImpl(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList, isTry bool) error {
	p1 := vector.GenerateFunctionStrParameter(params[0])
	if !params[0].IsConst() {
		return moerr.NewInvalidInput(proc.Ctx, "starlark code must be constant.")
	}
	code, isnull := p1.GetStrValue(0)
	if isnull {
		return moerr.NewInvalidInput(proc.Ctx, "starlark code cannot be null.")
	}
	if err := op.buildFn(proc, string(code)); err != nil {
		return err
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	parg := vector.GenerateFunctionStrParameter(params[1])

	for i := uint64(0); i < uint64(length); i++ {
		arg, isnull := parg.GetStrValue(i)
		if isnull {
			rs.AppendBytes(nil, true)
			continue
		}
		res, err := starlark.Call(op.th, op.fn, starlark.Tuple{starlark.String(string(arg))}, nil)
		if err != nil {
			if isTry {
				rs.AppendBytes(nil, true)
			} else {
				return err
			}
		}

		if res == nil {
			rs.AppendBytes(nil, true)
		} else {
			rs.AppendBytes([]byte(res.String()), false)
		}
	}
	return nil
}
