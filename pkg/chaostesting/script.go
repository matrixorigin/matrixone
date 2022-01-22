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

package fz

import (
	"errors"
	"os"
	"time"

	"github.com/reusee/dscope"
	"github.com/reusee/starlarkutil"
	"go.starlark.net/starlark"
)

type LoadScript func() (
	defs []any,
	err error,
)

func (_ Def) LoadScript(
	builtins ScriptBuiltins,
	get GetScriptDefs,
) LoadScript {
	return func() (
		defs []any,
		err error,
	) {

		const scriptFileName = "script.py"

		src, err := os.ReadFile(scriptFileName)
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		ce(err)

		_, err = starlark.ExecFile(
			new(starlark.Thread),
			scriptFileName,
			src,
			starlark.StringDict(builtins),
		)
		ce(err)

		defs = get()

		return
	}
}

type ScriptBuiltins starlark.StringDict

var _ dscope.Reducer = ScriptBuiltins{}

func (_ ScriptBuiltins) IsReducer() {}

func (_ Def) ScriptBuiltins(
	add AddScriptDef,
) ScriptBuiltins {
	return ScriptBuiltins{

		"execute_timeout": starlarkutil.MakeFunc("execute_timeout", func(t ExecuteTimeout) {
			add(&t)
		}),

		"hour":        starlark.MakeUint64(uint64(time.Hour)),
		"minute":      starlark.MakeUint64(uint64(time.Minute)),
		"second":      starlark.MakeUint64(uint64(time.Second)),
		"millisecond": starlark.MakeUint64(uint64(time.Millisecond)),
		"microsecond": starlark.MakeUint64(uint64(time.Microsecond)),
		"nanosecond":  starlark.MakeUint64(uint64(time.Nanosecond)),

		"temp_dir_model": starlarkutil.MakeFunc("temp_dir_model", func(model TempDirModel) {
			add(&model)
		}),

		"network_model": starlarkutil.MakeFunc("network_model", func(model NetworkModel) {
			add(&model)
		}),

		"port_range": starlarkutil.MakeFunc("port_range", func(lower uint16, upper uint16) {
			add(func() PortRange {
				return PortRange{lower, upper}
			})
		}),

		"debug_9p": starlarkutil.MakeFunc("debug_9p", func(debug Debug9P) {
			add(&debug)
		}),

		//
	}
}

type (
	AddScriptDef  func(any)
	GetScriptDefs func() []any
)

func (_ Def) ScriptDefs() (
	add AddScriptDef,
	get GetScriptDefs,
) {
	var defs []any
	add = func(def any) {
		defs = append(defs, def)
	}
	get = func() []any {
		return defs
	}
	return
}
