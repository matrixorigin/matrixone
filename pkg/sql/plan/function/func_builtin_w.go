// Copyright 2021 - 2022 Matrix Origin
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
	"context"

	extism "github.com/extism/go-sdk"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// see https://extism.org for golang,
// 		see https://github.com/extism/go-sdk and https://github.com/extism/go-pdk
//
// wasm(wasmurl, fn, arg) runs the wasm (as extism plugin), call function fn with
// the given arg. The wasmurl must be a url to a wasm file that can be accessed by CN.
// The fn must be a valid function name in the plugin.  Arg is passed in as string and
// the result is also returned as string.  For other types (and multiple args),
// user must econde the args into string -- usually using json.
//
// try_wasm is the same as startlark, but it will error if there is error
// when running wasm.  Instead, it will just return NULL.

type opBuiltInWasm struct {
	// do we need to call plugin.Close()?
	plugin *extism.Plugin
}

func newOpBuiltInWasm() *opBuiltInWasm {
	return &opBuiltInWasm{}
}

func (op *opBuiltInWasm) buildWasm(ctx context.Context, url string) error {
	var err error

	// manifest is created from wasm url.
	// We will need to handle stage in the future.
	manifest := extism.Manifest{
		Wasm: []extism.Wasm{
			extism.WasmUrl{
				Url: url,
			},
		},
	}
	// enable wasi: tinygo build wasm need wasi.
	config := extism.PluginConfig{
		EnableWasi: true,
	}
	op.plugin, err = extism.NewPlugin(ctx, manifest, config, []extism.HostFunction{})
	return err
}

func (op *opBuiltInWasm) runWasm(fn string, arg []byte) ([]byte, error) {
	_, out, err := op.plugin.Call(fn, arg)
	return out, err
}

func (op *opBuiltInWasm) wasm(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList) error {
	return op.tryWasmImpl(params, result, proc, length, selectList, false)
}

func (op *opBuiltInWasm) tryWasm(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList) error {
	return op.tryWasmImpl(params, result, proc, length, selectList, true)
}

func (op *opBuiltInWasm) tryWasmImpl(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList, isTry bool) error {
	p1 := vector.GenerateFunctionStrParameter(params[0])
	if !params[0].IsConst() {
		return moerr.NewInvalidInput(proc.Ctx, "wasm url must be constant.")
	}
	url, isnull := p1.GetStrValue(0)
	if isnull {
		return moerr.NewInvalidInput(proc.Ctx, "wasm url cannot be null.")
	}
	if err := op.buildWasm(proc.Ctx, string(url)); err != nil {
		return err
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	p2 := vector.GenerateFunctionStrParameter(params[1])
	p3 := vector.GenerateFunctionStrParameter(params[2])

	if selectList.IgnoreAllRow() {
		rs.AddNullRange(0, uint64(length))
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		fn, isnull := p2.GetStrValue(i)
		if isnull {
			rs.AppendBytes(nil, true)
			continue
		}
		arg, isnull := p3.GetStrValue(i)
		if isnull {
			rs.AppendBytes(nil, true)
			continue
		}

		res, err := op.runWasm(string(fn), arg)
		if err != nil {
			if isTry {
				rs.AppendBytes(nil, true)
			} else {
				return err
			}
		} else {
			rs.AppendBytes(res, false)
		}
	}
	return nil
}
