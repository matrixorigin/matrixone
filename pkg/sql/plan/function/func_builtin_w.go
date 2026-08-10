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
	"net/url"

	extism "github.com/extism/go-sdk"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/datalink"
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
// try_wasm has the same setup contract as wasm: URL parsing, image loading,
// and plugin construction errors are returned. Once the plugin is ready,
// per-row call errors are returned as NULL instead.

type opBuiltInWasm struct {
	plugin *extism.Plugin
}

func newOpBuiltInWasm() *opBuiltInWasm {
	return &opBuiltInWasm{}
}

// Close releases the plugin owned by this expression instance.  The function
// framework calls it both when an executor is reset for another query and when
// it is freed.
func (op *opBuiltInWasm) Close() error {
	if op.plugin == nil {
		return nil
	}
	plugin := op.plugin
	op.plugin = nil
	return plugin.Close()
}

func (op *opBuiltInWasm) Reset() error {
	return op.Close()
}

func (op *opBuiltInWasm) buildWasm(proc *process.Process, wasmurl string) error {
	var err error

	u, err := url.Parse(wasmurl)
	if err != nil {
		return err
	}

	var manifest extism.Manifest
	if u.Scheme == "http" || u.Scheme == "https" {
		// manifest is created from wasm url.
		manifest = extism.Manifest{
			Wasm: []extism.Wasm{
				extism.WasmUrl{
					Url: wasmurl,
				},
			},
		}
	} else {

		// treat as datalink
		wasmdl, err := datalink.NewDatalink(wasmurl, proc)
		if err != nil {
			return err
		}
		image, err := wasmdl.GetBytes(proc)
		if err != nil {
			return err
		}
		manifest = extism.Manifest{
			Wasm: []extism.Wasm{
				extism.WasmData{
					Data: image,
				},
			},
		}
	}

	// enable wasi: tinygo build wasm need wasi.
	config := extism.PluginConfig{
		EnableWasi: true,
	}
	// wasmurl is an external input and may name a different image on every
	// evaluation. Do not cache it across batches, but close the preceding batch's
	// instance before replacing it so the operator owns at most one plugin.
	if err = op.Close(); err != nil {
		return err
	}
	plugin, err := extism.NewPlugin(proc.Ctx, manifest, config, []extism.HostFunction{})
	if err != nil {
		return err
	}
	op.plugin = plugin
	return nil
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
	rs := vector.MustFunctionResult[types.Varlena](result)
	if length == 0 {
		return nil
	}
	if selectList.IgnoreAllRow() {
		rs.SetNullResult(uint64(length))
		return nil
	}

	p1 := vector.GenerateFunctionStrParameter(params[0])
	if !params[0].IsConst() {
		return moerr.NewInvalidInput(proc.Ctx, "wasm url must be constant.")
	}
	url, isnull := p1.GetStrValue(0)
	if isnull {
		return moerr.NewInvalidInput(proc.Ctx, "wasm url cannot be null.")
	}
	if err := op.buildWasm(proc, string(url)); err != nil {
		return err
	}

	p2 := vector.GenerateFunctionStrParameter(params[1])
	p3 := vector.GenerateFunctionStrParameter(params[2])

	for i := uint64(0); i < uint64(length); i++ {
		if selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		fn, isnull := p2.GetStrValue(i)
		if isnull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		arg, isnull := p3.GetStrValue(i)
		if isnull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		res, err := op.runWasm(string(fn), arg)
		if err != nil {
			if isTry {
				if err = rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				return err
			}
		} else {
			if err = rs.AppendBytes(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}
