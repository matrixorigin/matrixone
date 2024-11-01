// Copyright 2022 Matrix Origin
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

package table_function

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/buger/jsonparser"
	extism "github.com/extism/go-sdk"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/datalink"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Plugin is a framework to run an executable with input data (datalink or string) from stdin and
// output must be a JSON array
// Plugin needs two input columns
// 1. command line with arguments in JSON, e.g. ["cat", "filename"]. Note: file path must be an absolute path
// 2. input data with datalink or string. With offset and size parameter in datalink, only portion
// of data is read and send to stdin.
// Return:
// output buffer in JSON Array format
//
// One of the example is wikipedia dumps.  Wiki dump is a multisteam file which combine multiple bzip2
// data chunks and its index file has the (offset, ID, title).  The advantage is we don't need to unpack
// the whole file before getting the required data.
// Also, for LLM application, you can run the table_function plugin_exec() to convert the data from datalink
// into the embedding by calling external LLM vendor.
// All you have to do it to write a plugin executable in any language that
// 1. read the data from stdin, unzip data with bzip2, parse xml to get all pages
// 2. For each page, cut into multiple chunks
// 3. send each chunk to LLM to convert into embedding
// 4. format list of chunk embedding into JSON Array format
// 5. Write the JSON to stdout
// 6. Insert the embedding into table with the JSON array returned from stdout
//
// To do this, simple run the SQL like this:
// INSERT INTO t1 SELECT json_unquote(json_extract(result, '$.chunk')), json_unquote(json_extract(result, '$.e')) from
// plugin_exec('["python3", "gen_embedding.py", "arg1", "arg2"]', cast('stage://mys3/wiki.bz2?offset=0&size=123456' as datalink)) as f;
//

type pluginState struct {
	inited bool
	called bool
	// holding one call batch, pluginState owns it.
	batch *batch.Batch
}

func (u *pluginState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
	u.called = false
}

func (u *pluginState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	var res vm.CallResult
	if u.called {
		return res, nil
	}
	res.Batch = u.batch
	u.called = true
	return res, nil
}

func (u *pluginState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

func pluginPrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &pluginState{}
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))

	for i := range arg.Attrs {
		arg.Attrs[i] = strings.ToUpper(arg.Attrs[i])
	}
	return st, err

}

// start calling tvf on nthRow and put the result in u.batch.  Note that current tokenize impl will
// always return one batch per nthRow.
func (u *pluginState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {

	if !u.inited {
		u.batch = tf.createResultBatch()
		u.inited = true
	}

	u.called = false
	// cleanup the batch
	u.batch.CleanOnlyData()

	// plugin exec: number of args is always 4. (wasm_url string, func_name string, config map in JSON, datalink)

	// wasm
	wasmVec := tf.ctr.argVecs[0]
	switch wasmVec.GetType().Oid {
	case types.T_varchar, types.T_datalink, types.T_char, types.T_text:
	default:
		return moerr.NewInternalError(proc.Ctx, "wasm URL only support varchar, char, text and datalink type")
	}

	if wasmVec.IsNull(uint64(nthRow)) {
		u.batch.SetRowCount(0)
		return nil
	}
	wasmurl := wasmVec.GetStringAt(nthRow)

	// func name
	funcVec := tf.ctr.argVecs[1]
	if funcVec.IsNull(uint64(nthRow)) {
		u.batch.SetRowCount(0)
		return nil
	}
	funcname := funcVec.GetStringAt(nthRow)

	// config
	var cfgbytes []byte
	cfgVec := tf.ctr.argVecs[2]
	if !cfgVec.IsNull(uint64(nthRow)) {
		switch cfgVec.GetType().Oid {
		case types.T_json:

			cfg := cfgVec.GetBytesAt(nthRow)
			cfgjs := bytejson.ByteJson{}
			cfgjs.Unmarshal(cfg)

			if cfgjs.Type != bytejson.TpCodeObject {
				return moerr.NewInternalError(proc.Ctx, "config must be a JSON object")
			}

			cfgbytes, _ = cfgjs.MarshalJSON()
		case types.T_varchar, types.T_text, types.T_char:
			cfgbytes = cfgVec.GetBytesAt(nthRow)
		default:
			return moerr.NewInternalError(proc.Ctx, "config must be a JSON Object or string")
		}
	}

	cfgmap := make(map[string]string)

	if cfgbytes != nil {
		//logutil.Infof("COMMAND %s", string(cmdbytes))
		jsonparser.ObjectEach(cfgbytes, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {

			if dataType != jsonparser.String {
				return moerr.NewInternalError(proc.Ctx, "config value is not string")
			}

			cfgmap[string(key)] = string(value)
			return nil
		})
		logutil.Infof("CONFig %v", cfgmap)
	}
	//logutil.Infof("ARGS %v", args)

	// datalink
	dlVec := tf.ctr.argVecs[3]
	if dlVec.IsNull(uint64(nthRow)) {
		u.batch.SetRowCount(0)
		return nil
	}
	src := dlVec.GetStringAt(nthRow)

	var bytes []byte
	switch dlVec.GetType().Oid {
	case types.T_datalink:
		dl, err := datalink.NewDatalink(src, proc)
		if err != nil {
			return err
		}
		bytes, err = dl.GetPlainText(proc)
		if err != nil {
			return err
		}
	case types.T_varchar, types.T_text, types.T_char:
		bytes = []byte(src)
	default:
		return moerr.NewInternalError(proc.Ctx, "plugin_exec input type not supported")
	}

	wurl, err := url.Parse(wasmurl)
	if err != nil {
		return err
	}

	var manifest extism.Manifest
	if wurl.Scheme == "https" || wurl.Scheme == "http" {

		manifest = extism.Manifest{
			Wasm: []extism.Wasm{
				extism.WasmUrl{
					Url: wasmurl,
				},
			},
			Config: cfgmap,
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
			Config: cfgmap,
		}
	}

	config := extism.PluginConfig{
		EnableWasi: true,
	}
	plugin, err := extism.NewPlugin(proc.Ctx, manifest, config, []extism.HostFunction{})
	if err != nil {
		return err
	}

	exit, out, err := plugin.Call(funcname, bytes)
	if err != nil {
		return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("plugin exit with error %d. err %v", exit, err))
	}

	nitem := 0
	var jserr error
	jsonparser.ArrayEach(out, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {

		if err != nil {
			jserr = errors.Join(jserr, err)
			return
		}

		bj := bytejson.ByteJson{}
		isnull := false
		switch dataType {
		case jsonparser.String:
			jsdata, err := json.Marshal(string(value))
			if err != nil {
				jserr = errors.Join(jserr, err)
				return
			}
			err = bj.UnmarshalJSON(jsdata)
			if err != nil {
				jserr = errors.Join(jserr, err)
				return
			}
		case jsonparser.Number, jsonparser.Boolean, jsonparser.Object, jsonparser.Array:
			err = bj.UnmarshalJSON(value)
			if err != nil {
				jserr = errors.Join(jserr, err)
				return
			}
		case jsonparser.Null:
			isnull = true
		default:
			jserr = errors.Join(jserr, moerr.NewInternalErrorNoCtx("unknown json type"))
			return
		}
		vector.AppendByteJson(u.batch.Vecs[0], bj, isnull, proc.Mp())
		nitem += 1
	})

	if jserr != nil {
		return jserr
	}

	u.batch.SetRowCount(nitem)
	return nil
}
