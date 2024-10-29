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
	"os/exec"
	"strings"

	"github.com/buger/jsonparser"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/datalink"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

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

	// plugin exec
	vlen := len(tf.ctr.argVecs)
	if vlen != 2 {
		return moerr.NewInternalError(proc.Ctx, "plugin_exec: number of args != 2")
	}

	// command
	var cmdbytes []byte
	cmdVec := tf.ctr.argVecs[0]
	switch cmdVec.GetType().Oid {
	case types.T_json:

		if cmdVec.IsNull(uint64(nthRow)) {
			u.batch.SetRowCount(0)
			return nil
		}
		cmd := cmdVec.GetBytesAt(nthRow)
		cmdjs := bytejson.ByteJson{}
		cmdjs.Unmarshal(cmd)

		if cmdjs.Type != bytejson.TpCodeArray {
			return moerr.NewInternalError(proc.Ctx, "command is a JSON array")
		}

		cmdbytes, _ = cmdjs.MarshalJSON()
	case types.T_varchar, types.T_text, types.T_char:
		cmdbytes = cmdVec.GetBytesAt(nthRow)
	default:
		return moerr.NewInternalError(proc.Ctx, "command is a JSON or string")
	}

	//logutil.Infof("COMMAND %s", string(cmdbytes))
	var args []string
	var cmderr error
	jsonparser.ArrayEach(cmdbytes, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {

		if err != nil {
			cmderr = errors.Join(cmderr, err)
			return
		}

		if dataType != jsonparser.String {
			cmderr = errors.Join(cmderr, moerr.NewInternalError(proc.Ctx, "command argument is not string"))
			return
		}

		args = append(args, string(value))
	})

	if cmderr != nil {
		return cmderr
	}

	if len(args) == 0 {
		return moerr.NewInternalError(proc.Ctx, "command is empty")
	}

	//logutil.Infof("ARGS %v", args)

	// datalink
	dlVec := tf.ctr.argVecs[1]
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
		bytes, err = dl.GetBytes(proc)
		if err != nil {
			return err
		}
	case types.T_varchar, types.T_text, types.T_char:
		bytes = []byte(src)
	default:
		return moerr.NewInternalError(proc.Ctx, "plugin_exec input type not supported")
	}

	// run command
	var plexec *exec.Cmd
	if len(args) == 1 {
		plexec = exec.CommandContext(proc.Ctx, args[0])
	} else {
		plexec = exec.CommandContext(proc.Ctx, args[0], args[1:]...)
	}
	stdin, err := plexec.StdinPipe()
	if err != nil {
		return err
	}

	go func() error {
		defer stdin.Close()
		_, err := stdin.Write(bytes)
		return err
	}()

	out, err := plexec.Output()
	if err != nil {
		return err
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
