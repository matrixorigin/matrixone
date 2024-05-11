// Copyright 2022 Matrix Origin
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

package source

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	mokafka "github.com/matrixorigin/matrixone/pkg/stream/adapter/kafka"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "source"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": source scan")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	_, span := trace.Start(proc.Ctx, "SourcePrepare")
	defer span.End()

	arg.attrs = make([]string, len(arg.TblDef.Cols))
	arg.types = make([]types.Type, len(arg.TblDef.Cols))
	arg.Configs = make(map[string]interface{})
	for i, col := range arg.TblDef.Cols {
		arg.attrs[i] = col.Name
		arg.types[i] = types.Type{
			Oid:   types.T(col.Typ.Id),
			Scale: col.Typ.Scale,
			Width: col.Typ.Width,
		}
	}
	for _, def := range arg.TblDef.Defs {
		switch v := def.Def.(type) {
		case *plan.TableDef_DefType_Properties:
			for _, x := range v.Properties.Properties {
				arg.Configs[x.Key] = x.Value
			}
		}
	}

	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	_, span := trace.Start(proc.Ctx, "SourceCall")
	defer span.End()

	if arg.buf != nil {
		proc.PutBatch(arg.buf)
		arg.buf = nil
	}
	result := vm.NewCallResult()
	var err error

	switch arg.status {
	case retrieve:
		arg.buf, err = mokafka.RetrieveData(proc.Ctx, proc.SessionInfo.SourceInMemScanBatch, arg.Configs, arg.attrs, arg.types, arg.Offset, arg.Limit, proc.Mp(), mokafka.NewKafkaAdapter)
		if err != nil {
			result.Status = vm.ExecStop
			return result, err
		}
		arg.status = end
		result.Batch = arg.buf
		result.Status = vm.ExecNext
	case end:
		result.Status = vm.ExecStop
	}

	return result, nil
}
