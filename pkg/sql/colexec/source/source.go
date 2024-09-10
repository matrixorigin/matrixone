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

const opName = "source"

func (source *Source) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": source scan")
}

func (source *Source) OpType() vm.OpType {
	return vm.Source
}

func (source *Source) Prepare(proc *process.Process) error {
	_, span := trace.Start(proc.Ctx, "SourcePrepare")
	defer span.End()

	source.attrs = make([]string, len(source.TblDef.Cols))
	source.types = make([]types.Type, len(source.TblDef.Cols))
	source.Configs = make(map[string]interface{})
	for i, col := range source.TblDef.Cols {
		source.attrs[i] = col.GetOriginCaseName()
		source.types[i] = types.Type{
			Oid:   types.T(col.Typ.Id),
			Scale: col.Typ.Scale,
			Width: col.Typ.Width,
		}
	}
	for _, def := range source.TblDef.Defs {
		switch v := def.Def.(type) {
		case *plan.TableDef_DefType_Properties:
			for _, x := range v.Properties.Properties {
				source.Configs[x.Key] = x.Value
			}
		}
	}

	if source.ProjectList != nil {
		err := source.PrepareProjection(proc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (source *Source) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	_, span := trace.Start(proc.Ctx, "SourceCall")
	defer span.End()

	if source.ctr.buf != nil {
		source.ctr.buf.Clean(proc.GetMPool())
		source.ctr.buf = nil
	}
	result := vm.NewCallResult()
	var err error

	switch source.ctr.status {
	case retrieve:
		source.ctr.buf, err = mokafka.RetrieveData(proc.Ctx, proc.GetSessionInfo().SourceInMemScanBatch, source.Configs, source.attrs, source.types, source.Offset, source.Limit, proc.Mp(), mokafka.NewKafkaAdapter)
		if err != nil {
			result.Status = vm.ExecStop
			return result, err
		}
		source.ctr.status = end
		result.Batch = source.ctr.buf
		result.Status = vm.ExecNext
	case end:
		result.Status = vm.ExecStop
	}

	if source.ProjectList != nil {
		result.Batch, err = source.EvalProjection(result.Batch, proc)
	}

	return result, err
}
