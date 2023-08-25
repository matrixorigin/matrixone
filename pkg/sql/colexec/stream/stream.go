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

package stream

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("stream scan")
}

func Prepare(proc *process.Process, arg any) error {
	_, span := trace.Start(proc.Ctx, "StreamPrepare")
	defer span.End()

	p := arg.(*Argument)
	p.attrs = make([]string, len(p.TblDef.Cols))
	p.types = make([]types.Type, len(p.TblDef.Cols))
	p.configs = make(map[string]interface{})
	for i, col := range p.TblDef.Cols {
		p.attrs[i] = col.Name
		p.types[i] = types.Type{
			Oid:   types.T(col.Typ.Id),
			Scale: col.Typ.Scale,
			Width: col.Typ.Width,
		}
	}
	for _, def := range p.TblDef.Defs {
		switch v := def.Def.(type) {
		case *plan.TableDef_DefType_Properties:
			for _, x := range v.Properties.Properties {
				p.configs[x.Key] = x.Value
			}
		}
	}

	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	return process.ExecStop, moerr.NewNYI(proc.Ctx, "stream scan operator")
	//_, span := trace.Start(proc.Ctx, "StreamCall")
	//defer span.End()

	//p := arg.(*Argument)
	//if p.end {
	//	proc.SetInputBatch(nil)
	//	return process.ExecStop, nil
	//}
	//
	////TODO:
	////b, err := RetrieveData(ctx, p.configs, p.attrs, p.types, p.Offset, p.Limit, proc.Mp(), NewKafkaAdapter)
	////if err != nil {
	////	return process.ExecStop, err
	////}
	//p.end = true
	//proc.SetInputBatch(nil)
	//
	//return process.ExecStop, nil
}
