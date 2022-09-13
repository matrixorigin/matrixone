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

package unnest

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("unnest")
}

func Prepare(_ *process.Process, arg any) error {
	param := arg.(*Argument).Es
	param.colName = "UNNEST_DEFAULT"
	if param.Extern.IsCol {
		_, _, param.colName = param.Extern.Origin.(*tree.UnresolvedName).GetNames()
	}
	param.seq = 0
	param.end = false
	var filters []string
	for i := range param.Attrs {
		denied := false
		for j := range deniedFilters {
			if param.Attrs[i] == deniedFilters[j] {
				denied = true
				break
			}
		}
		if !denied {
			filters = append(filters, param.Attrs[i])
		}
	}
	param.filters = filters
	return nil
}

func Call(_ int, proc *process.Process, arg any) (bool, error) {
	param := arg.(*Argument).Es
	if param.end {
		return true, nil
	}
	if param.Extern.IsCol {
		return callByCol(param, proc)
	}
	return callByStr(param, proc)
}

func callByStr(param *Param, proc *process.Process) (bool, error) {
	json, err := types.ParseStringToByteJson(param.Extern.Origin.(string))
	if err != nil {
		return false, err
	}
	path, err := types.ParseStringToPath(param.Extern.Path)
	if err != nil {
		return false, err
	}
	ures, err := json.Unnest(&path, param.Extern.Outer, recursive, mode, param.filters)
	bat := batch.New(false, param.Attrs)
	for i := range param.Cols {
		bat.Vecs[i] = vector.New(dupType(param.Cols[i].Typ))
	}
	bat, err = makeBatch(bat, ures, param, proc)
	if err != nil {
		return false, err
	}
	bat.InitZsOne(len(ures))
	proc.SetInputBatch(bat)
	param.end = true
	return false, nil
}

func callByCol(param *Param, proc *process.Process) (bool, error) {
	reg := proc.Reg.MergeReceivers[0]
	select {
	case <-reg.Ctx.Done():
		param.end = true
		return true, nil
	case data := <-reg.Ch:
		if data == nil {
			param.end = true
			return true, nil
		}
		if len(data.Vecs) != 1 {
			return false, fmt.Errorf("unnest: invalid input batch,len(vecs)[%d] != 1", len(data.Vecs))
		}
		vec := data.GetVector(0)
		if vec.Typ.Oid != types.T_json {
			return false, fmt.Errorf("unnest: invalid column type:%s", vec.Typ)
		}
		path, err := types.ParseStringToPath(param.Extern.Path)
		if err != nil {
			return false, err
		}
		bat := batch.New(false, param.Attrs)
		for i := range param.Cols {
			bat.Vecs[i] = vector.New(dupType(param.Cols[i].Typ))
		}
		col := vector.GetBytesVectorValues(vec)
		rows := 0
		for i := 0; i < len(col); i++ {
			json := types.DecodeJson(col[i])
			if err != nil {
				return false, err
			}
			ures, err := json.Unnest(&path, param.Extern.Outer, recursive, mode, param.filters)
			bat, err = makeBatch(bat, ures, param, proc)
			if err != nil {
				return false, err
			}
			rows += len(ures)
		}
		bat.InitZsOne(rows)
		proc.SetInputBatch(bat)
	}
	return false, nil
}

func makeBatch(bat *batch.Batch, ures []bytejson.UnnestResult, param *Param, proc *process.Process) (*batch.Batch, error) {
	for i := 0; i < len(ures); i++ {
		for j := 0; j < len(param.Attrs); j++ {
			vec := bat.GetVector(int32(j))
			var err error
			switch param.Attrs[j] {
			case "col":
				err = vec.Append([]byte(param.colName), false, proc.Mp())
			case "seq":
				err = vec.Append(param.seq, false, proc.Mp())
			case "key", "path", "index", "value", "this":
				val, ok := ures[i][param.Attrs[j]]
				err = vec.Append([]byte(val), !ok, proc.Mp())
			default:
				err = fmt.Errorf("unnest: invalid column name:%s", param.Attrs[j])
			}
			if err != nil {
				return nil, err
			}
		}
	}
	param.seq += 1
	return bat, nil
}
func dupType(typ *plan.Type) types.Type {
	return types.Type{
		Oid:       types.T(typ.Id),
		Width:     typ.Width,
		Size:      typ.Size,
		Precision: typ.Precision,
	}
}
