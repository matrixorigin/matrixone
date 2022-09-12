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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("unnest")
}

func Prepare(_ *process.Process, arg any) error {
	end = false
	seq = 0
	param := arg.(*Argument).Es
	colName = "UNNEST_DEFAULT"
	if param.Extern.IsCol {
		_, _, colName = param.Extern.Origin.(*tree.UnresolvedName).GetNames()
	}
	cols = param.Attrs
	colDefs = param.Cols
	return nil
}

func Call(_ int, proc *process.Process, arg any) (bool, error) {
	if end {
		return true, nil
	}
	logutil.Infof("unnest.Call")
	param := arg.(*Argument).Es
	if param.Extern.IsCol {
		return callByCol(param.Extern.Path, param.Extern.Outer, proc)
	}
	return callByStr(param.Extern.Origin.(string), param.Extern.Path, param.Extern.Outer, proc)
}

func callByStr(originStr, pathStr string, outer bool, proc *process.Process) (bool, error) {
	json, err := types.ParseStringToByteJson(originStr)
	if err != nil {
		return false, err
	}
	path, err := types.ParseStringToPath(pathStr)
	if err != nil {
		return false, err
	}
	ures, err := json.Unnest(path, outer, recursive, mode)
	bat := batch.New(false, cols)
	for i := range colDefs {
		bat.Vecs[i] = vector.New(name2Types[colDefs[i].Name])
	}
	bat, err = makeBatch(bat, ures, proc, 0, cols)
	if err != nil {
		return false, err
	}
	bat.InitZsOne(len(ures))
	proc.SetInputBatch(bat)
	end = true
	return false, nil
}

func callByCol(pathStr string, outer bool, proc *process.Process) (bool, error) {
	reg := proc.Reg.MergeReceivers[0]
	select {
	case <-reg.Ctx.Done():
		end = true
		return true, nil
	case data := <-reg.Ch:
		if data == nil {
			end = true
			return true, nil
		}
		if len(data.Vecs) != 1 {
			return false, fmt.Errorf("unnest: invalid input batch,len(vecs)[%d] != 1", len(data.Vecs))
		}
		vec := data.GetVector(0)
		if vec.Typ.Oid != types.T_json {
			return false, fmt.Errorf("unnest: invalid column type:%s", vec.Typ)
		}
		path, err := types.ParseStringToPath(pathStr)
		if err != nil {
			return false, err
		}
		bat := batch.New(false, cols)
		for i := range colDefs {
			bat.Vecs[i] = vector.New(name2Types[colDefs[i].Name])
		}
		col := vec.Col.(*types.Bytes)
		rows := 0
		for i := 0; i < len(col.Lengths); i++ {
			json := types.DecodeJson(col.Get(int64(i)))
			if err != nil {
				return false, err
			}
			ures, err := json.Unnest(path, outer, recursive, mode)
			bat, err = makeBatch(bat, ures, proc, rows, cols)
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

func makeBatch(bat *batch.Batch, ures []*bytejson.UnnestResult, proc *process.Process, start int, cols []string) (*batch.Batch, error) {
	for i := 0; i < len(ures); i++ {
		for j := 0; j < len(cols); j++ {
			vec := bat.GetVector(int32(j))
			var err error
			switch cols[j] {
			case "col":
				err = vec.Append([]byte(colName), proc.Mp)
			case "seq":
				err = vec.Append(seq, proc.Mp)
			case "key":
				if len(ures[i].Key) == 0 {
					nulls.Add(vec.Nsp, uint64(i+start))
				} else {
					err = vec.Append([]byte(ures[i].Key), proc.Mp)
				}
			case "path":
				if len(ures[i].Path) == 0 {
					nulls.Add(vec.Nsp, uint64(i+start))
				} else {
					err = vec.Append([]byte(ures[i].Path), proc.Mp)
				}
			case "index":
				if len(ures[i].Index) == 0 {
					nulls.Add(vec.Nsp, uint64(i+start))
				} else {
					err = vec.Append([]byte(ures[i].Index), proc.Mp)
				}
			case "value":
				if len(ures[i].Value) == 0 {
					nulls.Add(vec.Nsp, uint64(i+start))
				} else {
					err = vec.Append([]byte(ures[i].Value), proc.Mp)
				}
			case "this":
				if len(ures[i].This) == 0 {
					nulls.Add(vec.Nsp, uint64(i+start))
				} else {
					err = vec.Append([]byte(ures[i].This), proc.Mp)
				}
			default:
				err = fmt.Errorf("unnest: invalid column name:%s", cols[j])
			}
			if err != nil {
				return nil, err
			}
		}
	}
	seq += 1
	return bat, nil
}
