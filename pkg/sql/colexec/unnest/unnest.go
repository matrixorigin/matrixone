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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strconv"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("unnest")
}

func Prepare(_ *process.Process, arg any) error {
	param := arg.(*Argument).Es
	param.colName = "UNNEST_DEFAULT"
	//if len(param.Extern.ColName) != 0 {
	//	param.colName = param.Extern.ColName
	//}
	//param.path = param.Extern.Path
	//param.outer = param.Extern.Outer
	//param.typ = param.Extern.Typ
	param.seq = 0
	if len(param.ExprList) < 2 {
		param.path = "$"
		param.outer = false
	}
	if len(param.ExprList) < 3 {
		param.outer = false
	}
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
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	if len(param.ExprList) == 1 {
		src, err := colexec.EvalExpr(bat, proc, param.ExprList[0])
		if err != nil {
			return false, err
		}
		if src.Typ.Oid == types.T_json {
			fmt.Println("json")
		}
		return callByStr(src, param, proc)
	}
	switch param.typ {
	case "col":
		return callByCol(param, proc)
	case "func":
		return callByFunc(param, proc)
	}
	fmt.Println(bat)
	return false, moerr.NewInvalidArg("unnest: invalid type:%s", param.typ)
}

func callByFunc(param *Param, proc *process.Process) (bool, error) {
	var (
		err    error
		vec    *vector.Vector
		rbat   *batch.Batch
		tmpBat *batch.Batch
		path   bytejson.Path
		json   bytejson.ByteJson
	)
	defer func() {
		if err != nil {
			if tmpBat != nil {
				tmpBat.Clean(proc.Mp())
			}
			if rbat != nil {
				rbat.Clean(proc.Mp())
			}
		}
	}()
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	tmpBat = batch.NewWithSize(0)
	tmpBat.Zs = []int64{1}
	tmpBat.Cnt = 1
	vec, err = colexec.EvalExpr(tmpBat, proc, param.ExprList[0])
	if err != nil {
		return false, err
	}
	col := vector.MustStrCols(vec)
	path, err = types.ParseStringToPath(param.path)
	if err != nil {
		return false, err
	}
	rbat = batch.New(false, param.Attrs)
	for i := range param.Cols {
		rbat.Vecs[i] = vector.New(dupType(param.Cols[i].Typ))
	}
	rows := 0
	for i := range col {
		var ures []bytejson.UnnestResult
		json, err = types.ParseStringToByteJson(col[i])
		if err != nil {
			return false, err
		}
		ures, err = json.Unnest(&path, param.outer, recursive, mode, param.filters)
		if err != nil {
			return false, err
		}
		rbat, err = makeBatch(rbat, ures, param, proc)
		if err != nil {
			return false, err
		}
		rows += len(ures)
	}
	rbat.InitZsOne(rows)
	proc.SetInputBatch(rbat)
	return false, nil
}

func callByStr(vec *vector.Vector, param *Param, proc *process.Process) (bool, error) {
	var (
		err  error
		rbat *batch.Batch
		path bytejson.Path
		json bytejson.ByteJson
		ures []bytejson.UnnestResult
	)
	defer func() {
		if err != nil && rbat != nil {
			rbat.Clean(proc.Mp())
		}
	}()

	json, err = types.ParseStringToByteJson(vec.GetString(0))
	if err != nil {
		return false, err
	}
	path, err = types.ParseStringToPath(param.path)
	if err != nil {
		return false, err
	}
	ures, err = json.Unnest(&path, param.outer, recursive, mode, param.filters)
	if err != nil {
		return false, err
	}
	rbat = batch.New(false, param.Attrs)
	rbat.Cnt = 1
	for i := range param.Cols {
		rbat.Vecs[i] = vector.New(dupType(param.Cols[i].Typ))
	}
	rbat, err = makeBatch(rbat, ures, param, proc)
	if err != nil {
		return false, err
	}
	rbat.InitZsOne(len(ures))
	proc.SetInputBatch(rbat)
	return false, nil
}

func callByCol(param *Param, proc *process.Process) (bool, error) {
	var (
		err  error
		vec  *vector.Vector
		rbat *batch.Batch
		path bytejson.Path
		json bytejson.ByteJson
	)
	defer func() {
		if err != nil && rbat != nil {
			rbat.Clean(proc.Mp())
		}
	}()
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	if len(bat.Vecs) != 1 {
		return false, moerr.NewInvalidArg("unnest: invalid input batch,len(vecs)[%d] != 1", len(bat.Vecs))
	}
	vec = bat.GetVector(0)
	if vec.Typ.Oid != types.T_json && vec.Typ.Oid != types.T_varchar {
		return false, moerr.NewInvalidArg("unnest: invalid column type:%s", vec.Typ)
	}
	path, err = types.ParseStringToPath(param.path)
	if err != nil {
		return false, err
	}
	rbat = batch.New(false, param.Attrs)
	rbat.Cnt = 1
	for i := range param.Cols {
		rbat.Vecs[i] = vector.New(dupType(param.Cols[i].Typ))
	}
	col := vector.GetBytesVectorValues(vec)
	rows := 0
	for i := 0; i < len(col); i++ {
		var ures []bytejson.UnnestResult
		if vec.Typ.Oid == types.T_json {
			json = types.DecodeJson(col[i])
		} else {
			json, err = types.ParseSliceToByteJson(col[i])
			if err != nil {
				return false, err
			}
		}
		ures, err = json.Unnest(&path, param.outer, recursive, mode, param.filters)
		if err != nil {
			return false, err
		}
		rbat, err = makeBatch(rbat, ures, param, proc)
		if err != nil {
			return false, err
		}
		rows += len(ures)
	}
	rbat.InitZsOne(rows)
	proc.SetInputBatch(rbat)
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
			case "index":
				val, ok := ures[i][param.Attrs[j]]
				if !ok {
					err = vec.Append(int32(0), true, proc.Mp())
				} else {
					intVal, _ := strconv.Atoi(val)
					err = vec.Append(int32(intVal), false, proc.Mp())
				}
			case "key", "path", "value", "this":
				val, ok := ures[i][param.Attrs[j]]
				err = vec.Append([]byte(val), !ok, proc.Mp())
			default:
				err = moerr.NewInvalidArg("unnest: invalid column name:%s", param.Attrs[j])
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
	return types.New(types.T(typ.Id), typ.Width, typ.Scale, typ.Precision)
}
