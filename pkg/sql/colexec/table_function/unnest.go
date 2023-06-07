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

package table_function

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func genFilterMap(filters []string) map[string]struct{} {
	if filters == nil {
		return defaultFilterMap
	}
	filterMap := make(map[string]struct{}, len(filters))
	for _, f := range filters {
		filterMap[f] = struct{}{}
	}
	return filterMap
}

func unnestString(arg any, buf *bytes.Buffer) {
	buf.WriteString("unnest")
}

func unnestPrepare(proc *process.Process, arg *Argument) error {
	param := unnestParam{}
	param.ColName = string(arg.Params)
	if len(param.ColName) == 0 {
		param.ColName = "UNNEST_DEFAULT"
	}
	var filters []string
	for i := range arg.Attrs {
		denied := false
		for j := range unnestDeniedFilters {
			if arg.Attrs[i] == unnestDeniedFilters[j] {
				denied = true
				break
			}
		}
		if !denied {
			filters = append(filters, arg.Attrs[i])
		}
	}
	param.FilterMap = genFilterMap(filters)
	if len(arg.Args) < 1 || len(arg.Args) > 3 {
		return moerr.NewInvalidInput(proc.Ctx, "unnest: argument number must be 1, 2 or 3")
	}
	if len(arg.Args) == 1 {
		vType := types.T_varchar.ToType()
		bType := types.T_bool.ToType()
		arg.Args = append(arg.Args, &plan.Expr{Typ: plan2.MakePlan2Type(&vType), Expr: &plan.Expr_C{C: &plan2.Const{Value: &plan.Const_Sval{Sval: "$"}}}})
		arg.Args = append(arg.Args, &plan.Expr{Typ: plan2.MakePlan2Type(&bType), Expr: &plan.Expr_C{C: &plan2.Const{Value: &plan.Const_Bval{Bval: false}}}})
	} else if len(arg.Args) == 2 {
		bType := types.T_bool.ToType()
		arg.Args = append(arg.Args, &plan.Expr{Typ: plan2.MakePlan2Type(&bType), Expr: &plan.Expr_C{C: &plan2.Const{Value: &plan.Const_Bval{Bval: false}}}})
	}
	dt, err := json.Marshal(param)
	if err != nil {
		return err
	}
	arg.Params = dt

	arg.ctr = new(container)
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	return err
}

func unnestCall(_ int, proc *process.Process, arg *Argument) (bool, error) {
	var (
		err      error
		rbat     *batch.Batch
		jsonVec  *vector.Vector
		pathVec  *vector.Vector
		outerVec *vector.Vector
		path     bytejson.Path
		outer    bool
	)
	defer func() {
		if err != nil && rbat != nil {
			rbat.Clean(proc.Mp())
		}
		if jsonVec != nil {
			jsonVec.Free(proc.Mp())
		}
		if pathVec != nil {
			pathVec.Free(proc.Mp())
		}
		if outerVec != nil {
			outerVec.Free(proc.Mp())
		}
	}()
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	jsonVec, err = arg.ctr.executorsForArgs[0].Eval(proc, []*batch.Batch{bat})
	if err != nil {
		return false, err
	}
	if jsonVec.GetType().Oid != types.T_json && jsonVec.GetType().Oid != types.T_varchar {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("unnest: first argument must be json or string, but got %s", jsonVec.GetType().String()))
	}
	pathVec, err = arg.ctr.executorsForArgs[1].Eval(proc, []*batch.Batch{bat})
	if err != nil {
		return false, err
	}
	if pathVec.GetType().Oid != types.T_varchar {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("unnest: second argument must be string, but got %s", pathVec.GetType().String()))
	}
	outerVec, err = arg.ctr.executorsForArgs[2].Eval(proc, []*batch.Batch{bat})
	if err != nil {
		return false, err
	}
	if outerVec.GetType().Oid != types.T_bool {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("unnest: third argument must be bool, but got %s", outerVec.GetType().String()))
	}
	if !pathVec.IsConst() || !outerVec.IsConst() {
		return false, moerr.NewInvalidInput(proc.Ctx, "unnest: second and third arguments must be scalar")
	}
	path, err = types.ParseStringToPath(pathVec.GetStringAt(0))
	if err != nil {
		return false, err
	}
	outer = vector.MustFixedCol[bool](outerVec)[0]
	param := unnestParam{}
	if err = json.Unmarshal(arg.Params, &param); err != nil {
		return false, err
	}
	switch jsonVec.GetType().Oid {
	case types.T_json:
		rbat, err = handle(jsonVec, &path, outer, &param, arg, proc, parseJson)
	case types.T_varchar:
		rbat, err = handle(jsonVec, &path, outer, &param, arg, proc, parseStr)
	}
	if err != nil {
		return false, err
	}
	proc.SetInputBatch(rbat)
	return false, nil
}

func handle(jsonVec *vector.Vector, path *bytejson.Path, outer bool, param *unnestParam, arg *Argument, proc *process.Process, fn func(dt []byte) (bytejson.ByteJson, error)) (*batch.Batch, error) {
	var (
		err  error
		rbat *batch.Batch
		json bytejson.ByteJson
		ures []bytejson.UnnestResult
	)

	rbat = batch.NewWithSize(len(arg.Attrs))
	rbat.Attrs = arg.Attrs
	rbat.Cnt = 1
	for i := range arg.retSchema {
		rbat.Vecs[i] = vector.NewVec(arg.retSchema[i])
	}

	if jsonVec.IsConst() {
		json, err = fn(jsonVec.GetBytesAt(0))
		if err != nil {
			return nil, err
		}
		ures, err = json.Unnest(path, outer, unnestRecursive, unnestMode, param.FilterMap)
		if err != nil {
			return nil, err
		}
		rbat, err = makeBatch(rbat, ures, param, arg, proc)
		if err != nil {
			return nil, err
		}
		rbat.InitZsOne(len(ures))
		return rbat, nil
	}
	jsonSlice := vector.ExpandBytesCol(jsonVec)
	rows := 0
	for i := range jsonSlice {
		json, err = fn(jsonSlice[i])
		if err != nil {
			return nil, err
		}
		ures, err = json.Unnest(path, outer, unnestRecursive, unnestMode, param.FilterMap)
		if err != nil {
			return nil, err
		}
		rbat, err = makeBatch(rbat, ures, param, arg, proc)
		if err != nil {
			return nil, err
		}
		rows += len(ures)
	}
	rbat.InitZsOne(rows)
	return rbat, nil
}

func makeBatch(bat *batch.Batch, ures []bytejson.UnnestResult, param *unnestParam, arg *Argument, proc *process.Process) (*batch.Batch, error) {
	for i := 0; i < len(ures); i++ {
		for j := 0; j < len(arg.Attrs); j++ {
			vec := bat.GetVector(int32(j))
			var err error
			switch arg.Attrs[j] {
			case "col":
				err = vector.AppendBytes(vec, []byte(param.ColName), false, proc.Mp())
			case "seq":
				err = vector.AppendFixed(vec, int32(i), false, proc.Mp())
			case "index":
				val, ok := ures[i][arg.Attrs[j]]
				if !ok || val == nil {
					err = vector.AppendFixed(vec, int32(0), true, proc.Mp())
				} else {
					intVal, _ := strconv.ParseInt(string(val), 10, 32)
					err = vector.AppendFixed(vec, int32(intVal), false, proc.Mp())
				}
			case "key", "path", "value", "this":
				val, ok := ures[i][arg.Attrs[j]]
				err = vector.AppendBytes(vec, val, !ok || val == nil, proc.Mp())
			default:
				err = moerr.NewInvalidArg(proc.Ctx, "unnest: invalid column name:%s", arg.Attrs[j])
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return bat, nil
}

func parseJson(dt []byte) (bytejson.ByteJson, error) {
	ret := types.DecodeJson(dt)
	return ret, nil
}
func parseStr(dt []byte) (bytejson.ByteJson, error) {
	return types.ParseSliceToByteJson(dt)
}
