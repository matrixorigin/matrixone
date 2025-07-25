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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type unnestTestCase struct {
	arg      *TableFunction
	proc     *process.Process
	jsons    []string
	paths    []string
	outers   []bool
	success  bool
	jsonType string
}

var (
	defaultAttrs = []string{"col", "seq", "key", "path", "index", "value", "this"}
	//defaultExprs   = []*plan.Expr{
	//	&plan.Expr_C{
	//		C: &plan.Const{
	//			Isnull: false,
	//			Value: &plan.Literal_Sval{}
	//		}
	//	}
	//}
	defaultColDefs = []*plan.ColDef{
		{
			Name: "col",
			Typ: plan.Type{
				Id:          int32(types.T_varchar),
				NotNullable: false,
				Width:       4,
			},
		},
		{
			Name: "seq",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
			},
		},
		{
			Name: "key",
			Typ: plan.Type{
				Id:          int32(types.T_varchar),
				NotNullable: false,
				Width:       256,
			},
		},
		{
			Name: "path",
			Typ: plan.Type{
				Id:          int32(types.T_varchar),
				NotNullable: false,
				Width:       256,
			},
		},
		{
			Name: "index",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
			},
		},
		{
			Name: "value",
			Typ: plan.Type{
				Id:          int32(types.T_varchar),
				NotNullable: false,
				Width:       1024,
			},
		},
		{
			Name: "this",
			Typ: plan.Type{
				Id:          int32(types.T_varchar),
				NotNullable: false,
				Width:       1024,
			},
		},
	}
)

func makeTestCases(t *testing.T) []unnestTestCase {
	return []unnestTestCase{
		newTestCase(t, mpool.MustNewZero(), defaultAttrs, []string{`{"a":1}`}, []string{`$`}, []bool{false}, "str", true),
		newTestCase(t, mpool.MustNewZero(), []string{"key", "col"}, []string{`{"a":1}`}, []string{`$`}, []bool{false}, "json", true),
		newTestCase(t, mpool.MustNewZero(), defaultAttrs, []string{`{"a":1}`, `{"b":1}`}, []string{`$`}, []bool{false}, "json", true),
		newTestCase(t, mpool.MustNewZero(), defaultAttrs, []string{`{"a":1}`, `{"b":1}`}, []string{`$`, `$`}, []bool{false}, "str", false),
		newTestCase(t, mpool.MustNewZero(), defaultAttrs, []string{`{"a":1}`, `{"b":1}`}, []string{`$`}, []bool{false, true}, "json", true),
	}
}

func newTestCase(t *testing.T, m *mpool.MPool, attrs []string, jsons, paths []string, outers []bool, jsonType string, success bool) unnestTestCase {
	proc := testutil.NewProcessWithMPool(t, "", m)
	colDefs := make([]*plan.ColDef, len(attrs))
	for i := range attrs {
		for j := range defaultColDefs {
			if attrs[i] == defaultColDefs[j].Name {
				colDefs[i] = defaultColDefs[j]
				break
			}
		}
	}

	ret := unnestTestCase{
		proc: proc,
		arg: &TableFunction{
			Attrs:    attrs,
			Rets:     colDefs,
			FuncName: "unnest",
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
		jsons:    jsons,
		paths:    paths,
		outers:   outers,
		success:  success,
		jsonType: jsonType,
	}

	return ret
}

func TestUnnestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestUnnestCall(t *testing.T) {
	for _, ut := range makeTestCases(t) {

		err := ut.arg.Prepare(ut.proc)
		require.NotNil(t, err)

		var inputBat *batch.Batch
		switch ut.jsonType {
		case "str":
			inputBat, err = makeUnnestBatch(ut.jsons, types.T_varchar, encodeStr, ut.proc)
			require.Nil(t, err)
			ut.arg.Args = makeConstInputExprs(ut.jsons, ut.paths, ut.jsonType, ut.outers)

			// fake retSchema
			retSchema := make([]types.Type, len(ut.arg.Rets))
			for i := range ut.arg.Rets {
				typ := ut.arg.Rets[i].Typ
				retSchema[i] = types.New(types.T(typ.Id), typ.Width, typ.Scale)
			}
			ut.arg.ctr.retSchema = retSchema

			tvfst, err := unnestPrepare(ut.proc, ut.arg)
			require.Nil(t, err)

			// faking args.  unnestPrepare should have build place holders for 3 args.
			require.True(t, len(ut.arg.Args) == 3)
			require.True(t, len(ut.arg.ctr.argVecs) == 3)

			// Got a valid batch, eval tbf args.  first eval input batch for args.
			for i := range ut.arg.ctr.executorsForArgs {
				ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inputBat}, nil)
				require.Nil(t, err)
			}

			for i := 0; i < inputBat.RowCount(); i++ {
				err = tvfst.start(ut.arg, ut.proc, i, nil)
				require.Nil(t, err)
				for {
					res, err := tvfst.call(ut.arg, ut.proc)
					if err != nil || res.Batch.IsDone() {
						break
					}
				}
			}
			// we do not check result correctness?
			tvfst.free(ut.arg, ut.proc, false, nil)

		case "json":
			inputBat, err = makeUnnestBatch(ut.jsons, types.T_json, encodeJson, ut.proc)
			require.Nil(t, err)
			ut.arg.Args = makeColExprs(ut.jsonType, ut.paths, ut.outers)

			// fake retSchema
			retSchema := make([]types.Type, len(ut.arg.Rets))
			for i := range ut.arg.Rets {
				typ := ut.arg.Rets[i].Typ
				retSchema[i] = types.New(types.T(typ.Id), typ.Width, typ.Scale)
			}
			ut.arg.ctr.retSchema = retSchema

			tvfst, err := unnestPrepare(ut.proc, ut.arg)
			require.Nil(t, err)

			// faking args.  unnestPrepare should have build place holders for 3 args.
			require.True(t, len(ut.arg.Args) == 3)
			require.True(t, len(ut.arg.ctr.argVecs) == 3)

			// Got a valid batch, eval tbf args.  first eval input batch for args.
			for i := range ut.arg.ctr.executorsForArgs {
				ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inputBat}, nil)
				require.Nil(t, err)
			}

			for i := 0; i < inputBat.RowCount(); i++ {
				err = tvfst.start(ut.arg, ut.proc, i, nil)
				require.Nil(t, err)
				for {
					res, err := tvfst.call(ut.arg, ut.proc)
					if err != nil || res.Batch.IsDone() {
						break
					}
				}
			}
			// we do not check result correctness?
			tvfst.free(ut.arg, ut.proc, false, nil)
		}
	}
}

func makeUnnestBatch(jsons []string, typ types.T, fn func(str string) ([]byte, error), proc *process.Process) (*batch.Batch, error) {
	bat := batch.NewWithSize(1)
	bat.Attrs = []string{"a"}
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.NewVec(types.New(typ, 256, 0))
	}
	for _, json := range jsons {
		bjBytes, err := fn(json)
		if err != nil {
			return nil, err
		}
		err = vector.AppendBytes(bat.GetVector(0), bjBytes, false, proc.Mp())
		if err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
	}
	bat.SetRowCount(len(jsons))
	return bat, nil
}

func encodeJson(json string) ([]byte, error) {
	bj, err := types.ParseStringToByteJson(json)
	if err != nil {
		return nil, err
	}
	return types.EncodeJson(bj)
}
func encodeStr(json string) ([]byte, error) {
	return []byte(json), nil
}

func makeConstInputExprs(jsons, paths []string, jsonType string, outers []bool) []*plan.Expr {
	ret := make([]*plan.Expr, 3)
	typeId := int32(types.T_varchar)
	if jsonType == "json" {
		typeId = int32(types.T_json)
	}
	ret[0] = &plan.Expr{
		Typ: plan.Type{
			Id:    typeId,
			Width: 256,
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Sval{
					Sval: jsons[0],
				},
			},
		},
	}
	ret = appendOtherExprs(ret, paths, outers)
	return ret
}

func makeColExprs(jsonType string, paths []string, outers []bool) []*plan.Expr {
	ret := make([]*plan.Expr, 3)
	typeId := int32(types.T_varchar)
	if jsonType == "json" {
		typeId = int32(types.T_json)
	}
	ret[0] = &plan.Expr{
		Typ: plan.Type{
			Id: typeId,
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: 0,
			},
		},
	}
	ret = appendOtherExprs(ret, paths, outers)
	return ret
}

func appendOtherExprs(ret []*plan.Expr, paths []string, outers []bool) []*plan.Expr {
	ret[1] = &plan.Expr{
		Typ: plan.Type{
			Id:    int32(types.T_varchar),
			Width: 256,
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Sval{
					Sval: paths[0],
				},
			},
		},
	}
	ret[2] = &plan.Expr{
		Typ: plan.Type{
			Id: int32(types.T_bool),
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Bval{
					Bval: outers[0],
				},
			},
		},
	}
	return ret
}
