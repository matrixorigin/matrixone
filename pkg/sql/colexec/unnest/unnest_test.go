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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type unnestTestCase struct {
	arg        *Argument
	proc       *process.Process
	isCol      bool
	jsons      []string
	inputTimes int
}

var (
	utc            []unnestTestCase
	defaultAttrs   = []string{"col", "seq", "key", "path", "index", "value", "this"}
	defaultColDefs = []*plan.ColDef{
		{
			Name: "col",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    4,
			},
		},
		{
			Name: "seq",
			Typ: &plan.Type{
				Id:       int32(types.T_int32),
				Nullable: true,
			},
		},
		{
			Name: "key",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    256,
			},
		},
		{
			Name: "path",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    256,
			},
		},
		{
			Name: "index",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    4,
			},
		},
		{
			Name: "value",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    1024,
			},
		},
		{
			Name: "this",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    1024,
			},
		},
	}
)

func init() {
	utc = []unnestTestCase{
		newTestCase(mpool.MustNewZero(), defaultAttrs, defaultColDefs, `{"a":1}`, "$", false, false, []string{`{"a": 1}`}, 0),
		newTestCase(mpool.MustNewZero(), defaultAttrs, defaultColDefs, tree.SetUnresolvedName("t1", "a"), "$", false, true, []string{`{"a":1}`}, 3),
	}
}

func newTestCase(m *mpool.MPool, attrs []string, colDefs []*plan.ColDef, origin interface{}, path string, outer, isCol bool, jsons []string, inputTimes int) unnestTestCase {
	proc := testutil.NewProcessWithMPool(m)
	ret := unnestTestCase{
		proc: proc,
		arg: &Argument{
			Es: &Param{
				Attrs: attrs,
				Cols:  colDefs,
				Extern: &ExternalParam{
					Path:  path,
					Outer: outer,
				},
			},
		},
		isCol:      isCol,
		jsons:      jsons,
		inputTimes: inputTimes,
	}
	switch o := origin.(type) {
	case string:
		break
	case *tree.UnresolvedName:
		_, _, ret.arg.Es.Extern.ColName = o.GetNames()
	}
	return ret
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, ut := range utc {
		String(ut.arg, buf)
	}
}

func TestUnnest(t *testing.T) {
	for i, ut := range utc {
		nb0 := ut.proc.Mp().CurrNB()
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			err := Prepare(ut.proc, ut.arg)
			require.Nil(t, err)
			if !ut.isCol {
				ut.proc.Reg.InputBatch, err = makeTestBatch1(ut.jsons[0], ut.proc)
				require.Nil(t, err)
				end, err := Call(0, ut.proc, ut.arg)
				require.Nil(t, err)
				require.False(t, end)
				require.NotNil(t, ut.proc.InputBatch())
				ut.proc.Reg.InputBatch.Clean(ut.proc.Mp())
				ut.proc.SetInputBatch(nil)
				end, err = Call(0, ut.proc, ut.arg)
				require.Nil(t, err)
				require.True(t, end)
				require.Nil(t, ut.proc.InputBatch())
				return
			}

			for i := 0; i < ut.inputTimes; i++ {
				ut.proc.Reg.InputBatch, err = makeTestBatch2(ut.jsons, ut.proc)
				require.Nil(t, err)
				end, err := Call(0, ut.proc, ut.arg)
				require.Nil(t, err)
				require.False(t, end)
				require.Nil(t, err)
				require.NotNil(t, ut.proc.InputBatch())
				ut.proc.Reg.InputBatch.Clean(ut.proc.Mp())
			}
			ut.proc.SetInputBatch(nil)
			end, err := Call(0, ut.proc, ut.arg)
			require.Nil(t, err)
			require.True(t, end)
		})
		if ut.proc.InputBatch() != nil {
			ut.proc.Reg.InputBatch.Clean(ut.proc.Mp())
		}
		ut.arg.Free(ut.proc, false)
		require.Equal(t, nb0, ut.proc.Mp().CurrNB())
	}
}

func makeTestBatch1(json string, proc *process.Process) (*batch.Batch, error) {
	bat := batch.New(true, []string{"src"})
	bat.Cnt = 1
	bat.Vecs[0] = vector.New(types.Type{
		Oid: types.T_varchar,
	})
	err := bat.Vecs[0].Append([]byte(json), false, proc.Mp())
	if err != nil {
		return nil, err
	}
	bat.InitZsOne(1)
	return bat, nil
}

func makeTestBatch2(jsons []string, proc *process.Process) (*batch.Batch, error) {
	bat := batch.New(true, []string{"a"})
	bat.Cnt = 1
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.New(types.Type{
			Oid:   types.T_json,
			Width: 256,
		})
	}
	for _, json := range jsons {
		bj, err := types.ParseStringToByteJson(json)
		if err != nil {
			return nil, err
		}
		bjBytes, err := types.EncodeJson(bj)
		if err != nil {
			return nil, err
		}
		err = bat.GetVector(0).Append(bjBytes, false, proc.Mp())
		if err != nil {
			return nil, err
		}
	}
	return bat, nil
}
