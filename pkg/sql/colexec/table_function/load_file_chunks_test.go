// Copyright 2026 Matrix Origin
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
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestLoadFileChunks(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test")
	err := os.WriteFile(filePath, []byte("abcdef"), 0o600)
	require.NoError(t, err)

	proc := testutil.NewProc(t)
	tf := &TableFunction{
		Attrs:    []string{"chunk_id", "offset", "data"},
		Rets:     loadFileChunksTestCols(),
		FuncName: "load_file_chunks",
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}

	tf.Args = []*plan.Expr{
		{
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: 256,
			},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_Sval{
						Sval: "file://" + filePath,
					},
				},
			},
		},
		{
			Typ: plan.Type{
				Id: int32(types.T_int64),
			},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_I64Val{
						I64Val: 4,
					},
				},
			},
		},
	}

	retSchema := make([]types.Type, len(tf.Rets))
	for i := range tf.Rets {
		typ := tf.Rets[i].Typ
		retSchema[i] = types.New(types.T(typ.Id), typ.Width, typ.Scale)
	}
	tf.ctr.retSchema = retSchema

	tvfst, err := loadFileChunksPrepare(proc, tf)
	require.NoError(t, err)

	for i := range tf.ctr.executorsForArgs {
		tf.ctr.argVecs[i], err = tf.ctr.executorsForArgs[i].Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		require.NoError(t, err)
	}

	err = tvfst.start(tf, proc, 0, nil)
	require.NoError(t, err)

	var ids []int64
	var offsets []int64
	var chunks []string
	for {
		res, err := tvfst.call(tf, proc)
		require.NoError(t, err)
		if res.Batch.IsDone() {
			break
		}
		bat := res.Batch
		for i := 0; i < bat.RowCount(); i++ {
			ids = append(ids, vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], i))
			offsets = append(offsets, vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], i))
			chunks = append(chunks, string(bat.Vecs[2].GetBytesAt(i)))
		}
	}
	tvfst.free(tf, proc, false, nil)

	require.Equal(t, []int64{0, 1}, ids)
	require.Equal(t, []int64{0, 4}, offsets)
	require.Equal(t, []string{"abcd", "ef"}, chunks)
}

func loadFileChunksTestCols() []*plan.ColDef {
	i64Typ := types.T_int64.ToType()
	blobTyp := types.T_blob.ToType()
	return []*plan.ColDef{
		{Name: "chunk_id", Typ: makeTestPlanType(&i64Typ)},
		{Name: "offset", Typ: makeTestPlanType(&i64Typ)},
		{Name: "data", Typ: makeTestPlanType(&blobTyp)},
	}
}

func makeTestPlanType(typ *types.Type) plan.Type {
	return plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}
