// Copyright 2021 Matrix Origin
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

package insert

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type mockRelation struct {
	engine.Relation
	result *batch.Batch
}

func (e *mockRelation) Write(_ context.Context, b *batch.Batch) error {
	e.result = b
	return nil
}

var (
	i64typ     = &plan.Type{Id: int32(types.T_int64)}
	varchartyp = &plan.Type{Id: int32(types.T_varchar)}
)

func TestInsertOperator(t *testing.T) {
	proc := testutil.NewProc()
	batch1 := &batch.Batch{
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector([]int64{1, 2, 0}, []uint64{3}),
			testutil.MakeScalarInt64(3, 3),
			testutil.MakeVarcharVector([]string{"a", "b", "c"}, nil),
			testutil.MakeScalarVarchar("d", 3),
			testutil.MakeScalarNull(3),
		},
		Zs: []int64{1, 1, 1},
	}
	argument1 := Argument{
		TargetTable: &mockRelation{},
		TargetColDefs: []*plan.ColDef{
			{Name: "int64_column", Typ: i64typ},
			{Name: "scalar_int64", Typ: i64typ},
			{Name: "varchar_column", Typ: varchartyp},
			{Name: "scalar_varchar", Typ: varchartyp},
			{Name: "int64_column", Typ: i64typ},
		},
	}
	proc.Reg.InputBatch = batch1
	_, err := Call(0, proc, &argument1)
	require.NoError(t, err)
	println(argument1.TargetTable.(*mockRelation).result.Vecs)
	{
		result := argument1.TargetTable.(*mockRelation).result
		// check attr names
		require.True(t, reflect.DeepEqual(
			[]string{"int64_column", "scalar_int64", "varchar_column", "scalar_varchar", "int64_column"},
			result.Attrs,
		))
		// check vector
		require.Equal(t, len(batch1.Vecs), len(result.Vecs))
		for i, vec := range result.Vecs {
			require.Equal(t, len(batch1.Zs), vector.Length(vec), fmt.Sprintf("column number: %d", i))
			switch vec.Typ.Oid {
			case types.T_int64:
				col := vector.MustTCols[int64](vec)
				require.Equal(t, len(batch1.Zs), len(col))
			case types.T_varchar:
				col := vector.MustBytesCols(vec)
				require.Equal(t, len(batch1.Zs), len(col.Lengths))
			}
		}
	}

	batch2 := &batch.Batch{
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector([]int64{1, 2, 0}, []uint64{3}),
		},
		Zs: []int64{1, 1, 1},
	}
	argument2 := Argument{
		TargetTable: &mockRelation{},
		TargetColDefs: []*plan.ColDef{
			{Name: "int64_column_primary", Primary: true, Typ: i64typ},
		},
	}
	proc.Reg.InputBatch = batch2
	_, err2 := Call(0, proc, &argument2)
	require.Errorf(t, err2, "should return error when insert null into primary key column")
}
