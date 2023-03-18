// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mergeblock

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

func TestMergeBlock(t *testing.T) {
	proc := testutil.NewProc()
	proc.Ctx = context.TODO()
	batch1 := &batch.Batch{
		Attrs: []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_MetaLoc},
		Vecs: []*vector.Vector{
			testutil.MakeUint16Vector([]uint16{0, 1, 2}, nil),
			testutil.MakeVarcharVector([]string{
				"a.seg:magic:15",
				"b.seg:magic:15",
				"c.seg:magic:15"}, nil),
		},
		Zs: []int64{1, 1, 1},
	}
	argument1 := Argument{
		Tbl:          &mockRelation{},
		Unique_tbls:  []engine.Relation{&mockRelation{}, &mockRelation{}},
		AffectedRows: 0,
		notFreeBatch: true,
	}
	proc.Reg.InputBatch = batch1
	Prepare(proc, &argument1)
	_, err := Call(0, proc, &argument1, false, false)
	require.NoError(t, err)
	require.Equal(t, uint64(15), argument1.AffectedRows)
	// Check Tbl
	{
		result := argument1.Tbl.(*mockRelation).result
		// check attr names
		require.True(t, reflect.DeepEqual(
			[]string{catalog.BlockMeta_MetaLoc},
			result.Attrs,
		))
		// check vector
		require.Equal(t, 1, len(result.Vecs))
		for i, vec := range result.Vecs {
			require.Equal(t, 1, vec.Length(), fmt.Sprintf("column number: %d", i))
		}
	}
	// Check UniqueTables
	for j := range argument1.Unique_tbls {
		tbl := argument1.Unique_tbls[j]
		result := tbl.(*mockRelation).result
		// check attr names
		require.True(t, reflect.DeepEqual(
			[]string{catalog.BlockMeta_MetaLoc},
			result.Attrs,
		))
		// check vector
		require.Equal(t, 1, len(result.Vecs))
		for i, vec := range result.Vecs {
			require.Equal(t, 1, vec.Length(), fmt.Sprintf("column number: %d", i))
		}
	}
	argument1.Free(proc, false)
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}
