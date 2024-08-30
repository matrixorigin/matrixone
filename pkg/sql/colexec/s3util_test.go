// Copyright 2022 Matrix Origin
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
package colexec

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSortKey(t *testing.T) {
	proc := testutil.NewProc()
	proc.Ctx = context.TODO()
	batch1 := &batch.Batch{
		Attrs: []string{"a"},
		Vecs: []*vector.Vector{
			testutil.MakeUint16Vector([]uint16{1, 2, 0}, nil),
		},
	}
	batch1.SetRowCount(3)
	err := sortByKey(proc, batch1, 0, false, proc.GetMPool())
	require.NoError(t, err)
	cols := vector.ExpandFixedCol[uint16](batch1.Vecs[0])
	for i := range cols {
		require.Equal(t, int(cols[i]), i)
	}

	batch2 := &batch.Batch{
		Attrs: []string{"a"},
		Vecs: []*vector.Vector{
			testutil.MakeTextVector([]string{"b", "a", "c"}, nil),
		},
	}
	batch2.SetRowCount(3)
	res := []string{"a", "b", "c"}
	err = sortByKey(proc, batch2, 0, false, proc.GetMPool())
	require.NoError(t, err)
	cols2 := vector.ExpandStrCol(batch2.Vecs[0])
	for i := range cols {
		require.Equal(t, cols2[i], res[i])
	}
}

func TestSetStatsCNCreated(t *testing.T) {
	proc := testutil.NewProc()
	s3writer := &S3Writer{}
	s3writer.sortIndex = 0
	s3writer.isTombstone = true
	_, err := s3writer.generateWriter(proc)
	require.NoError(t, err)

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())

	for i := 0; i < 100; i++ {
		row := types.RandomRowid()
		err = vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, proc.GetMPool())
		require.NoError(t, err)
	}
	bat.SetRowCount(100)

	s3writer.StashBatch(proc, bat)
	_, stats, err := s3writer.SortAndSync(proc)
	require.NoError(t, err)

	cnt := 0
	for _, s := range stats {
		if !s.IsZero() {
			cnt++
			require.True(t, s.GetCNCreated())
			require.Equal(t, uint32(bat.VectorCount()), s.BlkCnt())
			require.Equal(t, uint32(bat.Vecs[0].Length()), s.Rows())
		}
	}

	require.Equal(t, 1, cnt)
}
