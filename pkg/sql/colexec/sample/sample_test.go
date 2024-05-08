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

package sample

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

const nullFlag = int64(-65535)

func TestSamplePool(t *testing.T) {
	proc := testutil.NewProcess()

	// data source :
	// (1, 1), (2, 2), (3, 3), (3, 4), (4, 4),
	// (5, 5), (6, 6), (6, 6), (7, 7), (8, null).
	rows1 := [][]int64{
		{1, 1},
		{2, 2},
		{3, 3},
		{3, 4},
		{4, 4},
	}
	row2 := [][]int64{
		{5, 5},
		{6, 6},
		{6, 6},
		{7, 7},
		{8, nullFlag},
	}
	b1, e1 := genSampleBatch(proc, rows1)
	require.NoError(t, e1)
	b2, e2 := genSampleBatch(proc, row2)
	require.NoError(t, e2)

	{
		// sample 5 rows by second column.
		pool1 := newSamplePoolByRows(proc, 5, 1, true)
		err := pool1.sampleFromColumn(1, b1.Vecs[1], b1)
		require.NoError(t, err)
		err = pool1.sampleFromColumn(1, b2.Vecs[1], b2)
		require.NoError(t, err)

		// cannot get any result before end.
		tbat, err := pool1.Result(false)
		require.NoError(t, err)
		require.Equal(t, 0, tbat.RowCount())

		// check the result.
		// due to reorder, the result will be [sample column, normal column, rowsCount column].
		out, err := pool1.Result(true)
		require.NoError(t, err)
		require.Equal(t, 3, len(out.Vecs))
		require.Equal(t, 5, out.Vecs[0].Length())
		require.Equal(t, 5, out.Vecs[1].Length())
		// invalid scan row count was 9. the 10th row with null value at the sample column will be ignored.
		require.Equal(t, int64(9), vector.GetFixedAt[int64](out.Vecs[2], 0))

		out.Clean(proc.Mp())
		pool1.Free()
	}

	{
		// sample 5 rows by 2 columns.
		pool2 := newSamplePoolByRows(proc, 5, 2, false)
		err := pool2.sampleFromColumns(1, b1.Vecs, b1)
		require.NoError(t, err)
		err = pool2.sampleFromColumns(1, b2.Vecs, b2)
		require.NoError(t, err)

		tbat, err := pool2.Result(false)
		require.NoError(t, err)
		require.Equal(t, 0, tbat.RowCount())

		out, err := pool2.Result(true)
		require.NoError(t, err)
		// due to we set outputRowCount to false, the result will be [sample column, normal column].
		require.Equal(t, 2, len(out.Vecs))
		require.Equal(t, 5, out.Vecs[0].Length())
		require.Equal(t, 5, out.Vecs[1].Length())

		out.Clean(proc.Mp())
		pool2.Free()
	}

	{
		// sample 100 % rows by the second column.
		pool3 := newSamplePoolByPercent(proc, 100.0, 1)
		err := pool3.sampleFromColumn(1, b1.Vecs[1], b1)
		require.NoError(t, err)

		// can take out the result before an end.
		tbat, err := pool3.Result(false)
		require.NoError(t, err)
		require.Equal(t, 5, tbat.RowCount())

		err = pool3.sampleFromColumn(1, b2.Vecs[1], b2)
		require.NoError(t, err)

		out, err := pool3.Result(true)
		require.NoError(t, err)
		require.Equal(t, 4, out.RowCount())

		tbat.Clean(proc.Mp())
		out.Clean(proc.Mp())
		pool3.Free()
	}

	proc.FreeVectors()
	b1.Clean(proc.Mp())
	b2.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func genSampleBatch(proc *process.Process, rows [][]int64) (*batch.Batch, error) {
	b := batch.NewWithSize(len(rows[0]))

	var err error
	for i := range b.Vecs {
		b.Vecs[i] = proc.GetVector(types.T_int64.ToType())

		for _, rowValue := range rows {
			err = vector.AppendFixed[int64](b.Vecs[i], rowValue[i], rowValue[i] == nullFlag, proc.Mp())
			if err != nil {
				return nil, err
			}
		}
	}
	b.SetRowCount(len(rows))
	return b, nil
}

func TestSamplePoolOthers(t *testing.T) {
	// merge sample and sample by percent cannot be tested full.
	s1 := newSamplePoolByRows(nil, 1, 1, false)
	s2 := newSamplePoolByPercent(nil, 1.0, 1)
	s3 := newSamplePoolByRowsForMerge(nil, 1, 1, false)

	s1.setPerfFields(false)
	s2.setPerfFields(false)
	s3.setPerfFields(false)
	require.Equal(t, true, s1.canCheckFull)
	require.Equal(t, false, s2.canCheckFull)
	require.Equal(t, false, s3.canCheckFull)

	// once sample for each group, full check is not supported.
	s1.setPerfFields(true)
	s2.setPerfFields(true)
	s3.setPerfFields(true)
	require.Equal(t, false, s1.canCheckFull)
	require.Equal(t, false, s2.canCheckFull)
	require.Equal(t, false, s3.canCheckFull)
}
