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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSortKey(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.Ctx = context.TODO()
	batch1 := &batch.Batch{
		Attrs: []string{"a"},
		Vecs: []*vector.Vector{
			testutil.MakeUint16Vector([]uint16{1, 2, 0}, nil),
		},
	}
	batch1.SetRowCount(3)
	err := SortByKey(proc, batch1, 0, false, proc.GetMPool())
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
	err = SortByKey(proc, batch2, 0, false, proc.GetMPool())
	require.NoError(t, err)
	cols2 := vector.ExpandStrCol(batch2.Vecs[0])
	for i := range cols {
		require.Equal(t, cols2[i], res[i])
	}
}

func TestSetStatsCNCreated(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := proc.Ctx

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	for i := 0; i < 100; i++ {
		row := types.RandomRowid()
		err := vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, proc.GetMPool())
		require.NoError(t, err)

		err = vector.AppendFixed[int32](bat.Vecs[1], int32(i), false, proc.GetMPool())
		require.NoError(t, err)
	}
	bat.SetRowCount(100)

	fs, err := fileservice.Get[fileservice.FileService](proc.Base.FileService, defines.SharedFileServiceName)
	require.NoError(t, err)

	s3writer := NewCNS3TombstoneWriter(proc.Mp(), fs, types.T_int32.ToType())

	err = s3writer.Write(ctx, bat)
	require.NoError(t, err)

	stats, err := s3writer.Sync(ctx, proc.Mp())
	require.NoError(t, err)
	require.Equal(t, 1, len(stats))

	require.True(t, stats[0].GetCNCreated())
	require.Equal(t, uint32(1), stats[0].BlkCnt())
	require.Equal(t, uint32(bat.Vecs[0].Length()), stats[0].Rows())

}

func TestMergeSortBatches(t *testing.T) {
	pool, err := mpool.NewMPool("", mpool.GB, 0)
	require.NoError(t, err)
	var restult *batch.Batch
	sinker := func(bat *batch.Batch) error {
		var err2 error
		if restult != nil {
			restult.Clean(pool)
		}
		restult, err2 = bat.Dup(pool)
		if err2 != nil {
			return err2
		}
		return nil
	}
	// Test bool
	{
		bat1 := batch.NewWithSize(2)
		defer bat1.Clean(pool)
		bat1.SetVector(0, vector.NewVec(types.T_int32.ToType()))
		bat1.SetVector(1, vector.NewVec(types.T_bool.ToType()))
		bat2, err := bat1.Dup(pool)
		require.NoError(t, err)
		buffer, err := bat1.Dup(pool)
		defer buffer.Clean(pool)
		require.NoError(t, err)
		vector.AppendFixed(bat1.Vecs[1], true, false, pool)
		vector.AppendFixed(bat2.Vecs[1], false, false, pool)
		vector.AppendFixed(bat1.Vecs[0], int32(2), false, pool)
		vector.AppendFixed(bat2.Vecs[0], int32(1), false, pool)

		err = mergeutil.MergeSortBatches(
			[]*batch.Batch{bat1, bat2},
			1,
			buffer,
			sinker,
			pool,
			true,
		)
		require.NoError(t, err)
		require.Equal(t, restult.Vecs[0].Length(), 2)
		require.Equal(t, restult.Vecs[1].Length(), 2)
		require.Equal(t, []int32{1, 2}, vector.MustFixedColWithTypeCheck[int32](restult.Vecs[0]))
		require.Equal(t, []bool{false, true}, vector.MustFixedColWithTypeCheck[bool](restult.Vecs[1]))
	}
	// Test int8
	{
		bat1 := batch.NewWithSize(2)
		defer bat1.Clean(pool)
		bat1.SetVector(0, vector.NewVec(types.T_int32.ToType()))
		bat1.SetVector(1, vector.NewVec(types.T_int8.ToType()))
		bat2, err := bat1.Dup(pool)
		require.NoError(t, err)
		buffer, err := bat1.Dup(pool)
		defer buffer.Clean(pool)
		require.NoError(t, err)
		vector.AppendFixed(bat1.Vecs[1], int8(1), false, pool)
		vector.AppendFixed(bat2.Vecs[1], int8(2), false, pool)
		vector.AppendFixed(bat1.Vecs[0], int32(2), false, pool)
		vector.AppendFixed(bat2.Vecs[0], int32(1), false, pool)

		err = mergeutil.MergeSortBatches(
			[]*batch.Batch{bat1, bat2},
			1,
			buffer,
			sinker,
			pool,
			true,
		)
		require.NoError(t, err)
		require.Equal(t, restult.Vecs[0].Length(), 2)
		require.Equal(t, restult.Vecs[1].Length(), 2)
		require.Equal(t, []int32{2, 1}, vector.MustFixedColWithTypeCheck[int32](restult.Vecs[0]))
		require.Equal(t, []int8{1, 2}, vector.MustFixedColWithTypeCheck[int8](restult.Vecs[1]))
	}
	// Test int16
	{
		bat1 := batch.NewWithSize(2)
		defer bat1.Clean(pool)
		bat1.SetVector(0, vector.NewVec(types.T_int32.ToType()))
		bat1.SetVector(1, vector.NewVec(types.T_int16.ToType()))
		bat2, err := bat1.Dup(pool)
		require.NoError(t, err)
		buffer, err := bat1.Dup(pool)
		defer buffer.Clean(pool)
		require.NoError(t, err)
		vector.AppendFixed(bat1.Vecs[1], int16(1), false, pool)
		vector.AppendFixed(bat2.Vecs[1], int16(2), false, pool)
		vector.AppendFixed(bat1.Vecs[0], int32(2), false, pool)
		vector.AppendFixed(bat2.Vecs[0], int32(1), false, pool)

		err = mergeutil.MergeSortBatches(
			[]*batch.Batch{bat1, bat2},
			1,
			buffer,
			sinker,
			pool,
			true,
		)
		require.NoError(t, err)
		require.Equal(t, restult.Vecs[0].Length(), 2)
		require.Equal(t, restult.Vecs[1].Length(), 2)
		require.Equal(t, []int32{2, 1}, vector.MustFixedColWithTypeCheck[int32](restult.Vecs[0]))
		require.Equal(t, []int16{1, 2}, vector.MustFixedColWithTypeCheck[int16](restult.Vecs[1]))
	}
	// Test uint8
	{
		bat1 := batch.NewWithSize(2)
		defer bat1.Clean(pool)
		bat1.SetVector(0, vector.NewVec(types.T_int32.ToType()))
		bat1.SetVector(1, vector.NewVec(types.T_uint8.ToType()))
		bat2, err := bat1.Dup(pool)
		require.NoError(t, err)
		buffer, err := bat1.Dup(pool)
		defer buffer.Clean(pool)
		require.NoError(t, err)
		vector.AppendFixed(bat1.Vecs[1], uint8(1), false, pool)
		vector.AppendFixed(bat2.Vecs[1], uint8(2), false, pool)
		vector.AppendFixed(bat1.Vecs[0], int32(2), false, pool)
		vector.AppendFixed(bat2.Vecs[0], int32(1), false, pool)

		err = mergeutil.MergeSortBatches(
			[]*batch.Batch{bat1, bat2},
			1,
			buffer,
			sinker,
			pool,
			true,
		)
		require.NoError(t, err)
		require.Equal(t, restult.Vecs[0].Length(), 2)
		require.Equal(t, restult.Vecs[1].Length(), 2)
		require.Equal(t, []int32{2, 1}, vector.MustFixedColWithTypeCheck[int32](restult.Vecs[0]))
		require.Equal(t, []uint8{1, 2}, vector.MustFixedColWithTypeCheck[uint8](restult.Vecs[1]))
	}
	// Test uint16
	{
		bat1 := batch.NewWithSize(2)
		defer bat1.Clean(pool)
		bat1.SetVector(0, vector.NewVec(types.T_int32.ToType()))
		bat1.SetVector(1, vector.NewVec(types.T_uint16.ToType()))
		bat2, err := bat1.Dup(pool)
		require.NoError(t, err)
		buffer, err := bat1.Dup(pool)
		defer buffer.Clean(pool)
		require.NoError(t, err)
		vector.AppendFixed(bat1.Vecs[1], uint16(1), false, pool)
		vector.AppendFixed(bat2.Vecs[1], uint16(2), false, pool)
		vector.AppendFixed(bat1.Vecs[0], int32(2), false, pool)
		vector.AppendFixed(bat2.Vecs[0], int32(1), false, pool)

		err = mergeutil.MergeSortBatches(
			[]*batch.Batch{bat1, bat2},
			1,
			buffer,
			sinker,
			pool,
			true,
		)
		require.NoError(t, err)
		require.Equal(t, restult.Vecs[0].Length(), 2)
		require.Equal(t, restult.Vecs[1].Length(), 2)
		require.Equal(t, []int32{2, 1}, vector.MustFixedColWithTypeCheck[int32](restult.Vecs[0]))
		require.Equal(t, []uint16{1, 2}, vector.MustFixedColWithTypeCheck[uint16](restult.Vecs[1]))
	}
	// Test uint32
	{
		bat1 := batch.NewWithSize(2)
		defer bat1.Clean(pool)
		bat1.SetVector(0, vector.NewVec(types.T_int32.ToType()))
		bat1.SetVector(1, vector.NewVec(types.T_uint32.ToType()))
		bat2, err := bat1.Dup(pool)
		require.NoError(t, err)
		buffer, err := bat1.Dup(pool)
		defer buffer.Clean(pool)
		require.NoError(t, err)
		vector.AppendFixed(bat1.Vecs[1], uint32(1), false, pool)
		vector.AppendFixed(bat2.Vecs[1], uint32(2), false, pool)
		vector.AppendFixed(bat1.Vecs[0], int32(2), false, pool)
		vector.AppendFixed(bat2.Vecs[0], int32(1), false, pool)

		err = mergeutil.MergeSortBatches(
			[]*batch.Batch{bat1, bat2},
			1,
			buffer,
			sinker,
			pool,
			true,
		)
		require.NoError(t, err)
		require.Equal(t, restult.Vecs[0].Length(), 2)
		require.Equal(t, restult.Vecs[1].Length(), 2)
		require.Equal(t, []int32{2, 1}, vector.MustFixedColWithTypeCheck[int32](restult.Vecs[0]))
		require.Equal(t, []uint32{1, 2}, vector.MustFixedColWithTypeCheck[uint32](restult.Vecs[1]))
	}
	// Test bit
	{
		bat1 := batch.NewWithSize(2)
		defer bat1.Clean(pool)
		bat1.SetVector(0, vector.NewVec(types.T_int32.ToType()))
		bat1.SetVector(1, vector.NewVec(types.T_bit.ToType()))
		bat2, err := bat1.Dup(pool)
		require.NoError(t, err)
		buffer, err := bat1.Dup(pool)
		defer buffer.Clean(pool)
		require.NoError(t, err)
		vector.AppendFixed(bat1.Vecs[1], uint64(1), false, pool)
		vector.AppendFixed(bat2.Vecs[1], uint64(2), false, pool)
		vector.AppendFixed(bat1.Vecs[0], int32(2), false, pool)
		vector.AppendFixed(bat2.Vecs[0], int32(1), false, pool)

		err = mergeutil.MergeSortBatches(
			[]*batch.Batch{bat1, bat2},
			1,
			buffer,
			sinker,
			pool,
			true,
		)
		require.NoError(t, err)
		require.Equal(t, restult.Vecs[0].Length(), 2)
		require.Equal(t, restult.Vecs[1].Length(), 2)
		require.Equal(t, []int32{2, 1}, vector.MustFixedColWithTypeCheck[int32](restult.Vecs[0]))
		require.Equal(t, []uint64{1, 2}, vector.MustFixedColWithTypeCheck[uint64](restult.Vecs[1]))
	}
	// Test float32
	{
		bat1 := batch.NewWithSize(2)
		defer bat1.Clean(pool)
		bat1.SetVector(0, vector.NewVec(types.T_int32.ToType()))
		bat1.SetVector(1, vector.NewVec(types.T_float32.ToType()))
		bat2, err := bat1.Dup(pool)
		require.NoError(t, err)
		buffer, err := bat1.Dup(pool)
		defer buffer.Clean(pool)
		require.NoError(t, err)
		vector.AppendFixed(bat1.Vecs[1], float32(1), false, pool)
		vector.AppendFixed(bat2.Vecs[1], float32(2), false, pool)
		vector.AppendFixed(bat1.Vecs[0], int32(2), false, pool)
		vector.AppendFixed(bat2.Vecs[0], int32(1), false, pool)

		err = mergeutil.MergeSortBatches(
			[]*batch.Batch{bat1, bat2},
			1,
			buffer,
			sinker,
			pool,
			true,
		)
		require.NoError(t, err)
		require.Equal(t, restult.Vecs[0].Length(), 2)
		require.Equal(t, restult.Vecs[1].Length(), 2)
		require.Equal(t, []int32{2, 1}, vector.MustFixedColWithTypeCheck[int32](restult.Vecs[0]))
		require.Equal(t, []float32{1, 2}, vector.MustFixedColWithTypeCheck[float32](restult.Vecs[1]))
	}
	// Test float64
	{
		bat1 := batch.NewWithSize(2)
		defer bat1.Clean(pool)
		bat1.SetVector(0, vector.NewVec(types.T_int32.ToType()))
		bat1.SetVector(1, vector.NewVec(types.T_float64.ToType()))
		bat2, err := bat1.Dup(pool)
		require.NoError(t, err)
		buffer, err := bat1.Dup(pool)
		defer buffer.Clean(pool)
		require.NoError(t, err)
		vector.AppendFixed(bat1.Vecs[1], float64(1), false, pool)
		vector.AppendFixed(bat2.Vecs[1], float64(2), false, pool)
		vector.AppendFixed(bat1.Vecs[0], int32(2), false, pool)
		vector.AppendFixed(bat2.Vecs[0], int32(1), false, pool)

		err = mergeutil.MergeSortBatches(
			[]*batch.Batch{bat1, bat2},
			1,
			buffer,
			sinker,
			pool,
			true,
		)
		require.NoError(t, err)
		require.Equal(t, restult.Vecs[0].Length(), 2)
		require.Equal(t, restult.Vecs[1].Length(), 2)
		require.Equal(t, []int32{2, 1}, vector.MustFixedColWithTypeCheck[int32](restult.Vecs[0]))
		require.Equal(t, []float64{1, 2}, vector.MustFixedColWithTypeCheck[float64](restult.Vecs[1]))
	}
	// Test date
	{
		bat1 := batch.NewWithSize(2)
		defer bat1.Clean(pool)
		bat1.SetVector(0, vector.NewVec(types.T_int32.ToType()))
		bat1.SetVector(1, vector.NewVec(types.T_date.ToType()))
		bat2, err := bat1.Dup(pool)
		require.NoError(t, err)
		buffer, err := bat1.Dup(pool)
		defer buffer.Clean(pool)
		require.NoError(t, err)
		vector.AppendFixed(bat1.Vecs[1], types.Date(1), false, pool)
		vector.AppendFixed(bat2.Vecs[1], types.Date(2), false, pool)
		vector.AppendFixed(bat1.Vecs[0], int32(2), false, pool)
		vector.AppendFixed(bat2.Vecs[0], int32(1), false, pool)

		err = mergeutil.MergeSortBatches(
			[]*batch.Batch{bat1, bat2},
			1,
			buffer,
			sinker,
			pool,
			true,
		)
		require.NoError(t, err)
		require.Equal(t, restult.Vecs[0].Length(), 2)
		require.Equal(t, restult.Vecs[1].Length(), 2)
		require.Equal(t, []int32{2, 1}, vector.MustFixedColWithTypeCheck[int32](restult.Vecs[0]))
		require.Equal(t, []types.Date{1, 2}, vector.MustFixedColWithTypeCheck[types.Date](restult.Vecs[1]))
	}
}

func TestS3Writer_SortAndSync(t *testing.T) {
	pool, err := mpool.NewMPool("", mpool.GB, 0)
	require.NoError(t, err)

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	for i := 0; i < 10; i++ {
		row := types.RandomRowid()
		err = vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, pool)
		require.NoError(t, err)

		err = vector.AppendFixed[int32](bat.Vecs[1], int32(i), false, pool)
		require.NoError(t, err)
	}
	bat.SetRowCount(10)

	// test no data to flush
	{
		proc := testutil.NewProc(t)
		ctx := proc.Ctx

		fs, err := fileservice.Get[fileservice.FileService](proc.Base.FileService, defines.SharedFileServiceName)
		require.NoError(t, err)

		s3writer := NewCNS3TombstoneWriter(proc.Mp(), fs, types.T_int32.ToType())

		s, err := s3writer.Sync(ctx, proc.Mp())
		require.NoError(t, err)
		require.Nil(t, s)

		fmt.Println(s3writer.String())
	}

	// test no SHARED service err
	{
		proc := testutil.NewProc(t)
		ctx := proc.Ctx

		s3writer := NewCNS3TombstoneWriter(proc.Mp(), proc.GetFileService(), types.T_int32.ToType())
		err = s3writer.Write(ctx, bat)
		require.NoError(t, err)

		_, err = s3writer.Sync(ctx, proc.Mp())
		require.Equal(t, err.(*moerr.Error).ErrorCode(), moerr.ErrNoService)

		fmt.Println(s3writer.String())
	}

	// test normal flush
	{
		proc := testutil.NewProc(t)
		ctx := proc.Ctx

		fs, err := fileservice.Get[fileservice.FileService](proc.Base.FileService, defines.SharedFileServiceName)
		require.NoError(t, err)

		s3writer := NewCNS3TombstoneWriter(proc.Mp(), fs, types.T_int32.ToType())

		err = s3writer.Write(ctx, bat)
		require.NoError(t, err)

		_, err = s3writer.Sync(ctx, proc.Mp())
		require.NoError(t, err)

		fmt.Println(s3writer.String())
	}

	// test data size larger than object size limit
	{
		pool, err = mpool.NewMPool("", mpool.GB, 0)
		require.NoError(t, err)

		proc := testutil.NewProc(
			t,
			testutil.WithMPool(pool))
		ctx := proc.Ctx

		bat2 := batch.NewWithSize(2)
		bat2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		bat2.Vecs[1] = vector.NewVec(types.T_int32.ToType())

		objectio.SetObjectSizeLimit(mpool.KB)
		cnt := (objectio.ObjectSizeLimit) / types.RowidSize * 3

		for i := 0; i < cnt; i++ {
			row := types.RandomRowid()
			err = vector.AppendFixed[types.Rowid](bat2.Vecs[0], row, false, pool)
			require.NoError(t, err)

			err = vector.AppendFixed[int32](bat2.Vecs[1], int32(i), false, pool)
			require.NoError(t, err)
		}
		bat2.SetRowCount(cnt)

		fs, err := fileservice.Get[fileservice.FileService](proc.Base.FileService, defines.SharedFileServiceName)
		require.NoError(t, err)
		s3writer := NewCNS3TombstoneWriter(proc.Mp(), fs, types.T_int32.ToType())

		err = s3writer.Write(ctx, bat2)
		require.NoError(t, err)

		_, err = s3writer.Sync(ctx, proc.Mp())
		require.Equal(t, err.(*moerr.Error).ErrorCode(), moerr.ErrTooLargeObjectSize)

		fmt.Println(s3writer.String())
	}
}

func TestGetSharedFSFromProc(t *testing.T) {
	{
		proc := testutil.NewProc(t)
		fs, err := GetSharedFSFromProc(proc)
		require.NoError(t, err)
		require.NotNil(t, fs)
		require.NotEmpty(t, fs.Name())
	}

	{
		proc := &process.Process{
			Base: &process.BaseProcess{
				FileService: nil,
			},
		}

		fs, err := GetSharedFSFromProc(proc)
		require.NotNil(t, err)
		require.Nil(t, fs)
	}
}
