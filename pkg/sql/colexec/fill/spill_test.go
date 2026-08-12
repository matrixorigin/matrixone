// Copyright 2026 Matrix Origin
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

package fill

import (
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type fillShortWriter struct{}

func (fillShortWriter) Write(value []byte) (int, error) {
	if len(value) == 0 {
		return 0, nil
	}
	return len(value) - 1, nil
}

func TestFillSpillRoundTripPreservesPrepareParamKinds(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	spill, err := newFillSpill(&container{}, proc)
	require.NoError(t, err)
	defer spill.close(proc)

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("5"), false, proc.Mp()))
	bat.Vecs[0].SetPrepareParamKind(vector.PrepareParamFloat)
	bat.SetRowCount(1)
	_, err = spill.writeRecord(proc, spill.input, bat)
	require.NoError(t, err)
	require.NoError(t, spill.input.finishWriting())
	bat.Clean(proc.Mp())

	pos := int64(-1)
	got, err := spill.readRecordReverse(spill.input, &pos, proc.Mp(), nil)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamFloat, got.Vecs[0].GetPrepareParamKindAt(0))
	got.Clean(proc.Mp())
}

func TestFillSpillRoundTripPreservesHeterogeneousPrepareParamKinds(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	spill, err := newFillSpill(&container{}, proc)
	require.NoError(t, err)
	defer spill.close(proc)

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
	for range 2 {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("5"), false, proc.Mp()))
	}
	bat.Vecs[0].SetPrepareParamKinds([]vector.PrepareParamKind{
		vector.PrepareParamFloat,
		vector.PrepareParamNone,
	})
	bat.SetRowCount(2)
	_, err = spill.writeRecord(proc, spill.input, bat)
	require.NoError(t, err)
	require.NoError(t, spill.input.finishWriting())
	bat.Clean(proc.Mp())

	pos := int64(-1)
	got, err := spill.readRecordReverse(spill.input, &pos, proc.Mp(), nil)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamFloat, got.Vecs[0].GetPrepareParamKindAt(0))
	require.Equal(t, vector.PrepareParamNone, got.Vecs[0].GetPrepareParamKindAt(1))
	got.Clean(proc.Mp())
}

func TestFillSpillWriteRejectsShortIO(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	writer, err := spillutil.NewAccountedWriter(
		proc.Ctx,
		proc.Mp(),
		nil,
		mpool.AllocationOwnerFill,
		fillAllocationSiteSpillWriteBuffer,
		fillShortWriter{},
		fillSpillWriteBufferSize,
	)
	require.NoError(t, err)
	defer writer.Free()
	spill := &fillSpill{}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, proc.Mp()))
	bat.SetRowCount(1)
	defer bat.Clean(proc.Mp())
	_, err = spill.writeRecord(proc, &fillSpillFile{writer: writer}, bat)
	require.ErrorIs(t, err, io.ErrShortWrite)
}

func TestFillSpillBorrowedMarkerFailurePreservesSource(t *testing.T) {
	const rows = 600_000
	sourceMP := mpool.MustNewZero()
	source := batch.NewWithSize(2)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	source.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	for _, vec := range source.Vecs {
		require.NoError(t, vec.PreExtend(rows, sourceMP))
		vec.SetLength(rows)
	}
	vector.MustFixedColWithTypeCheck[int64](source.Vecs[0])[0] = 7
	vector.MustFixedColWithTypeCheck[int64](source.Vecs[1])[0] = 9
	source.SetRowCount(rows)
	markerMP, err := mpool.NewMPool(
		"fill-marker-rollback", mpool.MB, mpool.NoFixed,
	)
	require.NoError(t, err)

	_, err = makeBorrowedSpillBatch(source, 2, markerMP, nil)
	require.Error(t, err)
	require.Equal(t, int64(7),
		vector.GetFixedAtNoTypeCheck[int64](source.Vecs[0], 0))
	require.Equal(t, int64(9),
		vector.GetFixedAtNoTypeCheck[int64](source.Vecs[1], 0))
	require.Zero(t, markerMP.CurrNB())
	source.Clean(sourceMP)
	require.Zero(t, sourceMP.CurrNB())
}

func TestFillSpillReverseReadRejectsCorruptAndTruncatedRecords(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, file *fillSpillFile)
	}{
		{
			name: "truncated",
			mutate: func(t *testing.T, file *fillSpillFile) {
				end, err := file.file.Seek(0, io.SeekEnd)
				require.NoError(t, err)
				require.NoError(t, file.file.Truncate(end-1))
			},
		},
		{
			name: "corrupt magic",
			mutate: func(t *testing.T, file *fillSpillFile) {
				end, err := file.file.Seek(0, io.SeekEnd)
				require.NoError(t, err)
				_, err = file.file.WriteAt(make([]byte, 8), end-8)
				require.NoError(t, err)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			spill, err := newFillSpill(&container{}, proc)
			require.NoError(t, err)
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t,
				vector.AppendFixed(bat.Vecs[0], int64(1), false, proc.Mp()))
			bat.SetRowCount(1)
			_, err = spill.writeRecord(proc, spill.input, bat)
			require.NoError(t, err)
			bat.Clean(proc.Mp())
			require.NoError(t, spill.input.finishWriting())
			test.mutate(t, spill.input)
			pos := int64(-1)
			_, err = spill.readRecordReverse(spill.input, &pos, proc.Mp(), nil)
			require.Error(t, err)
			spill.close(proc)
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}
