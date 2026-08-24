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

package group

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/stretchr/testify/require"
)

type groupShortWriter struct {
	remaining int
	err       error
}

func (w *groupShortWriter) Write(value []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, w.err
	}
	n := min(len(value), w.remaining)
	w.remaining -= n
	return n, w.err
}

type prepareAccessor struct {
	vectors []*vector.Vector
}

func (a *prepareAccessor) PrepareParamKindChunkCount() int { return len(a.vectors) }
func (a *prepareAccessor) PrepareParamKindVectorForChunk(chunk int) *vector.Vector {
	if chunk < 0 || chunk >= len(a.vectors) {
		return nil
	}
	return a.vectors[chunk]
}

var _ aggexec.PrepareParamKindStateAccessor = (*prepareAccessor)(nil)

func TestGroupSpillRowCodecAndWriterEdges(t *testing.T) {
	require.ErrorIs(t, appendSpillGroupByRows(nil, nil, nil), io.ErrClosedPipe)
	require.ErrorIs(t, writeSpillBool(&groupShortWriter{remaining: 0}, true), io.ErrShortWrite)
	injected := errors.New("spill write failed")
	require.ErrorIs(t, writeSpillBool(&groupShortWriter{remaining: 1, err: injected}, true), injected)

	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("alpha"), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("beta"), false, mp))
	bat.SetRowCount(2)
	var encoded bytes.Buffer
	require.NoError(t, appendSpillGroupByRows(&encoded, bat, []int32{1, 0}))

	for cut := 0; cut < encoded.Len(); cut++ {
		target := batch.NewWithSize(1)
		target.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		require.Error(t, unmarshalSpillGroupByRows(
			bytes.NewReader(encoded.Bytes()[:cut]), target, 2, mp), "cut=%d", cut)
		target.Clean(mp)
	}
	target := batch.NewWithSize(1)
	target.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, unmarshalSpillGroupByRows(
		bytes.NewReader(encoded.Bytes()), target, 2, mp))
	require.Equal(t, "beta", string(target.Vecs[0].GetBytesAt(0)))
	require.Equal(t, "alpha", string(target.Vecs[0].GetBytesAt(1)))
	target.Clean(mp)

	for _, tc := range []struct {
		reader io.Reader
		bat    *batch.Batch
		rows   int
	}{
		{nil, bat, 1},
		{bytes.NewReader(nil), nil, 1},
		{bytes.NewReader(nil), bat, -1},
	} {
		require.Error(t, unmarshalSpillGroupByRows(tc.reader, tc.bat, tc.rows, mp))
	}
	var wrongColumns bytes.Buffer
	require.NoError(t, types.WriteInt32(&wrongColumns, 2))
	require.Error(t, unmarshalSpillGroupByRows(&wrongColumns, bat, 1, mp))

	var nilRecordWriter *spillRecordWriter
	_, err := nilRecordWriter.Write([]byte("x"))
	require.ErrorIs(t, err, io.ErrClosedPipe)
	short := &spillRecordWriter{target: &groupShortWriter{remaining: 1}}
	n, err := short.Write([]byte("xy"))
	require.Equal(t, 1, n)
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, int64(1), short.written)

	_, err = newGroupSpillBuffer(nil, GroupAllocationSiteSpillRead)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	buffer, err := newGroupSpillBuffer(&container{mp: mp}, GroupAllocationSiteSpillRead)
	require.NoError(t, err)
	require.NoError(t, buffer.Resize(16))
	buffer.Free()

	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestPrepareParamKindRowsCodecEdges(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	source, err := newPrepareParamKindRowsSource(nil, nil)
	require.NoError(t, err)
	require.False(t, source.summary.seen)
	vec := vector.NewVec(types.T_varchar.ToType())
	for _, value := range []string{"a", "b", "c"} {
		require.NoError(t, vector.AppendBytes(vec, []byte(value), false, mp))
	}
	require.NoError(t, vec.SetPrepareParamKindAtWithMP(0, vector.PrepareParamInteger, mp))
	require.NoError(t, vec.SetPrepareParamKindAtWithMP(1, vector.PrepareParamFloat, mp))
	require.NoError(t, vec.SetIsBinaryStringAt(1, true, mp))
	_, err = newPrepareParamKindRowsSource(vec, []uint8{1})
	require.Error(t, err)
	_, err = newPrepareParamKindRowsSource(vec, []uint8{1, 2, 1})
	require.Error(t, err)
	_, err = newPrepareParamKindSelectedRowsSource(vec, []int32{-1})
	require.Error(t, err)
	_, err = newPrepareParamKindSelectedRowsSource(vec, []int32{3})
	require.Error(t, err)
	source, err = newPrepareParamKindRowsSource(vec, []uint8{1, 1, 0})
	require.NoError(t, err)
	require.Equal(t, 2, source.rowCount)
	require.True(t, source.summary.rows)
	require.True(t, source.summary.binaryString)

	var encoded bytes.Buffer
	require.NoError(t, source.writeRows(&encoded, true))
	require.Len(t, encoded.Bytes(), 2)
	require.NoError(t, (*prepareParamKindRowsSource)(nil).writeRows(io.Discard, false))
	invalidSource := source
	invalidSource.vec = nil
	require.Error(t, invalidSource.writeRows(io.Discard, false))
	invalidSource = source
	invalidSource.vec.SetPrepareParamKindAt(0, vector.PrepareParamKind(255))
	require.Error(t, invalidSource.writeRows(io.Discard, false))
	invalidSource.vec.SetPrepareParamKindAt(0, vector.PrepareParamInteger)
	require.Error(t, invalidSource.writeRows(&groupShortWriter{remaining: 0}, false))

	target := prepareParamKindRowsTarget{expectedRows: 2}
	_, err = target.restore(bytes.NewReader(encoded.Bytes()), 2, mp, true)
	require.NoError(t, err)
	_, err = target.restore(bytes.NewReader([]byte{255, 0}), 2, mp, false)
	require.Error(t, err)
	_, err = target.restore(bytes.NewReader([]byte{0}), 2, mp, false)
	require.Error(t, err)
	_, err = (*prepareParamKindRowsTarget)(nil).restore(bytes.NewReader(nil), 1, mp, false)
	require.Error(t, err)

	restoreVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(restoreVec, []byte("x"), false, mp))
	require.NoError(t, vector.AppendBytes(restoreVec, []byte("y"), false, mp))
	accessor := &prepareAccessor{vectors: []*vector.Vector{restoreVec}}
	chunkTarget := prepareParamKindChunkTarget(accessor, 0)
	summary, err := chunkTarget.restore(bytes.NewReader(encoded.Bytes()), 2, mp, true)
	require.NoError(t, err)
	require.True(t, summary.binaryString)
	require.Equal(t, vector.PrepareParamInteger, restoreVec.GetPrepareParamKindAt(0))
	require.Equal(t, vector.PrepareParamFloat, restoreVec.GetPrepareParamKindAt(1))

	flatTarget := prepareParamKindFlatTarget(accessor)
	require.Equal(t, 2, flatTarget.expectedRows)
	flatTarget.setBinarySummary(true)
	require.True(t, restoreVec.GetIsBinaryString())
	flatTarget.expectedRows = 3
	_, err = flatTarget.restore(bytes.NewReader([]byte{0, 0, 0}), 3, mp, false)
	require.Error(t, err)
	(*prepareParamKindRowsTarget)(nil).setBinarySummary(true)

	vec.Free(mp)
	restoreVec.Free(mp)
}

func TestGroupSpillReaderWriterBoundaryMatrix(t *testing.T) {
	mp := mpool.MustNewZero()
	ctr := &container{mp: mp}
	ctx := context.Background()
	file, err := os.CreateTemp(t.TempDir(), "group-spill-edge")
	require.NoError(t, err)
	defer file.Close()
	payload := bytes.Repeat([]byte("x"), spillIOBufSize+17)
	_, err = file.Write(payload)
	require.NoError(t, err)
	_, err = file.Seek(0, io.SeekStart)
	require.NoError(t, err)

	_, err = newGroupSpillReader(nil, file, ctx)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	reader, err := newGroupSpillReader(ctr, file, ctx)
	require.NoError(t, err)
	require.Zero(t, reader.Position())
	first := make([]byte, 7)
	n, err := reader.Read(first)
	require.NoError(t, err)
	require.Equal(t, len(first), n)
	require.Equal(t, int64(n), reader.Position())
	require.NoError(t, reader.Rewind(0))
	large := make([]byte, spillIOBufSize)
	n, err = reader.Read(large)
	require.NoError(t, err)
	require.Equal(t, len(large), n)
	reader.DropReadAhead()
	reader.Free()
	require.Zero(t, (*groupSpillReader)(nil).Position())
	_, err = (*groupSpillReader)(nil).Read([]byte{1})
	require.ErrorIs(t, err, io.EOF)
	require.ErrorIs(t, (*groupSpillReader)(nil).Rewind(0), mpool.ErrAllocationAccountInvalid)
	disabled, err := (*groupSpillReader)(nil).DisableReadAheadAndRewind(0)
	require.NoError(t, err)
	require.False(t, disabled)

	var output bytes.Buffer
	_, err = newGroupSpillWriter(nil, &output, ctx, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	writer, err := newGroupSpillWriter(ctr, &output, ctx, nil)
	require.NoError(t, err)
	n, err = writer.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.NoError(t, writer.Flush())
	require.Equal(t, payload, output.Bytes())
	writer.Free()
	_, err = (*groupSpillWriter)(nil).Write([]byte{1})
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.NoError(t, (*groupSpillWriter)(nil).Flush())

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	cancelledWriter, err := newGroupSpillWriter(ctr, io.Discard, cancelled, nil)
	require.NoError(t, err)
	_, err = cancelledWriter.Write([]byte{1})
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, cancelledWriter.Flush(), context.Canceled)
	cancelledWriter.Free()
	require.Zero(t, mp.CurrNB())
}

func TestGroupSpillAndBuildPreflightInvalidBoundaries(t *testing.T) {
	ctr := &container{}
	require.ErrorIs(t, ctr.preflightBuildChunk(nil, -1, 0, nil, 0),
		mpool.ErrAllocationAccountInvalid)
	require.NoError(t, ctr.preflightBuildChunk(nil, 0, 0, nil, 0))
	require.ErrorIs(t, ctr.preflightBuildChunk(nil, 0, 1, []uint8{1}, 0),
		mpool.ErrAllocationAccountInvariant)
	require.ErrorIs(t, ctr.preflightBuildChunk(nil, 0, 1, []uint8{1}, 1),
		mpool.ErrAllocationAccountInvariant)

	_, err := resizeDiscardableGroupScratch[int](nil, nil, 1, 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, err = resizeDiscardableGroupScratch[int](ctr, nil, -1, 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	require.Error(t, ctr.openSpillBucket(nil, nil, nil))
	retry, err := ctr.retrySpillReloadRecord(
		nil, nil, nil, nil, nil, 0, io.ErrClosedPipe)
	require.False(t, retry)
	require.ErrorIs(t, err, io.ErrClosedPipe)
}
