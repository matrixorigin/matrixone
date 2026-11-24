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

package objectio

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type testColumnWindowSpill struct {
	*os.File
	grown  uint64
	closed bool
}

type failingColumnWindowSpill struct {
	growErr  error
	writeErr error
}

func (s *failingColumnWindowSpill) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }
func (s *failingColumnWindowSpill) Write([]byte) (int, error)         { return 0, s.writeErr }
func (s *failingColumnWindowSpill) Close() error                      { return nil }
func (s *failingColumnWindowSpill) Grow(uint64) error                 { return s.growErr }

func (s *testColumnWindowSpill) Grow(size uint64) error {
	s.grown += size
	return nil
}

func (s *testColumnWindowSpill) Close() error {
	s.closed = true
	return s.File.Close()
}

func marshalLegacyTestColumn(t *testing.T, source *vector.Vector) []byte {
	t.Helper()
	payload, err := source.MarshalBinary()
	require.NoError(t, err)
	encoded := append([]byte(nil), EncodeIOEntryHeader(&IOEntryHeader{
		Type: IOET_ColData, Version: IOET_ColumnData_V2,
	})...)
	return append(encoded, payload...)
}

func writeLegacyTestExtent(
	t *testing.T,
	fs fileservice.FileService,
	name string,
	encoded []byte,
	algorithm uint8,
) Extent {
	t.Helper()
	stored := encoded
	if algorithm == compress.Lz4 {
		compressed := make([]byte, lz4.CompressBlockBound(len(encoded)))
		n, err := lz4.CompressBlock(encoded, compressed, nil)
		require.NoError(t, err)
		require.Greater(t, n, 0)
		stored = compressed[:n]
	}
	require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
		FilePath: name,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(stored)), Data: stored}},
	}))
	return NewExtent(algorithm, 0, uint32(len(stored)), uint32(len(encoded)))
}

func TestReadLegacyColumnWindowLZ4AndNone(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	fs, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil,
	)
	require.NoError(t, err)

	var spills []*testColumnWindowSpill
	factory := func(context.Context) (ColumnWindowSpill, error) {
		file, err := os.CreateTemp(t.TempDir(), "legacy-column-*")
		if err != nil {
			return nil, err
		}
		spill := &testColumnWindowSpill{File: file}
		spills = append(spills, spill)
		return spill, nil
	}

	stringsSource := vector.NewVec(types.T_varchar.ToType())
	for row := 0; row < 100; row++ {
		value := strings.Repeat(string(rune('a'+row%20)), 1024)
		require.NoError(t, vector.AppendBytes(stringsSource, []byte(value), row == 57, mp))
	}
	stringsExtent := writeLegacyTestExtent(
		t, fs, "legacy-lz4", marshalLegacyTestColumn(t, stringsSource), compress.Lz4,
	)
	window, err := readLegacyColumnWindow(
		ctx, "legacy-lz4", stringsExtent, 55, 5, fs, mp, factory,
	)
	require.NoError(t, err)
	require.Equal(t, 5, window.Length())
	require.Equal(t, stringsSource.GetStringAt(55), window.GetStringAt(0))
	require.True(t, window.GetNulls().Contains(2))
	require.Equal(t, stringsSource.GetStringAt(59), window.GetStringAt(4))
	window.Free(mp)
	meta := BuildMetaData(1, 1)
	column := meta.GetBlockMeta(0).ColumnMeta(0)
	column.setDataType(uint8(types.T_varchar))
	column.setLocation(stringsExtent)
	bat, err := ReadOneBlockAllColumnsWindow(
		ctx, &meta, "legacy-lz4", 0, []uint16{0}, 55, 5,
		fileservice.SkipAllCache, fs, mp, 8<<10, factory,
	)
	require.NoError(t, err)
	require.Equal(t, stringsSource.GetStringAt(55), bat.Vecs[0].GetStringAt(0))
	require.True(t, bat.Vecs[0].GetNulls().Contains(2))
	bat.Clean(mp)
	stringsSource.Free(mp)

	fixedSource := vector.NewVec(types.T_int64.ToType())
	for row := int64(0); row < 10; row++ {
		require.NoError(t, vector.AppendFixed(fixedSource, row, row == 6, mp))
	}
	fixedExtent := writeLegacyTestExtent(
		t, fs, "legacy-none", marshalLegacyTestColumn(t, fixedSource), compress.None,
	)
	window, err = readLegacyColumnWindow(
		ctx, "legacy-none", fixedExtent, 4, 4, fs, mp, factory,
	)
	require.NoError(t, err)
	require.Equal(t, []int64{4, 5, 0, 7}, vector.MustFixedColWithTypeCheck[int64](window))
	require.True(t, window.GetNulls().Contains(2))
	window.Free(mp)
	fixedSource.Free(mp)

	require.Len(t, spills, 3)
	for _, spill := range spills {
		require.True(t, spill.closed)
		require.Greater(t, spill.grown, uint64(0))
	}
	require.Zero(t, mp.CurrNB())
}

func TestMaterializeLegacyConstantColumnWindow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	fixed, err := vector.NewConstFixed(types.T_int64.ToType(), int64(17), 6, mp)
	require.NoError(t, err)
	defer fixed.Free(mp)
	encoded := marshalLegacyTestColumn(t, fixed)
	window, err := materializeLegacyColumnWindow(bytes.NewReader(encoded), int64(len(encoded)), 2, 3, mp)
	require.NoError(t, err)
	require.Equal(t, 3, window.Length())
	for row := 0; row < 3; row++ {
		require.Equal(t, int64(17), vector.GetFixedAtNoTypeCheck[int64](window, row))
	}
	window.Free(mp)

	text, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("repeated value"), 6, mp)
	require.NoError(t, err)
	defer text.Free(mp)
	encoded = marshalLegacyTestColumn(t, text)
	window, err = materializeLegacyColumnWindow(bytes.NewReader(encoded), int64(len(encoded)), 1, 4, mp)
	require.NoError(t, err)
	require.Equal(t, 4, window.Length())
	for row := 0; row < 4; row++ {
		require.Equal(t, "repeated value", window.GetStringAt(row))
	}
	window.Free(mp)

	nullValue := vector.NewConstNull(types.T_int64.ToType(), 6, mp)
	defer nullValue.Free(mp)
	encoded = marshalLegacyTestColumn(t, nullValue)
	window, err = materializeLegacyColumnWindow(bytes.NewReader(encoded), int64(len(encoded)), 1, 4, mp)
	require.NoError(t, err)
	for row := 0; row < 4; row++ {
		require.True(t, window.IsNull(uint64(row)))
	}
	window.Free(mp)

	nullText := vector.NewConstNull(types.T_varchar.ToType(), 6, mp)
	defer nullText.Free(mp)
	encoded = marshalLegacyTestColumn(t, nullText)
	costs := make([]uint64, 3)
	require.NoError(t, addLegacyColumnWindowCosts(bytes.NewReader(encoded), int64(len(encoded)), 2, costs))
	for _, cost := range costs {
		require.Equal(t, uint64(types.T_varchar.ToType().TypeSize()), cost)
	}
	window, err = materializeLegacyColumnWindow(bytes.NewReader(encoded), int64(len(encoded)), 2, 3, mp)
	require.NoError(t, err)
	require.Equal(t, 3, window.Length())
	for row := 0; row < 3; row++ {
		require.True(t, window.IsNull(uint64(row)))
	}
	window.Free(mp)
}

func TestReadLegacyColumnWindowDeduplicatesSharedVarlenaArea(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	fs, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil,
	)
	require.NoError(t, err)

	const (
		rows        = 64
		payloadSize = 1 << 20
		budget      = 2 << 20
	)
	constant, err := vector.NewConstBytes(
		types.T_text.ToType(), bytes.Repeat([]byte("x"), payloadSize), rows, mp,
	)
	require.NoError(t, err)
	defer constant.Free(mp)
	shared := vector.NewVec(types.T_text.ToType())
	require.NoError(t, shared.UnionBatch(constant, 0, rows, nil, mp))
	defer shared.Free(mp)
	require.False(t, shared.VarlenaAreaIsDisjoint())

	extent := writeLegacyTestExtent(
		t, fs, "legacy-shared", marshalLegacyTestColumn(t, shared), compress.Lz4,
	)
	var spill *testColumnWindowSpill
	factory := func(context.Context) (ColumnWindowSpill, error) {
		file, createErr := os.CreateTemp(t.TempDir(), "legacy-shared-*")
		if createErr != nil {
			return nil, createErr
		}
		spill = &testColumnWindowSpill{File: file}
		return spill, nil
	}
	window, err := readLegacyColumnWindow(
		ctx, "legacy-shared", extent, 0, rows, fs, mp, factory,
	)
	require.NoError(t, err)
	defer window.Free(mp)
	require.True(t, spill.closed)
	require.Equal(t, rows, window.Length())
	require.Less(t, window.Allocated(), budget,
		"shared source payload must be materialized once, not once per row")
	for row := range rows {
		require.Equal(t, constant.GetBytesAt(0), window.GetBytesAt(row))
	}
}

func TestReadLegacyColumnWindowRejectsInvalidInputs(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	_, err := readLegacyColumnWindow(
		context.Background(), "missing", NewExtent(compress.Lz4, 0, 1, 1),
		0, 1, nil, mp, nil,
	)
	require.Error(t, err)

	bad := bytes.NewReader([]byte("bad"))
	_, err = materializeLegacyColumnWindow(bad, 3, 0, 1, mp)
	require.Error(t, err)

	var output bytes.Buffer
	err = decodeLegacyLZ4Block(
		context.Background(), bufioReader([]byte{0x00, 0x00, 0x00}), &output, 4,
	)
	require.Error(t, err)
}

func bufioReader(data []byte) *bufio.Reader {
	return bufio.NewReader(bytes.NewReader(data))
}

func TestColumnWindowSpillPathIsAnonymous(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spill")
	file, err := os.Create(path)
	require.NoError(t, err)
	spill := &testColumnWindowSpill{File: file}
	require.NoError(t, spill.Grow(10))
	require.Equal(t, uint64(10), spill.grown)
	require.NoError(t, spill.Close())
	require.True(t, spill.closed)
}

func TestLegacyColumnDecoderMatchAndFailurePaths(t *testing.T) {
	var output bytes.Buffer
	// One literal followed by the minimum four-byte match at offset one.
	require.NoError(t, decodeLegacyLZ4Block(
		context.Background(), bufioReader([]byte{0x10, 'a', 0x01, 0x00}), &output, 5,
	))
	require.Equal(t, "aaaaa", output.String())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, decodeLegacyLZ4Block(ctx, bufioReader([]byte{0}), io.Discard, 1))

	output.Reset()
	require.Error(t, decodeLegacyLZ4Block(
		context.Background(), bufioReader([]byte{0x00, 0x01}), &output, 1,
	))
	output.Reset()
	require.Error(t, decodeLegacyLZ4Block(
		context.Background(), bufioReader([]byte{0x10, 'a', 0x01, 0x00, 0xff}), &output, 4,
	))
}

func TestLegacyColumnDecoderBoundaryCases(t *testing.T) {
	for _, tc := range []struct {
		name     string
		payload  []byte
		expected int64
		wantErr  string
	}{
		{name: "negative size", payload: nil, expected: -1, wantErr: "invalid legacy object column size"},
		{name: "zero size with trailing input", payload: []byte{0}, expected: 0, wantErr: "trailing bytes"},
		{name: "zero offset", payload: []byte{0x00, 0x00, 0x00}, expected: 4, wantErr: "invalid legacy LZ4 match offset"},
		{name: "offset beyond output", payload: []byte{0x10, 'a', 0x02, 0x00}, expected: 5, wantErr: "invalid legacy LZ4 match offset"},
		{name: "truncated literal", payload: []byte{0x10}, expected: 1, wantErr: "EOF"},
		{name: "truncated literal length", payload: []byte{0xf0}, expected: 16, wantErr: "EOF"},
		{name: "literal exceeds size", payload: append([]byte{0x20}, []byte("ab")...), expected: 1, wantErr: "exceeds declared size"},
		{name: "truncated match offset", payload: []byte{0x10, 'a', 0x01}, expected: 5, wantErr: "EOF"},
		{name: "truncated match length", payload: []byte{0x1f, 'a', 0x01, 0x00}, expected: 20, wantErr: "EOF"},
		{name: "match exceeds size", payload: []byte{0x10, 'a', 0x01, 0x00}, expected: 4, wantErr: "exceeds declared size"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			err := decodeLegacyLZ4Block(context.Background(), bufioReader(tc.payload), &output, tc.expected)
			require.Error(t, err)
			if tc.wantErr != "" {
				require.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
	var output bytes.Buffer
	// A long literal and an overlapping match exercise both LZ4 extension forms.
	encoded := append([]byte{0xff, 0x00}, bytes.Repeat([]byte{'a'}, 15)...)
	encoded = append(encoded, 0x01, 0x00, 0x00)
	require.NoError(t, decodeLegacyLZ4Block(context.Background(), bufioReader(encoded), &output, 34))
	require.Equal(t, bytes.Repeat([]byte{'a'}, 34), output.Bytes())

	err := decodeLegacyLZ4Block(context.Background(), bufioReader([]byte{0x10, 'a'}), failingWriter{}, 1)
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestReservedSpillWriterErrorPaths(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	writer := &reservedSpillWriter{ctx: ctx, spill: &failingColumnWindowSpill{}}
	_, err := writer.Write([]byte("cancelled"))
	require.ErrorIs(t, err, context.Canceled)

	writer = &reservedSpillWriter{
		ctx:   context.Background(),
		spill: &failingColumnWindowSpill{growErr: io.ErrClosedPipe},
	}
	_, err = writer.Write([]byte("grow"))
	require.ErrorIs(t, err, io.ErrClosedPipe)

	writer = &reservedSpillWriter{
		ctx:   context.Background(),
		spill: &failingColumnWindowSpill{writeErr: io.ErrClosedPipe},
	}
	_, err = writer.Write([]byte("write"))
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestMaterializeLegacyColumnWindowRange(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	source := vector.NewVec(types.T_int32.ToType())
	defer source.Free(mp)
	for i := 0; i < 4; i++ {
		require.NoError(t, vector.AppendFixed(source, int32(i), false, mp))
	}
	encoded := marshalLegacyTestColumn(t, source)
	originSize := int64(len(encoded))
	window, err := materializeLegacyColumnWindow(bytes.NewReader(encoded), originSize, 1, 2, mp)
	require.NoError(t, err)
	require.Equal(t, 2, window.Length())
	window.Free(mp)
	_, err = materializeLegacyColumnWindow(bytes.NewReader(encoded), originSize, -1, 1, mp)
	require.Error(t, err)
	_, err = materializeLegacyColumnWindow(bytes.NewReader(encoded), originSize, 4, 1, mp)
	require.Error(t, err)
}

func TestLegacyColumnWindowRejectsCorruptLayout(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	source := vector.NewVec(types.T_int64.ToType())
	defer source.Free(mp)
	require.NoError(t, vector.AppendFixed(source, int64(17), false, mp))
	valid := marshalLegacyTestColumn(t, source)
	baseline := mp.CurrNB()
	dataLengthAt := IOEntryHeaderSize + 1 + types.TSize + 4
	areaLengthAt := dataLengthAt + 4 + source.GetType().TypeSize()
	cases := []struct {
		name   string
		mutate func([]byte)
		size   int64
	}{
		{"header", func(data []byte) { data[0] = 0 }, int64(len(valid))},
		{"class", func(data []byte) { data[IOEntryHeaderSize] = 255 }, int64(len(valid))},
		{"data length", func(data []byte) { binary.LittleEndian.PutUint32(data[dataLengthAt:], 1<<20) }, int64(len(valid))},
		{"area length", func(data []byte) { binary.LittleEndian.PutUint32(data[areaLengthAt:], 1<<20) }, int64(len(valid))},
		{"declared size", func([]byte) {}, int64(len(valid) + 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := bytes.Clone(valid)
			tc.mutate(data)
			_, err := materializeLegacyColumnWindow(bytes.NewReader(data), tc.size, 0, 1, mp)
			require.Error(t, err)
			costs := make([]uint64, 1)
			require.Error(t, addLegacyColumnWindowCosts(bytes.NewReader(data), tc.size, 0, costs))
		})
	}
	require.Equal(t, baseline, mp.CurrNB())
}

func TestLegacyColumnStreamRejectsUnsupportedAndInvalidSpill(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil,
	)
	require.NoError(t, err)
	encoded := []byte("legacy")
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: "legacy-unsupported",
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(encoded)), Data: encoded}},
	}))
	file, err := os.CreateTemp(t.TempDir(), "legacy-spill-*")
	require.NoError(t, err)
	spill := &testColumnWindowSpill{File: file}
	require.ErrorContains(t, streamLegacyColumnToSpill(
		ctx, "legacy-unsupported", NewExtent(99, 0, uint32(len(encoded)), uint32(len(encoded))), fs, spill,
	), "unsupported")
	require.True(t, spill.closed == false)
	require.NoError(t, spill.Close())

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	_, err = readLegacyColumnWindow(ctx, "missing", NewExtent(compress.None, 0, 0, 1), 0, 1, fs, mp, func(context.Context) (ColumnWindowSpill, error) {
		return nil, nil
	})
	require.Error(t, err)
}
