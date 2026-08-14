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
