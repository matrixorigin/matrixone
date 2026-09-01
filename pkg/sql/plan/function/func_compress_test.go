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

package function

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMySQLCompressFormatAndRoundTrip(t *testing.T) {
	incompressible := make([]byte, 1<<18)
	_, err := rand.New(rand.NewSource(1)).Read(incompressible)
	require.NoError(t, err)
	inputs := [][]byte{
		[]byte("Hello World"),
		{0, 1, 2, 0xff, ' '},
		bytes.Repeat([]byte("MatrixOne"), 1<<17),
		incompressible,
	}
	for _, input := range inputs {
		compressed, err := mysqlCompress(input, types.MaxBlobLen)
		require.NoError(t, err)
		require.Equal(t, uint32(len(input)), binary.LittleEndian.Uint32(compressed[:4]))

		reader, err := zlib.NewReader(bytes.NewReader(compressed[4:]))
		require.NoError(t, err)
		decoded, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		require.Equal(t, input, decoded)

		roundTrip, err := mysqlUncompress(compressed, types.MaxBlobLen)
		require.NoError(t, err)
		require.Equal(t, input, roundTrip)
	}

	empty, err := mysqlCompress(nil, types.MaxBlobLen)
	require.NoError(t, err)
	require.Empty(t, empty)
	decoded, err := mysqlUncompress(empty, types.MaxBlobLen)
	require.NoError(t, err)
	require.Empty(t, decoded)
}

func TestMySQLCompressBufferBoundaries(t *testing.T) {
	random := rand.New(rand.NewSource(2))
	for _, size := range []int{0, 1, 2, 3, 4, 31, 32, 33, 255, 256, 257, (32 << 10) - 1, 32 << 10, (32 << 10) + 1} {
		input := make([]byte, size)
		_, err := random.Read(input)
		require.NoError(t, err)

		compressed, err := mysqlCompress(input, types.MaxBlobLen)
		require.NoError(t, err)
		decoded, err := mysqlUncompress(compressed, types.MaxBlobLen)
		require.NoError(t, err)
		require.Equal(t, input, decoded)

		exactlyBounded, err := mysqlCompress(input, len(compressed))
		require.NoError(t, err)
		require.Equal(t, compressed, exactlyBounded)
		if len(compressed) > 0 {
			_, err = mysqlCompress(input, len(compressed)-1)
			require.Error(t, err)
		}
	}
}

func TestMySQLCompressPreservesTrailingSpace(t *testing.T) {
	// The Adler-32 checksum for this input ends in 0x20. MySQL appends a dot
	// so a CHAR column cannot trim that byte from the zlib stream.
	compressed, err := mysqlCompress([]byte{0x1f}, types.MaxBlobLen)
	require.NoError(t, err)
	require.Equal(t, []byte{' ', '.'}, compressed[len(compressed)-2:])

	decoded, err := mysqlUncompress(compressed, types.MaxBlobLen)
	require.NoError(t, err)
	require.Equal(t, []byte{0x1f}, decoded)
}

type compressFailWriter struct {
	err error
}

func (w compressFailWriter) Write([]byte) (int, error) {
	return 0, w.err
}

func TestMySQLCompressFailuresAreBounded(t *testing.T) {
	wantErr := errors.New("write failed")
	require.ErrorIs(t, writeZlibCompressed(compressFailWriter{err: wantErr}, []byte("value")), wantErr)

	var zeroLimit mysqlCompressBuffer
	written, err := zeroLimit.Write(nil)
	require.NoError(t, err)
	require.Zero(t, written)
	_, err = zeroLimit.Write([]byte{1})
	require.ErrorIs(t, err, io.ErrShortWrite)

	_, err = mysqlCompress([]byte("value"), 5)
	require.Error(t, err)

	withMarker, err := mysqlCompress([]byte{0x1f}, types.MaxBlobLen)
	require.NoError(t, err)
	_, err = mysqlCompress([]byte{0x1f}, len(withMarker)-1)
	require.Error(t, err)
}

func TestMySQLUncompressRejectsInvalidAndOversizedInput(t *testing.T) {
	valid, err := mysqlCompress([]byte("abc"), types.MaxBlobLen)
	require.NoError(t, err)

	shortHeader := append([]byte(nil), valid...)
	binary.LittleEndian.PutUint32(shortHeader[:4], 2)
	badChecksum := append([]byte(nil), valid...)
	badChecksum[len(badChecksum)-1] ^= 0xff
	oversized := append([]byte(nil), valid...)
	binary.LittleEndian.PutUint32(oversized[:4], uint32(types.MaxBlobLen+1))

	invalid := [][]byte{
		{1, 2, 3, 4},
		{3, 0, 0, 0, 0, 3, 0, 0xfc, 0xff, 'a', 'b', 'c', 3, 0}, // raw DEFLATE
		shortHeader,
		badChecksum,
		oversized,
		{16, 0, 0, 0, 0x78, 0xbb, 0x16, 0xc0, 0x04, 0x37, 0x4b, 0x81, 0xb3, 0x74, 0xcb, 0x12, 0x73, 0x4a, 0x53, 0x01, 0x37, 0x7d, 0x06, 0x81}, // preset dictionary
	}
	for cut := 1; cut < len(valid); cut++ {
		invalid = append(invalid, valid[:cut])
	}
	for _, input := range invalid {
		_, err = mysqlUncompress(input, types.MaxBlobLen)
		require.Error(t, err)
	}
	_, err = mysqlUncompress(valid, -1)
	require.Error(t, err)
}

func TestMySQLUncompressLengthIsCapacity(t *testing.T) {
	fixture := []byte{3, 0, 0, 0, 0x78, 0x9c, 0x4b, 0x4c, 0x4a, 0x06, 0x00, 0x02, 0x4d, 0x01, 0x27}
	for _, advertised := range []uint32{3, 4, 1 << 26, 0xc0000003} {
		input := append([]byte(nil), fixture...)
		binary.LittleEndian.PutUint32(input[:4], advertised)
		decoded, err := mysqlUncompress(input, types.MaxBlobLen)
		require.NoError(t, err)
		require.Equal(t, []byte("abc"), decoded)
		if advertised == 1<<26 {
			require.Less(t, cap(decoded), 1024, "advertised capacity must not be eagerly allocated")
		}
	}

	emptyStream := []byte{0x78, 0x9c, 0x03, 0x00, 0x00, 0x00, 0x00, 0x01}
	for _, advertised := range []uint32{0, 1} {
		input := make([]byte, 4, 4+len(emptyStream))
		binary.LittleEndian.PutUint32(input, advertised)
		input = append(input, emptyStream...)
		decoded, err := mysqlUncompress(input, types.MaxBlobLen)
		require.NoError(t, err)
		require.Empty(t, decoded)
	}

	withTrailingGarbage := append(append([]byte(nil), fixture...), 0)
	decoded, err := mysqlUncompress(withTrailingGarbage, types.MaxBlobLen)
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), decoded)

	withSecondStream := append(append([]byte(nil), fixture...), fixture[4:]...)
	decoded, err = mysqlUncompress(withSecondStream, types.MaxBlobLen)
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), decoded)
}

type trackedReadCloser struct {
	io.Reader
	closeErr   error
	closeCalls int
}

func (r *trackedReadCloser) Close() error {
	r.closeCalls++
	return r.closeErr
}

func TestReadCompressedClosesOnEveryTerminalPath(t *testing.T) {
	closeFailure := errors.New("close failed")
	reader := &trackedReadCloser{Reader: bytes.NewReader(nil), closeErr: closeFailure}
	_, err := readCompressed(reader, 1)
	require.ErrorIs(t, err, closeFailure)
	require.Equal(t, 1, reader.closeCalls)

	reader = &trackedReadCloser{Reader: bytes.NewReader([]byte{1})}
	_, err = readCompressed(reader, 0)
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, 1, reader.closeCalls)
}

func TestLegacyMatrixOneCompressedDataRemainsReadable(t *testing.T) {
	legacy := []byte{3, 0, 0, 0, 0, 3, 0, 0xfc, 0xff, 'a', 'b', 'c', 3, 0}
	decoded, err := legacyMatrixOneUncompress(legacy, types.MaxBlobLen)
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), decoded)

	_, err = legacyMatrixOneUncompress([]byte{1, 2, 3, 4}, types.MaxBlobLen)
	require.Error(t, err)

	longHeader := append([]byte(nil), legacy...)
	binary.LittleEndian.PutUint32(longHeader[:4], 4)
	_, err = legacyMatrixOneUncompress(longHeader, types.MaxBlobLen)
	require.Error(t, err)

	legacyHello := []byte{0x0b, 0, 0, 0, 0, 0x0b, 0, 0xf4, 0xff, 'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd', 3, 0}
	decoded, err = legacyMatrixOneUncompress(legacyHello, types.MaxBlobLen)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello World"), decoded)

	var rawEmpty bytes.Buffer
	writer, err := flate.NewWriter(&rawEmpty, flate.DefaultCompression)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	oldEmpty := append(make([]byte, 4), rawEmpty.Bytes()...)
	decoded, err = legacyMatrixOneUncompress(oldEmpty, types.MaxBlobLen)
	require.NoError(t, err)
	require.Empty(t, decoded)
}

func TestCompressFunctionsNullsSelectListAndLengthMask(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := testutil.NewVectorWithNulls(
		3,
		types.T_blob.ToType(),
		proc.Mp(),
		false,
		[]bool{false, true, false},
		[]string{"Hello World", "ignored null", "ignored row"},
	)

	compressedResult := vector.NewFunctionResultWrapper(types.T_blob.ToType(), proc.Mp())
	require.NoError(t, compressedResult.PreExtendAndReset(3))
	require.NoError(t, Compress(
		[]*vector.Vector{input}, compressedResult, proc, 3,
		&FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, false}},
	))
	compressed := compressedResult.GetResultVector()
	require.False(t, compressed.IsNull(0))
	require.True(t, compressed.IsNull(1))
	require.True(t, compressed.IsNull(2))

	uncompressedResult := vector.NewFunctionResultWrapper(types.T_blob.ToType(), proc.Mp())
	require.NoError(t, uncompressedResult.PreExtendAndReset(3))
	require.NoError(t, Uncompress([]*vector.Vector{compressed}, uncompressedResult, proc, 3, nil))
	uncompressed := uncompressedResult.GetResultVector()
	require.Equal(t, []byte("Hello World"), uncompressed.GetBytesAt(0))
	require.True(t, uncompressed.IsNull(1))
	require.True(t, uncompressed.IsNull(2))

	legacy := []byte{3, 0, 0, 0, 0, 3, 0, 0xfc, 0xff, 'a', 'b', 'c', 3, 0}
	invalidInput := testutil.NewVector(
		3,
		types.T_blob.ToType(),
		proc.Mp(),
		false,
		[]string{"invalid", string(compressed.GetBytesAt(0)), string(legacy)},
	)
	invalidResult := vector.NewFunctionResultWrapper(types.T_blob.ToType(), proc.Mp())
	require.NoError(t, invalidResult.PreExtendAndReset(3))
	require.NoError(t, Uncompress(
		[]*vector.Vector{invalidInput}, invalidResult, proc, 3,
		&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}},
	))
	require.True(t, invalidResult.GetResultVector().IsNull(0))
	require.True(t, invalidResult.GetResultVector().IsNull(1))
	require.Equal(t, []byte("abc"), invalidResult.GetResultVector().GetBytesAt(2))

	var maskedHeader [5]byte
	binary.LittleEndian.PutUint32(maskedHeader[:4], 0xffffffff)
	lengthInput := testutil.NewVectorWithNulls(
		5,
		types.T_blob.ToType(),
		proc.Mp(),
		false,
		[]bool{false, false, false, false, true},
		[]string{"", "abc", string(maskedHeader[:]), "invalid", "ignored"},
	)
	lengthResult := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	require.NoError(t, lengthResult.PreExtendAndReset(5))
	require.NoError(t, UncompressedLength([]*vector.Vector{lengthInput}, lengthResult, proc, 5, nil))
	lengthVector := lengthResult.GetResultVector()
	require.Equal(t, []int64{0, 0, int64(mysqlCompressedLengthMask), 561409641, 0}, vector.MustFixedColNoTypeCheck[int64](lengthVector))
	require.False(t, lengthVector.IsNull(0))
	require.False(t, lengthVector.IsNull(1))
	require.True(t, lengthVector.IsNull(4))

	lengthResult.Free()
	invalidResult.Free()
	uncompressedResult.Free()
	compressedResult.Free()
	lengthInput.Free(proc.Mp())
	invalidInput.Free(proc.Mp())
	input.Free(proc.Mp())
	proc.Free()
}

func TestCompressFunctionBinaryReturnTypes(t *testing.T) {
	for _, name := range []string{"compress", "uncompress"} {
		for _, inputType := range []types.Type{
			types.T_char.ToType(),
			types.T_varchar.ToType(),
			types.T_text.ToType(),
			types.T_blob.ToType(),
			types.T_binary.ToType(),
			types.T_varbinary.ToType(),
		} {
			resolved, err := GetFunctionByName(context.Background(), name, []types.Type{inputType})
			require.NoError(t, err, "%s(%s)", name, inputType.String())
			require.Equal(t, types.T_blob, resolved.GetReturnType().Oid)
		}
	}
}

func FuzzCompressedInputBounds(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 2, 3, 4})
	f.Add([]byte{3, 0, 0, 0, 0x78, 0x9c, 0x4b, 0x4c, 0x4a, 0x06, 0x00, 0x02, 0x4d, 0x01, 0x27})
	f.Add([]byte{3, 0, 0, 0, 0, 3, 0, 0xfc, 0xff, 'a', 'b', 'c', 3, 0})

	const maxResultSize = 1 << 20
	f.Fuzz(func(t *testing.T, input []byte) {
		decoded, err := mysqlUncompress(input, maxResultSize)
		if err == nil {
			require.LessOrEqual(t, len(decoded), maxResultSize)
			if len(input) > 4 {
				advertised := binary.LittleEndian.Uint32(input[:4]) & mysqlCompressedLengthMask
				require.LessOrEqual(t, uint32(len(decoded)), advertised)
			}
		}

		decoded, err = legacyMatrixOneUncompress(input, maxResultSize)
		if err == nil && len(input) > 4 {
			advertised := binary.LittleEndian.Uint32(input[:4]) & mysqlCompressedLengthMask
			require.Equal(t, advertised, uint32(len(decoded)))
			require.LessOrEqual(t, len(decoded), maxResultSize)
		}
	})
}
