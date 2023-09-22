// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
)

func TestFileWithChecksumOffsets(t *testing.T) {
	ctx := context.Background()
	f := NewFileWithChecksum[*os.File](ctx, nil, 64, nil)

	blockOffset, offsetInBlock := f.contentOffsetToBlockOffset(0)
	assert.Equal(t, int64(0), blockOffset)
	assert.Equal(t, int64(0), offsetInBlock)

	blockOffset, offsetInBlock = f.contentOffsetToBlockOffset(1)
	assert.Equal(t, int64(0), blockOffset)
	assert.Equal(t, int64(1), offsetInBlock)

	blockOffset, offsetInBlock = f.contentOffsetToBlockOffset(int64(f.blockContentSize))
	assert.Equal(t, int64(f.blockSize), blockOffset)
	assert.Equal(t, int64(0), offsetInBlock)

	blockOffset, offsetInBlock = f.contentOffsetToBlockOffset(int64(f.blockContentSize) + 1)
	assert.Equal(t, int64(f.blockSize), blockOffset)
	assert.Equal(t, int64(1), offsetInBlock)

	blockOffset, offsetInBlock = f.contentOffsetToBlockOffset(int64(f.blockContentSize)*2 + 1)
	assert.Equal(t, int64(f.blockSize*2), blockOffset)
	assert.Equal(t, int64(1), offsetInBlock)

	blockOffset, offsetInBlock = f.contentOffsetToBlockOffset(int64(f.blockContentSize)*3 + 1)
	assert.Equal(t, int64(f.blockSize*3), blockOffset)
	assert.Equal(t, int64(1), offsetInBlock)
}

func TestFileWithChecksum(t *testing.T) {
	blockContentSize := 8
	tempDir := t.TempDir()

	testFileWithChecksum(
		t,
		blockContentSize,
		func() FileLike {
			f, err := os.CreateTemp(tempDir, "*")
			assert.Nil(t, err)
			t.Cleanup(func() {
				f.Close()
			})
			return f
		},
	)
}

func testFileWithChecksum(
	t *testing.T,
	blockContentSize int,
	newUnderlying func() FileLike,
) {

	for i := 0; i < blockContentSize*4; i++ {

		underlying := newUnderlying()
		ctx := context.Background()
		fileWithChecksum := NewFileWithChecksum(ctx, underlying, blockContentSize, nil)

		check := func(data []byte) {
			// check content
			pos, err := fileWithChecksum.Seek(0, io.SeekStart)
			assert.Nil(t, err)
			assert.Equal(t, int64(0), pos)
			content, err := io.ReadAll(fileWithChecksum)
			assert.Nil(t, err)
			assert.Equal(t, data, content)

			// seek
			n, err := fileWithChecksum.Seek(0, io.SeekEnd)
			assert.Nil(t, err)
			assert.Equal(t, int64(len(data)), n)

			// iotest
			pos, err = fileWithChecksum.Seek(0, io.SeekStart)
			assert.Nil(t, err)
			assert.Equal(t, int64(0), pos)
			err = iotest.TestReader(fileWithChecksum, data)
			if err != nil {
				t.Logf("%s", err)
			}
			assert.Nil(t, err)
		}

		// random bytes
		data := make([]byte, i)
		_, err := rand.Read(data)
		assert.Nil(t, err)

		// write
		n, err := fileWithChecksum.Write(data)
		assert.Nil(t, err)
		assert.Equal(t, i, n)

		// underlying size
		underlyingSize, err := underlying.Seek(0, io.SeekEnd)
		assert.Nil(t, err)
		expectedSize := len(data) / blockContentSize * (blockContentSize + _ChecksumSize)
		mod := len(data) % blockContentSize
		if mod != 0 {
			expectedSize += _ChecksumSize + mod
		}
		assert.Equal(t, expectedSize, int(underlyingSize))

		check(data)

		for j := 0; j < len(data); j++ {

			// seek and write random bytes
			_, err = rand.Read(data[j:])
			assert.Nil(t, err)
			pos, err := fileWithChecksum.Seek(int64(j), io.SeekStart)
			assert.Nil(t, err)
			assert.Equal(t, int64(j), pos)
			n, err = fileWithChecksum.Write(data[j:])
			assert.Nil(t, err)
			assert.Equal(t, len(data[j:]), n)

			// seek and read
			pos, err = fileWithChecksum.Seek(int64(j), io.SeekStart)
			assert.Nil(t, err)
			assert.Equal(t, int64(j), pos)
			content, err := io.ReadAll(fileWithChecksum)
			assert.Nil(t, err)
			assert.Equal(t, data[j:], content)

			check(data)

		}

	}
}

func TestMultiLayerFileWithChecksum(t *testing.T) {
	blockContentSize := 8
	tempDir := t.TempDir()

	testFileWithChecksum(
		t,
		blockContentSize,
		func() FileLike {
			f, err := os.CreateTemp(tempDir, "*")
			assert.Nil(t, err)
			t.Cleanup(func() {
				f.Close()
			})
			ctx := context.Background()
			f2 := NewFileWithChecksum(ctx, f, blockContentSize, nil)
			f3 := NewFileWithChecksum(ctx, f2, blockContentSize, nil)
			f4 := NewFileWithChecksum(ctx, f3, blockContentSize, nil)
			return f4
		},
	)
}

func BenchmarkFileWithChecksumRead(b *testing.B) {
	ctx := context.Background()

	dir := b.TempDir()
	f, err := os.CreateTemp(dir, "*")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	f2 := NewFileWithChecksum(ctx, f, _BlockContentSize, nil)
	_, err = f2.Write(bytes.Repeat([]byte("a"), 65536))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = f2.Seek(0, io.SeekStart)
		if err != nil {
			b.Fatal(err)
		}
		n, err := io.Copy(io.Discard, f2)
		if err != nil {
			b.Fatal(err)
		}
		if n != 65536 {
			b.Fatal()
		}
	}
}

func BenchmarkFileWithChecksumWrite(b *testing.B) {
	ctx := context.Background()

	dir := b.TempDir()
	f, err := os.CreateTemp(dir, "*")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	content := bytes.Repeat([]byte("a"), 65536)
	f2 := NewFileWithChecksum(ctx, f, _BlockContentSize, nil)
	_, err = f2.Write(content)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = f2.Seek(0, io.SeekStart)
		if err != nil {
			b.Fatal(err)
		}
		n, err := f2.Write(content)
		if err != nil {
			b.Fatal(err)
		}
		if n != 65536 {
			b.Fatal()
		}
	}
}
