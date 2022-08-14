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
	"crypto/rand"
	"io"
	"os"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
)

func TestContentOffsetToBlockOffset(t *testing.T) {
	mapper := NewBlockMapper(nil, 64)

	blockOffset, offsetInBlock := mapper.contentOffsetToBlockOffset(0)
	assert.Equal(t, int64(0), blockOffset)
	assert.Equal(t, int64(0), offsetInBlock)

	blockOffset, offsetInBlock = mapper.contentOffsetToBlockOffset(1)
	assert.Equal(t, int64(0), blockOffset)
	assert.Equal(t, int64(1), offsetInBlock)

	blockOffset, offsetInBlock = mapper.contentOffsetToBlockOffset(int64(mapper.blockContentSize))
	assert.Equal(t, int64(mapper.blockSize), blockOffset)
	assert.Equal(t, int64(0), offsetInBlock)

	blockOffset, offsetInBlock = mapper.contentOffsetToBlockOffset(int64(mapper.blockContentSize) + 1)
	assert.Equal(t, int64(mapper.blockSize), blockOffset)
	assert.Equal(t, int64(1), offsetInBlock)

	blockOffset, offsetInBlock = mapper.contentOffsetToBlockOffset(int64(mapper.blockContentSize)*2 + 1)
	assert.Equal(t, int64(mapper.blockSize*2), blockOffset)
	assert.Equal(t, int64(1), offsetInBlock)

	blockOffset, offsetInBlock = mapper.contentOffsetToBlockOffset(int64(mapper.blockContentSize)*3 + 1)
	assert.Equal(t, int64(mapper.blockSize*3), blockOffset)
	assert.Equal(t, int64(1), offsetInBlock)
}

func TestBlockMapper(t *testing.T) {
	blockContentSize := 8
	tempDir := t.TempDir()

	for i := 0; i < blockContentSize*4; i++ {

		// create file and mapper
		f, err := os.CreateTemp(tempDir, "*")
		assert.Nil(t, err)
		defer f.Close()
		mapper := NewBlockMapper(f, blockContentSize)

		// random bytes
		data := make([]byte, i)
		_, err = rand.Read(data)
		assert.Nil(t, err)

		// write
		n, err := mapper.Write(data)
		assert.Nil(t, err)
		assert.Equal(t, i, n)

		// check content
		pos, err := mapper.Seek(0, io.SeekStart)
		assert.Nil(t, err)
		assert.Equal(t, int64(0), pos)
		content, err := io.ReadAll(mapper)
		assert.Nil(t, err)
		assert.Equal(t, data, content)

		// file size
		stat, err := f.Stat()
		assert.Nil(t, err)
		expectedSize := len(data) / blockContentSize * (blockContentSize + _ChecksumSize)
		mod := len(data) % blockContentSize
		if mod != 0 {
			expectedSize += _ChecksumSize + mod
		}
		assert.Equal(t, expectedSize, int(stat.Size()))

		// iotest
		pos, err = mapper.Seek(0, io.SeekStart)
		assert.Nil(t, err)
		assert.Equal(t, int64(0), pos)
		err = iotest.TestReader(mapper, data)
		if err != nil {
			t.Logf("%s", err)
		}
		assert.Nil(t, err)

		for j := 0; j < len(data); j++ {

			// seek and write random bytes
			_, err = rand.Read(data[j:])
			assert.Nil(t, err)
			pos, err = mapper.Seek(int64(j), io.SeekStart)
			assert.Nil(t, err)
			assert.Equal(t, int64(j), pos)
			n, err = mapper.Write(data[j:])
			assert.Nil(t, err)
			assert.Equal(t, len(data[j:]), n)

			// check content
			pos, err = mapper.Seek(0, io.SeekStart)
			assert.Nil(t, err)
			assert.Equal(t, int64(0), pos)
			content, err = io.ReadAll(mapper)
			assert.Nil(t, err)
			assert.Equal(t, data, content)

			// seek and read
			pos, err = mapper.Seek(int64(j), io.SeekStart)
			assert.Nil(t, err)
			assert.Equal(t, int64(j), pos)
			content, err = io.ReadAll(mapper)
			assert.Nil(t, err)
			assert.Equal(t, data[j:], content)

		}

	}
}
