// Copyright 2023 Matrix Origin
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
	"crypto/rand"
	"io"
	mathrand "math/rand"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
)

func TestRetryableReaderIOTest(t *testing.T) {
	data := make([]byte, 4096)
	_, err := rand.Read(data)
	assert.Nil(t, err)

	r, err := newRetryableReader(func(offset int64) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data[offset:])), nil
	}, 0, isRetryableError)
	assert.Nil(t, err)

	err = iotest.TestReader(r, data)
	assert.Nil(t, err)
}

type unexpectedEOFReader struct {
	r io.Reader
}

var _ io.Reader = new(unexpectedEOFReader)

func (u *unexpectedEOFReader) Read(buf []byte) (int, error) {
	if mathrand.Intn(2) == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	return u.r.Read(buf)
}

func TestRetryableReaderUnexpectedEOFIOTest(t *testing.T) {
	data := make([]byte, 4096)
	_, err := rand.Read(data)
	assert.Nil(t, err)

	r, err := newRetryableReader(func(offset int64) (io.ReadCloser, error) {
		return io.NopCloser(&unexpectedEOFReader{
			r: bytes.NewReader(data[offset:]),
		}), nil
	}, 0, isRetryableError)
	assert.Nil(t, err)

	err = iotest.TestReader(r, data)
	assert.Nil(t, err)
}

func TestRetryableReaderUnexpectedEOF(t *testing.T) {
	data := make([]byte, 4096)
	_, err := rand.Read(data)
	assert.Nil(t, err)

	for i := 0; i < 1024; i++ {
		r, err := newRetryableReader(func(offset int64) (io.ReadCloser, error) {
			return io.NopCloser(&unexpectedEOFReader{
				r: bytes.NewReader(data[offset:]),
			}), nil
		}, 0, isRetryableError)
		assert.Nil(t, err)
		got, err := io.ReadAll(r)
		assert.Nil(t, err)
		assert.Equal(t, data, got)
	}
}
