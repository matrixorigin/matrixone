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

package substring

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetSliceFromLeft(t *testing.T) {
	cases := []struct {
		name      string
		bytes     []byte
		offset    int64
		wantBytes []byte
		wantLen   int64
	}{
		{
			name:      "TEST01",
			bytes:     []byte("abcdefghijklmn"),
			offset:    4,
			wantBytes: []byte("efghijklmn"),
			wantLen:   10,
		},
		{
			name:      "TEST02",
			bytes:     []byte("abcdefghijklmn"),
			offset:    6,
			wantBytes: []byte("ghijklmn"),
			wantLen:   8,
		},
		{
			name:      "Test03",
			bytes:     []byte("abcdefghijklmn"),
			offset:    10,
			wantBytes: []byte("klmn"),
			wantLen:   4,
		},
		{
			name:      "TEST04",
			bytes:     []byte("abcdefghijklmn"),
			offset:    15,
			wantBytes: []byte(""),
			wantLen:   0,
		},
		{
			name:      "TEST05",
			bytes:     []byte("你好asd世界"),
			offset:    3,
			wantBytes: []byte("sd世界"),
			wantLen:   8,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got1, got2 := getSliceFromLeft(c.bytes, c.offset)
			require.Equal(t, c.wantBytes, got1)
			require.Equal(t, c.wantLen, got2)
		})
	}
}

func TestGetSliceFromLeftWithLength(t *testing.T) {
	cases := []struct {
		name      string
		bytes     []byte
		offset    int64
		length    int64
		wantBytes []byte
		wantlen   int64
	}{
		{
			name:      "Test01",
			bytes:     []byte("abcdefghijklmn"),
			offset:    5 - 1,
			length:    6,
			wantBytes: []byte("efghij"),
			wantlen:   6,
		},
		{
			name:      "Test02",
			bytes:     []byte("abcdefghijklmn"),
			offset:    5 - 1,
			length:    10,
			wantBytes: []byte("efghijklmn"),
			wantlen:   10,
		},
		{
			name:      "Test03",
			bytes:     []byte("abcdefghijklmn"),
			offset:    5 - 1,
			length:    0,
			wantBytes: []byte(""),
			wantlen:   0,
		},
		{
			name:      "Test04",
			bytes:     []byte("abcdefghijklmn"),
			offset:    5,
			length:    -8,
			wantBytes: []byte(""),
			wantlen:   0,
		},
		{
			name:      "Test05",
			bytes:     []byte("abcdefghijklmn"),
			offset:    5,
			length:    -9,
			wantBytes: []byte(""),
			wantlen:   0,
		},
		{
			name:      "Test06",
			bytes:     []byte("abcdefghijklmn"),
			offset:    5,
			length:    -4,
			wantBytes: []byte(""),
			wantlen:   0,
		},
		{
			name:      "Test07",
			bytes:     []byte("abcdefghijklmn"),
			offset:    5,
			length:    -1,
			wantBytes: []byte(""),
			wantlen:   0,
		},
		{
			name:      "Test08",
			bytes:     []byte("abcdefghijklmn"),
			offset:    5,
			length:    -1,
			wantBytes: []byte(""),
			wantlen:   0,
		},
		{
			name:      "Test09",
			bytes:     []byte("明天abcdef我爱你中国"),
			offset:    5 - 1,
			length:    6,
			wantBytes: []byte("cdef我爱"),
			wantlen:   10,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotBytes, gotLen := getSliceFromLeftWithLength(c.bytes, c.offset, c.length)
			require.Equal(t, c.wantBytes, gotBytes)
			require.Equal(t, c.wantlen, gotLen)
		})
	}
}

func TestGetSliceFromRight(t *testing.T) {
	cases := []struct {
		name      string
		bytes     []byte
		offset    int64
		wantBytes []byte
		wantLen   int64
	}{
		{
			name:      "Test01",
			bytes:     []byte("abcdefghijklmn"),
			offset:    4,
			wantBytes: []byte("klmn"),
			wantLen:   4,
		},
		{
			name:      "Test02",
			bytes:     []byte("abcdefghijklmn"),
			offset:    14,
			wantBytes: []byte("abcdefghijklmn"),
			wantLen:   14,
		},
		{
			name:      "Test03",
			bytes:     []byte("abcdefghijklmn"),
			offset:    16,
			wantBytes: []byte(""),
			wantLen:   0,
		},
		{
			name:      "Test04",
			bytes:     []byte("abcdef我爱你中国"),
			offset:    7,
			wantBytes: []byte("ef我爱你中国"),
			wantLen:   17,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotBytes, gotLen := getSliceFromRight(c.bytes, c.offset)
			require.Equal(t, c.wantBytes, gotBytes)
			require.Equal(t, c.wantLen, gotLen)
		})
	}
}

func TestGetSliceFromRightWithLength(t *testing.T) {
	cases := []struct {
		name      string
		bytes     []byte
		offset    int64
		length    int64
		wantBytes []byte
		wantLen   int64
	}{
		{
			name:      "Test01",
			bytes:     []byte("abcdefghijklmn"),
			offset:    4,
			length:    3,
			wantBytes: []byte("klm"),
			wantLen:   3,
		},
		{
			name:      "Test02",
			bytes:     []byte("abcdefghijklmn"),
			offset:    14,
			length:    10,
			wantBytes: []byte("abcdefghij"),
			wantLen:   10,
		},
		{
			name:      "Test03",
			bytes:     []byte("abcdefghijklmn"),
			offset:    14,
			length:    15,
			wantBytes: []byte("abcdefghijklmn"),
			wantLen:   14,
		},
		{
			name:      "Test04",
			bytes:     []byte("abcdefghijklmn"),
			offset:    16,
			length:    10,
			wantBytes: []byte(""),
			wantLen:   0,
		},
		{
			name:      "Test05",
			bytes:     []byte("abcdefghijklmn"),
			offset:    16,
			length:    20,
			wantBytes: []byte(""),
			wantLen:   0,
		},
		{
			name:      "Test06",
			bytes:     []byte("abcdefghijklmn"),
			offset:    16,
			length:    2,
			wantBytes: []byte(""),
			wantLen:   0,
		},
		{
			name:      "Test07",
			bytes:     []byte("abcdefghijklmn"),
			offset:    12,
			length:    2,
			wantBytes: []byte("cd"),
			wantLen:   2,
		},
		{
			name:      "Test08",
			bytes:     []byte("abcdefghijklmn"),
			offset:    12,
			length:    14,
			wantBytes: []byte("cdefghijklmn"),
			wantLen:   12,
		},
		{
			name:      "Test09",
			bytes:     []byte("abcdefghijklmn"),
			offset:    12,
			length:    0,
			wantBytes: []byte(""),
			wantLen:   0,
		},
		{
			name:      "Test10",
			bytes:     []byte("abcdefghijklmn"),
			offset:    6,
			length:    -5,
			wantBytes: []byte(""),
			wantLen:   0,
		},
		{
			name:      "Test10",
			bytes:     []byte("abcdefghijklmn"),
			offset:    6,
			length:    -10,
			wantBytes: []byte(""),
			wantLen:   0,
		},
		{
			name:      "Test11",
			bytes:     []byte("abcdefghijklmn"),
			offset:    6,
			length:    -1,
			wantBytes: []byte(""),
			wantLen:   0,
		},
		{
			name:      "Test12",
			bytes:     []byte("明天abcdef我爱你中国"),
			offset:    8,
			length:    5,
			wantBytes: []byte("def我爱"),
			wantLen:   9,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotBytes, gotLen := getSliceFromRightWithLength(c.bytes, c.offset, c.length)
			require.Equal(t, c.wantBytes, gotBytes)
			require.Equal(t, c.wantLen, gotLen)
		})
	}
}
