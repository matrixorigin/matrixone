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
		name   string
		bytes  string
		offset int64
		want   string
	}{
		{
			name:   "TEST01",
			bytes:  "abcdefghijklmn",
			offset: 4,
			want:   "efghijklmn",
		},
		{
			name:   "TEST02",
			bytes:  "abcdefghijklmn",
			offset: 6,
			want:   "ghijklmn",
		},
		{
			name:   "Test03",
			bytes:  "abcdefghijklmn",
			offset: 10,
			want:   "klmn",
		},
		{
			name:   "TEST04",
			bytes:  "abcdefghijklmn",
			offset: 15,
			want:   "",
		},
		{
			name:   "TEST05",
			bytes:  "你好asd世界",
			offset: 3,
			want:   "sd世界",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := getSliceFromLeft(c.bytes, c.offset)
			require.Equal(t, c.want, got)
		})
	}
}

func TestGetSliceFromLeftWithLength(t *testing.T) {
	cases := []struct {
		name      string
		bytes     string
		offset    int64
		length    int64
		wantBytes string
	}{
		{
			name:      "Test01",
			bytes:     "abcdefghijklmn",
			offset:    5 - 1,
			length:    6,
			wantBytes: "efghij",
		},
		{
			name:      "Test02",
			bytes:     "abcdefghijklmn",
			offset:    5 - 1,
			length:    10,
			wantBytes: "efghijklmn",
		},
		{
			name:      "Test03",
			bytes:     "abcdefghijklmn",
			offset:    5 - 1,
			length:    0,
			wantBytes: "",
		},
		{
			name:      "Test04",
			bytes:     "abcdefghijklmn",
			offset:    5,
			length:    -8,
			wantBytes: "",
		},
		{
			name:      "Test05",
			bytes:     "abcdefghijklmn",
			offset:    5,
			length:    -9,
			wantBytes: "",
		},
		{
			name:      "Test06",
			bytes:     "abcdefghijklmn",
			offset:    5,
			length:    -4,
			wantBytes: "",
		},
		{
			name:      "Test07",
			bytes:     "abcdefghijklmn",
			offset:    5,
			length:    -1,
			wantBytes: "",
		},
		{
			name:      "Test08",
			bytes:     "abcdefghijklmn",
			offset:    5,
			length:    -1,
			wantBytes: "",
		},
		{
			name:      "Test09",
			bytes:     "明天abcdef我爱你中国",
			offset:    5 - 1,
			length:    6,
			wantBytes: "cdef我爱",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotBytes := getSliceFromLeftWithLength(c.bytes, c.offset, c.length)
			require.Equal(t, c.wantBytes, gotBytes)
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
			gotBytes := getSliceFromRight(string(c.bytes), c.offset)
			require.Equal(t, string(c.wantBytes), gotBytes)
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
			gotBytes := getSliceFromRightWithLength(string(c.bytes), c.offset, c.length)
			require.Equal(t, string(c.wantBytes), gotBytes)
		})
	}
}
