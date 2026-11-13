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

package fileservice

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

type incorrectSeekReader struct {
	io.Reader
	err error
}

func (r incorrectSeekReader) Seek(int64, int) (int64, error) { return 0, r.err }

func TestReadObjectRange(t *testing.T) {
	ptr := func(n int64) *int64 { return &n }
	for _, tc := range []struct {
		name     string
		min, max *int64
		want     string
		empty    bool
	}{
		{"all", nil, nil, "abcdef", false},
		{"from", ptr(2), nil, "cdef", false},
		{"to", nil, ptr(3), "abc", false},
		{"bounded", ptr(1), ptr(4), "bcd", false},
		{"past end", ptr(20), nil, "", false},
		{"zero", ptr(3), ptr(3), "", true},
		{"reversed", ptr(4), ptr(2), "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := readObjectRange(strings.NewReader("abcdef"), "key", tc.min, tc.max)
			if tc.empty {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrEmptyRange))
				return
			}
			require.NoError(t, err)
			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(got))
		})
	}
	fail := errors.New("seek failed")
	_, err := readObjectRange(incorrectSeekReader{Reader: strings.NewReader("abc"), err: fail}, "key", ptr(1), nil)
	require.ErrorIs(t, err, fail)
	_, err = readObjectRange(incorrectSeekReader{Reader: strings.NewReader("abc")}, "key", ptr(1), nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrEmptyRange))
}
