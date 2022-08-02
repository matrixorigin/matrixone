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

package lengthutf8

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLengthUTF8(t *testing.T) {
	cases := map[string]uint64{
		"abc":   3,
		"":      0,
		"   ":   3,
		"中国123": 5,
		"abc😄":  4,
		"中国中国中国中国中国中国中国中国中国中国1234":      24,
		"中国中国中国中国中国中国中国中国中国中国1234😄ggg!": 29,
	}

	for input, expected := range cases {
		xs := MakeBytes([]string{input})
		re := make([]uint64, 1)
		actual := StrLengthUTF8(xs, re)[0]
		require.Equal(t, expected, actual)
	}
}

func TestLengthUTF8WithMultiString(t *testing.T) {
	xs := MakeBytes([]string{"你好", "中国", "abc", " ", "", "abc😄哈"})
	re := make([]uint64, 6)
	expected := []uint64{2, 2, 3, 1, 0, 5}
	actual := StrLengthUTF8(xs, re)
	require.Equal(t, expected, actual)
}

func MakeBytes(strs []string) *types.Bytes {
	result := &types.Bytes{
		Lengths: make([]uint32, len(strs)),
		Offsets: make([]uint32, len(strs)),
	}

	cursor := 0
	var buf bytes.Buffer
	for i, str := range strs {
		buf.WriteString(str)
		result.Lengths[i] = uint32(len(str))
		result.Offsets[i] = uint32(cursor)
		cursor += len(str)
	}
	result.Data = buf.Bytes()

	return result
}
