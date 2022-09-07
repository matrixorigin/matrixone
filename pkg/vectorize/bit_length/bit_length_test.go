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

package bit_length

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func makeBytes(strs []string) [][]byte {
	ret := make([][]byte, len(strs))
	for i, s := range strs {
		ret[i] = []byte(s)
	}
	return ret
}

func TestEmpty(t *testing.T) {
	tt := []struct {
		name string
		xs   [][]byte
		rs   []int64
		want []int64
	}{
		{
			name: "Empty",
			xs:   makeBytes([]string{""}),
			rs:   make([]int64, 1),
			want: []int64{0},
		},
		{
			name: "Simple condition",
			xs:   makeBytes([]string{"a", "boy", " ", "\t", "\n", "dead"}),
			rs:   make([]int64, 6),
			want: []int64{8, 24, 8, 8, 8, 32},
		},
	}
	for _, tc := range tt {
		require.Equal(t, tc.want, StrBitLength(tc.xs, tc.rs))
	}
}
