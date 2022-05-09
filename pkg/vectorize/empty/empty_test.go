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

package empty

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func MakeBytes(strs []string) *types.Bytes {
	ret := &types.Bytes{
		Lengths: make([]uint32, len(strs)),
		Offsets: make([]uint32, len(strs)),
	}
	cur := 0
	var buf bytes.Buffer
	for i, s := range strs {
		buf.WriteString(s)
		ret.Lengths[i] = uint32(len(s))
		ret.Offsets[i] = uint32(cur)
		cur += len(s)
	}
	ret.Data = buf.Bytes()
	return ret
}

func TestEmpty(t *testing.T) {
	tt := []struct {
		name string
		xs   *types.Bytes
		rs   []uint8
		want []uint8
	}{
		{
			name: "Not Empty",
			xs:   MakeBytes([]string{"Hello", "World", " ", "\t", "\n", "\r"}),
			rs:   make([]uint8, 6),
			want: []uint8{0, 0, 0, 0, 0, 0},
		},
		{
			name: "Empty",
			xs:   MakeBytes([]string{""}),
			rs:   make([]uint8, 1),
			want: []uint8{1},
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			if got := Empty(tc.xs, tc.rs); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Empty() = %v, want %v", got, tc.want)
			}
		})
	}
}
