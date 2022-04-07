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

package startsWith

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

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

func TestStartsWith(t *testing.T) {
	tests := []struct {
		name string
		lv   *types.Bytes
		rv   *types.Bytes
		want []uint8
		rs   []uint8
	}{
		{
			name: "English Match",
			lv:   MakeBytes([]string{"Hello", "World", "Hello", "world"}),
			rv:   MakeBytes([]string{"He", "Wor", "Hell", "world"}),
			rs:   make([]uint8, 4),
			want: []uint8{1, 1, 1, 1},
		},
		{
			name: "English Mismatch",
			lv:   MakeBytes([]string{"Hello", "World", "Hello", "world"}),
			rv:   MakeBytes([]string{"Ho", "wor", "Helloo", "abc"}),
			rs:   make([]uint8, 4),
			want: []uint8{0, 0, 0, 0},
		},
		{
			name: "Chinese Match",
			lv:   MakeBytes([]string{"你好世界", "世界你好", "你好 世界", "你好，世界"}),
			rv:   MakeBytes([]string{"你好", "世", "你好 ", "你好，世界"}),
			rs:   make([]uint8, 4),
			want: []uint8{1, 1, 1, 1},
		},
		{
			name: "Chinese Mismatch",
			lv:   MakeBytes([]string{"你好世界", "世界你好", "你好 世界", "你好，世界"}),
			rv:   MakeBytes([]string{"世界", "世 界", "你好 世界 ", "你好,世界"}),
			rs:   make([]uint8, 4),
			want: []uint8{0, 0, 0, 0},
		},
		{
			name: "Chinese + English",
			lv:   MakeBytes([]string{"你好World", "Hello世界", "你好World", "Hello世界"}),
			rv:   MakeBytes([]string{"你好Wor", "Hello世界", "你好world", "Hello界世"}),
			rs:   make([]uint8, 4),
			want: []uint8{1, 1, 0, 0},
		},
		{
			name: "Special Match",
			lv:   MakeBytes([]string{"Hello", "  ", " 你好", ""}),
			rv:   MakeBytes([]string{"", " ", " 你", ""}),
			rs:   make([]uint8, 4),
			want: []uint8{1, 1, 1, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StartsWith(tt.lv, tt.rv, tt.rs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StartsWith() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStartsWithRightConst(t *testing.T) {
	tests := []struct {
		name string
		lv   *types.Bytes
		rv   *types.Bytes
		want []uint8
		rs   []uint8
	}{
		{
			name: "Right Const",
			lv:   MakeBytes([]string{"H", "He", "Hello", "world"}),
			rv:   MakeBytes([]string{"He"}),
			rs:   make([]uint8, 4),
			want: []uint8{0, 1, 1, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StartsWithRightConst(tt.lv, tt.rv, tt.rs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StartsWithRightConst() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStartsWithLeftConst(t *testing.T) {
	tests := []struct {
		name string
		lv   *types.Bytes
		rv   *types.Bytes
		want []uint8
		rs   []uint8
	}{
		{
			name: "Left Const",
			lv:   MakeBytes([]string{"Hello"}),
			rv:   MakeBytes([]string{"He", "Hello", "", "Helloo"}),
			rs:   make([]uint8, 4),
			want: []uint8{1, 1, 1, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StartsWithLeftConst(tt.lv, tt.rv, tt.rs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StartsWithLeftConst() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStartsWithAllConst(t *testing.T) {
	tests := []struct {
		name string
		lv   *types.Bytes
		rv   *types.Bytes
		want []uint8
		rs   []uint8
	}{
		{
			name: "All Const",
			lv:   MakeBytes([]string{"Hello"}),
			rv:   MakeBytes([]string{"He"}),
			rs:   make([]uint8, 1),
			want: []uint8{1},
		},
		{
			name: "All Const2",
			lv:   MakeBytes([]string{"Hello"}),
			rv:   MakeBytes([]string{"World"}),
			rs:   make([]uint8, 1),
			want: []uint8{0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StartsWithAllConst(tt.lv, tt.rv, tt.rs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StartsWithAllConst() = %v, want %v", got, tt.want)
			}
		})
	}
}
