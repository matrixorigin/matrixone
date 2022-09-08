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
	"reflect"
	"testing"
)

func TestEmpty(t *testing.T) {
	tt := []struct {
		name string
		xs   []string
		rs   []uint8
		want []uint8
	}{
		{
			name: "Not Empty",
			xs:   []string{"Hello", "World", " ", "\t", "\n", "\r"},
			rs:   make([]uint8, 6),
			want: []uint8{0, 0, 0, 0, 0, 0},
		},
		{
			name: "Empty",
			xs:   []string{""},
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
