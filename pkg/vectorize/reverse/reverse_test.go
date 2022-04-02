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
package reverse

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestReverse(t *testing.T) {
	cases := []struct {
		name string
		args *types.Bytes
		want *types.Bytes
	}{
		{
			name: "English",
			args: &types.Bytes{
				Data:    []byte("HelloWorld"),
				Lengths: []uint32{uint32(len("HelloWorld"))},
				Offsets: []uint32{0},
			},
			want: &types.Bytes{
				Data:    []byte("dlroWolleH"),
				Lengths: []uint32{uint32(len("HelloWorld"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Chinese",
			args: &types.Bytes{
				Data:    []byte("你好世界"),
				Lengths: []uint32{uint32(len("你好世界"))},
				Offsets: []uint32{0},
			},
			want: &types.Bytes{
				Data:    []byte("界世好你"),
				Lengths: []uint32{uint32(len("界世好你"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Englist + Chinese",
			args: &types.Bytes{
				Data:    []byte("Hello 世界"),
				Lengths: []uint32{uint32(len("Hello 世界"))},
				Offsets: []uint32{0},
			},
			want: &types.Bytes{
				Data:    []byte("界世 olleH"),
				Lengths: []uint32{uint32(len("界世 olleH"))},
				Offsets: []uint32{0},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := &types.Bytes{
				Data:    make([]byte, len(c.args.Data)),
				Lengths: make([]uint32, len(c.args.Lengths)),
				Offsets: make([]uint32, len(c.args.Offsets)),
			}
			got := Reverse(c.args, out)
			require.Equal(t, c.want, got)
		})
	}

}
