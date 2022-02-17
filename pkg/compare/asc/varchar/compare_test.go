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

package varchar

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNew(t *testing.T) {
	require.Equal(t, &compare{vs: make([]*vector.Vector, 2)}, New())
}

func TestCompare_Vector(t *testing.T) {
	c := New()
	c.vs[0] = vector.New(types.Type{Oid: types.T(types.T_char)})
	require.Equal(t, vector.New(types.Type{Oid: types.T(types.T_char)}), c.Vector())
}

func TestCompare_Set(t *testing.T) {
	c := New()
	vector := vector.New(types.Type{Oid: types.T(types.T_char)})
	c.Set(1, vector)
	require.Equal(t, vector, c.vs[1])
}

func TestCompare_Compare(t *testing.T) {
	c := New()
	c.vs[0] = vector.New(types.Type{Oid: types.T(types.T_char)})
	c.vs[1] = vector.New(types.Type{Oid: types.T(types.T_char)})
	c.vs[0].Col = &types.Bytes{
		Data:    []byte("helloGutkonichiwanihao"),
		Offsets: []uint32{0, 5, 8, 17},
		Lengths: []uint32{5, 3, 9, 5},
	}
	c.vs[0].Data = c.vs[0].Col.(*types.Bytes).Data
	c.vs[1].Col = &types.Bytes{
		Data:    []byte("helloGutkonichiwanihao"),
		Offsets: []uint32{0, 5, 8, 17},
		Lengths: []uint32{5, 3, 9, 5},
	}
	c.vs[1].Data = c.vs[1].Col.(*types.Bytes).Data
	result := c.Compare(0, 1, 0, 0)
	require.Equal(t, 0, result)
	c.vs[1].Col = &types.Bytes{
		Data:    []byte("heyheyheyhey"),
		Offsets: []uint32{0, 3, 6, 9},
		Lengths: []uint32{3, 3, 3, 3},
	}
	c.vs[1].Data = c.vs[1].Col.(*types.Bytes).Data
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, -1, result)
	c.vs[1].Col = &types.Bytes{
		Data:    []byte("aaaa"),
		Offsets: []uint32{0, 1, 2, 3},
		Lengths: []uint32{1, 1, 1, 1},
	}
	c.vs[1].Data = c.vs[1].Col.(*types.Bytes).Data
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, 1, result)

}
