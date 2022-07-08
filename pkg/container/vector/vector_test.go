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

package vector

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/require"
)

func TestAppend(t *testing.T) {
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	m := mheap.New(gm)
	vx := New[types.Int64](types.New(types.T_int64, 0, 0, 0))
	fmt.Printf("type: %v\n", vx.Type())
	vx.Append(types.Int64(1), m)
	vx.Append(types.Int64(2), m)
	vx.Append(types.Int64(3), m)
	fmt.Printf("%v\n", vx)
	fmt.Printf("vx: %v: %v\n", vx.Col, vx.Data)
	vx.Reset()
	vx.Append(types.Int64(3), m)
	vx.Append(types.Int64(1), m)
	vx.SetLength(2)
	vx.Append(types.Int64(2), m)
	fmt.Printf("vx: %v: %v\n", vx.Col, vx.Data)
	fmt.Printf("length: %v\n", vx.Length())
	vy, err := vx.Dup(m)
	require.NoError(t, err)
	fmt.Printf("vy: %v\n", vy)
	vy.Shrink([]int64{0, 1, 2})
	err = vx.UnionOne(vy, 0, m)
	require.NoError(t, err)
	err = vx.UnionNull(vy, 0, m)
	require.NoError(t, err)
	err = vx.UnionBatch(vy, 0, 1, []uint8{1}, m)
	require.NoError(t, err)
	err = vy.Shuffle([]int64{1}, m)
	require.NoError(t, err)

	vx.Free(m)
	vy.Free(m)
	require.Equal(t, int64(0), mheap.Size(m))
}

func TestAppendStr(t *testing.T) {
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	m := mheap.New(gm)
	vx := New[types.String](types.New(types.T_char, 0, 0, 0))
	fmt.Printf("type: %v\n", vx.Type())
	vx.Append(types.String("1"), m)
	vx.Append(types.String("2"), m)
	vx.Append(types.String("3"), m)
	fmt.Printf("%v\n", vx)
	fmt.Printf("vx: %v: %v\n", vx.Col, vx.Data)
	vx.Reset()
	vx.Append(types.String("3"), m)
	vx.Append(types.String("1"), m)
	vx.SetLength(2)
	vx.Append(types.String("2"), m)
	fmt.Printf("vx: %v: %v\n", vx.Col, vx.Data)
	fmt.Printf("length: %v\n", vx.Length())
	vy, err := vx.Dup(m)
	require.NoError(t, err)
	fmt.Printf("vy: %v\n", vy)
	vy.Shrink([]int64{0, 1, 2})
	err = vx.UnionOne(vy, 0, m)
	require.NoError(t, err)
	err = vx.UnionNull(vy, 0, m)
	require.NoError(t, err)
	err = vx.UnionBatch(vy, 0, 1, []uint8{1}, m)
	require.NoError(t, err)
	err = vy.Shuffle([]int64{1}, m)
	require.NoError(t, err)

	vx.Free(m)
	vy.Free(m)
	require.Equal(t, int64(0), mheap.Size(m))
}
