// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package anyvalue

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSerializationForAnyValueRing1(t *testing.T) {
	tests := []struct {
		info string
		ring *AnyVRing1[int64]
	}{
		{
			info: "test1",
			ring: &AnyVRing1[int64]{
				Typ: types.Type{Oid: types.T_int64},
				Vs:  []int64{1, 2, 0},
				Ns:  []int64{0, 0, 2},
				Set: []bool{true, true, false},
			},
		},
		{
			info: "test2",
			ring: &AnyVRing1[int64]{
				Typ: types.Type{Oid: types.T_int64},
				Vs:  []int64{-1, -2, -3, -5},
				Ns:  []int64{3, 1, 2, 4},
				Set: []bool{false, false, false},
			},
		},
	}
	for _, tt := range tests {
		b := make([]byte, 0, 10000)
		buf := bytes.NewBuffer(b)
		t.Run(tt.info, func(t *testing.T) {
			EncodeAnyValueRing1[int64](tt.ring, buf)
			finalRing, _, _ := DecodeAnyValueRing1[int64](buf.Bytes(), types.Type{Oid: types.T_int64})
			require.Equal(t, tt.ring.Vs, finalRing.Vs)
			require.Equal(t, tt.ring.Ns, finalRing.Ns)
			require.Equal(t, tt.ring.Set, finalRing.Set)
		})
	}
}

func TestSerializationForAnyValueRing2(t *testing.T) {
	tests := []struct {
		info string
		ring *AnyVRing2
	}{
		{
			info: "test1",
			ring: &AnyVRing2{
				Typ: types.Type{Oid: types.T_varchar},
				Vs:  [][]byte{[]byte("abc"), []byte("a%_123"), []byte(nil)},
				Ns:  []int64{0, 0, 2},
				Set: []bool{true, true, false},
			},
		},
	}
	for _, tt := range tests {
		b := make([]byte, 0, 10000)
		buf := bytes.NewBuffer(b)
		t.Run(tt.info, func(t *testing.T) {
			EncodeAnyRing2(tt.ring, buf)
			finalRing, _, _ := DecodeAnyRing2(buf.Bytes())
			require.Equal(t, tt.ring.Vs, finalRing.Vs)
			require.Equal(t, tt.ring.Ns, finalRing.Ns)
			require.Equal(t, tt.ring.Set, finalRing.Set)
		})
	}
}

func TestAnyValueRing1(t *testing.T) {
	contains := func(item int32, list []int32) bool {
		for i := range list {
			if item == list[i] {
				return true
			}
		}
		return false
	}

	ring1 := &AnyVRing1[int32]{
		Typ: types.Type{Oid: types.T_int32},
		Vs:  []int32{1, 2, 3},
		Ns:  []int64{0, 0, 0},
		Set: []bool{true, true, false},
	}
	ring2 := &AnyVRing1[int32]{
		Typ: types.Type{Oid: types.T_int32},
		Vs:  []int32{4, 5, 6},
		Ns:  []int64{0, 0, 0},
		Set: []bool{true, false, true},
	}
	ring1.Add(ring2, 0, 0)
	ring1.Add(ring2, 1, 1)
	ring1.Add(ring2, 2, 2)
	ring1.Mul(ring2, 1, 1, 1)
	ring1.Mul(ring2, 2, 2, 2)
	ring1.Mul(ring2, 0, 0, 3)
	vec := ring1.Eval([]int64{1, 1, 1})
	require.True(t, contains(vec.Col.([]int32)[0], []int32{1, 4}))
	require.True(t, contains(vec.Col.([]int32)[1], []int32{2, 5}))
	require.True(t, contains(vec.Col.([]int32)[2], []int32{6}))

}
