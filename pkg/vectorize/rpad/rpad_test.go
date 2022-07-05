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

package rpad

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// getBytes converts a string slice to a *types.Bytes
func getBytes(s ...string) *types.Bytes {
	result := &types.Bytes{}
	for i, v := range s {
		result.Offsets = append(result.Offsets, uint32(len(result.Data)))
		result.Data = append(result.Data, []byte(v)...)
		result.Lengths = append(result.Lengths, uint32(len(result.Data))-result.Offsets[i])
	}
	return result
}

func TestRpadInt(t *testing.T) {
	// test: no nulls and all args are attribute names
	isConst := []bool{false, false, false}
	sizes := []int16{3, 5, 6, 0, 1, 6, 9}
	padstrs := getBytes("", "", "", "111", "222", "333", "444")
	strs := getBytes("hello你好", "hello", "hello", "hello", "hello", "hello", "hello你好")
	expectedStrs := getBytes("hel", "hello", "", "", "h", "hello3", "hello你好44")
	oriNsps := make([]*nulls.Nulls, 3)
	for i := 0; i < 3; i++ {
		oriNsps[i] = new(nulls.Nulls)
	}
	oriNsps = append(oriNsps, new(nulls.Nulls))
	expectedNsp := new(nulls.Nulls)

	actualStrs, actualNsp, _ := Rpad(len(sizes), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)

	// test: no nulls and the 2nd and 3rd args are constant
	isConst = []bool{false, true, true}
	sizes = []int16{3}
	padstrs = getBytes("111")
	expectedStrs = getBytes("hel", "hel", "hel", "hel", "hel", "hel", "hel")
	actualStrs, actualNsp, _ = Rpad(len(strs.Lengths), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)

	// test: nulls
	/**
	Here is a test table like :
	+------+-----+-------+
	|a     |b    |c      |
	+--------------------+
	|NULL  |2    |a      |
	|hello |NULL |a      |
	|hello |2    |NULL   |
	|hello |-2   |a      |
	+------+-----+-------+
	all results of rpad(a,b,c) should be NULLs
	*/
	isConst = []bool{false, false, false}
	strs = getBytes("", "hello", "hello", "hello")
	nulls.Add(oriNsps[0], uint64(0))
	nulls.Add(oriNsps[1], uint64(1))
	nulls.Add(oriNsps[2], uint64(2))
	sizes = []int16{2, 2, 2, -2}
	padstrs = getBytes("a", "a", "", "a")

	expectedStrs = getBytes("", "", "", "")
	expectedNsp = new(nulls.Nulls)
	for i := 0; i < len(strs.Lengths); i++ {
		nulls.Add(expectedNsp, uint64(i)) // all strings are NULLs

	}
	actualStrs, actualNsp, _ = Rpad(len(sizes), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)
}

func TestRpadFloat(t *testing.T) {
	// test float64
	isConst := []bool{false, false, false}
	sizes := []float64{0.0, 0.1, -0.1, 8, 8.4, 8.6}
	padstrs := getBytes("你好", "你好", "你好", "你好", "你好", "你好")
	strs := getBytes("hello", "hello", "hello", "hello", "hello", "hello")
	oriNsps := make([]*nulls.Nulls, 3)
	for i := 0; i < 3; i++ {
		oriNsps[i] = new(nulls.Nulls)
	}
	oriNsps = append(oriNsps, new(nulls.Nulls))

	expectedStrs := getBytes("", "", "", "hello你好你", "hello你好你", "hello你好你好")
	expectedNsp := new(nulls.Nulls)
	actualStrs, actualNsp, _ := Rpad(len(sizes), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)

	// test float32
	sizes2 := []float32{0.0, 0.1, -0.1, 8, 8.4, 8.6}
	actualStrs, actualNsp, _ = Rpad(len(sizes2), strs, sizes2, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)
}

func TestRpadUint(t *testing.T) {
	// test: no nulls and all args are attribute names
	isConst := []bool{false, false, false}
	sizes := []uint32{3, 5, 6, 0, 1, 6, 9}
	padstrs := getBytes("", "", "", "111", "222", "333", "444")
	strs := getBytes("hello你好", "hello", "hello", "hello", "hello", "hello", "hello你好")
	expectedStrs := getBytes("hel", "hello", "", "", "h", "hello3", "hello你好44")
	oriNsps := make([]*nulls.Nulls, 3)
	for i := 0; i < 3; i++ {
		oriNsps[i] = new(nulls.Nulls)
	}
	oriNsps = append(oriNsps, new(nulls.Nulls))
	expectedNsp := new(nulls.Nulls)

	actualStrs, actualNsp, _ := Rpad(len(sizes), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)

	// test: no nulls and the 2nd and 3rd args are constant
	isConst = []bool{false, true, true}
	sizes = []uint32{3}
	padstrs = getBytes("111")
	expectedStrs = getBytes("hel", "hel", "hel", "hel", "hel", "hel", "hel")
	actualStrs, actualNsp, _ = Rpad(len(strs.Lengths), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)

	// test: nulls
	/**
	Here is a test table like :
	+------+-----+-------+
	|a     |b    |c      |
	+--------------------+
	|NULL  |2    |a      |
	|hello |NULL |a      |
	|hello |2    |NULL   |
	+------+-----+-------+
	all results of rpad(a,b,c) should be NULLs
	*/
	isConst = []bool{false, false, false}
	strs = getBytes("", "hello", "hello")
	nulls.Add(oriNsps[0], uint64(0))
	nulls.Add(oriNsps[1], uint64(1))
	nulls.Add(oriNsps[2], uint64(2))
	sizes = []uint32{2, 2, 2}
	padstrs = getBytes("a", "a", "")

	expectedStrs = getBytes("", "", "")
	expectedNsp = new(nulls.Nulls)
	for i := 0; i < len(strs.Lengths); i++ {
		nulls.Add(expectedNsp, uint64(i)) // all strings are NULLs

	}
	actualStrs, actualNsp, _ = Rpad(len(sizes), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)
}

func TestTypes(t *testing.T) {
	isConst := []bool{false, true, false}

	// test sizes with a non-numerical type
	sizes := getBytes("aaasdasdsada")
	padstrs := getBytes("a", "a")
	strs := getBytes("", "test")
	oriNsps := []*nulls.Nulls{new(nulls.Nulls), new(nulls.Nulls), new(nulls.Nulls)}
	// nulls.Add(oriNsps[0], 0) // the first str is NULL

	expectedNsp := new(nulls.Nulls)
	// nulls.Add(expectedNsp, 0)
	expectedStrs := getBytes("", "")

	actualStrs, actualNsp, _ := Rpad(len(strs.Lengths), strs, sizes, padstrs, isConst, oriNsps)
	require.Equal(t, expectedStrs, actualStrs)
	require.Equal(t, expectedNsp, actualNsp)

	// test padstrs with a numerical type
	isConst = []bool{false, false, false}
	sizes2 := []int64{-1, 2, 10}
	padstrs2 := []int32{8, 9, 10}
	strs2 := getBytes("test", "test", "test")
	oriNsps2 := []*nulls.Nulls{new(nulls.Nulls), new(nulls.Nulls), new(nulls.Nulls)}

	expectedNsp2 := new(nulls.Nulls)
	nulls.Add(expectedNsp2, 0)
	expectedStrs2 := getBytes("", "te", "test101010")
	actualStrs, actualNsp, _ = Rpad(len(sizes2), strs2, sizes2, padstrs2, isConst, oriNsps2)
	require.Equal(t, expectedStrs2, actualStrs)
	require.Equal(t, expectedNsp2, actualNsp)
}
