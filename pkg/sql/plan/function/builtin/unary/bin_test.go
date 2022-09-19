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

package unary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBinUint64(t *testing.T) {
	procs := testutil.NewProc()
	vecs := make([]*vector.Vector, 1)
	vecs[0] = testutil.MakeUint64Vector([]uint64{12, 99, 100, 255}, nil)
	expected := []string{"1100", "1100011", "1100100", "11111111"}
	
	result, err := Bin[uint64](vecs, procs)
	if err != nil {
		panic(err)
	}	
	cols := vector.MustTCols[string](result)
	require.Equal(t, expected, cols)	
}


func TestBinInt64(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeInt64Vector([]int64{12, 99, 100, 255}, nil)}
	expected := []string{"1100", "1100011", "1100100", "11111111"}
	
	result, err := Bin[int64](vecs, procs)
	if err != nil {
		panic(err)
	}	
	cols := vector.MustTCols[string](result)
	require.Equal(t, expected, cols)	
}