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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAsciiInt(t *testing.T) {
	proc := testutil.NewProc()
	v1 := testutil.MakeScalarInt32(1, 1)
	_, err := AsciiInt[int32]([]*vector.Vector{v1}, proc)
	require.NoError(t, err)
	v1.Nsp.Set(0)
	_, err = AsciiInt[int32]([]*vector.Vector{v1}, proc)
	require.NoError(t, err)
	v1 = testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	_, err = AsciiInt[int32]([]*vector.Vector{v1}, proc)
	require.NoError(t, err)
}

func TestAsciiUint(t *testing.T) {
	proc := testutil.NewProc()
	v1 := testutil.MakeScalarUint16(1, 1)
	_, err := AsciiUint[uint16]([]*vector.Vector{v1}, proc)
	require.NoError(t, err)
	v1.Nsp.Set(0)
	_, err = AsciiUint[uint16]([]*vector.Vector{v1}, proc)
	require.NoError(t, err)
	v1 = testutil.MakeUint32Vector([]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	_, err = AsciiUint[uint32]([]*vector.Vector{v1}, proc)
	require.NoError(t, err)
}

func TestAsciiString(t *testing.T) {
	proc := testutil.NewProc()
	v1 := testutil.MakeScalarVarchar("1", 1)
	_, err := AsciiString([]*vector.Vector{v1}, proc)
	require.NoError(t, err)
	v1.Nsp.Set(0)
	_, err = AsciiString([]*vector.Vector{v1}, proc)
	require.NoError(t, err)
	v1 = testutil.MakeVarcharVector([]string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, nil)
	_, err = AsciiString([]*vector.Vector{v1}, proc)
	require.NoError(t, err)
}
