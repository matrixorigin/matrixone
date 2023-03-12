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

func TestBinUint8(t *testing.T) {
	procs := testutil.NewProc()

	as := []uint8{2, 4, 6, 8, 16, 32, 64, 128}
	vecs := make([]*vector.Vector, 1)
	vecs[0] = testutil.MakeUint8Vector(as, nil)

	resultV, err := Bin[uint8](vecs, procs)
	vecLen := resultV.Length()
	if err != nil {
		panic(err)
	}
	tempC := make([]string, 8)

	for i := 0; i < vecLen; i++ {
		tempC[i] = resultV.GetStringAt(i)
	}
	require.Equal(t, []string{"10", "100", "110", "1000", "10000", "100000", "1000000", "10000000"}, tempC)
}

func TestBinUint16(t *testing.T) {
	procs := testutil.NewProc()

	as := []uint16{2, 4, 6, 8, 16, 32, 64, 128}
	vecs := make([]*vector.Vector, 1)
	vecs[0] = testutil.MakeUint16Vector(as, nil)

	resultV, err := Bin[uint16](vecs, procs)
	vecLen := resultV.Length()
	if err != nil {
		panic(err)
	}
	tempC := make([]string, 8)

	for i := 0; i < vecLen; i++ {
		tempC[i] = resultV.GetStringAt(i)
	}
	require.Equal(t, []string{"10", "100", "110", "1000", "10000", "100000", "1000000", "10000000"}, tempC)
}

func TestBinUint32(t *testing.T) {
	procs := testutil.NewProc()

	as := []uint32{2, 4, 6, 8, 16, 32, 64, 128}
	vecs := make([]*vector.Vector, 1)
	vecs[0] = testutil.MakeUint32Vector(as, nil)

	resultV, err := Bin[uint32](vecs, procs)
	vecLen := resultV.Length()
	if err != nil {
		panic(err)
	}
	tempC := make([]string, 8)

	for i := 0; i < vecLen; i++ {
		tempC[i] = resultV.GetStringAt(i)
	}
	require.Equal(t, []string{"10", "100", "110", "1000", "10000", "100000", "1000000", "10000000"}, tempC)
}
func TestBinUint64(t *testing.T) {
	procs := testutil.NewProc()

	as := []uint64{2, 4, 6, 8, 16, 32, 64, 128}
	vecs := make([]*vector.Vector, 1)
	vecs[0] = testutil.MakeUint64Vector(as, nil)

	resultV, err := Bin[uint64](vecs, procs)
	vecLen := resultV.Length()
	if err != nil {
		panic(err)
	}
	tempC := make([]string, 8)

	for i := 0; i < vecLen; i++ {
		tempC[i] = resultV.GetStringAt(i)
	}
	require.Equal(t, []string{"10", "100", "110", "1000", "10000", "100000", "1000000", "10000000"}, tempC)
}

func TestBinInt8(t *testing.T) {
	procs := testutil.NewProc()

	as := []int8{2, 4, 6, 8, 16, 32, 64}
	vecs := make([]*vector.Vector, 1)
	vecs[0] = testutil.MakeInt8Vector(as, nil)

	resultV, err := Bin[int8](vecs, procs)
	vecLen := resultV.Length()
	if err != nil {
		panic(err)
	}
	tempC := make([]string, 7)

	for i := 0; i < vecLen; i++ {
		tempC[i] = resultV.GetStringAt(i)
	}
	require.Equal(t, []string{"10", "100", "110", "1000", "10000", "100000", "1000000"}, tempC)
}

func TestBinInt16(t *testing.T) {
	procs := testutil.NewProc()

	as := []int16{2, 4, 6, 8, 16, 32, 64, 128}
	vecs := make([]*vector.Vector, 1)
	vecs[0] = testutil.MakeInt16Vector(as, nil)

	resultV, err := Bin[int16](vecs, procs)
	vecLen := resultV.Length()
	if err != nil {
		panic(err)
	}
	tempC := make([]string, 8)

	for i := 0; i < vecLen; i++ {
		tempC[i] = resultV.GetStringAt(i)
	}
	require.Equal(t, []string{"10", "100", "110", "1000", "10000", "100000", "1000000", "10000000"}, tempC)
}

func TestBinInt32(t *testing.T) {
	procs := testutil.NewProc()

	as := []int32{2, 4, 6, 8, 16, 32, 64, 128}
	vecs := make([]*vector.Vector, 1)
	vecs[0] = testutil.MakeInt32Vector(as, nil)

	resultV, err := Bin[int32](vecs, procs)
	vecLen := resultV.Length()
	if err != nil {
		panic(err)
	}
	tempC := make([]string, 8)

	for i := 0; i < vecLen; i++ {
		tempC[i] = resultV.GetStringAt(i)
	}
	require.Equal(t, []string{"10", "100", "110", "1000", "10000", "100000", "1000000", "10000000"}, tempC)
}

func TestBinInt64(t *testing.T) {
	procs := testutil.NewProc()

	as := []int64{2, 4, 6, 8, 16, 32, 64, 128}
	vecs := make([]*vector.Vector, 1)
	vecs[0] = testutil.MakeInt64Vector(as, nil)

	resultV, err := Bin[int64](vecs, procs)
	vecLen := resultV.Length()
	if err != nil {
		panic(err)
	}
	tempC := make([]string, 8)

	for i := 0; i < vecLen; i++ {
		tempC[i] = resultV.GetStringAt(i)
	}
	require.Equal(t, []string{"10", "100", "110", "1000", "10000", "100000", "1000000", "10000000"}, tempC)
}

func TestBinFloat32(t *testing.T) {
	procs := testutil.NewProc()

	as := []float32{2.1111, 4.4261264, 6.1151275, 8.48484, 16.266, 32.3338787, 64.0000000, 128.26454}
	vecs := make([]*vector.Vector, 1)
	vecs[0] = testutil.MakeFloat32Vector(as, nil)

	resultV, err := BinFloat[float32](vecs, procs)
	vecLen := resultV.Length()
	if err != nil {
		panic(err)
	}
	tempC := make([]string, 8)

	for i := 0; i < vecLen; i++ {
		tempC[i] = resultV.GetStringAt(i)
	}
	require.Equal(t, []string{"10", "100", "110", "1000", "10000", "100000", "1000000", "10000000"}, tempC)
}
func TestBinFloat64(t *testing.T) {
	procs := testutil.NewProc()

	as := []float64{2.1111, 4.4261264, 6.1151275, 8.48484, 16.266, 32.3338787, 64.0000000, 128.26454}
	vecs := make([]*vector.Vector, 1)
	vecs[0] = testutil.MakeFloat64Vector(as, nil)

	resultV, err := BinFloat[float64](vecs, procs)
	vecLen := resultV.Length()
	if err != nil {
		panic(err)
	}
	tempC := make([]string, 8)

	for i := 0; i < vecLen; i++ {
		tempC[i] = resultV.GetStringAt(i)
	}
	require.Equal(t, []string{"10", "100", "110", "1000", "10000", "100000", "1000000", "10000000"}, tempC)
}

func TestBinNegativeInt(t *testing.T) {
	procs := testutil.NewProc()

	as := []int64{-2, -4, -6, -8, -16, -32, -64, -128}
	vecs := make([]*vector.Vector, 1)
	vecs[0] = testutil.MakeInt64Vector(as, nil)

	resultV, err := Bin[int64](vecs, procs)
	vecLen := resultV.Length()
	if err != nil {
		panic(err)
	}
	tempC := make([]string, 8)

	for i := 0; i < vecLen; i++ {
		tempC[i] = resultV.GetStringAt(i)
	}
	require.Equal(t, []string{"1111111111111111111111111111111111111111111111111111111111111110",
		"1111111111111111111111111111111111111111111111111111111111111100",
		"1111111111111111111111111111111111111111111111111111111111111010",
		"1111111111111111111111111111111111111111111111111111111111111000",
		"1111111111111111111111111111111111111111111111111111111111110000",
		"1111111111111111111111111111111111111111111111111111111111100000",
		"1111111111111111111111111111111111111111111111111111111111000000",
		"1111111111111111111111111111111111111111111111111111111110000000"}, tempC)
}
