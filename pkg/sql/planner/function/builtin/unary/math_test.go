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

package unary

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorize/momath"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

var proc *process.Process

func init() {
	proc = testutil.NewProc()
}

func TestLn(t *testing.T) {
	as := []float64{1, math.Exp(0), math.Exp(1), math.Exp(10), math.Exp(100), math.Exp(99), math.Exp(-1)}
	av := testutil.MakeFloat64Vector(as, nil)

	cv, err := Ln([]*vector.Vector{av}, proc)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[float64](cv)
	require.Equal(t, []float64{0.0, 0.0, 1.0, 10.0, 100.0, 99.0, -1}, cols)
}

func TestExP(t *testing.T) {
	as := []float64{-1, 0, 1, 2, 10, 100}
	av := testutil.MakeFloat64Vector(as, nil)

	cv, err := Exp([]*vector.Vector{av}, proc)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[float64](cv)
	require.Equal(t, []float64{math.Exp(-1), math.Exp(0), math.Exp(1), math.Exp(2), math.Exp(10), math.Exp(100)}, cols)
}

func TestSin(t *testing.T) {
	as := []float64{-math.Pi / 2, 0, math.Pi / 2}
	av := testutil.MakeFloat64Vector(as, nil)

	cv, err := Sin([]*vector.Vector{av}, proc)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[float64](cv)
	require.Equal(t, []float64{-1, 0, 1}, cols)
}

func TestCos(t *testing.T) {
	as := []float64{-math.Pi, 0, math.Pi}
	av := testutil.MakeFloat64Vector(as, nil)

	cv, err := Cos([]*vector.Vector{av}, proc)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[float64](cv)
	require.Equal(t, []float64{-1, 1, -1}, cols)
}

func TestTan(t *testing.T) {
	as := []float64{0}
	av := testutil.MakeFloat64Vector(as, nil)

	cv, err := Tan([]*vector.Vector{av}, proc)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[float64](cv)
	require.Equal(t, []float64{0}, cols)
}

func TestSinh(t *testing.T) {
	as := []float64{0}
	av := testutil.MakeFloat64Vector(as, nil)

	cv, err := Sinh([]*vector.Vector{av}, proc)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[float64](cv)
	require.Equal(t, []float64{0}, cols)
}

func TestAcos(t *testing.T) {
	as := []float64{1}
	av := testutil.MakeFloat64Vector(as, nil)

	cv, err := Acos([]*vector.Vector{av}, proc)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[float64](cv)
	require.Equal(t, []float64{0}, cols)
}

func TestAtan(t *testing.T) {
	as := []float64{0}
	av := testutil.MakeFloat64Vector(as, nil)

	cv, err := Atan([]*vector.Vector{av}, proc)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[float64](cv)
	require.Equal(t, []float64{0}, cols)
}

func TestAtanWithTwoArg(t *testing.T) {
	firstCol := []float64{-1, 1, 1, 1, 1.0, 1.0}
	secondCol := []float64{1, 0, -1, 1, -1.0, 1.0}
	resultCol := make([]float64, 6)
	firstVec := testutil.MakeFloat64Vector(firstCol, nil)
	secondVec := testutil.MakeFloat64Vector(secondCol, nil)
	ovec := testutil.MakeFloat64Vector(resultCol, nil)
	err := momath.AtanWithTwoArg(firstVec, secondVec, ovec)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[float64](ovec)
	require.Equal(t, []float64{-0.7853981633974483, 0, -0.7853981633974483, 0.7853981633974483, -0.7853981633974483, 0.7853981633974483}, cols)

}
