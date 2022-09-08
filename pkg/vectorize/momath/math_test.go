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

package momath

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"math"
	"testing"
)

func TestLn(t *testing.T){
	as := []float64{1, math.Exp(0), math.Exp(1), math.Exp(10), math.Exp(100), math.Exp(99)}
	cs := make([]float64, 6)

	av := testutil.MakeFloat64Vector(as, nil)
	cv := testutil.MakeFloat64Vector(cs, nil)

	err := Ln(av, cv)
	if err != nil{
		panic(err)
	}
	cols := vector.MustTCols[float64](cv)
	require.Equal(t, []float64{0.0, 0.0, 1.0, 10.0, 100.0, 99.0}, cols)
}

func TestExP(t *testing.T){
	as := []float64{-1, 0, 1, 2, 10, 100}
	cs := make([]float64, 6)

	av := testutil.MakeFloat64Vector(as, nil)
	cv := testutil.MakeFloat64Vector(cs, nil)

	err := Exp(av, cv)
	if err != nil{
		panic(err)
	}
	cols := vector.MustTCols[float64](cv)
	require.Equal(t, []float64{math.Exp(-1), math.Exp(0), math.Exp(1), math.Exp(2), math.Exp(10), math.Exp(100)}, cols)
}

func TestSin(t *testing.T){
	as := []float64{-math.Pi / 2, 0, math.Pi / 2}
	cs := make([]float64, 3)

	av := testutil.MakeFloat64Vector(as, nil)
	cv := testutil.MakeFloat64Vector(cs, nil)

	err := Sin(av, cv)
	if err != nil{
		panic(err)
	}
	cols := vector.MustTCols[float64](cv)
	require.Equal(t, []float64{-1, 0, 1}, cols)
}

func TestCos(t *testing.T){
	as := []float64{-math.Pi, 0, math.Pi}
	cs := make([]float64, 3)

	av := testutil.MakeFloat64Vector(as, nil)
	cv := testutil.MakeFloat64Vector(cs, nil)

	err := Cos(av, cv)
	if err != nil{
		panic(err)
	}
	cols := vector.MustTCols[float64](cv)
	require.Equal(t, []float64{-1, 1, -1}, cols)
}

func TestTan(t *testing.T){
	as := []float64{0}
	cs := make([]float64, 1)

	av := testutil.MakeFloat64Vector(as, nil)
	cv := testutil.MakeFloat64Vector(cs, nil)

	err := Tan(av, cv)
	if err != nil{
		panic(err)
	}
	cols := vector.MustTCols[float64](cv)
	require.Equal(t, []float64{0}, cols)
}


