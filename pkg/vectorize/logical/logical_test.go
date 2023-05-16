// Copyright 2021 - 2022 Matrix Origin
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

package logical

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestAnd(t *testing.T) {
	as := []bool{true, false, false, true, false, true}
	bs := []bool{true, false, true, false, true, true}

	cs := make([]bool, 6)
	av := testutil.MakeBoolVector(as)
	bv := testutil.MakeBoolVector(bs)
	cv := testutil.MakeBoolVector(cs)

	err := And(av, bv, cv)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[bool](cv)
	require.Equal(t, []bool{true, false, false, false, false, true}, cols)
}

func TestAnd2(t *testing.T) {
	as := make([]bool, 10)
	bs := make([]bool, 10)
	for i := 0; i < 10; i++ {
		as[i] = true
		bs[i] = false
	}
	cs := make([]bool, 10)

	av := testutil.MakeBooleanlVector(as, []uint64{3, 7})
	bv := testutil.MakeBooleanlVector(bs, []uint64{3, 5})
	cv := testutil.MakeBoolVector(cs)
	nulls.Or(av.GetNulls(), bv.GetNulls(), cv.GetNulls())

	err := And(av, bv, cv)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[bool](cv)
	require.Equal(t, []bool{false, false, false, false, false, false, false, false, false, false}, cols)
	require.Equal(t, []uint64{3, 5}, cv.GetNulls().ToArray())
}

func TestOr(t *testing.T) {
	as := []bool{true, false, false, true, false, true}
	bs := []bool{true, false, true, false, true, true}

	cs := make([]bool, 6)
	av := testutil.MakeBoolVector(as)
	bv := testutil.MakeBoolVector(bs)
	cv := testutil.MakeBoolVector(cs)

	err := Or(av, bv, cv)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[bool](cv)
	require.Equal(t, []bool{true, false, true, true, true, true}, cols)
}

func TestOr2(t *testing.T) {
	as := make([]bool, 10)
	bs := make([]bool, 10)
	for i := 0; i < 10; i++ {
		as[i] = true
		bs[i] = false
	}
	cs := make([]bool, 10)

	av := testutil.MakeBooleanlVector(as, []uint64{3, 7})
	bv := testutil.MakeBooleanlVector(bs, []uint64{3, 5})
	cv := testutil.MakeBoolVector(cs)
	nulls.Or(av.GetNulls(), bv.GetNulls(), cv.GetNulls())

	err := Or(av, bv, cv)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[bool](cv)
	require.Equal(t, []bool{true, true, true, true, true, true, true, true, true, true}, cols)
	require.Equal(t, []uint64{3, 7}, cv.GetNulls().ToArray())
}

func TestXor(t *testing.T) {
	as := []bool{true, false, false, true, false, true}
	bs := []bool{true, false, true, false, true, true}

	cs := make([]bool, 6)
	av := testutil.MakeBoolVector(as)
	bv := testutil.MakeBoolVector(bs)
	cv := testutil.MakeBoolVector(cs)

	err := Xor(av, bv, cv)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[bool](cv)
	require.Equal(t, []bool{false, false, true, true, true, false}, cols)
}

func TestXor2(t *testing.T) {
	as := make([]bool, 10)
	bs := make([]bool, 10)
	for i := 0; i < 10; i++ {
		as[i] = true
		bs[i] = false
	}
	cs := make([]bool, 10)

	av := testutil.MakeBooleanlVector(as, []uint64{3, 7})
	bv := testutil.MakeBooleanlVector(bs, []uint64{3, 5})
	cv := testutil.MakeBoolVector(cs)
	nulls.Or(av.GetNulls(), bv.GetNulls(), cv.GetNulls())

	err := Xor(av, bv, cv)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[bool](cv)
	require.Equal(t, []bool{true, true, true, true, true, true, true, true, true, true}, cols)
	require.Equal(t, []uint64{3, 5, 7}, cv.GetNulls().ToArray())
}

func TestNot(t *testing.T) {
	as := []bool{true, false, false, true, true}

	cs := make([]bool, 5)
	av := testutil.MakeBoolVector(as)
	cv := testutil.MakeBoolVector(cs)

	err := Not(av, cv)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[bool](cv)
	require.Equal(t, []bool{false, true, true, false, false}, cols)
}

func TestNot2(t *testing.T) {
	as := []bool{true, false, false, true, true}

	cs := make([]bool, 5)
	av := testutil.MakeBooleanlVector(as, []uint64{5})
	cv := testutil.MakeBoolVector(cs)
	nulls.Or(av.GetNulls(), nil, cv.GetNulls())

	err := Not(av, cv)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[bool](cv)
	require.Equal(t, []bool{false, true, true, false, false}, cols)
	require.Equal(t, []uint64{5}, cv.GetNulls().ToArray())
}

func TestNot3(t *testing.T) {
	cs := make([]bool, 1)
	av := testutil.MakeScalarBool(true, 5)

	cv := testutil.MakeBoolVector(cs)
	nulls.Or(av.GetNulls(), nil, cv.GetNulls())

	err := Not(av, cv)
	if err != nil {
		panic(err)
	}
	cols := vector.MustFixedCol[bool](cv)
	require.Equal(t, []bool{false}, cols)
}
