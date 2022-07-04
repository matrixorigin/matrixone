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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDateAdd(t *testing.T) {
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want string
	}{
		{
			name: "TEST01",
			vecs: makeDateAddVectors("2022-01-01", true, 1, types.Day),
			proc: makeProcess(),
			want: "2022-01-02",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DateAdd(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.want, date.Col.([]types.Date)[0].String())
		})
	}

}

func TestDatetimeAdd(t *testing.T) {
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want string
	}{
		{
			name: "TEST01",
			vecs: makeDatetimeAddVectors("2022-01-01 00:00:00", true, 1, types.Day),
			proc: makeProcess(),
			want: "2022-01-02 00:00:00",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DatetimeAdd(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.want, date.Col.([]types.Datetime)[0].String())
		})
	}

}

func TestDateStringAdd(t *testing.T) {
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want string
		err  error
	}{
		{
			name: "TEST01",
			vecs: makeDateStringAddVectors("2022-01-01", true, 1, types.Day),
			proc: makeProcess(),
			want: "2022-01-02 00:00:00",
			err:  nil,
		},
		{
			name: "TEST02",
			vecs: makeDateStringAddVectors("2022-01-01 00:00:00", true, 1, types.Day),
			proc: makeProcess(),
			want: "2022-01-02 00:00:00",
			err:  nil,
		},
		{
			name: "TEST03",
			vecs: makeDateStringAddVectors("2022-01-01", true, 1, types.Second),
			proc: makeProcess(),
			want: "2022-01-01 00:00:01",
			err:  nil,
		},
		{
			name: "TEST04",
			vecs: makeDateStringAddVectors("xxxx", true, 1, types.Second),
			proc: makeProcess(),
			want: "0001-01-01 00:00:00",
			err:  types.ErrIncorrectDatetimeValue,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DateStringAdd(c.vecs, c.proc)
			require.Equal(t, c.want, date.Col.([]types.Datetime)[0].String())
			require.Equal(t, c.err, err)
		})
	}

}

func makeDateAddVectors(str string, isConst bool, num int64, unit types.IntervalType) []*vector.Vector {
	vec := make([]*vector.Vector, 3)

	date, _ := types.ParseDate(str)

	vec[0] = &vector.Vector{
		Col:     []types.Date{date},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: types.T_date},
		IsConst: isConst,
		Length:  1,
	}

	vec[1] = &vector.Vector{
		Col:     []int64{num},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: types.T_int64},
		IsConst: true,
		Length:  1,
	}

	vec[2] = &vector.Vector{
		Col:     []int64{int64(unit)},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: types.T_int64},
		IsConst: true,
		Length:  1,
	}
	return vec
}

func makeDatetimeAddVectors(str string, isConst bool, num int64, unit types.IntervalType) []*vector.Vector {
	vec := make([]*vector.Vector, 3)

	datetime, _ := types.ParseDatetime(str, 0)

	vec[0] = &vector.Vector{
		Col:     []types.Datetime{datetime},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: types.T_datetime},
		IsConst: isConst,
		Length:  1,
	}

	vec[1] = &vector.Vector{
		Col:     []int64{num},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: types.T_int64},
		IsConst: true,
		Length:  1,
	}

	vec[2] = &vector.Vector{
		Col:     []int64{int64(unit)},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: types.T_int64},
		IsConst: true,
		Length:  1,
	}
	return vec
}

func makeDateStringAddVectors(str string, isConst bool, num int64, unit types.IntervalType) []*vector.Vector {
	vec := make([]*vector.Vector, 3)

	srcBytes := &types.Bytes{
		Data:    []byte(str),
		Offsets: []uint32{0},
		Lengths: []uint32{uint32(len(str))},
	}

	vec[0] = &vector.Vector{
		Col:     srcBytes,
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: types.T_varchar, Size: 26},
		IsConst: isConst,
		Length:  1,
	}

	vec[1] = &vector.Vector{
		Col:     []int64{num},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: types.T_int64},
		IsConst: true,
		Length:  1,
	}

	vec[2] = &vector.Vector{
		Col:     []int64{int64(unit)},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: types.T_int64},
		IsConst: true,
		Length:  1,
	}
	return vec
}
