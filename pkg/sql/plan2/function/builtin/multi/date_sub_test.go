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
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDateSub(t *testing.T) {
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want string
	}{
		{
			name: "TEST01",
			vecs: makeDateSubVectors("2022-01-02", true, 1, types.Day),
			proc: process.New(mheap.New(nil)),
			want: "2022-01-01",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DateSub(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.want, date.Col.([]types.Date)[0].String())
		})
	}

}

func TestDatetimeSub(t *testing.T) {
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want string
	}{
		{
			name: "TEST01",
			vecs: makeDatetimeSubVectors("2022-01-02 00:00:00", true, 1, types.Day),
			proc: process.New(mheap.New(nil)),
			want: "2022-01-01 00:00:00",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DatetimeSub(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.want, date.Col.([]types.Datetime)[0].String())
		})
	}

}

func TestDateStringSub(t *testing.T) {
	cases := []struct {
		name    string
		vecs    []*vector.Vector
		proc    *process.Process
		want    string
		contain bool
	}{
		{
			name:    "TEST01",
			vecs:    makeDateStringSubVectors("2022-01-02", true, 1, types.Day),
			proc:    process.New(mheap.New(nil)),
			want:    "2022-01-01 00:00:00",
			contain: false,
		},
		{
			name:    "TEST02",
			vecs:    makeDateStringSubVectors("2022-01-02 00:00:00", true, 1, types.Day),
			proc:    process.New(mheap.New(nil)),
			want:    "2022-01-01 00:00:00",
			contain: false,
		},
		{
			name:    "TEST03",
			vecs:    makeDateStringSubVectors("2022-01-01", true, 1, types.Second),
			proc:    process.New(mheap.New(nil)),
			want:    "2021-12-31 23:59:59",
			contain: false,
		},
		{
			name:    "TEST04",
			vecs:    makeDateStringSubVectors("xxxx", true, 1, types.Second),
			proc:    process.New(mheap.New(nil)),
			want:    "0001-01-01 00:00:00",
			contain: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DateStringSub(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.want, date.Col.([]types.Datetime)[0].String())
			require.Equal(t, c.contain, nulls.Contains(date.Nsp, 0))
		})
	}

}

func makeDateSubVectors(str string, isConst bool, num int64, unit types.IntervalType) []*vector.Vector {
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

func makeDatetimeSubVectors(str string, isConst bool, num int64, unit types.IntervalType) []*vector.Vector {
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

func makeDateStringSubVectors(str string, isConst bool, num int64, unit types.IntervalType) []*vector.Vector {
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
