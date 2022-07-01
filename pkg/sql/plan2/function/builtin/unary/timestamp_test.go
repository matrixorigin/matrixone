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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDateToTimestamp(t *testing.T) {
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want []types.Timestamp
	}{
		{
			name: "TEST01",
			vecs: makeDateToTimestampVectors("2022-01-01", true),
			proc: process.New(mheap.New(nil)),
			want: []types.Timestamp{types.FromClockUTC(2022, 1, 1, 0, 0, 0, 0)},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result, err := DateToTimestamp(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.want, result.Col.([]types.Timestamp))
		})
	}

}

func TestDatetimeToTimestamp(t *testing.T) {
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want []types.Timestamp
	}{
		{
			name: "TEST01",
			vecs: makeDatetimeToTimestampVectors("2022-01-01 00:00:00", true),
			proc: process.New(mheap.New(nil)),
			want: []types.Timestamp{types.FromClockUTC(2022, 1, 1, 0, 0, 0, 0)},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DatetimeToTimestamp(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.want, date.Col.([]types.Timestamp))
		})
	}

}

func TestDateStringAdd(t *testing.T) {
	cases := []struct {
		name    string
		vecs    []*vector.Vector
		proc    *process.Process
		want    []types.Timestamp
		contain bool
	}{
		{
			name:    "TEST01",
			vecs:    makeDateStringToTimestampVectors("2022-01-01", true),
			proc:    process.New(mheap.New(nil)),
			want:    []types.Timestamp{types.FromClockUTC(2022, 1, 1, 0, 0, 0, 0)},
			contain: false,
		},
		{
			name:    "TEST02",
			vecs:    makeDateStringToTimestampVectors("2022-01-01 00:00:00", true),
			proc:    process.New(mheap.New(nil)),
			want:    []types.Timestamp{types.FromClockUTC(2022, 1, 1, 0, 0, 0, 0)},
			contain: false,
		},
		{
			name:    "TEST03",
			vecs:    makeDateStringToTimestampVectors("xxxx", true),
			proc:    process.New(mheap.New(nil)),
			want:    []types.Timestamp{types.Timestamp(0)},
			contain: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DateStringToTimestamp(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.want, date.Col.([]types.Timestamp))
			require.Equal(t, c.contain, nulls.Contains(date.Nsp, 0))
		})
	}

}

func makeDateToTimestampVectors(str string, isConst bool) []*vector.Vector {
	vec := make([]*vector.Vector, 1)

	date, _ := types.ParseDate(str)

	vec[0] = &vector.Vector{
		Col:     []types.Date{date},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: types.T_date},
		IsConst: isConst,
		Length:  1,
	}

	return vec
}

func makeDatetimeToTimestampVectors(str string, isConst bool) []*vector.Vector {
	vec := make([]*vector.Vector, 1)

	datetime, _ := types.ParseDatetime(str, 0)

	vec[0] = &vector.Vector{
		Col:     []types.Datetime{datetime},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: types.T_datetime},
		IsConst: isConst,
		Length:  1,
	}

	return vec
}

func makeDateStringToTimestampVectors(str string, isConst bool) []*vector.Vector {
	vec := make([]*vector.Vector, 1)

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

	return vec
}
