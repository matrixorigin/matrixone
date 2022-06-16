// Copyright 2021 Matrix Origin
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
package date_add

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestDateAdd(t *testing.T) {
	testCases := []struct {
		name    string
		args1   []types.Date
		args2   []int64
		args3   []int64
		want    []types.Date
		success bool
	}{
		{
			args1:   []types.Date{types.FromCalendar(2021, 8, 13)},
			args2:   []int64{1},
			args3:   []int64{int64(types.Day)},
			want:    []types.Date{types.FromCalendar(2021, 8, 14)},
			success: true,
		},
		{
			args1:   []types.Date{types.FromCalendar(2021, 1, 31)},
			args2:   []int64{1},
			args3:   []int64{int64(types.Month)},
			want:    []types.Date{types.FromCalendar(2021, 2, 28)},
			success: true,
		},
		{
			args1:   []types.Date{types.FromCalendar(9999, 12, 31)},
			args2:   []int64{1},
			args3:   []int64{int64(types.Day)},
			want:    []types.Date{types.FromCalendar(1, 1, 1)},
			success: false,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			got := make([]types.Date, len(c.args1))
			nu := &nulls.Nulls{}
			require.Equal(t, c.want, dateAdd(c.args1, c.args2, c.args3, nu, got))
			require.Equal(t, c.success, !nulls.Contains(nu, 0))
		})
	}

}

func TestDatetimeAdd(t *testing.T) {
	testCases := []struct {
		name    string
		args1   []types.Datetime
		args2   []int64
		args3   []int64
		want    []types.Datetime
		success bool
	}{
		{
			args1:   []types.Datetime{types.FromClock(2020, 1, 1, 1, 1, 1, 1)},
			args2:   []int64{1},
			args3:   []int64{int64(types.MicroSecond)},
			want:    []types.Datetime{types.FromClock(2020, 1, 1, 1, 1, 1, 2)},
			success: true,
		},
		{
			args1:   []types.Datetime{types.FromClock(2020, 1, 1, 1, 1, 1, 1)},
			args2:   []int64{1},
			args3:   []int64{int64(types.Second)},
			want:    []types.Datetime{types.FromClock(2020, 1, 1, 1, 1, 2, 1)},
			success: true,
		},
		{
			args1:   []types.Datetime{types.FromClock(9999, 1, 1, 1, 1, 1, 1)},
			args2:   []int64{1},
			args3:   []int64{int64(types.Year)},
			want:    []types.Datetime{types.FromClock(1, 1, 1, 0, 0, 0, 0)},
			success: false,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			got := make([]types.Datetime, len(c.args1))
			nu := &nulls.Nulls{}
			require.Equal(t, c.want, datetimeAdd(c.args1, c.args2, c.args3, nu, got))
			require.Equal(t, c.success, !nulls.Contains(nu, 0))
		})
	}

}

func TestDateStringAdd(t *testing.T) {
	testCases := []struct {
		name    string
		args1   *types.Bytes
		args2   []int64
		args3   []int64
		want    *types.Bytes
		contain bool
	}{
		{
			args1:   &types.Bytes{Data: []byte("2018-01-01"), Offsets: []uint32{0}, Lengths: []uint32{10}},
			args2:   []int64{1},
			args3:   []int64{int64(types.Day)},
			want:    &types.Bytes{Data: []byte("2018-01-02"), Offsets: []uint32{0}, Lengths: []uint32{10}},
			contain: false,
		},
		{
			args1:   &types.Bytes{Data: []byte("2018-01-01"), Offsets: []uint32{0}, Lengths: []uint32{10}},
			args2:   []int64{1},
			args3:   []int64{int64(types.Second)},
			want:    &types.Bytes{Data: []byte("2018-01-01 00:00:01"), Offsets: []uint32{0}, Lengths: []uint32{19}},
			contain: false,
		},
		{
			args1:   &types.Bytes{Data: []byte("2018-01-01 00:00:01"), Offsets: []uint32{0}, Lengths: []uint32{19}},
			args2:   []int64{1},
			args3:   []int64{int64(types.Second)},
			want:    &types.Bytes{Data: []byte("2018-01-01 00:00:02"), Offsets: []uint32{0}, Lengths: []uint32{19}},
			contain: false,
		},
		{
			args1:   &types.Bytes{Data: []byte("xxxx"), Offsets: []uint32{0}, Lengths: []uint32{4}},
			args2:   []int64{1},
			args3:   []int64{int64(types.Second)},
			want:    &types.Bytes{Data: []byte(""), Offsets: []uint32{0}, Lengths: []uint32{0}},
			contain: true,
		},
		{
			args1:   &types.Bytes{Data: []byte("xxxx2018-01-01 00:00:012018-01-01"), Offsets: []uint32{0, 4, 23}, Lengths: []uint32{4, 19, 10}},
			args2:   []int64{1},
			args3:   []int64{int64(types.Day)},
			want:    &types.Bytes{Data: []byte("2018-01-02 00:00:012018-01-02"), Offsets: []uint32{0, 0, 19}, Lengths: []uint32{0, 19, 10}},
			contain: true,
		},
		{
			args1:   &types.Bytes{Data: []byte("9999-01-01"), Offsets: []uint32{0}, Lengths: []uint32{10}},
			args2:   []int64{1},
			args3:   []int64{int64(types.Year)},
			want:    &types.Bytes{Data: []byte(""), Offsets: []uint32{0}, Lengths: []uint32{0}},
			contain: true,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			got := &types.Bytes{
				Data:    make([]byte, 0),
				Offsets: make([]uint32, 0),
				Lengths: make([]uint32, 0),
			}
			nu := &nulls.Nulls{}
			require.Equal(t, c.want, dateStringAdd(c.args1, c.args2, c.args3, nu, got))
			require.Equal(t, c.contain, nulls.Contains(nu, 0))
		})
	}

}
