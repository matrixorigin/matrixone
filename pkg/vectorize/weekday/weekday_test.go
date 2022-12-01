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

package weekday

import (
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func ParseDateCast(s string) types.Date {
	date, _ := types.ParseDateCast(s)
	return date
}

func parseDatetime(s string) types.Datetime {
	datetime, _ := types.ParseDatetime(s, 6)
	return datetime
}

func TestDateToWeekday(t *testing.T) {
	type args struct {
		xs []types.Date
		rs []int64
	}
	tests := []struct {
		name string
		args args
		want []int64
	}{
		{
			name: "normal date test",
			args: args{
				xs: []types.Date{
					ParseDateCast("2022-01-01"),
					ParseDateCast("2022-01-02"),
					ParseDateCast("2022-01-03"),
					ParseDateCast("2022-01-04"),
					ParseDateCast("2022-01-05"),
					ParseDateCast("2022-01-06"),
					ParseDateCast("2022-01-07"),
					ParseDateCast("2022-01-08"),
				},
				rs: make([]int64, 8),
			},
			want: []int64{5, 6, 0, 1, 2, 3, 4, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DateToWeekday(tt.args.xs, tt.args.rs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DateToWeekday() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDatetimeToWeekday(t *testing.T) {
	type args struct {
		xs []types.Datetime
		rs []int64
	}
	tests := []struct {
		name string
		args args
		want []int64
	}{
		{
			name: "normal datetime test",
			args: args{
				xs: []types.Datetime{
					parseDatetime("2022-01-01 22:23:00"),
					parseDatetime("2022-01-02 22:23:00"),
					parseDatetime("2022-01-03 22:23:00"),
					parseDatetime("2022-01-04 22:23:00"),
					parseDatetime("2022-01-05 22:23:00"),
					parseDatetime("2022-01-06 22:23:00"),
					parseDatetime("2022-01-07 22:23:00"),
					parseDatetime("2022-01-08 22:23:00"),
				},
				rs: make([]int64, 8),
			},
			want: []int64{5, 6, 0, 1, 2, 3, 4, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DatetimeToWeekday(tt.args.xs, tt.args.rs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DatetimeToWeekday() = %v, want %v", got, tt.want)
			}
		})
	}
}
