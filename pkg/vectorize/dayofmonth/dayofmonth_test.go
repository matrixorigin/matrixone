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

package dayofmonth

import (
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func parseDate(s string) types.Date {
	d, _ := types.ParseDate(s)
	return d
}

func parseDatetime(s string) types.Datetime {
	dt, _ := types.ParseDatetime(s)
	return dt
}

func TestDateDayOfMonth(t *testing.T) {
	type args struct {
		xs []types.Date
		rs []uint8
	}
	tests := []struct {
		name string
		args args
		want []uint8
	}{
		{
			name: "normal date test",
			args: args{
				xs: []types.Date{
					parseDate("2022-01-01"),
					parseDate("2022-01-02"),
					parseDate("2022-01-03"),
					parseDate("2022-02-28"),
					parseDate("2022-10-15"),
				},
				rs: make([]uint8, 5),
			},
			want: []uint8{1, 2, 3, 28, 15},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DateDayOfMonth(tt.args.xs, tt.args.rs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DateDayOfMonth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDatetimeDayofMonth(t *testing.T) {
	type args struct {
		xs []types.Datetime
		rs []uint8
	}
	tests := []struct {
		name string
		args args
		want []uint8
	}{
		{
			name: "normal date test",
			args: args{
				xs: []types.Datetime{
					parseDatetime("2022-01-01 00:00:00"),
					parseDatetime("2022-01-02 00:00:00"),
					parseDatetime("2022-01-03 00:00:00"),
					parseDatetime("2022-02-28 00:00:00"),
					parseDatetime("2022-10-15 00:00:00"),
				},
				rs: make([]uint8, 5),
			},
			want: []uint8{1, 2, 3, 28, 15},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DatetimeDayOfMonth(tt.args.xs, tt.args.rs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DatetimeDayOfMonth() = %v, want %v", got, tt.want)
			}
		})
	}
}
