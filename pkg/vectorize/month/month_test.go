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

package month

import (
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func ParseDateCast(s string) types.Date {
	d, _ := types.ParseDateCast(s)
	return d
}

func parseDatetime(s string) types.Datetime {
	dt, _ := types.ParseDatetime(s, 6)
	return dt
}

func TestDateToMonth(t *testing.T) {
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
					ParseDateCast("2022-01-01"),
					ParseDateCast("2022-02-02"),
					ParseDateCast("2022-03-03"),
					ParseDateCast("2022-04-01"),
					ParseDateCast("2022-05-01"),
					ParseDateCast("2022-06-01"),
					ParseDateCast("2022-07-01"),
					ParseDateCast("2022-08-01"),
					ParseDateCast("2022-09-01"),
					ParseDateCast("2022-10-01"),
					ParseDateCast("2022-11-01"),
					ParseDateCast("2022-12-01"),
				},
				rs: make([]uint8, 12),
			},
			want: []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DateToMonth(tt.args.xs, tt.args.rs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DateToMonth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDatetimeToMonth(t *testing.T) {
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
					parseDatetime("2022-02-01 00:00:00"),
					parseDatetime("2022-03-01 00:00:00"),
					parseDatetime("2022-04-01 00:00:00"),
					parseDatetime("2022-05-01 00:00:00"),
					parseDatetime("2022-06-01 00:00:00"),
					parseDatetime("2022-07-01 00:00:00"),
					parseDatetime("2022-08-01 00:00:00"),
					parseDatetime("2022-09-01 00:00:00"),
					parseDatetime("2022-10-01 00:00:00"),
					parseDatetime("2022-11-01 00:00:00"),
					parseDatetime("2022-12-01 00:00:00"),
				},
				rs: make([]uint8, 12),
			},
			want: []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DatetimeToMonth(tt.args.xs, tt.args.rs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DatetimeToMonth() = %v, want %v", got, tt.want)
			}
		})
	}
}
