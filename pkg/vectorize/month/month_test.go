package month

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
					parseDate("2022-01-01"),
					parseDate("2022-02-02"),
					parseDate("2022-03-03"),
					parseDate("2022-04-01"),
					parseDate("2022-05-01"),
					parseDate("2022-06-01"),
					parseDate("2022-07-01"),
					parseDate("2022-08-01"),
					parseDate("2022-09-01"),
					parseDate("2022-10-01"),
					parseDate("2022-11-01"),
					parseDate("2022-12-01"),
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
