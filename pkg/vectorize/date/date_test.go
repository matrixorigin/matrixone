package date

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"testing"
)

func TestDateToDate(t *testing.T) {
	testCases := []struct {
		name string
		args []types.Date
		want []types.Date
	}{
		{
			name: "normal test cases",
			args: []types.Date{types.FromCalendar(2022, 3, 30)},
			want: []types.Date{types.FromCalendar(2022, 3, 30)},
		},
		{
			name: "unnormal test case",
			args: []types.Date{types.FromCalendar(2022, 2, 30)},
			want: []types.Date{types.FromCalendar(2022, 3, 02)},
		},
	}

	for _, v := range testCases {
		reply := make([]types.Date, len(v.args))
		actual := DateToDate(v.args, reply)

		if !isEqual(actual, v.want) {
			t.Errorf("expect the %v, but got %v", v.want, actual)
		}
	}
}
func TestDateTimeToDate(t *testing.T) {
	testCases := []struct {
		name string
		args []types.Datetime
		want []types.Date
	}{
		{
			args: []types.Datetime{types.FromClock(2022, 3, 30, 20, 20, 20, 20)},
			want: []types.Date{types.FromCalendar(2022, 3, 30)},
		},
	}

	for _, v := range testCases {
		reply := make([]types.Date, len(v.args))
		actual := DateTimeToDate(v.args, reply)

		if !isEqual(actual, v.want) {
			t.Errorf("expect the %v, but got %v", v.want, actual)
		}
	}
}

func isEqual(a, b []types.Date) bool {

	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
