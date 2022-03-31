package date

import "github.com/matrixorigin/matrixone/pkg/container/types"

var (
	dateToDate     func([]types.Date, []types.Date) []types.Date
	datetimeToDate func([]types.Datetime, []types.Date) []types.Date
)

func init() {
	dateToDate = dateToDatePure
	datetimeToDate = datetimeToDatePure
}

func DateToDate(xs []types.Date, rs []types.Date) []types.Date {
	return dateToDate(xs, rs)
}
func DateTimeToDate(xs []types.Datetime, rs []types.Date) []types.Date {
	return datetimeToDate(xs, rs)
}

func dateToDatePure(xs []types.Date, rs []types.Date) []types.Date {
	copy(rs, xs)
	return rs
}

func datetimeToDatePure(xs []types.Datetime, rs []types.Date) []types.Date {
	for i, x := range xs {
		rs[i] = x.ToDate()
	}
	return rs
}
