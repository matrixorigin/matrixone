package date

import "github.com/matrixorigin/matrixone/pkg/container/types"

var (
	DateToDate     func([]types.Date, []types.Date) []types.Date
	DatetimeToDate func([]types.Datetime, []types.Date) []types.Date
)

func init() {
	DateToDate = dateToDate
	DatetimeToDate = datetimeToDate
}

func dateToDate(xs []types.Date, rs []types.Date) []types.Date {
	return xs
}

func datetimeToDate(xs []types.Datetime, rs []types.Date) []types.Date {
	for i, x := range xs {
		rs[i] = x.ToDate()
	}
	return rs
}
