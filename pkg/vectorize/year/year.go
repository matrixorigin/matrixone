package year

import "github.com/matrixorigin/matrixone/pkg/container/types"

var (
	dateToYear     func([]types.Date, []uint16) []uint16
	datetimeToYear func([]types.Datetime, []uint16) []uint16
)

func init() {
	dateToYear = dateToYearPure
	datetimeToYear = datetimeToYearPure
}

func DateToYear(xs []types.Date, rs []uint16) []uint16 {
	return dateToYear(xs, rs)
}

func dateToYearPure(xs []types.Date, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x.Year()
	}
	return rs
}

func DatetimeToYear(xs []types.Datetime, rs []uint16) []uint16 {
	return datetimeToYear(xs, rs)
}

func datetimeToYearPure(xs []types.Datetime, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x.Year()
	}
	return rs
}
