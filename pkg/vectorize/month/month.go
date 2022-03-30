package month

import "github.com/matrixorigin/matrixone/pkg/container/types"

var (
	dataToMonth     func([]types.Date, []uint8) []uint8
	datetimeToMonth func([]types.Datetime, []uint8) []uint8
)

func init() {
	dataToMonth = dataToMonthPure
	datetimeToMonth = datetimeToMonthPure
}

func DateToMonth(xs []types.Date, rs []uint8) []uint8 {
	return dataToMonth(xs, rs)
}

func dataToMonthPure(xs []types.Date, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x.Month()
	}
	return rs
}

func DatetimeToMonth(xs []types.Datetime, rs []uint8) []uint8 {
	return datetimeToMonth(xs, rs)
}

func datetimeToMonthPure(xs []types.Datetime, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x.Month()
	}
	return rs
}
