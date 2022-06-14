package extract

import (
	"errors"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

//func extractFromDate()

var validDateUnit = map[string]struct{}{
	"year":       {},
	"month":      {},
	"day":        {},
	"year_month": {},
	"quarter":    {},
}

/*
func ExtractFromInputBytes(unit string, inputBytes *types.Bytes, inputNsp *nulls.Nulls, results []uint32) ([]uint32, *nulls.Nulls, error) {
	resultNsp := new(nulls.Nulls)
	if _, ok := validDateUnit[unit]; !ok {
		return []uint32{}, nil, errors.New("invalid unit")
	}
	for i := range inputBytes.Lengths {
		if nulls.Contains(inputNsp, uint64(i)) {
			nulls.Add(resultNsp, uint64(i))
			continue
		}
		inputValue := string(inputBytes.Get(int64(i)))
		date, err := types.ParseDate(inputValue)
		if err != nil {
			return []uint32{}, nil, errors.New("invalid date string")
		}
		results[i] = ExtractFromOneDate(unit, date)
	}
	return results, resultNsp, nil
}

*/

func ExtractFromOneDate(unit string, date types.Date) uint32 {
	switch unit {
	case "day":
		return uint32(date.Day())
	case "month":
		return uint32(date.Month())
	case "quarter":
		return date.Quarter()
	case "year":
		return uint32(date.Year())
	case "year_month":
		return date.YearMonth()
	default:
		return 0
	}
}

func ExtractFromDate(unit string, dates []types.Date, results []uint32) ([]uint32, error) {
	if _, ok := validDateUnit[unit]; !ok {
		return []uint32{}, errors.New("invalid unit")
	}
	switch unit {
	case "day":
		for i, d := range dates {
			results[i] = uint32(d.Day())
		}
	case "month":
		for i, d := range dates {
			results[i] = uint32(d.Month())
		}
	case "year":
		for i, d := range dates {
			results[i] = uint32(d.Year())
		}
	case "year_month":
		for i, d := range dates {
			results[i] = d.YearMonth()
		}
	case "quarter":
		for i, d := range dates {
			results[i] = d.Quarter()
		}
	}
	return results, nil
}
