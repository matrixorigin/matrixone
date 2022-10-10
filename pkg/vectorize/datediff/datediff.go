// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package datediff

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func DateDiff(lv, rv []types.Date, rs []int64) []int64 {
	for i := range lv {
		rs[i] = int64(lv[i] - rv[i])
	}
	return rs
}

func DateDiffRightConst(lv []types.Date, rv types.Date, rs []int64) []int64 {
	for i := range lv {
		rs[i] = int64(lv[i] - rv)
	}
	return rs
}

func DateDiffLeftConst(lv types.Date, rv []types.Date, rs []int64) []int64 {
	for i := range rv {
		rs[i] = int64(lv - rv[i])
	}
	return rs
}

func DateDiffAllConst(lv, rv types.Date, rs []int64) []int64 {
	rs[0] = int64(lv - rv)
	return rs
}

func TimeStampDiff(unit string, expr1, expr2 types.Datetime) (int64, error) {
	return (expr2 - expr1).ConvertToInterval(unit)
}

func TimeStampDiffWithCols(units []string, expr1, expr2 []types.Datetime, unitNs, firstNs, secondNs, rsNs *nulls.Nulls, rs []int64, maxLen int) error {
	var unit string
	if len(expr1) == 1 && len(expr2) == 1 {
		unitsLen := len(units)
		for i := 0; i < maxLen; i++ {
			if determineNulls(unitsLen, 1, 1, unitNs, firstNs, secondNs, i) {
				nulls.Add(rsNs, uint64(i))
				continue
			}
			if unitsLen == 1 {
				unit = units[0]
			} else {
				unit = units[i]
			}

			res, err := TimeStampDiff(unit, expr1[0], expr2[0])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	} else if len(expr1) == 1 {
		unitsLen := len(units)
		for i := 0; i < maxLen; i++ {
			if determineNulls(unitsLen, 1, maxLen, unitNs, firstNs, secondNs, i) {
				nulls.Add(rsNs, uint64(i))
				continue
			}
			if unitsLen == 1 {
				unit = units[0]
			} else {
				unit = units[i]
			}

			res, err := TimeStampDiff(unit, expr1[0], expr2[i])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	} else if len(expr2) == 1 {
		unitsLen := len(units)
		for i := 0; i < maxLen; i++ {
			if determineNulls(unitsLen, maxLen, 1, unitNs, firstNs, secondNs, i) {
				nulls.Add(rsNs, uint64(i))
				continue
			}
			if unitsLen == 1 {
				unit = units[0]
			} else {
				unit = units[i]
			}

			res, err := TimeStampDiff(unit, expr1[i], expr2[0])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	} else {
		unitsLen := len(units)
		for i := 0; i < maxLen; i++ {
			if determineNulls(unitsLen, maxLen, maxLen, unitNs, firstNs, secondNs, i) {
				nulls.Add(rsNs, uint64(i))
				continue
			}
			if unitsLen == 1 {
				unit = units[0]
			} else {
				unit = units[i]
			}

			res, err := TimeStampDiff(unit, expr1[i], expr2[i])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	}
	return nil
}

func determineNulls(unitL, firstL, secondL int, unitNs, firstNs, secondNs *nulls.Nulls, i int) bool {
	var uintIndex int
	var firstIndex int
	var secondIndex int

	if unitL == 1 {
		uintIndex = 0
	} else {
		uintIndex = i
	}

	if firstL == 1 {
		firstIndex = 0
	} else {
		firstIndex = i
	}

	if secondL == 1 {
		secondIndex = 0
	} else {
		secondIndex = 1
	}
	return nulls.Contains(unitNs, uint64(uintIndex)) || nulls.Contains(firstNs, uint64(firstIndex)) || nulls.Contains(secondNs, uint64(secondIndex))
}
