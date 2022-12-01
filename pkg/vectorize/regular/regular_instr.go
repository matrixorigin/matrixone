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

package regular

import (
	"regexp"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
)

func RegularInstr(expr, pat string, pos, occurrence int64, return_option uint8, match_type string) (int64, error) {
	if pos < 1 || occurrence < 1 || (return_option != 0 && return_option != 1) || pos >= int64(len(expr)) {
		return 0, moerr.NewInvalidInputNoCtx("regexp_instr have invalid input")
	}
	//regular expression pattern
	reg, err := regexp.Compile(pat)
	if err != nil {
		return 0, moerr.NewInvalidArgNoCtx("regexp_instr have invalid regexp pattern arg", pat)
	}
	//match result indexs
	matchRes := reg.FindAllStringIndex(expr, -1)
	if matchRes == nil {
		return 0, nil
	}
	//find the match position
	index := 0
	for int64(matchRes[index][0]) < pos-1 {
		index++
		if index == len(matchRes) {
			return 0, nil
		}
	}

	matchRes = matchRes[index:]
	if int64(len(matchRes)) < occurrence {
		return 0, nil
	}

	if return_option == 0 {
		return int64(matchRes[occurrence-1][0] + 1), nil
	} else {
		return int64(matchRes[occurrence-1][1] + 1), nil
	}
}

func RegularInstrWithReg(expr string, pat *regexp.Regexp, pos, occurrence int64, return_option uint8, match_type string) (int64, error) {
	if pos < 1 || occurrence < 1 || (return_option != 0 && return_option != 1) || pos >= int64(len(expr)) {
		return 0, moerr.NewInvalidInputNoCtx("regexp_instr have invalid input")
	}
	//match result indexs
	matchRes := pat.FindAllStringIndex(expr, -1)
	if matchRes == nil {
		return 0, nil
	}
	//find the match position
	index := 0
	for int64(matchRes[index][0]) < pos-1 {
		index++
		if index == len(matchRes) {
			return 0, nil
		}
	}
	matchRes = matchRes[index:]
	if int64(len(matchRes)) < occurrence {
		return 0, nil
	}

	if return_option == 0 {
		return int64(matchRes[occurrence-1][0] + 1), nil
	} else {
		return int64(matchRes[occurrence-1][1] + 1), nil
	}
}

func RegularInstrWithArrays(expr, pat []string, pos, occ []int64, return_option []uint8, match_type []string, exprN, patN, rns *nulls.Nulls, rs []int64, maxLen int) error {
	var posValue int64
	var occValue int64
	var optValue uint8
	if len(expr) == 1 && len(pat) == 1 {
		reg, err := regexp.Compile(pat[0])
		if err != nil {
			return moerr.NewInvalidArgNoCtx("regexp_instr have invalid regexp pattern arg", pat)
		}
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(exprN, uint64(0)) || nulls.Contains(patN, uint64(0)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			posValue, occValue, optValue = determineValues(pos, occ, return_option, i)
			res, err := RegularInstrWithReg(expr[0], reg, posValue, occValue, optValue, match_type[0])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	} else if len(expr) == 1 {
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(exprN, uint64(0)) || nulls.Contains(patN, uint64(i)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			posValue, occValue, optValue = determineValues(pos, occ, return_option, i)
			res, err := RegularInstr(expr[0], pat[i], posValue, occValue, optValue, match_type[0])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	} else if len(pat) == 1 {
		reg, err := regexp.Compile(pat[0])
		if err != nil {
			return moerr.NewInvalidArgNoCtx("regexp_instr have invalid regexp pattern arg", pat)
		}
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(exprN, uint64(i)) || nulls.Contains(patN, uint64(0)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			posValue, occValue, optValue = determineValues(pos, occ, return_option, i)
			res, err := RegularInstrWithReg(expr[i], reg, posValue, occValue, optValue, match_type[0])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	} else {
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(exprN, uint64(i)) || nulls.Contains(patN, uint64(i)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			posValue, occValue, optValue = determineValues(pos, occ, return_option, i)
			res, err := RegularInstr(expr[i], pat[i], posValue, occValue, optValue, match_type[0])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	}
	return nil
}

func determineValues(pos, occ []int64, return_option []uint8, i int) (int64, int64, uint8) {
	var posValue int64
	var occValue int64
	var optValue uint8
	if len(pos) == 1 {
		posValue = pos[0]
	} else {
		posValue = pos[i]
	}

	if len(occ) == 1 {
		occValue = occ[0]
	} else {
		occValue = occ[i]
	}

	if len(return_option) == 1 {
		optValue = return_option[0]
	} else {
		optValue = return_option[i]
	}

	return posValue, occValue, optValue
}
