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
	if pos < 1 || occurrence < 1 || (return_option != 0 && return_option != 1) {
		return 0, moerr.NewInvalidInput("regexp_instr have invalid input")
	}
	//regular expression pattern
	reg := regexp.MustCompile(pat)
	//match result indexs
	matchRes := reg.FindAllStringIndex(expr, -1)
	if matchRes == nil {
		return 0, nil
	}
	//find the match position
	index := 0
	for int64(matchRes[index][0]) < pos-1 {
		index++
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


func RegularInstrWithArrays(expr, pat []string, pos, occurrence []int64, return_option []uint8, match_type []string, exprN, patN, rns *nulls.Nulls,  rs []int64) ([]int64, error){
	if len(expr) == len(pat){
		for i := range expr{
			if nulls.Contains(exprN, uint64(i)) || nulls.Contains(patN, uint64(i)){
				nulls.Add(rns, uint64(i))
				continue
			}
			res, err := RegularInstr(expr[i], pat[i], pos[i], occurrence[i], return_option[i], match_type[i])
			if err != nil{
				return nil, err
			}
			rs[i] = res
		}
	}else if len(expr) == 1{
		for i := range pat{
			if nulls.Contains(exprN, uint64(0)) || nulls.Contains(patN, uint64(i)){
				nulls.Add(rns, uint64(i))
				continue
			}
			res, err := RegularInstr(expr[0], pat[i], pos[i], occurrence[i], return_option[i], match_type[i])
			if err != nil{
				return nil, err
			}
			rs[i] = res
		}
	}else if len(pat) == 1{
		for i := range expr{
			if nulls.Contains(exprN, uint64(i)) || nulls.Contains(patN, uint64(0)){
				nulls.Add(rns, uint64(i))
				continue
			}
			res, err := RegularInstr(expr[i], pat[0], pos[i], occurrence[i], return_option[i], match_type[i])
			if err != nil{
				return nil, err
			}
			rs[i] = res
		}
	}
	return rs, nil
} 