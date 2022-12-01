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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func RegularSubstr(expr, pat string, pos, occurrence int64, match_type string) ([]string, error) {
	if pos < 1 || occurrence < 1 || pos >= int64(len(expr)) {
		return nil, moerr.NewInvalidInputNoCtx("regexp_substr have invalid input")
	}
	//regular expression pattern
	reg, err := regexp.Compile(pat)
	if err != nil {
		return nil, moerr.NewInvalidArgNoCtx("regexp_substr have invalid regexp pattern arg", pat)
	}
	//match result strings
	matchRes := reg.FindAllString(expr[pos-1:], -1)
	if matchRes == nil || int64(len(matchRes)) < occurrence {
		return nil, nil
	}
	return matchRes, nil
}

func RegularSubstrWithReg(expr string, pat *regexp.Regexp, pos, occurrence int64, match_type string) ([]string, error) {
	if pos < 1 || occurrence < 1 || pos >= int64(len(expr)) {
		return nil, moerr.NewInvalidInputNoCtx("regexp_substr have invalid input")
	}
	//match result strings
	matchRes := pat.FindAllString(expr[pos-1:], -1)
	if matchRes == nil || int64(len(matchRes)) < occurrence {
		return nil, nil
	}
	return matchRes, nil
}

func RegularSubstrWithArrays(expr, pat []string, pos, occ []int64, match_type []string, exprN, patN *nulls.Nulls, resultVector *vector.Vector, proc *process.Process, maxLen int) error {
	rs := make([]string, maxLen)
	var posValue int64
	var occValue int64
	if len(expr) == 1 && len(pat) == 1 {
		reg, err := regexp.Compile(pat[0])
		if err != nil {
			return moerr.NewInvalidArgNoCtx("regexp_substr have invalid regexp pattern arg", pat)
		}
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(exprN, uint64(0)) || nulls.Contains(patN, uint64(0)) || pat[0] == "" {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			posValue, occValue = determineValuesWithTwo(pos, occ, i)
			res, err := RegularSubstrWithReg(expr[0], reg, posValue, occValue, match_type[0])
			if err != nil {
				return err
			}
			if res == nil {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			rs[i] = res[occValue-1]
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	} else if len(expr) == 1 {
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(exprN, uint64(0)) || nulls.Contains(patN, uint64(i)) || pat[i] == "" {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			posValue, occValue = determineValuesWithTwo(pos, occ, i)
			res, err := RegularSubstr(expr[0], pat[i], posValue, occValue, match_type[0])
			if err != nil {
				return err
			}
			if res == nil {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			rs[i] = res[occValue-1]
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	} else if len(pat) == 1 {
		reg, err := regexp.Compile(pat[0])
		if err != nil {
			return moerr.NewInvalidArgNoCtx("regexp_substr have invalid regexp pattern arg", pat)
		}
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(exprN, uint64(i)) || nulls.Contains(patN, uint64(0)) || pat[0] == "" {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			posValue, occValue = determineValuesWithTwo(pos, occ, i)
			res, err := RegularSubstrWithReg(expr[i], reg, posValue, occValue, match_type[0])
			if err != nil {
				return err
			}
			if res == nil {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			rs[i] = res[occValue-1]
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	} else {
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(exprN, uint64(i)) || nulls.Contains(patN, uint64(i)) || pat[i] == "" {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			posValue, occValue = determineValuesWithTwo(pos, occ, i)
			res, err := RegularSubstr(expr[0], pat[i], posValue, occValue, match_type[0])
			if err != nil {
				return err
			}
			if res == nil {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			rs[i] = res[occValue-1]
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	}
	return nil
}

func determineValuesWithTwo(pos, occ []int64, i int) (int64, int64) {
	var posValue int64
	var occValue int64

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

	return posValue, occValue
}
