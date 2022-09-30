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
	if pos < 1 || occurrence < 1 {
		return nil, moerr.NewInvalidInput("regexp_substr have invalid input")
	}
	//regular expression pattern
	reg := regexp.MustCompile(pat)
	//match result strings
	matchRes := reg.FindAllString(expr[pos-1:], -1)
	if matchRes == nil || int64(len(matchRes)) < occurrence {
		return nil, nil
	}
	return matchRes, nil
}

func RegularSubstrWithReg(expr string, pat *regexp.Regexp, pos, occurrence int64, match_type string) ([]string, error) {
	if pos < 1 || occurrence < 1 {
		return nil, moerr.NewInvalidInput("regexp_substr have invalid input")
	}
	//match result strings
	matchRes := pat.FindAllString(expr[pos-1:], -1)
	if matchRes == nil || int64(len(matchRes)) < occurrence {
		return nil, nil
	}
	return matchRes, nil
}

func RegularSubstrWithArrays(expr, pat []string, pos, occurrence []int64, match_type []string, exprN, patN *nulls.Nulls, resultVector *vector.Vector, proc *process.Process) error {
	rs := make([]string, len(pos))
	if len(expr) == len(pat) {
		for i := range expr {
			if nulls.Contains(exprN, uint64(i)) || nulls.Contains(patN, uint64(i)) {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			res, err := RegularSubstr(expr[i], pat[i], pos[i], occurrence[i], match_type[i])
			if err != nil {
				return err
			}
			if res == nil {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			rs[i] = res[occurrence[i]-1]
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	} else if len(expr) == 1 {
		for i := range pat {
			if nulls.Contains(exprN, uint64(0)) || nulls.Contains(patN, uint64(i)) {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			res, err := RegularSubstr(expr[0], pat[i], pos[i], occurrence[i], match_type[i])
			if err != nil {
				return err
			}
			if res == nil {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			rs[i] = res[occurrence[i]-1]
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	} else if len(pat) == 1 {
		for i := range expr {
			if nulls.Contains(exprN, uint64(i)) || nulls.Contains(patN, uint64(0)) {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			reg := regexp.MustCompile(pat[0])
			res, err := RegularSubstrWithReg(expr[i], reg, pos[i], occurrence[i], match_type[i])
			if err != nil {
				return err
			}
			if res == nil {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			rs[i] = res[occurrence[i]-1]
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	}
	return nil
}
