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

func RegularReplace(expr, pat, repl string, pos, occurrence int64, match_type string) (string, error) {
	if pos < 1 || occurrence < 0 || pos >= int64(len(expr)) {
		return expr, moerr.NewInvalidInputNoCtx("regexp_replace have invalid input")
	}
	//regular expression pattern
	reg, err := regexp.Compile(pat)
	if err != nil {
		return "", moerr.NewInvalidArgNoCtx("regexp_replace have invalid regexp pattern arg", pat)
	}
	//match result indexs
	matchRes := reg.FindAllStringIndex(expr, -1)
	if matchRes == nil {
		return expr, nil
	} //find the match position
	index := 0
	for int64(matchRes[index][0]) < pos-1 {
		index++
		if index == len(matchRes) {
			return expr, nil
		}
	}
	matchRes = matchRes[index:]
	if int64(len(matchRes)) < occurrence {
		return expr, nil
	}
	if occurrence == 0 {
		return reg.ReplaceAllLiteralString(expr, repl), nil
	} else if occurrence == int64(len(matchRes)) {
		// the string won't be replaced
		notRepl := expr[:matchRes[occurrence-1][0]]
		// the string will be replaced
		replace := expr[matchRes[occurrence-1][0]:]
		return notRepl + reg.ReplaceAllLiteralString(replace, repl), nil
	} else {
		// the string won't be replaced
		notRepl := expr[:matchRes[occurrence-1][0]]
		// the string will be replaced
		replace := expr[matchRes[occurrence-1][0]:matchRes[occurrence][0]]
		left := expr[matchRes[occurrence][0]:]
		return notRepl + reg.ReplaceAllLiteralString(replace, repl) + left, nil
	}
}

func RegularReplaceWithReg(expr string, pat *regexp.Regexp, repl string, pos, occurrence int64, match_type string) (string, error) {
	if pos < 1 || occurrence < 0 || pos >= int64(len(expr)) {
		return expr, moerr.NewInvalidInputNoCtx("regexp_replace have invalid input")
	}
	//match result indexs
	matchRes := pat.FindAllStringIndex(expr, -1)
	if matchRes == nil {
		return expr, nil
	} //find the match position
	index := 0
	for int64(matchRes[index][0]) < pos-1 {
		index++
		if index == len(matchRes) {
			return expr, nil
		}
	}
	matchRes = matchRes[index:]
	if int64(len(matchRes)) < occurrence {
		return expr, nil
	}

	if occurrence == 0 {
		return pat.ReplaceAllLiteralString(expr, repl), nil
	} else if occurrence == int64(len(matchRes)) {
		// the string won't be replaced
		notRepl := expr[:matchRes[occurrence-1][0]]
		// the string will be replaced
		replace := expr[matchRes[occurrence-1][0]:]
		return notRepl + pat.ReplaceAllLiteralString(replace, repl), nil
	} else {
		// the string won't be replaced
		notRepl := expr[:matchRes[occurrence-1][0]]
		// the string will be replaced
		replace := expr[matchRes[occurrence-1][0]:matchRes[occurrence][0]]
		left := expr[matchRes[occurrence][0]:]
		return notRepl + pat.ReplaceAllLiteralString(replace, repl) + left, nil
	}
}

func RegularReplaceWithArrays(expr, pat, rpls []string, pos, occ []int64, match_type []string, exprN, patN, rplN *nulls.Nulls, resultVector *vector.Vector, proc *process.Process, maxLen int) error {
	rs := make([]string, maxLen)
	var rpl string
	var posValue int64
	var occValue int64
	if len(expr) == 1 && len(pat) == 1 {
		reg, err := regexp.Compile(pat[0])
		if err != nil {
			return moerr.NewInvalidArgNoCtx("regexp_replace have invalid regexp pattern arg", pat)
		}
		for i := 0; i < maxLen; i++ {
			if determineNulls(expr, pat, rpls, exprN, patN, rplN, i) {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			rpl, posValue, occValue = determineValuesWithThree(rpls, pos, occ, i)
			res, err := RegularReplaceWithReg(expr[0], reg, rpl, posValue, occValue, match_type[0])
			if err != nil {
				return err
			}
			rs[i] = res
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	} else if len(expr) == 1 {
		for i := 0; i < maxLen; i++ {
			if determineNulls(expr, pat, rpls, exprN, patN, rplN, i) {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			rpl, posValue, occValue = determineValuesWithThree(rpls, pos, occ, i)
			res, err := RegularReplace(expr[0], pat[i], rpl, posValue, occValue, match_type[0])
			if err != nil {
				return err
			}
			rs[i] = res
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	} else if len(pat) == 1 {
		reg, err := regexp.Compile(pat[0])
		if err != nil {
			return moerr.NewInvalidArgNoCtx("regexp_replace have invalid regexp pattern arg", pat)
		}
		for i := 0; i < maxLen; i++ {
			if determineNulls(expr, pat, rpls, exprN, patN, rplN, i) {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			rpl, posValue, occValue = determineValuesWithThree(rpls, pos, occ, i)
			res, err := RegularReplaceWithReg(expr[i], reg, rpl, posValue, occValue, match_type[0])
			if err != nil {
				return err
			}
			rs[i] = res
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	} else {
		for i := 0; i < maxLen; i++ {
			if determineNulls(expr, pat, rpls, exprN, patN, rplN, i) {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			rpl, posValue, occValue = determineValuesWithThree(rpls, pos, occ, i)
			res, err := RegularReplace(expr[i], pat[i], rpl, posValue, occValue, match_type[0])
			if err != nil {
				return err
			}
			rs[i] = res
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	}
	return nil
}

func determineNulls(expr, pat, rpls []string, exprN, patN, rplN *nulls.Nulls, i int) bool {
	var exprIndex int
	var patIndex int
	var rplIndex int

	if len(expr) == 1 {
		exprIndex = 0
	} else {
		exprIndex = i
	}

	if len(pat) == 1 {
		patIndex = 0
	} else {
		patIndex = i
	}

	if len(rpls) == 1 {
		rplIndex = 0
	} else {
		rplIndex = 1
	}
	return nulls.Contains(exprN, uint64(exprIndex)) || nulls.Contains(patN, uint64(patIndex)) || nulls.Contains(rplN, uint64(rplIndex))
}

func determineValuesWithThree(rpls []string, pos, occ []int64, i int) (string, int64, int64) {
	var rpl string
	var posValue int64
	var occValue int64

	if len(rpls) == 1 {
		rpl = rpls[0]
	} else {
		rpl = rpls[i]
	}

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

	return rpl, posValue, occValue
}
