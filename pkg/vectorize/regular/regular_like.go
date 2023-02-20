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
	"fmt"
	"regexp"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
)

// Support four arguments:
// i: case insensitive.
// c: case sensitive.
// m: multiple line mode.
// n: '.' can match line terminator.
func getPureMatchType(input string) (string, error) {
	retstring := ""
	caseType := ""
	foundn := false
	foundm := false

	for _, c := range input {
		switch string(c) {
		case "i":
			caseType = "i"
		case "c":
			caseType = ""
		case "m":
			if !foundm {
				retstring += "m"
				foundm = true
			}
		case "n":
			if !foundn {
				retstring += "s"
				foundn = true
			}
		default:
			return "", moerr.NewInvalidInputNoCtx("regexp_like got invalid match_type input!")
		}
	}

	retstring += caseType

	return retstring, nil
}

func compileWithMatchType(pat, match_type string) (*regexp.Regexp, error) {
	if len(pat) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("regexp_like got invalid input!")
	}
	match_type, err := getPureMatchType(match_type)
	if err != nil {
		return nil, err
	}
	re, err := regexp.Compile(fmt.Sprintf("(?%s)%s", match_type, pat))
	if err != nil {
		return nil, err
	}
	return re, nil
}

func RegularLike(expr, pat, match_type string) (bool, error) {
	re, err := compileWithMatchType(pat, match_type)
	if err != nil {
		return false, err
	}
	matchRes := re.MatchString(expr)
	return matchRes, nil
}

func RegularLikeWithArrays(expr, pat []string, match_type []string, exprN, patN, matchN, rns *nulls.Nulls, rs []bool, maxLen int) error {
	matchIndex := 0
	if len(expr) == 1 && len(pat) == 1 {
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(exprN, uint64(0)) || nulls.Contains(patN, uint64(0)) || nulls.Contains(matchN, uint64(0)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			res, err := RegularLike(expr[0], pat[0], match_type[i])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	} else if len(expr) == 1 {
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(exprN, uint64(0)) || nulls.Contains(patN, uint64(i)) || nulls.Contains(matchN, uint64(0)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			if len(match_type) != 1 {
				matchIndex = i
			}
			res, err := RegularLike(expr[0], pat[i], match_type[matchIndex])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	} else if len(pat) == 1 {
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(exprN, uint64(i)) || nulls.Contains(patN, uint64(0)) || nulls.Contains(matchN, uint64(0)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			if len(match_type) != 1 {
				matchIndex = i
			}
			res, err := RegularLike(expr[i], pat[0], match_type[matchIndex])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	} else {
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(exprN, uint64(i)) || nulls.Contains(patN, uint64(i)) || nulls.Contains(matchN, uint64(0)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			if len(match_type) != 1 {
				matchIndex = i
			}
			res, err := RegularLike(expr[i], pat[i], match_type[matchIndex])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	}
	return nil
}
