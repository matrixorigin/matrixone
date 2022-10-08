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

func RegularLike(expr, pat, match_type string) (bool, error) {
	matchRes, err := regexp.MatchString(pat, expr)
	if err != nil {
		return false, moerr.NewInvalidInput("regexp_like have invalid input")
	}
	if matchRes {
		return true, nil
	} else {
		return false, nil
	}
}

func RegularLikeWithArrays(expr, pat []string, match_type []string, exprN, patN, rns *nulls.Nulls, rs []bool, maxLen int) error {
	if len(expr) == 1 && len(pat) == 1 {
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(exprN, uint64(0)) || nulls.Contains(patN, uint64(0)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			res, err := RegularLike(expr[0], pat[0], match_type[0])
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
			res, err := RegularLike(expr[0], pat[i], match_type[0])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	} else if len(pat) == 1 {
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(exprN, uint64(i)) || nulls.Contains(patN, uint64(0)) {
				nulls.Add(rns, uint64(i))
				continue
			}
			res, err := RegularLike(expr[i], pat[0], match_type[0])
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
			res, err := RegularLike(expr[0], pat[0], match_type[0])
			if err != nil {
				return err
			}
			rs[i] = res
		}
	}
	return nil
}
