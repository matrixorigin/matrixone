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
)

func RegularReplace(expr, pat, repl string, pos, occurrence int, match_type string) (string, error) {
	if pos < 1 || occurrence < 1 {
		return expr, moerr.NewInvalidInput("regexp_replace have invalid input")
	}
	//regular expression pattern
	reg := regexp.MustCompile(pat)
	//match result indexs
	matchRes := reg.FindAllStringIndex(expr, -1)
	if matchRes == nil {
		return expr, nil
	} //find the match position
	index := 0
	for matchRes[index][0] < pos-1 {
		index++
	}
	matchRes = matchRes[index:]
	if len(matchRes) < occurrence {
		return expr, nil
	}

	// the string won't be replaced
	notRepl := expr[:matchRes[occurrence-1][0]]
	// the string will be replaced
	replace := expr[matchRes[occurrence-1][0]:]
	return notRepl + reg.ReplaceAllLiteralString(replace, repl), nil
}
