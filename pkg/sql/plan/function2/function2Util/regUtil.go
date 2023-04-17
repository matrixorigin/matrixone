// Copyright 2021 Matrix Origin
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

package function2Util

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"regexp"
)

type regContainer struct {
	regMap map[string]*regexp.Regexp
}

func NewRegContainer() *regContainer {
	return &regContainer{regMap: make(map[string]*regexp.Regexp)}
}

func (rc *regContainer) RegularSubstr(expr, pat string, pos, occurrence int64) (match bool, substr string, err error) {
	if pos < 1 || occurrence < 1 || pos >= int64(len(expr)) {
		return false, "", moerr.NewInvalidInputNoCtx("regexp_substr have invalid input")
	}

	reg, ok := rc.regMap[pat]
	if !ok {
		reg, err = regexp.Compile(pat)
		if err != nil {
			return false, "", moerr.NewInvalidArgNoCtx("regexp_substr have invalid regexp pattern arg", pat)
		}
		rc.regMap[pat] = reg
	}

	// match and return
	matches := reg.FindAllString(expr[pos-1:], -1)
	if matches == nil || int64(len(matches)) < occurrence {
		return false, "", nil
	}
	return true, matches[occurrence-1], nil
}
