// Copyright 2021 - 2022 Matrix Origin
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

package types

import (
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func ParseBool(s string) (bool, error) {
	s = strings.ToLower(s)
	if s == "true" {
		return true, nil
	} else if s == "false" {
		return false, nil
	}
	val, err := strconv.ParseFloat(s, 64)
	if err == nil {
		if val != 0 {
			return true, nil
		} else {
			return false, nil
		}
	}
	return false, moerr.NewInvalidInputNoCtx("'%s' is not a valid bool expression", s)
}
