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
	"go/constant"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func ParseValueToBool(num *tree.NumVal) (bool, error) {
	val := num.Value
	str := num.String()
	if !num.Negative() {
		v, _ := constant.Uint64Val(val)
		if v == 0 {
			return false, nil
		} else if v == 1 {
			return true, nil
		}
	}
	return false, moerr.NewInvalidInputNoCtx("'%s' is not a valid bool expression", str)
}

func AppendBoolToByteArray(b bool, arr []byte) []byte {
	if b {
		arr = append(arr, '1')
	} else {
		arr = append(arr, '0')
	}
	return arr
}

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

// ToIntString print out 1 or 0 as true/false.
func BoolToIntString(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
