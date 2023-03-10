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

// for input value greater than 8000, function space will output NULL,
// for input value less than 0, function space will output empty string ""
// for positive float inputs, function space will round(away from zero)

package space

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	MaxAllowedValue = 8000
)

func FillSpacesNumber[T types.BuiltinNumber](originalVecCol []T, rs []string) ([]string, error) {
	for i, length := range originalVecCol {
		var ilen int
		if length < 0 {
			ilen = 0
		} else {
			ilen = int(length)
			if ilen > MaxAllowedValue || ilen < 0 {
				return nil, moerr.NewInvalidInputNoCtx("the space count is greater than max allowed value %d", MaxAllowedValue)
			}
		}
		rs[i] = strings.Repeat(" ", ilen)
	}
	return rs, nil
}
