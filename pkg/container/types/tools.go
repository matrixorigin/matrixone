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

package types

import (
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func NewProtoType(oid T) plan.Type {
	typ := New(oid, 0, 0)
	return plan.Type{
		Id:    int32(oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}

func ParseBool(s string) (bool, error) {
	// try to parse as a bool, we treat TuRe as true, therefore ToLower.
	v, err := strconv.ParseBool(strings.ToLower(s))
	if err == nil {
		return v, nil
	}

	// try to parse as a number.   We treat 0 as false, and other numbers as true.
	num, err := strconv.ParseFloat(s, 64)
	if err == nil {
		return num != 0.0, nil
	}

	return false, moerr.NewInvalidInputNoCtxf("'%s' is not a valid bool expression", s)
}
