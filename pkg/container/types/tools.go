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

// TypeFromPlan is the checked conversion at the wire/runtime boundary.
func TypeFromPlan(typ plan.Type) (Type, error) {
	if err := typ.ValidateCollation(); err != nil {
		return Type{}, err
	}
	result := NewWithCharset(T(typ.Id), typ.Width, typ.Scale, uint8(typ.Charset))
	result.CollationVersion = uint8(typ.CollationVersion)
	return result, nil
}

// MustTypeFromPlan is for value-only internal interfaces after the plan's
// admission check. Invalid metadata must never be narrowed into a legacy type.
func MustTypeFromPlan(typ plan.Type) Type {
	result, err := TypeFromPlan(typ)
	if err != nil {
		panic(err)
	}
	return result
}

// PlanType carries runtime identity/revision; expression provenance remains
// owned by the source plan and must be copied with that plan, not invented here.
func (t Type) PlanType() plan.Type {
	return plan.Type{Id: int32(t.Oid), Width: t.Width, Scale: t.Scale,
		Charset: uint32(t.Charset), CollationVersion: uint32(t.CollationVersion)}
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
