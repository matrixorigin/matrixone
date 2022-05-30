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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2/function/builtin/multi"
)

func initBuiltIns() {
	var err error

	for name, fs := range builtins {
		for _, f := range fs {
			err = appendFunction(name, f)
			if err != nil {
				panic(err)
			}
		}
	}
}

// builtins contains the builtin function indexed by function id.
var builtins = map[int][]Function{
	EXTRACT: {
		{
			Index:       0,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_varchar, types.T_date},
			ReturnTyp:   types.T_float64,
			TypeCheckFn: strictTypeCheck,
			Fn:          nil,
		},
		{
			Index:       1,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_varchar, types.T_datetime},
			ReturnTyp:   types.T_float64,
			TypeCheckFn: strictTypeCheck,
			Fn:          nil,
		},
	},
	YEAR: {
		{
			Index:       0,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_date},
			ReturnTyp:   types.T_uint16,
			TypeCheckFn: strictTypeCheck,
			Fn:          nil,
		},
		{
			Index:       1,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_datetime},
			ReturnTyp:   types.T_uint16,
			TypeCheckFn: strictTypeCheck,
			Fn:          nil,
		},
	},
	SUBSTRING: {
		{
			Index:       0,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_varchar, types.T_int64},
			ReturnTyp:   types.T_varchar,
			TypeCheckFn: strictTypeCheck,
			Fn:          multi.Substring,
		},
		{
			Index:       1,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_varchar, types.T_uint64},
			ReturnTyp:   types.T_varchar,
			TypeCheckFn: strictTypeCheck,
			Fn:          multi.Substring,
		},
		{
			Index:       2,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_char, types.T_int64},
			ReturnTyp:   types.T_char,
			TypeCheckFn: strictTypeCheck,
			Fn:          multi.Substring,
		},
		{
			Index:       3,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_char, types.T_uint64},
			ReturnTyp:   types.T_char,
			TypeCheckFn: strictTypeCheck,
			Fn:          multi.Substring,
		},
		{
			Index:       4,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_varchar, types.T_int64, types.T_int64},
			ReturnTyp:   types.T_varchar,
			TypeCheckFn: strictTypeCheck,
			Fn:          multi.Substring,
		},
		{
			Index:       5,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_varchar, types.T_int64, types.T_uint64},
			ReturnTyp:   types.T_varchar,
			TypeCheckFn: strictTypeCheck,
			Fn:          multi.Substring,
		},
		{
			Index:       6,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_varchar, types.T_uint64, types.T_int64},
			ReturnTyp:   types.T_varchar,
			TypeCheckFn: strictTypeCheck,
			Fn:          multi.Substring,
		},
		{
			Index:       7,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_varchar, types.T_uint64, types.T_uint64},
			ReturnTyp:   types.T_varchar,
			TypeCheckFn: strictTypeCheck,
			Fn:          multi.Substring,
		},
		{
			Index:       8,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_char, types.T_int64, types.T_int64},
			ReturnTyp:   types.T_char,
			TypeCheckFn: strictTypeCheck,
			Fn:          multi.Substring,
		},
		{
			Index:       9,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_char, types.T_int64, types.T_uint64},
			ReturnTyp:   types.T_char,
			TypeCheckFn: strictTypeCheck,
			Fn:          multi.Substring,
		},
		{
			Index:       10,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_char, types.T_uint64, types.T_int64},
			ReturnTyp:   types.T_char,
			TypeCheckFn: strictTypeCheck,
			Fn:          multi.Substring,
		},
		{
			Index:       11,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_char, types.T_uint64, types.T_uint64},
			ReturnTyp:   types.T_char,
			TypeCheckFn: strictTypeCheck,
			Fn:          multi.Substring,
		},
	},
	DATE_ADD: {
		{
			Index:       0,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_date, types.T_int64, types.T_int64},
			ReturnTyp:   types.T_date,
			TypeCheckFn: strictTypeCheck,
			Fn:          nil,
		},
	},
	DATE_SUB: {
		{
			Index:       0,
			Flag:        plan.Function_STRICT,
			Layout:      STANDARD_FUNCTION,
			Args:        []types.T{types.T_date, types.T_int64, types.T_int64},
			ReturnTyp:   types.T_date,
			TypeCheckFn: strictTypeCheck,
			Fn:          nil,
		},
	},
}
