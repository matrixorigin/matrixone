// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
)

// nonNullArgs is a proof only for explicitly annotated implementations that
// cannot introduce NULL (errors are allowed). STRICT alone is insufficient.
func nonNullArgs(args []*plan.Expr) bool {
	for _, arg := range args {
		if !arg.Typ.NotNullable {
			return false
		}
	}
	return true
}

// CASE without ELSE produces NULL when no condition matches, even when every
// supplied argument is non-NULL.
func nonNullCase(args []*plan.Expr) bool {
	return len(args) >= 3 && len(args)%2 == 1 && nonNullArgs(args)
}

func nonNullCast(args []*plan.Expr) bool {
	if len(args) != 2 || !args[0].Typ.NotNullable {
		return false
	}
	from, to := types.T(args[0].Typ.Id), types.T(args[1].Typ.Id)
	if from == to {
		return true
	}
	// Numeric casts either return a value or an error; YEAR and temporal/string
	// parsing have invalid-input NULL cases and must not inherit this proof.
	numeric := func(t types.T) bool { return t.IsInteger() || t.IsFloat() || t.IsDecimal() }
	if numeric(from) && numeric(to) {
		return true
	}
	// Text conversion of an existing scalar does not parse a domain value.
	if to == types.T_char || to == types.T_varchar {
		return numeric(from) || from == types.T_char || from == types.T_varchar || from == types.T_bool
	}
	return false
}
