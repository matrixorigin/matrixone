// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// ExpressionContainsRuntimeBinaryString reports whether evaluating expr can
// depend on binary-string metadata which a plan Literal cannot represent.
func ExpressionContainsRuntimeBinaryString(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if literal := expr.GetLit(); literal != nil && literal.IsBin {
		return true
	}
	fn := expr.GetF()
	if fn == nil {
		return false
	}
	if fn.Func != nil && strings.EqualFold(fn.Func.ObjName, "bit_cast") {
		return false
	}
	if fn.Func != nil && strings.EqualFold(fn.Func.ObjName, "cast") {
		_, overload := DecodeOverloadID(fn.Func.Obj)
		resultType := types.T(expr.Typ.Id)
		if overload == 1 || !resultType.IsMySQLString() ||
			resultType == types.T_binary || resultType == types.T_varbinary || resultType == types.T_blob {
			return false
		}
	}
	if fn.Func != nil && strings.EqualFold(fn.Func.ObjName, "char") {
		return true
	}
	for _, arg := range fn.Args {
		if ExpressionContainsRuntimeBinaryString(arg) {
			return true
		}
	}
	return false
}
