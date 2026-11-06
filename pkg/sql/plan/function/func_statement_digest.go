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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func statementDigestFunction(id int, text bool) FuncNew {
	ret := types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8)
	if text {
		ret = types.NewWithCharset(types.T_text, types.MaxLongTextLen, 0, types.CharsetUTF8)
	}
	return FuncNew{
		functionId: id, class: plan.Function_STRICT, layout: STANDARD_FUNCTION,
		checkFn: stringDomainFixedTypeMatch,
		Overloads: []overload{{
			overloadId: 0, args: []types.T{types.T_varchar},
			// SQL mode is live at EXECUTE, not frozen at PREPARE/constant folding.
			volatile: true, executionStable: true,
			retType: func([]types.Type) types.Type { return ret },
			newOp: func() executeLogicOfOverload {
				return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
					mode := proc.GetSessionInfo().SqlMode
					if resolver := proc.GetResolveVariableFunc(); resolver != nil {
						if value, err := resolver("sql_mode", true, false); err == nil {
							if current, ok := value.(string); ok && (current != "" || proc.Base.IsFrontend || mode == "") {
								mode = current
							}
						}
					}
					if mode == process.EmptySqlModeSentinel {
						mode = ""
					}
					return opUnaryBytesToStrWithErrorCheck(parameters, result, proc, length, func(value []byte) (string, error) {
						normalized, digest, err := mysql.StatementDigest(proc.Ctx, string(value), mode)
						if text {
							return normalized, err
						}
						return digest, err
					}, selection)
				}
			},
		}},
	}
}
