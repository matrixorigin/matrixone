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

package plan

// RequiresGroupConcatTimeZone finds TIMESTAMP rendering owned by GROUP_CONCAT.
func RequiresGroupConcatTimeZone(owner any) (bool, error) {
	required := false
	err := walkExpressionsInOwner(owner, func(expr *Expr) error {
		return VisitExprTree(expr, func(current *Expr) error {
			fn := current.GetF()
			// GROUP_CONCAT ID 185, excluding its DISTINCT flag.
			if fn == nil || fn.Func == nil || int32((uint64(fn.Func.Obj)&0x7fffffffffffffff)>>32) != 185 {
				return nil
			}
			for _, arg := range fn.Args {
				if arg != nil && arg.Typ.Id == 53 {
					required = true
				}
			} // TIMESTAMP
			return nil
		})
	})
	return required, err
}
