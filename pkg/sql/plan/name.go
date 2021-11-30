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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func getColumnName(expr tree.Expr) (string, bool) {
	e, ok := expr.(*tree.UnresolvedName)
	if !ok {
		return "", false
	}
	switch e.NumParts {
	case 1:
		return e.Parts[0], true
	case 2:
		return e.Parts[1] + "." + e.Parts[0], true
	case 3:
		return e.Parts[2] + "." + e.Parts[1] + "." + e.Parts[0], true
	default: // 4
		return e.Parts[3] + "." + e.Parts[2] + "." + e.Parts[1] + "." + e.Parts[0], true
	}
}
