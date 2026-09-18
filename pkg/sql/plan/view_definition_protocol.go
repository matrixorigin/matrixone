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

import (
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// requiredViewDefinitionFunctionProtocolVersion reports the protocol floor
// needed by the parser-aware information_schema.VIEWS functions when they are
// persisted inside a view definition. Keep this fence separate from the
// integer-parameter feature walk: these functions are a distinct v86 catalog
// contract, and the generated ViewData marker is what protects the real bind /
// Prepare path on a mixed-version CN.
func requiredViewDefinitionFunctionProtocolVersion(owner any) (int64, error) {
	required := false
	err := planpb.VisitExpressionsInOwner(owner, func(expr *planpb.Expr) error {
		return planpb.VisitExprTree(expr, func(current *planpb.Expr) error {
			if current == nil || current.GetF() == nil || current.GetF().Func == nil {
				return nil
			}
			functionID, _ := function.DecodeOverloadID(current.GetF().Func.Obj)
			switch functionID {
			case function.MO_VIEW_DEFINITION, function.MO_VIEW_CHECK_OPTION:
				required = true
			}
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	if required {
		return defines.MORPCVersion86, nil
	}
	return 0, nil
}
