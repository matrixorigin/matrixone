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

// HasBoundStringVariable reports whether an expression owner retains a frozen
// user-variable row domain, including sources retained by folding/rewriting.
// This is an admission check, not a validator: malformed nonzero bindings must
// also prevent a lossy migration or remote handoff.
func HasBoundStringVariable(owner any) bool {
	found := false
	var visit func(*Expr) error
	visit = func(expr *Expr) error {
		if expr.GetV().GetBoundStringDomain() != 0 {
			found = true
		}
		// VisitExprTree covers executable children and folded literal sources,
		// but not this planner-only provenance edge.
		if source := expr.GetPreparedNumeric().GetStringDomainSource(); source != nil && !found {
			return VisitExprTree(source, visit)
		}
		return nil
	}
	_ = VisitExpressionsInOwner(owner, func(root *Expr) error {
		if found {
			return nil
		}
		return VisitExprTree(root, visit)
	})
	return found
}
