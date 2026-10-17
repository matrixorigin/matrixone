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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func queryUsesScalarSetReaggregation(qry *plan.Query) bool {
	if qry != nil {
		for _, node := range qry.Nodes {
			if node != nil && node.ScalarSetReaggregation {
				return true
			}
		}
	}
	return false
}

// Runtime admission is owned by one local compile attempt. Check every scope,
// including detached producer steps, before any scope can start executing.
func validateScalarSetScopePlacement(qry *plan.Query, current engine.Node, scopes []*Scope) error {
	if !queryUsesScalarSetReaggregation(qry) {
		return nil
	}
	for _, n := range qry.Nodes {
		if n == nil || !n.ScalarSetReaggregation {
			continue
		}
		if n.NodeType != plan.Node_WINDOW && (n.NodeType != plan.Node_JOIN || n.JoinType != plan.Node_LEFT) {
			return moerr.NewInvalidInputNoCtx("invalid scalar set reaggregation stage")
		}
	}
	seen := make(map[*Scope]struct{})
	var visit func(*Scope) error
	visit = func(s *Scope) error {
		if s == nil {
			return nil
		}
		if _, ok := seen[s]; ok {
			return nil
		}
		seen[s] = struct{}{}
		if !sameExecutionNode(s.NodeInfo, current) || s.NodeInfo.Mcpu > 1 {
			return moerr.NewNotSupportedNoCtx("bounded scalar set reaggregation requires coordinator-local DOP 1")
		}
		for _, child := range s.PreScopes {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	for _, s := range scopes {
		if err := visit(s); err != nil {
			return err
		}
	}
	return nil
}
