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
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertunique"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightdedupjoin"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

func validateODKUPlanNode(n *plan.Node) error {
	if n != nil {
		if err := preinsertunique.ValidateODKUContext(n.PreInsertUkCtx); err != nil {
			return err
		}
		if c := n.PreInsertUkCtx.GetOdkuActions(); c != nil &&
			len(n.ProjectList) != len(c.SourceColumns)+len(c.OldColumns) {
			return moerr.NewInternalErrorNoCtx("invalid ODKU output layout")
		}
	}
	if n != nil && n.NodeType == plan.Node_JOIN && n.JoinType == plan.Node_DEDUP {
		return colexec.ValidateDedupAction(n.OnDuplicateAction)
	}
	return nil
}

// validateODKUQueryProtocol also identifies queries that need an action owner.
func validateODKUQueryProtocol(q *plan.Query) (bool, error) {
	ordered := false
	for _, n := range q.GetNodes() {
		if err := validateODKUPlanNode(n); err != nil {
			return false, err
		}
		ordered = ordered || n.GetPreInsertUkCtx().GetOdkuActions() != nil
	}
	return ordered, nil
}

func (c *Compile) preflightODKU() (*preinsertunique.PreInsertUnique, error) {
	q := c.pn.GetQuery()
	ordered, err := validateODKUQueryProtocol(q)
	if err != nil {
		return nil, err
	}
	var owner *preinsertunique.PreInsertUnique
	checkOwner := func(s *Scope, candidate *preinsertunique.PreInsertUnique) error {
		if candidate == owner {
			return nil
		}
		if s.IsRemote || s.NodeInfo.Mcpu > 1 || !sameExecutionAddr(s.NodeInfo.Addr, c.addr) {
			return moerr.NewInternalError(c.proc.Ctx, "ODKU action owner must execute on the coordinator")
		}
		if owner != nil {
			return moerr.NewInternalError(c.proc.Ctx, "ODKU requires one action owner")
		}
		owner = candidate
		return nil
	}
	validated := map[*plan.Query]struct{}{q: {}}
	for _, scope := range c.scopes {
		if err := validateODKUScopeProtocol(scope, validated, checkOwner); err != nil {
			return nil, err
		}
	}
	if ordered && owner == nil {
		return nil, moerr.NewInternalError(c.proc.Ctx, "ODKU action owner is missing")
	}
	return owner, nil
}

// Root scopes of a compiled statement normally share the same query. Check
// each distinct query once, without skipping validation of any operator tree.
func validateODKUScopeProtocol(s *Scope, validated map[*plan.Query]struct{}, checkOwner func(*Scope, *preinsertunique.PreInsertUnique) error) error {
	if s == nil {
		return nil
	}
	var visit func(vm.Operator) error
	visit = func(op vm.Operator) error {
		if op == nil {
			return nil
		}
		switch x := op.(type) {
		case *preinsertunique.PreInsertUnique:
			if x.PreInsertCtx.GetOdkuActions() != nil {
				if err := preinsertunique.ValidateODKUContext(x.PreInsertCtx); err != nil {
					return err
				}
				if checkOwner != nil {
					if err := checkOwner(s, x); err != nil {
						return err
					}
				}
			}
		case *dedupjoin.DedupJoin:
			if e := colexec.ValidateDedupAction(x.OnDuplicateAction); e != nil {
				return e
			}
		case *rightdedupjoin.RightDedupJoin:
			if e := colexec.ValidateDedupAction(x.OnDuplicateAction); e != nil {
				return e
			}
		case *hashbuild.HashBuild:
			if x.IsDedup {
				if e := colexec.ValidateDedupAction(x.OnDuplicateAction); e != nil {
					return e
				}
			}
		}
		for _, child := range op.GetOperatorBase().Children {
			if e := visit(child); e != nil {
				return e
			}
		}
		return nil
	}
	if e := visit(s.RootOp); e != nil {
		return e
	}
	if q := s.Plan.GetQuery(); q != nil {
		if _, ok := validated[q]; !ok {
			if _, err := validateODKUQueryProtocol(q); err != nil {
				return err
			}
			validated[q] = struct{}{}
		}
	}

	for _, c := range s.PreScopes {
		if e := validateODKUScopeProtocol(c, validated, checkOwner); e != nil {
			return e
		}
	}
	return nil
}
func validateODKUPipelineProtocol(p *pipeline.Pipeline) error {
	if p == nil {
		return nil
	}
	if _, err := validateODKUQueryProtocol(p.Qry.GetQuery()); err != nil {
		return err
	}

	for _, i := range p.InstructionList {
		if err := preinsertunique.ValidateODKUContext(i.GetPreInsertUnique().GetPreInsertUkCtx()); err != nil {
			return err
		}
		if d := i.GetDedupJoin(); d != nil {
			if e := colexec.ValidateDedupAction(d.OnDuplicateAction); e != nil {
				return e
			}
		}
		if d := i.GetRightDedupJoin(); d != nil {
			if e := colexec.ValidateDedupAction(d.OnDuplicateAction); e != nil {
				return e
			}
		}
		if h := i.GetHashBuild(); h != nil && h.IsDedup {
			if e := colexec.ValidateDedupAction(h.OnDuplicateAction); e != nil {
				return e
			}
		}
	}
	for _, c := range p.Children {
		if e := validateODKUPipelineProtocol(c); e != nil {
			return e
		}
	}
	return nil
}
