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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertunique"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	materializedWriterSiteData mpool.AllocationSite = iota + 1
	materializedWriterSiteArea
	materializedWriterSiteNulls
	materializedWriterSiteGrouping
)

func (c *Compile) bindODKUPriorData() error {
	var visit func(*Scope) error
	seen := make(map[*preinsertunique.PreInsertUnique]struct{})
	visit = func(s *Scope) error {
		if s == nil {
			return nil
		}
		err := vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
			owner, ok := op.(*preinsertunique.PreInsertUnique)
			if !ok || owner.PreInsertCtx.GetOdkuActions().GetPriorData() == nil {
				return nil
			}
			if _, exists := seen[owner]; exists {
				return nil
			}
			seen[owner] = struct{}{}
			if s.IsRemote || !sameExecutionAddr(s.NodeInfo.Addr, c.addr) {
				return moerr.NewInternalError(c.proc.Ctx, "ODKU action owner must execute on the coordinator")
			}
			if c.allocationAttempt == nil {
				return moerr.NewInternalError(c.proc.Ctx, "ODKU prior data has no allocation attempt")
			}
			actions := owner.PreInsertCtx.OdkuActions
			refs := []*plan.ObjectRef{actions.PriorData.Ref}
			for _, key := range actions.Constraints {
				refs = append(refs, key.LookupRef)
			}
			relations := make([]engine.Relation, 0, len(refs))
			byID := make(map[int64]engine.Relation, len(refs))
			for _, ref := range refs {
				if byID[ref.Obj] != nil {
					continue
				}
				relation, e := colexec.GetRelAndPartitionRelsByObjRef(c.proc.Ctx, c.proc, c.e, ref)
				if e != nil {
					return e
				}
				if relation.GetTableID(c.proc.Ctx) != uint64(ref.Obj) {
					return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
				}
				byID[ref.Obj] = relation
				relations = append(relations, relation)
			}
			provider, ok := relations[0].(engine.StatementKeyViewProvider)
			if !ok {
				return moerr.NewNotSupported(c.proc.Ctx, "engine does not support a statement key view")
			}
			view, e := provider.NewStatementKeyView(c.proc.Ctx, relations, c.proc, c.allocationAttempt.account, func(bytes uint64) (engine.StatementViewReservation, error) {
				generation, err := c.proc.GetHashBuildBudget()
				if err != nil {
					return nil, err
				}
				return generation.ReserveMemory(c.proc.Ctx, bytes)
			})
			if e != nil {
				return e
			}
			c.odkuViews = append(c.odkuViews, view)
			owner.PriorDataView = view
			owner.LockKeys = func(proc *process.Process, slot int, keys *vector.Vector) error {
				ref := actions.Constraints[slot].LookupRef
				b := batch.NewWithSize(1)
				b.Vecs[0] = keys
				b.SetRowCount(keys.Length())
				return lockop.LockRows(c.e, proc, byID[ref.Obj], uint64(ref.Obj), b, 0, *keys.GetType(), lock.LockMode_Exclusive, lock.Sharding_None, 0)
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, pre := range s.PreScopes {
			if err = visit(pre); err != nil {
				return err
			}
		}
		return nil
	}
	for _, s := range c.scopes {
		if err := visit(s); err != nil {
			return err
		}
	}
	return nil
}

func (c *Compile) closeODKUPriorData() error {
	if len(c.odkuViews) == 0 {
		return nil
	}
	var result error
	for _, view := range c.odkuViews {
		result = joinAllocationLifecycleErrors(result, view.Close())
	}
	c.odkuViews = nil
	var detach func(*Scope)
	detach = func(scope *Scope) {
		if scope == nil {
			return
		}
		if scope.RootOp != nil {
			_ = vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
				if owner, ok := op.(*preinsertunique.PreInsertUnique); ok {
					owner.PriorDataView = nil
					owner.LockKeys = nil
				}
				return nil
			})
		}
		for _, child := range scope.PreScopes {
			detach(child)
		}
	}
	for _, scope := range c.scopes {
		detach(scope)
	}
	return result
}

// fenceODKUWriterInput retains the already-compiled, flat writer input in the
// existing materialized source. Thus all old base/regular/irregular identities
// finish reading before any writer runs, without a second materializer or
// another planner column namespace. Ordinary DML keeps its existing streaming.
func (c *Compile) fenceODKUWriterInput(ss []*Scope) []*Scope {
	needed := false
	if query := c.pn.GetQuery(); query != nil {
		for _, node := range query.Nodes {
			if node != nil && node.PreInsertUkCtx.GetOdkuActions().GetPriorData() != nil {
				needed = true
				break
			}
		}
	}
	if !needed {
		return ss
	}
	source := materialized.NewSource(1)
	if c.materializedSources == nil {
		c.materializedSources = make(map[int32]*materialized.Source)
	}
	key := int32(len(c.pn.GetQuery().Steps) + len(c.odkuWriterSources))
	c.materializedSources[key] = source
	c.odkuWriterSources = append(c.odkuWriterSources, source)
	producer := c.newMergeScope(c.groupShuffleBucketsByCNIfNeeded(ss))
	dispatch := constructDispatchLocal(true, true, false, false, nil)
	dispatch.MaterializedSource = source
	dispatch.SetAnalyzeControl(c.anal.curNodeIdx, false)
	producer.setRootOperator(dispatch)
	consumer := c.newEmptyMergeScope()
	consumer.Proc = c.proc.NewNoContextChildProc(0)
	consumer.PreScopes = []*Scope{producer}
	reader := merge.NewArgument().WithSinkScan(true)
	reader.MaterializedSource = source
	reader.MaterializedReaderID = 0
	reader.SetAnalyzeControl(c.anal.curNodeIdx, false)
	consumer.setRootOperator(reader)
	return []*Scope{consumer}
}
