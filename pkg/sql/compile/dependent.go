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
	"context"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"unsafe"
)

func (c *Compile) compileDependentQuery(q *plan.Query) ([]*Scope, error) {
	if err := c.proc.RequireDependentProtocol(); err != nil {
		return nil, err
	}
	c.execType = plan2.ExecTypeAP_ONECN
	c.foundRowsOwnerNode = c.selectFoundRowsOwnerNode(q)
	if err := plan.WalkQueryNodes(q, func(_ int32, n *plan.Node) error {
		if err := c.validateDMLTarget(n); err != nil {
			return err
		}
		if n.NodeType == plan.Node_INSERT || n.NodeType == plan.Node_MULTI_UPDATE {
			n.NotCacheable = true
		}
		if n.InsertCtx != nil && n.InsertCtx.Ref != nil {
			c.appendMetaTables(n.InsertCtx.Ref)
		}
		if n.DeleteCtx != nil && n.DeleteCtx.Ref != nil {
			c.appendMetaTables(n.DeleteCtx.Ref)
		}
		for _, u := range n.UpdateCtxList {
			if u.ObjRef != nil {
				c.appendMetaTables(u.ObjRef)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	c.initAnalyzeModule(q)
	template, err := dependent.Compile(c.proc.Ctx, q, c.compileDependentSource, c.compileDependentKernel)
	if err != nil {
		return nil, err
	}
	if c.foundRowsOwnerNode != nil {
		if err = template.CountFoundRowsAt(c.foundRowsOwnerNode.NodeId); err != nil {
			return nil, err
		}
	}
	s := newScope(Merge)
	s.Proc = c.proc.NewNoContextChildProc(0)
	s.NodeInfo = engine.Node{Addr: c.addr, Mcpu: 1, CNCNT: 1}
	op := &dependent.Operator{Template: template}
	root := q.Steps[q.MainOutputStep()]
	op.SetAnalyzeControl(int(root), true)
	s.setRootOperator(op)
	c.anal.curNodeIdx = int(root)
	return c.compileSteps(q, []*Scope{s}, root)
}

// Only source metadata is frozen here. Opening a source neither recompiles a
// scope nor runs another statement; it obtains readers from this transaction.
func (c *Compile) compileDependentSource(node *plan.Node) (dependent.SourceFactory, error) {
	if node.NodeType == plan.Node_EXTERNAL_SCAN {
		return c.compileDependentExternal(node)
	}
	if node.NodeType == plan.Node_VALUE_SCAN {
		return dependentSourceFactory(uint64(unsafe.Sizeof(value_scan.ValueScan{})), func(proc *process.Process, sourceAllocation *vector.AllocationAccountSelection) (vm.Operator, error) {
			op, err := constructValueScan(proc, node, sourceAllocation)
			if err != nil {
				return nil, err
			}
			op.SetAnalyzeControl(int(node.NodeId), true)
			return op, nil
		}), nil
	}
	if node.ObjRef != nil {
		c.appendMetaTables(node.ObjRef)
	}
	attrs := make([]string, len(node.TableDef.Cols))
	typs := make([]plan.Type, len(attrs))
	for i, col := range node.TableDef.Cols {
		attrs[i], typs[i] = col.GetOriginCaseName(), col.Typ
	}
	return dependentSourceFactory(uint64(unsafe.Sizeof(table_scan.TableScan{})), func(proc *process.Process, sourceAllocation *vector.AllocationAccountSelection) (vm.Operator, error) {
		ctx, txn := proc.Ctx, proc.GetTxnOperator()
		if node.ScanSnapshot != nil && node.ScanSnapshot.TS != nil && !node.ScanSnapshot.TS.IsEmpty() && node.ScanSnapshot.TS.Less(txn.Txn().SnapshotTS) {
			txn = txn.CloneSnapshotOp(*node.ScanSnapshot.TS)
			if node.ScanSnapshot.Tenant != nil {
				ctx = context.WithValue(ctx, defines.TenantIDKey{}, node.ScanSnapshot.Tenant.TenantID)
			}
		}
		if util.TableIsClusterTable(node.TableDef.TableType) || util.TableIsLoggingTable(node.ObjRef.SchemaName, node.ObjRef.ObjName) {
			ctx = defines.AttachAccountId(ctx, catalog.System_Account)
		}
		if node.ObjRef.PubInfo != nil {
			ctx = defines.AttachAccountId(ctx, uint32(node.ObjRef.PubInfo.TenantId))
		}
		db, err := c.e.Database(ctx, node.ObjRef.SchemaName, txn)
		if err != nil {
			return nil, err
		}
		relation, err := db.Relation(ctx, node.TableDef.Name, proc)
		if err != nil {
			return nil, err
		}
		data, err := relation.Ranges(ctx, engine.RangesParam{PreAllocBlocks: 2, TxnOffset: c.TxnOffset, Policy: engine.Policy_CollectAllData})
		if err != nil {
			return nil, err
		}
		readers, err := relation.BuildReaders(ctx, proc, nil, data, 1, c.TxnOffset, false, engine.Policy_CheckAll, engine.FilterHint{})
		if err != nil || len(readers) != 1 {
			for _, reader := range readers {
				if reader != nil {
					_ = reader.Close()
				}
			}
			if err == nil {
				err = moerr.NewInternalError(ctx, "dependent scan requires one reader")
			}
			return nil, err
		}
		op := table_scan.NewArgument().WithTypes(typs)
		op.OutputAllocation = sourceAllocation
		op.Reader, op.Attrs, op.TableID = readers[0], attrs, node.TableDef.TblId
		op.SetAnalyzeControl(int(node.NodeId), true)
		return op, nil
	}), nil
}

// Admission precedes operator construction and source I/O. The factory hands
// even a partially initialized operator to this single terminal owner.
func dependentSourceFactory(metadata uint64, open func(*process.Process, *vector.AllocationAccountSelection) (vm.Operator, error)) dependent.SourceFactory {
	return func(proc *process.Process, allocation *vector.AllocationAccountSelection, account *mpool.AllocationAccount) (dependent.Source, error) {
		budget, err := proc.GetExecutionResourceBudget()
		if err != nil {
			return nil, err
		}
		size := metadata + uint64(unsafe.Sizeof(dependentNativeSource{}))
		if size < metadata {
			return nil, moerr.NewOOM(proc.Ctx)
		}
		if err = budget.AcquireAllocationCapacity(size); err != nil {
			return nil, err
		}
		s := &dependentNativeSource{proc: proc, account: account, budget: budget, charged: size}
		s.op, err = open(proc, allocation)
		if err == nil && s.op == nil {
			err = moerr.NewInternalError(proc.Ctx, "dependent source factory returned no operator")
		}
		if err == nil && account != nil {
			if owner, ok := s.op.(interface {
				SetAllocationAccount(*mpool.AllocationAccount) error
			}); ok {
				err = owner.SetAllocationAccount(account)
			}
		}
		if err == nil {
			err = s.op.Prepare(proc)
		}
		if err != nil {
			return nil, errors.Join(err, s.CloseWithCause(err))
		}
		return s, nil
	}
}

type dependentNativeSource struct {
	proc     *process.Process
	op       vm.Operator
	account  *mpool.AllocationAccount
	budget   *process.ExecutionResourceGeneration
	charged  uint64
	closeErr error
}

func (s *dependentNativeSource) Read(ctx context.Context) (*batch.Batch, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	r, err := s.op.Call(s.proc)
	return r.Batch, err
}
func (s *dependentNativeSource) Close() error { return s.CloseWithCause(nil) }
func (s *dependentNativeSource) CloseWithCause(cause error) error {
	defer func() {
		if s.budget != nil {
			s.budget.ReleaseAllocationCapacity(s.charged)
			s.budget, s.charged = nil, 0
		}
	}()
	if s.op == nil {
		return s.closeErr
	}
	var err error
	// TableScan's ordinary Free logs reader errors. This owner observes Close
	// before publishing the scalar result, so preserve the actual reader error.
	if scan, ok := s.op.(*table_scan.TableScan); ok && scan.Reader != nil {
		err = scan.Reader.Close()
		scan.Reader = nil
	}
	if closer, ok := s.op.(interface{ CloseReader() error }); ok {
		err = errors.Join(err, closer.CloseReader())
	}
	terminal := errors.Join(cause, err)
	s.op.Reset(s.proc, terminal != nil, terminal)
	s.op.Free(s.proc, terminal != nil, terminal)
	if owner, ok := s.op.(interface {
		ClearAllocationAccount(*mpool.AllocationAccount) error
	}); ok && s.account != nil {
		err = errors.Join(err, owner.ClearAllocationAccount(s.account))
	}
	s.op.Release()
	s.op = nil
	s.closeErr = err
	return err
}
