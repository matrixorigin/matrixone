// Copyright 2026 Matrix Origin
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

package compile

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (c *Compile) compileEmbeddedSiriusRead(ctx context.Context, queryPlan *planpb.Plan, runtime *SiriusRuntime) (bool, error) {
	if queryPlan == nil || queryPlan.GetQuery() == nil || !siriusPlanEligible(queryPlan) {
		return false, moerr.NewNotSupported(ctx, "statement is not eligible for embedded Sirius")
	}
	candidate, err := substrait.ExportEmbeddedMO(queryPlan.GetQuery())
	if err != nil {
		return false, err // explicit selection never falls back
	}
	reads, err := candidate.EmbeddedMOReads()
	if err != nil {
		return false, err
	}
	if len(reads) == 0 || len(reads) > 16 {
		return false, moerr.NewNotSupported(ctx, "embedded Sirius requires one to sixteen MO read bindings")
	}
	txn := c.proc.GetTxnOperator()
	if txn == nil || txn.GetWorkspace() == nil {
		return false, moerr.NewInternalError(ctx, "embedded Sirius has no statement transaction")
	}
	account, err := defines.GetAccountId(ctx)
	if err != nil {
		return false, err
	}
	bindings := make(map[int32]substrait.EmbeddedReadBinding, len(reads))
	descriptors := make([]SiriusReadDescriptor, len(reads))
	for i, read := range reads {
		binding := uint64(i + 1)
		bindings[read.NodeID] = substrait.EmbeddedReadBinding{BindingID: binding, Source: substrait.EmbeddedReadMO}
		columns := make([]SiriusReadColumn, len(read.Columns))
		for j, col := range read.Columns {
			columns[j] = SiriusReadColumn{Type: col.Type, Name: col.Name, PhysicalID: col.PhysicalID, Sequence: col.Sequence}
		}
		// Capture an execution-local specification, never a pooled Compile.
		// Relation/readers and their mutable process are created only when
		// Query.Run invokes this producer after successful native admission.
		spec := siriusReaderSpec{
			parent: c.proc, e: c.e, addr: c.addr, db: c.db, sql: c.sql,
			tenant: c.tenant, uid: c.uid, txnReadView: c.TxnReadView,
			ncpu: max(1, c.ncpu), node: plan2.DeepCopyNode(queryPlan.GetQuery().Nodes[read.NodeID]),
			columns: columns,
		}
		descriptors[i] = SiriusReadDescriptor{BindingID: binding, Database: read.Database, Table: read.Table, Schema: read.Schema, Columns: columns, Producer: spec.run}
	}
	wire, err := candidate.BuildEmbedded(bindings)
	if err != nil {
		return false, err
	}
	snapshotTS := txn.Txn().SnapshotTS
	if c.hasPlanSnapshotTS {
		snapshotTS = c.planSnapshotTS
	}
	statementSnapshot := types.TimestampToTS(snapshotTS)
	snapshot, err := statementSnapshot.Marshal()
	if err != nil {
		return false, err
	}
	var snapshotID [12]byte
	copy(snapshotID[:], snapshot)
	id := c.proc.GetStmtProfile().GetStmtId()
	requestTimeout := runtime.RequestTimeout
	if requestTimeout <= 0 {
		requestTimeout = 15 * time.Minute
	}
	deadline := time.Now().Add(requestTimeout)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	execution, err := runtime.Backend.Prepare(ctx, SiriusPrepareRequest{
		AccountID: uint64(account), QueryID: append([]byte(nil), id[:]...), Snapshot: snapshotID,
		Plan: wire, OutputTypes: candidate.OutputTypes(), Headings: append([]string(nil), queryPlan.GetQuery().Headings...),
		Reads: descriptors, Deadline: deadline,
	})
	if err != nil {
		return false, err
	}
	c.siriusRead = newSiriusReadOwner(execution, runtime)
	return true, nil
}

type siriusReaderSpec struct {
	parent                     *process.Process
	e                          engine.Engine
	addr, db, sql, tenant, uid string
	txnReadView                client.WorkspaceReadView
	ncpu                       int
	node                       *planpb.Node
	columns                    []SiriusReadColumn
}

func (s siriusReaderSpec) run(ctx context.Context, input SiriusInput) (err error) {
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	proc := s.parent.NewViewBindingProcess(ctx)
	c := allocateNewCompile(proc)
	defer c.Release()
	defer c.FreeOperator()
	c.e, c.addr, c.db, c.sql, c.tenant, c.uid = s.e, s.addr, s.db, s.sql, s.tenant, s.uid
	c.TxnReadView, c.ncpu, c.disableRetry = s.txnReadView, s.ncpu, true
	c.lockMeta = NewLockMeta()
	proc.SetMessageBoard(c.MessageBoard)
	c.planSnapshotTS, c.hasPlanSnapshotTS = proc.GetPlanSnapshotTS()
	c.applyPlanSnapshot()
	proc.BuildPipelineContext(ctx)
	node := s.node
	// GPU joins own their runtime filters. No corresponding MO sender will
	// execute, so a normal scan's probe/message waits would deadlock here.
	node.RuntimeFilterProbeList, node.RuntimeFilterBuildList, node.RecvMsgList, node.SendMsgList = nil, nil, nil, nil
	node.AggList = nil // never replace scan rows with MO aggregate shortcuts
	node.NodeId = 0
	query := &planpb.Query{StmtType: planpb.Query_SELECT, Nodes: []*planpb.Node{node}, Steps: []int32{0}}
	c.pn = &planpb.Plan{Plan: &planpb.Plan_Query{Query: query}}
	c.execType = plan2.ExecTypeAP_ONECN
	plan2.CalcQueryDOP(c.pn, int32(c.ncpu), 1, c.execType)
	c.initAnalyzeModule(query)
	c.setAnalyzeCurrent(nil, 0)
	dop := 1
	if node.Stats != nil {
		dop = max(1, min(c.ncpu, int(node.Stats.Dop)))
	}
	scope, err := c.compileTableScanWithNode(node, engine.Node{Addr: c.addr, Mcpu: dop, CNCNT: 1, CNIDX: 0}, true)
	if err != nil {
		return err
	}
	c.scopes = []*Scope{scope}
	if err := scope.initDataSource(c); err != nil {
		return err
	}
	if err := validateSiriusReaderSchema(node, scope.DataSource.TableDef); err != nil {
		return err
	}
	scopes := c.compileTableScanFiltersAndProjection(node, c.scopes)
	if node.Offset != nil {
		scopes = c.compileOffset(node, scopes)
	}
	if node.Limit != nil {
		scopes = c.compileLimit(node, scopes)
	}
	root := scopes[0]
	if !c.IsSingleScope(scopes) {
		// One scope may still have several scan workers. Output is a single
		// publisher and must remain above the parallel operator duplication.
		root = c.newMergeScope(scopes)
	}
	root.setRootOperator(output.NewArgument().WithBlock(false).WithFunc(func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
		return publishSiriusBatch(ctx, input, bat, s.columns)
	}))
	c.scopes = []*Scope{root}
	setContextForParallelScope(root, proc.Ctx, proc.Cancel)
	return c.run(root)
}

func validateSiriusReaderSchema(node *planpb.Node, actual *planpb.TableDef) error {
	if actual == nil || actual.TblId != node.TableDef.TblId || actual.Version != node.TableDef.Version {
		return moerr.NewInvalidStateNoCtx("Sirius reader table definition changed")
	}
	for _, expected := range node.TableDef.Cols {
		matched := false
		for _, col := range actual.Cols {
			if col.Seqnum == expected.Seqnum && col.ColId == expected.ColId && col.Name == expected.Name &&
				col.Typ.Id == expected.Typ.Id && col.Typ.Width == expected.Typ.Width && col.Typ.Scale == expected.Typ.Scale {
				matched = true
				break
			}
		}
		if !matched {
			return moerr.NewInvalidStateNoCtx("Sirius reader column definition changed")
		}
	}
	return nil
}
