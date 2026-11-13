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
	"reflect"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"go.uber.org/zap"
)

func (c *Compile) generateNodes(node *plan.Node) (engine.Nodes, error) {
	rel, _, ctx, err := c.handleDbRelContext(node, false)
	if err != nil {
		return nil, err
	}

	forceSingle := forceSingleScan(node)
	if node.NodeType == plan.Node_TABLE_CLONE {
		forceSingle = true
	}

	stats := toScheduleScanStats(node)
	var createdInTxn bool
	if creator, ok := rel.(engine.RelationCreatedInCurrentTxn); ok {
		createdInTxn, err = creator.CreatedInCurrentTxn(ctx)
		if err != nil {
			return nil, err
		}
	}
	if createdInTxn {
		// Remote CNs cannot see the coordinator's uncommitted catalog changes.
		// A COPY ALTER replacement may even resolve to the old, same-name table.
		// Keep this scan local without serializing its intra-CN readers.
		if stats == nil {
			stats = &schedule.ScanStats{}
		}
		stats.ForceOneCN = true
	}
	scanRequest := schedule.ScanRequest{
		QueryWorkers:         c.scheduledQueryWorkers(),
		CurrentCN:            c.currentCNWorker(),
		QueryPlacementReason: c.queryPlacement.Reason,
		Stats:                stats,
		ForceSingle:          forceSingle,
		ForceMultiCN:         plan2.GetForceScanOnMultiCN(),
		OneCNBlockThreshold:  int32(plan2.BlockThresholdForOneCN),
	}
	scanPlacement := schedule.DecideScanPlacement(scanRequest)
	if createdInTxn && len(scanPlacement.Workers) > 0 &&
		!c.canUseLocalExecutionRoute(scanPlacement.Workers[0]) {
		return nil, moerr.NewInternalError(ctx,
			"scan of a table created in the current transaction requires the coordinator CN")
	}
	c.schedulingTrace.RecordScan(c.schedulingAttempt, scanRequest, scanPlacement)
	c.recordScanSchedulingMetrics(scanPlacement, stats, forceSingle)
	c.maybeLogScanPlacement(scanPlacement, stats, forceSingle)
	if scanPlacement.LocalOnly {
		return c.localScanNodes(scanPlacement.Workers, stats, forceSingle, node, rel)
	}

	return c.materializeScanNodes(scanPlacement.Workers, stats, node, rel)
}

func forceSingleScan(node *plan.Node) bool {
	if len(node.AggList) == 0 {
		return false
	}
	partialResults, _, _ := checkAggOptimize(node)
	if partialResults != nil {
		return true
	}
	if node.Stats != nil && node.Stats.ForceOneCN {
		return true
	}
	return plan2.RequiresSingleStageDistinctAgg(node)
}

func (c *Compile) localScanNodes(
	workers schedule.Workers,
	stats *schedule.ScanStats,
	forceSingle bool,
	node *plan.Node,
	rel engine.Relation,
) (engine.Nodes, error) {
	mcpu := 1
	if stats != nil && stats.Dop > 0 {
		mcpu = int(stats.Dop)
	}
	if forceSingle {
		mcpu = 1
	}
	if len(workers) == 0 {
		return engine.Nodes{{
			Addr:  c.addr,
			Mcpu:  normalizeMcpu(mcpu),
			CNCNT: 1,
		}}, nil
	}

	engNode := c.materializeScheduledWorker(workers[0])
	engNode.Mcpu = normalizeMcpu(mcpu)
	engNode.CNCNT = 1
	engNode.CNIDX = 0
	if engNode.Addr != "" && engNode.Addr != c.addr {
		remoteTombstones := remoteScanTombstoneAttacher{
			c:    c,
			node: node,
			rel:  rel,
		}
		if err := remoteTombstones.attach(&engNode); err != nil {
			return nil, err
		}
	}
	return engine.Nodes{engNode}, nil
}

func (c *Compile) materializeScanNodes(
	workers schedule.Workers,
	stats *schedule.ScanStats,
	node *plan.Node,
	rel engine.Relation,
) (engine.Nodes, error) {
	remoteTombstones := remoteScanTombstoneAttacher{
		c:    c,
		node: node,
		rel:  rel,
	}
	nodes := make(engine.Nodes, 0, len(workers))
	for i := range workers {
		mcpu := normalizeMcpu(workers[i].Mcpu)
		if stats != nil && stats.Dop > 0 {
			mcpu = min(mcpu, int(stats.Dop))
		}
		engNode := c.materializeScheduledWorker(workers[i])
		engNode.Mcpu = mcpu
		engNode.CNCNT = int32(len(workers))
		engNode.CNIDX = int32(i)
		if engNode.Addr != c.addr {
			if err := remoteTombstones.attach(&engNode); err != nil {
				return nil, err
			}
		}
		nodes = append(nodes, engNode)
	}
	return nodes, nil
}

type remoteScanTombstoneAttacher struct {
	c          *Compile
	node       *plan.Node
	rel        engine.Relation
	collected  bool
	tombstones engine.Tombstoner
}

// The certificate is coordinator-local. Only the embedded RelData is encoded;
// the receiver reconstructs committed deletions at its admitted snapshot.
type m0SingleRemoteReadData struct {
	engine.RelData
	validate func() error
	offset   int
	node     *plan.Node
	snapshot timestamp.Timestamp
}

func validateM0SingleRemoteReadData(s *Scope) error {
	if s == nil {
		return nil
	}
	if activeM0Enabled && s.NodeInfo.CNCNT == 1 && s.DataSource != nil && m0SingleRemoteScanNode(s.DataSource.node) {
		if _, ok := s.NodeInfo.Data.(*m0SingleRemoteReadData); !ok {
			return moerr.NewInternalErrorNoCtx("M0: single remote scan lacks workspace certificate")
		}
	}
	if data, ok := s.NodeInfo.Data.(*m0SingleRemoteReadData); ok {
		if data == nil || data.RelData == nil || data.validate == nil || data.node == nil {
			return moerr.NewInternalErrorNoCtx("M0: single remote read certificate is incomplete")
		}
		if s.Proc != nil && s.Proc.Ctx != nil {
			if err := s.Proc.Ctx.Err(); err != nil {
				return err
			}
		}
		if s.TxnOffset != data.offset || s.NodeInfo.CNCNT != 1 || s.DataSource == nil || s.DataSource.node == nil ||
			!s.DataSource.Timestamp.Equal(data.snapshot) ||
			s.DataSource.node.NodeType != data.node.NodeType || !proto.Equal(s.DataSource.node.ObjRef, data.node.ObjRef) ||
			!proto.Equal(s.DataSource.node.TableDef, data.node.TableDef) || !proto.Equal(s.DataSource.node.ScanSnapshot, data.node.ScanSnapshot) {
			return moerr.NewTxnNeedRetryNoCtx()
		}
		if err := data.validate(); err != nil {
			return err
		}
	}
	for _, child := range s.PreScopes {
		if err := validateM0SingleRemoteReadData(child); err != nil {
			return err
		}
	}
	return nil
}

func m0HasSingleRemoteScan(s *Scope) bool {
	var hasSingleScan func(*Scope) bool
	hasSingleScan = func(scope *Scope) bool {
		if scope == nil {
			return false
		}
		if scope.NodeInfo.CNCNT == 1 && scope.DataSource != nil && m0SingleRemoteScanNode(scope.DataSource.node) {
			return true
		}
		for _, child := range scope.PreScopes {
			if hasSingleScan(child) {
				return true
			}
		}
		return false
	}
	return hasSingleScan(s)
}

func m0SingleRemoteScanNode(node *plan.Node) bool {
	return node != nil && (node.NodeType == plan.Node_TABLE_SCAN || node.NodeType == plan.Node_TABLE_CLONE)
}

func m0SingleRemoteScanOffsetsZero(s *Scope) bool {
	if s == nil {
		return true
	}
	if s.NodeInfo.CNCNT == 1 && s.DataSource != nil && m0SingleRemoteScanNode(s.DataSource.node) && s.TxnOffset != 0 {
		return false
	}
	for _, child := range s.PreScopes {
		if !m0SingleRemoteScanOffsetsZero(child) {
			return false
		}
	}
	return true
}

func validateM0SingleRemoteDispatch(s *Scope, c *Compile) error {
	if !activeM0Enabled || c == nil || !m0HasSingleRemoteScan(s) {
		return nil
	}
	if s.Proc != nil && s.Proc.Ctx != nil {
		if err := s.Proc.Ctx.Err(); err != nil {
			return err
		}
	}
	// Scope scan offsets are not encoded. Require the value reconstructed by
	// the remote scope and compile to match every single-remote scan.
	if c.TxnOffset != 0 || !m0SingleRemoteScanOffsetsZero(s) {
		return moerr.NewTxnNeedRetryNoCtx()
	}
	if c.proc == nil {
		return moerr.NewInternalErrorNoCtx("M0: single remote scan lacks workspace authority")
	}
	txnOp := c.proc.GetTxnOperator()
	if txnOp == nil {
		return moerr.NewInternalErrorNoCtx("M0: single remote scan lacks workspace authority")
	}
	stamper, ok := txnOp.(interface {
		CatalogReadStamp() (client.CatalogReadStamp, error)
	})
	if !ok {
		return moerr.NewInternalErrorNoCtx("M0: single remote scan lacks transaction generation")
	}
	stamp, err := stamper.CatalogReadStamp()
	if err != nil {
		return err
	}
	workspace := stamp.Workspace
	if workspace == nil {
		return moerr.NewInternalErrorNoCtx("M0: single remote scan lacks workspace authority")
	}
	visibility, ok := workspace.(interface{ CatalogVisibility() (uint64, bool) })
	if !ok {
		return moerr.NewInternalErrorNoCtx("M0: single remote scan lacks workspace visibility")
	}
	revision, stable := visibility.CatalogVisibility()
	if !stable || !workspace.Readonly() {
		return moerr.NewTxnNeedRetryNoCtx()
	}
	after, stable := visibility.CatalogVisibility()
	if !stable || revision != after {
		return moerr.NewTxnNeedRetryNoCtx()
	}
	return nil
}

// The visibility revision and transaction stamp bind both checks to the same
// attempt. Readonly by itself can return to true after a statement boundary or
// snapshot transition without proving the encoded scope is still current.
func captureM0SingleRemoteDispatch(s *Scope, c *Compile) (func() error, error) {
	if !activeM0Enabled || c == nil || !m0HasSingleRemoteScan(s) {
		return nil, nil
	}
	if c.proc == nil {
		return nil, moerr.NewInternalErrorNoCtx("M0: single remote scan lacks transaction owner")
	}
	op := c.proc.GetTxnOperator()
	if op == nil || !reflect.ValueOf(op).Comparable() {
		return nil, moerr.NewInternalErrorNoCtx("M0: single remote scan has unbound transaction identity")
	}
	if err := validateM0SingleRemoteDispatch(s, c); err != nil {
		return nil, err
	}
	currentOp := c.proc.GetTxnOperator()
	if !reflect.ValueOf(currentOp).Comparable() || currentOp != op {
		return nil, moerr.NewTxnNeedRetryNoCtx()
	}
	stamper, ok := op.(interface {
		CatalogReadStamp() (client.CatalogReadStamp, error)
	})
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("M0: single remote scan lacks transaction generation")
	}
	stamp, err := stamper.CatalogReadStamp()
	if err != nil {
		return nil, err
	}
	workspace := stamp.Workspace
	if workspace == nil || !reflect.ValueOf(workspace).Comparable() {
		return nil, moerr.NewInternalErrorNoCtx("M0: single remote scan has unbound workspace identity")
	}
	visibility, ok := workspace.(interface{ CatalogVisibility() (uint64, bool) })
	if !ok {
		return nil, moerr.NewTxnNeedRetryNoCtx()
	}
	revision, stable := visibility.CatalogVisibility()
	if !stable || !workspace.Readonly() {
		return nil, moerr.NewTxnNeedRetryNoCtx()
	}
	if after, stable := visibility.CatalogVisibility(); !stable || after != revision {
		return nil, moerr.NewTxnNeedRetryNoCtx()
	}
	return func() error {
		if err := validateM0SingleRemoteDispatch(s, c); err != nil {
			return err
		}
		currentOp := c.proc.GetTxnOperator()
		if !reflect.ValueOf(currentOp).Comparable() || currentOp != op {
			return moerr.NewTxnNeedRetryNoCtx()
		}
		current, err := stamper.CatalogReadStamp()
		if err != nil {
			return err
		}
		if current.Workspace == nil || !reflect.ValueOf(current.Workspace).Comparable() || current.Workspace != workspace ||
			current.TransactionID != stamp.TransactionID || current.Revision != stamp.Revision || !current.Snapshot.Equal(stamp.Snapshot) {
			return moerr.NewTxnNeedRetryNoCtx()
		}
		if after, stable := visibility.CatalogVisibility(); !stable || after != revision {
			return moerr.NewTxnNeedRetryNoCtx()
		}
		return nil
	}, nil
}

func (a *remoteScanTombstoneAttacher) attach(node *engine.Node) error {
	// A single remote reader rebuilds its own ranges and cannot reconstruct
	// coordinator-local workspace writes. Recheck after placement because the
	// workspace may have become writable since scheduling. A retry can then
	// choose the coordinator; do not ship an incomplete scan.
	if activeM0Enabled && node.CNCNT == 1 && a.c.executionRequiresCurrentCN() {
		return moerr.NewTxnNeedRetryNoCtx()
	}
	if activeM0Enabled && node.CNCNT == 1 && m0SingleRemoteScanNode(a.node) {
		// The receiver can rebuild committed deletes per member at the bound
		// snapshot. Coordinator-local deletes cannot be reconstructed there.
		// Prove the visible workspace contribution is empty before omitting
		// committed tombstones from the unchanged wire payload.
		offset := a.c.TxnOffset
		boundNode := &plan.Node{NodeType: a.node.NodeType}
		if a.node.ObjRef != nil {
			boundNode.ObjRef = proto.Clone(a.node.ObjRef).(*plan.ObjectRef)
		}
		if a.node.TableDef != nil {
			boundNode.TableDef = proto.Clone(a.node.TableDef).(*plan.TableDef)
		}
		if a.node.ScanSnapshot != nil {
			boundNode.ScanSnapshot = proto.Clone(a.node.ScanSnapshot).(*plan.Snapshot)
		}
		probe := &Scope{Proc: a.c.proc, NodeInfo: engine.Node{CNCNT: 1}, DataSource: &Source{node: a.node}}
		validate, err := captureM0SingleRemoteDispatch(probe, a.c)
		if err != nil {
			return err
		}
		if validate == nil {
			return moerr.NewInternalErrorNoCtx("M0: single remote read cannot bind workspace")
		}
		stamper, ok := a.c.proc.GetTxnOperator().(interface {
			CatalogReadStamp() (client.CatalogReadStamp, error)
		})
		if !ok {
			return moerr.NewInternalErrorNoCtx("M0: single remote read has no transaction stamp")
		}
		stamp, err := stamper.CatalogReadStamp()
		if err != nil {
			return err
		}
		if stamp.TransactionID == "" || stamp.Snapshot.IsEmpty() || stamp.Workspace == nil {
			return moerr.NewInternalErrorNoCtx("M0: single remote read has no transaction snapshot")
		}
		// A Scope's scan TxnOffset is not serialized. The receiver therefore
		// observes offset zero; admit reconstruction only when that is also the
		// sender's complete visible workspace prefix. Later writes made by this
		// statement are outside that prefix and need not be sent to the reader.
		owner := a.c
		validateAttempt := func() error {
			if err := validate(); err != nil {
				return err
			}
			if offset != 0 || owner.TxnOffset != offset {
				return moerr.NewTxnNeedRetryNoCtx()
			}
			return nil
		}
		if err = validateAttempt(); err != nil {
			return err
		}
		snapshot := stamp.Snapshot
		if requested := boundNode.ScanSnapshot.GetTS(); requested != nil && !requested.IsEmpty() && requested.Less(snapshot) {
			snapshot = *requested
		}
		tombstones, err := collectTombstones(a.c, a.node, a.rel, engine.Policy_CollectUncommittedTombstones)
		if err != nil {
			return err
		}
		if tombstones == nil {
			return moerr.NewInternalErrorNoCtx("M0: single remote workspace deletes unavailable")
		}
		if tombstones.HasAnyInMemoryTombstone() || tombstones.HasAnyTombstoneFile() {
			return moerr.NewTxnNeedRetryNoCtx()
		}
		if err = validateAttempt(); err != nil {
			return err
		}
		data := readutil.BuildEmptyRelData()
		if err = data.AttachTombstones(tombstones); err != nil {
			return err
		}
		node.Data = &m0SingleRemoteReadData{RelData: data, validate: validateAttempt, offset: offset, node: boundNode, snapshot: snapshot}
		return nil
	}
	if !a.collected {
		tombstones, err := collectTombstones(a.c, a.node, a.rel, engine.Policy_CollectAllTombstones)
		if err != nil {
			return err
		}
		a.tombstones = tombstones
		a.collected = true
	}
	node.Data = readutil.BuildEmptyRelData()
	_ = node.Data.AttachTombstones(a.tombstones)
	return nil
}

func toScheduleScanStats(node *plan.Node) *schedule.ScanStats {
	if node == nil || node.Stats == nil {
		return nil
	}
	return &schedule.ScanStats{
		BlockNum:   node.Stats.BlockNum,
		Dop:        node.Stats.Dop,
		ForceOneCN: node.Stats.ForceOneCN,
	}
}

func (c *Compile) maybeLogScanPlacement(
	decision schedule.ScanDecision,
	stats *schedule.ScanStats,
	forceSingle bool,
) {
	if c.execType != plan2.ExecTypeAP_MULTICN || !decision.LocalOnly || !shouldWarnScanPlacement(decision.Reason) {
		return
	}

	fields := []zap.Field{
		zap.String("reason", decision.Reason),
		zap.String("query-placement-reason", c.queryPlacement.Reason),
		zap.String("exec-type", queryExecTypeString(c.execType)),
		zap.String("current-cn-policy", c.queryPlacement.CurrentCNPolicy.String()),
		zap.Int("query-worker-count", len(c.cnList)),
		zap.Int("scan-worker-count", len(decision.Workers)),
		zap.Bool("force-single", forceSingle),
		zap.Bool("is-internal", c.isInternal),
	}
	currentCN := c.currentCNWorker()
	fields = append(fields,
		zap.String("current-cn-id", currentCN.ID),
		zap.String("current-cn-address", currentCN.Addr),
	)
	if stats != nil {
		fields = append(fields,
			zap.Int32("block-num", stats.BlockNum),
			zap.Int32("dop", stats.Dop),
			zap.Bool("force-one-cn", stats.ForceOneCN),
		)
	}
	if len(decision.Workers) > 0 {
		fields = append(fields,
			zap.String("selected-worker-id", decision.Workers[0].ID),
			zap.String("selected-worker-address", decision.Workers[0].Addr),
		)
	}

	getQueryScheduleLogger().WarnWithConfig(
		"scan-schedule-local-only-"+decision.Reason,
		"scan schedule kept execution on a single CN",
		queryScheduleLogRateLimit,
		fields...)
}

func shouldWarnScanPlacement(reason string) bool {
	switch reason {
	case schedule.ReasonScanNoWorkers,
		schedule.ReasonScanMissingStats,
		schedule.ReasonScanSingleWorker,
		schedule.ReasonScanQueryLocalExec,
		schedule.ReasonScanQueryFallbackCN:
		return true
	default:
		return false
	}
}
