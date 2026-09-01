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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/crt"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const loadUniqueIndexPromotionMinBytes = float64(1 << 30)

type loadUniqueIndexPromotionPhase uint8

const (
	loadUniqueIndexPromotionDisabled loadUniqueIndexPromotionPhase = iota
	loadUniqueIndexPromotionEligible
	loadUniqueIndexPromotionAcquiring
	loadUniqueIndexPromotionFenced
)

// loadUniqueIndexPromotionTarget keeps both identities involved in promotion:
// rowTarget is the exact canonical LockOp target that a later physical compile
// may omit, while tablePKType is the authoritative hidden-table primary-key
// type used for the full-domain lock.
type loadUniqueIndexPromotionTarget struct {
	rowTarget   *plan.LockTarget
	tablePKType plan.Type
}

// loadUniqueIndexPromotionState is owned by the root Compile for one Run. Retry
// compiles borrow the pointer only while the root remains alive. Run and retry
// compilation are coordinator-serialized, so no independent mutex is needed.
type loadUniqueIndexPromotionState struct {
	phase       loadUniqueIndexPromotionPhase
	logicalPlan *plan.Plan
	targets     []loadUniqueIndexPromotionTarget

	txnID                   []byte
	firstPhysicalGeneration uint64
	frontier                timestamp.Timestamp
	installedSnapshot       timestamp.Timestamp
}

func (s *loadUniqueIndexPromotionState) disable() {
	if s == nil {
		return
	}
	s.phase = loadUniqueIndexPromotionDisabled
	s.txnID = nil
	s.frontier = timestamp.Timestamp{}
	s.installedSnapshot = timestamp.Timestamp{}
}

func (s *loadUniqueIndexPromotionState) clear() {
	if s == nil {
		return
	}
	for i := range s.targets {
		s.targets[i].rowTarget = nil
	}
	s.targets = nil
	s.logicalPlan = nil
	s.disable()
}

func (c *Compile) clearLoadUniqueIndexPromotion() {
	if c.loadUniqueIndexPromotionOwner {
		c.loadUniqueIndexPromotion.clear()
	}
	c.loadUniqueIndexPromotion = nil
	c.loadUniqueIndexPromotionOwner = false
}

func (c *Compile) inheritLoadUniqueIndexPromotion(root *Compile) {
	c.loadUniqueIndexPromotion = root.loadUniqueIndexPromotion
	c.loadUniqueIndexPromotionOwner = false
}

// bindLoadUniqueIndexPromotionSnapshot gives the retried physical source
// generation the snapshot installed after the barrier. Ordinary retries keep
// the immutable logical-plan binding; only the exact completed local proof can
// authorize this narrow physical rebind.
func (c *Compile) bindLoadUniqueIndexPromotionSnapshot(runC *Compile, rebuildPlan bool) {
	state := c.loadUniqueIndexPromotion
	if rebuildPlan || state == nil || state.phase != loadUniqueIndexPromotionFenced ||
		runC == nil || runC.loadUniqueIndexPromotion != state ||
		state.logicalPlan != c.pn || !state.installedSnapshot.Greater(state.frontier) {
		return
	}
	runC.planSnapshotTS = state.installedSnapshot
	runC.hasPlanSnapshotTS = true
	runC.planGenerationReused = false
	runC.applyPlanSnapshot()
}

// prepareLoadUniqueIndexPromotion performs only atomic positive admission. A
// failed or ambiguous check leaves the ordinary canonical row-lock plan intact.
func (c *Compile) prepareLoadUniqueIndexPromotion(pn *plan.Plan) {
	if c.loadUniqueIndexPromotion != nil || c.executionGeneration != 0 ||
		c.isInternal || c.isPrepare || c.planGenerationReused || c.disableRetry ||
		pn == nil || pn.GetIsPrepare() || !loadUniqueIndexPromotionTopLevelCandidate(pn) {
		return
	}
	txnOp := c.proc.GetTxnOperator()
	if !loadUniqueIndexPromotionTxnEligible(txnOp) ||
		client.LockWaitTimeoutFromTxn(txnOp) <= 0 ||
		!supportsLoadLogtailReadBarrier(c.proc.GetService()) {
		return
	}
	if _, ok := loadLogtailReadBarrier(c.e); !ok {
		return
	}
	lockService := c.proc.GetLockService()
	if lockService == nil {
		return
	}
	maxRowLocks := float64(lockService.GetConfig().MaxLockRowCount)
	targets, ok := analyzeLoadUniqueIndexPromotionPlan(pn, maxRowLocks)
	if !ok {
		return
	}
	c.loadUniqueIndexPromotion = &loadUniqueIndexPromotionState{
		phase:       loadUniqueIndexPromotionEligible,
		logicalPlan: pn,
		targets:     targets,
	}
	c.loadUniqueIndexPromotionOwner = true
}

func loadUniqueIndexPromotionTopLevelCandidate(pn *plan.Plan) bool {
	qry := pn.GetQuery()
	// LoadWriteS3 selects an insert implementation and is not a lock-safety
	// property. In particular, the modern LOAD binder intentionally leaves it
	// unset. Promotion is instead admitted from the source, plan, transaction,
	// and lock-budget proofs below.
	return qry != nil && qry.StmtType == plan.Query_INSERT && qry.LoadTag &&
		!qry.HasForeignKeyAction && len(qry.DetectSqls) == 0
}

func loadUniqueIndexPromotionTxnEligible(txnOp client.TxnOperator) bool {
	if txnOp == nil {
		return false
	}
	meta := txnOp.Txn()
	opts := txnOp.TxnOptions()
	return len(meta.ID) > 0 && meta.Status == txn.TxnStatus_Active && !meta.Mirror &&
		meta.IsPessimistic() && meta.IsRCIsolation() &&
		opts.Autocommit && !opts.ByBegin
}

func supportsLoadLogtailReadBarrier(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	return ok && valid && version >= defines.MORPCVersion39
}

// loadLogtailReadBarrier unwraps EntireEngine before capability admission.
// EntireEngine itself exposes a forwarding method even when its underlying
// engine lacks the optional capability, so a direct type assertion would
// acquire hidden locks before discovering an unsupported engine.
func loadLogtailReadBarrier(eng engine.Engine) (engine.LogtailReadBarrier, bool) {
	for eng != nil {
		if entire, ok := eng.(*engine.EntireEngine); ok {
			if entire == nil || entire.Engine == nil {
				return nil, false
			}
			eng = entire.Engine
			continue
		}
		barrier, ok := eng.(engine.LogtailReadBarrier)
		return barrier, ok
	}
	return nil, false
}

func analyzeLoadUniqueIndexPromotionPlan(
	pn *plan.Plan,
	maxRowLocks float64,
) ([]loadUniqueIndexPromotionTarget, bool) {
	qry := pn.GetQuery()
	if !loadUniqueIndexPromotionTopLevelCandidate(pn) {
		return nil, false
	}

	reachable, ok := reachableLoadPlanNodes(qry)
	if !ok || len(qry.Steps) != 1 {
		return nil, false
	}
	var lockNode, updateNode *plan.Node
	var lockNodeID, updateNodeID, externalNodeID int32 = -1, -1, -1
	externalScans := 0
	for nodeID := range reachable {
		node := qry.Nodes[nodeID]
		switch node.NodeType {
		case plan.Node_LOCK_OP:
			if lockNode != nil {
				return nil, false
			}
			lockNode, lockNodeID = node, nodeID
		case plan.Node_MULTI_UPDATE:
			if updateNode != nil {
				return nil, false
			}
			updateNode, updateNodeID = node, nodeID
		case plan.Node_EXTERNAL_SCAN:
			if !loadEstimateEligible(node.Stats, maxRowLocks) ||
				!loadExternalSourceEligible(node) {
				return nil, false
			}
			externalScans++
			externalNodeID = nodeID
		}
	}
	if lockNode == nil || updateNode == nil || externalScans != 1 ||
		qry.Steps[0] != updateNodeID ||
		!planNodeDescendsFrom(qry.Nodes, updateNodeID, lockNodeID) ||
		!planNodeDescendsFrom(qry.Nodes, lockNodeID, externalNodeID) {
		return nil, false
	}

	base := lockNode.TableDef
	if !ordinaryLoadBaseTable(base, lockNode.LockTargets) {
		return nil, false
	}
	basePKType, ok := admittedLoadBasePKType(base)
	if !ok {
		return nil, false
	}

	baseTargets := 0
	rowTargetsByTable := make(map[uint64]*plan.LockTarget, len(lockNode.LockTargets))
	for _, target := range lockNode.LockTargets {
		if target == nil || target.TableId == 0 || target.ObjRef == nil ||
			target.Mode != lockpb.LockMode_Exclusive || target.HasPartitionCol {
			return nil, false
		}
		if target.TableId == base.TblId {
			if !target.LockTable || !planTypeEqual(target.PrimaryColTyp, basePKType) ||
				!objectRefMatchesTable(target.ObjRef, base) {
				return nil, false
			}
			baseTargets++
			continue
		}
		if target.LockTable {
			return nil, false
		}
		if _, exists := rowTargetsByTable[target.TableId]; exists {
			return nil, false
		}
		rowTargetsByTable[target.TableId] = target
	}
	if baseTargets != 1 {
		return nil, false
	}

	ctxByTable := make(map[uint64]*plan.UpdateCtx, len(updateNode.UpdateCtxList))
	ctxByName := make(map[string]*plan.UpdateCtx, len(updateNode.UpdateCtxList))
	for _, updateCtx := range updateNode.UpdateCtxList {
		if updateCtx == nil || updateCtx.ObjRef == nil || updateCtx.TableDef == nil ||
			updateCtx.TableDef.TblId == 0 || !objectRefMatchesTable(updateCtx.ObjRef, updateCtx.TableDef) {
			return nil, false
		}
		if _, exists := ctxByTable[updateCtx.TableDef.TblId]; exists {
			return nil, false
		}
		if _, exists := ctxByName[updateCtx.TableDef.Name]; exists {
			return nil, false
		}
		ctxByTable[updateCtx.TableDef.TblId] = updateCtx
		ctxByName[updateCtx.TableDef.Name] = updateCtx
	}
	baseUpdate, ok := ctxByTable[base.TblId]
	if !ok || baseUpdate.TableDef.Name != base.Name || len(ctxByTable) != len(base.Indexes)+1 {
		return nil, false
	}

	indexNames := make(map[string]struct{}, len(base.Indexes))
	promoted := make([]loadUniqueIndexPromotionTarget, 0, len(rowTargetsByTable))
	for _, indexDef := range base.Indexes {
		if indexDef == nil || !indexDef.TableExist || indexDef.IndexTableName == "" ||
			!admittedLoadIndexAlgorithm(indexDef) {
			return nil, false
		}
		if _, exists := indexNames[indexDef.IndexTableName]; exists {
			return nil, false
		}
		indexNames[indexDef.IndexTableName] = struct{}{}

		indexCtx := ctxByName[indexDef.IndexTableName]
		if indexCtx == nil || indexCtx.TableDef.TblId == base.TblId ||
			indexCtx.TableDef.TableType != catalog.SystemIndexRel {
			return nil, false
		}
		if !indexDef.Unique {
			if _, hasRowTarget := rowTargetsByTable[indexCtx.TableDef.TblId]; hasRowTarget {
				return nil, false
			}
			continue
		}
		if !catalog.IsUniqueIndexTable(indexCtx.TableDef.Name) {
			return nil, false
		}
		hiddenPKType, ok := physicalPrimaryKeyType(indexCtx.TableDef)
		if !ok || !admittedLoadTableLockPKType(hiddenPKType) {
			return nil, false
		}
		rowTarget, ok := rowTargetsByTable[indexCtx.TableDef.TblId]
		if !ok || !objectRefMatchesTable(rowTarget.ObjRef, indexCtx.TableDef) ||
			!planTypeEqual(rowTarget.PrimaryColTyp, hiddenPKType) {
			return nil, false
		}
		promoted = append(promoted, loadUniqueIndexPromotionTarget{
			rowTarget:   plan2.DeepCopyLockTarget(rowTarget),
			tablePKType: hiddenPKType,
		})
	}
	if len(promoted) == 0 || len(promoted) != len(rowTargetsByTable) {
		return nil, false
	}
	slices.SortFunc(promoted, func(a, b loadUniqueIndexPromotionTarget) int {
		if a.rowTarget.TableId < b.rowTarget.TableId {
			return -1
		}
		if a.rowTarget.TableId > b.rowTarget.TableId {
			return 1
		}
		return 0
	})
	return promoted, true
}

func reachableLoadPlanNodes(qry *plan.Query) (map[int32]struct{}, bool) {
	reachable := make(map[int32]struct{}, len(qry.Nodes))
	visiting := make(map[int32]struct{}, len(qry.Nodes))
	var visit func(int32) bool
	visit = func(nodeID int32) bool {
		if nodeID < 0 || int(nodeID) >= len(qry.Nodes) || qry.Nodes[nodeID] == nil {
			return false
		}
		if _, ok := reachable[nodeID]; ok {
			return true
		}
		if _, ok := visiting[nodeID]; ok {
			return false
		}
		visiting[nodeID] = struct{}{}
		defer delete(visiting, nodeID)
		for _, child := range qry.Nodes[nodeID].Children {
			if !visit(child) {
				return false
			}
		}
		reachable[nodeID] = struct{}{}
		return true
	}
	for _, root := range qry.Steps {
		if !visit(root) {
			return nil, false
		}
	}
	return reachable, len(reachable) > 0
}

func planNodeDescendsFrom(nodes []*plan.Node, root, target int32) bool {
	seen := make(map[int32]struct{})
	var visit func(int32) bool
	visit = func(nodeID int32) bool {
		if nodeID == target {
			return true
		}
		if nodeID < 0 || int(nodeID) >= len(nodes) || nodes[nodeID] == nil {
			return false
		}
		if _, ok := seen[nodeID]; ok {
			return false
		}
		seen[nodeID] = struct{}{}
		for _, child := range nodes[nodeID].Children {
			if visit(child) {
				return true
			}
		}
		return false
	}
	return visit(root)
}

func loadEstimateEligible(stats *plan.Stats, maxRowLocks float64) bool {
	// Above the configured row budget, the ordinary path is expected to enter
	// owner-side cumulative coarsening, while every later batch still pays to
	// encode, sort, submit, and merge row keys. This estimate is only a cost
	// admission signal: correctness is established independently by the exact
	// source, plan, transaction, lock-target, and snapshot-barrier proofs.
	// Keep the large-input floor as a second gate so the one physical retry is
	// amortized and medium LOAD latency remains on the canonical path.
	if stats == nil || maxRowLocks <= 0 || math.IsNaN(maxRowLocks) || math.IsInf(maxRowLocks, 0) ||
		stats.Outcnt <= maxRowLocks || math.IsNaN(stats.Outcnt) || math.IsInf(stats.Outcnt, 0) ||
		stats.Cost <= 0 || stats.Rowsize <= 0 || math.IsNaN(stats.Cost) ||
		math.IsNaN(stats.Rowsize) || math.IsInf(stats.Cost, 0) || math.IsInf(stats.Rowsize, 0) {
		return false
	}
	estimatedBytes := stats.Cost * stats.Rowsize
	return !math.IsNaN(estimatedBytes) && !math.IsInf(estimatedBytes, 0) &&
		estimatedBytes >= loadUniqueIndexPromotionMinBytes
}

type loadExternalSourceMetadata struct {
	ScanType     int
	FileSize     int64
	Filepath     string
	CompressType string
	Format       string
	Local        bool
}

func loadExternalSourceEligible(node *plan.Node) bool {
	if node == nil || node.ExternScan == nil ||
		node.ExternScan.Type != int32(plan.ExternType_LOAD) ||
		node.ExternScan.LoadType == int32(tree.INLINE) || node.TableDef == nil ||
		node.TableDef.Createsql == "" {
		return false
	}
	var source loadExternalSourceMetadata
	if err := json.Unmarshal([]byte(node.TableDef.Createsql), &source); err != nil ||
		source.ScanType != int(node.ExternScan.LoadType) || source.Local ||
		source.FileSize <= 0 || source.Filepath == "" ||
		(source.Format != tree.CSV && source.Format != tree.JSONLINE) ||
		node.ExternScan.Format != source.Format {
		return false
	}
	return crt.GetCompressType(source.CompressType, source.Filepath) == tree.NOCOMPRESS
}

func ordinaryLoadBaseTable(base *plan.TableDef, targets []*plan.LockTarget) bool {
	if base == nil || base.TblId == 0 || base.Name == "" ||
		base.TableType != catalog.SystemOrdinaryRel || base.IsTemporary ||
		base.Partition != nil || len(base.Fkeys) != 0 || len(base.RefChildTbls) != 0 ||
		catalog.IsHiddenTable(base.Name) {
		return false
	}
	for _, target := range targets {
		if target != nil && target.TableId == base.TblId && target.ObjRef != nil {
			schema, ok := objectRefSchema(target.ObjRef)
			return ok && !strings.EqualFold(schema, catalog.MO_CATALOG) &&
				target.ObjRef.SubscriptionName == "" && target.ObjRef.PubInfo == nil
		}
	}
	return false
}

func objectRefSchema(ref *plan.ObjectRef) (string, bool) {
	if ref == nil {
		return "", false
	}
	if ref.DbName != "" && ref.SchemaName != "" &&
		!strings.EqualFold(ref.DbName, ref.SchemaName) {
		return "", false
	}
	if ref.SchemaName != "" {
		return ref.SchemaName, true
	}
	return ref.DbName, ref.DbName != ""
}

func admittedLoadBasePKType(base *plan.TableDef) (plan.Type, bool) {
	if base == nil || base.Pkey == nil || catalog.IsFakePkName(base.Pkey.PkeyColName) {
		return plan.Type{}, false
	}
	if base.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		col := base.Pkey.CompPkeyCol
		if col == nil || len(base.Pkey.Names) < 2 || types.T(col.Typ.Id) != types.T_varchar ||
			col.Typ.Width != types.MaxVarcharLen || col.Typ.Charset != uint32(types.CharsetBinary) {
			return plan.Type{}, false
		}
		return col.Typ, true
	}
	typ, ok := physicalPrimaryKeyType(base)
	if !ok || !admittedLoadTableLockPKType(typ) {
		return plan.Type{}, false
	}
	return typ, true
}

// admittedLoadTableLockPKType mirrors the types for which lockop can encode a
// full-domain range. UNIQUE hidden tables preserve a single key's physical type
// and use binary varchar for composite keys, so restricting this to one integer
// type would silently make the optimization inapplicable to the target LOADs.
func admittedLoadTableLockPKType(typ plan.Type) bool {
	return lockop.SupportsTotalLockTableRange(plan2.MakeTypeByPlan2Type(typ))
}

func physicalPrimaryKeyType(table *plan.TableDef) (plan.Type, bool) {
	if table == nil || table.Pkey == nil || table.Pkey.PkeyColName == "" {
		return plan.Type{}, false
	}
	if table.Pkey.PkeyColName == catalog.CPrimaryKeyColName && table.Pkey.CompPkeyCol != nil {
		return table.Pkey.CompPkeyCol.Typ, true
	}
	idx, ok := table.Name2ColIndex[table.Pkey.PkeyColName]
	if !ok || idx < 0 || int(idx) >= len(table.Cols) || table.Cols[idx] == nil ||
		table.Cols[idx].Name != table.Pkey.PkeyColName {
		return plan.Type{}, false
	}
	return table.Cols[idx].Typ, true
}

func admittedLoadIndexAlgorithm(indexDef *plan.IndexDef) bool {
	algo := strings.ToLower(indexDef.IndexAlgo)
	if algo != strings.ToLower(catalog.MoIndexDefaultAlgo.ToString()) &&
		algo != strings.ToLower(catalog.MoIndexBTreeAlgo.ToString()) {
		return false
	}
	async, err := indexplugin.IsAsync(indexDef.IndexAlgo, indexDef.IndexAlgoParams)
	return err == nil && !async
}

func objectRefMatchesTable(ref *plan.ObjectRef, table *plan.TableDef) bool {
	return ref != nil && table != nil && ref.Obj > 0 && uint64(ref.Obj) == table.TblId &&
		ref.ObjName == table.Name
}

func planTypeEqual(left, right plan.Type) bool {
	return plan2.MakeTypeByPlan2Type(left).Eq(plan2.MakeTypeByPlan2Type(right))
}

// maybePromoteLoadUniqueIndexes runs after ordinary base-table locking and
// before any source initialization. nil means exact-main execution continues;
// successful promotion returns the existing physical-retry signal.
func (c *Compile) maybePromoteLoadUniqueIndexes() error {
	state := c.loadUniqueIndexPromotion
	if state == nil || state.phase == loadUniqueIndexPromotionDisabled ||
		state.phase == loadUniqueIndexPromotionFenced {
		return nil
	}
	if state.phase != loadUniqueIndexPromotionEligible || !c.loadUniqueIndexPromotionOwner ||
		!c.resourceAttemptOwnerEligible || c.executionGeneration != 0 ||
		state.logicalPlan != c.pn || c.planGenerationReused || c.disableRetry ||
		!loadUniqueIndexPromotionTxnEligible(c.proc.GetTxnOperator()) ||
		!supportsLoadLogtailReadBarrier(c.proc.GetService()) {
		state.disable()
		return nil
	}
	barrier, ok := loadLogtailReadBarrier(c.e)
	if !ok {
		state.disable()
		return nil
	}
	txnOp := c.proc.GetTxnOperator()
	budget := client.LockWaitTimeoutFromTxn(txnOp)
	if budget <= 0 {
		state.disable()
		return nil
	}
	hardCap := time.Duration(defines.DefaultLockWaitTimeoutSeconds) * time.Second
	if budget > hardCap {
		budget = hardCap
	}
	parent := c.proc.Ctx
	if parent == nil {
		state.disable()
		return nil
	}
	promotionCtx, cancel := context.WithTimeoutCause(parent, budget, lockservice.ErrLockTimeout)
	defer cancel()

	state.phase = loadUniqueIndexPromotionAcquiring
	frontier, installed, err := executeLoadUniqueIndexPromotion(
		parent,
		promotionCtx,
		state.targets,
		func(ctx context.Context, target loadUniqueIndexPromotionTarget) error {
			return lockop.LockTableForSnapshotRefreshWithContext(
				ctx,
				c.e,
				c.proc,
				target.rowTarget.TableId,
				plan2.MakeTypeByPlan2Type(target.tablePKType),
				lockpb.LockMode_Exclusive,
				false,
			)
		},
		barrier.AcquireLogtailReadBarrier,
		txnOp.UpdateSnapshot,
		func() timestamp.Timestamp { return txnOp.Txn().SnapshotTS },
		recordLoadLogtailReadBarrierDuration,
	)
	if err != nil {
		state.disable()
		return normalizeLoadUniqueIndexPromotionError(parent, promotionCtx, err)
	}
	meta := txnOp.Txn()
	if !installed.Greater(frontier) || !meta.SnapshotTS.Greater(frontier) ||
		!loadUniqueIndexPromotionTxnEligible(txnOp) {
		state.disable()
		return moerr.NewInternalError(parent,
			"LOAD unique-index promotion did not install a snapshot after the logtail frontier")
	}

	state.txnID = bytes.Clone(meta.ID)
	state.firstPhysicalGeneration = c.executionGeneration
	state.frontier = frontier
	state.installedSnapshot = installed
	state.phase = loadUniqueIndexPromotionFenced
	return moerr.NewTxnNeedRetry(parent)
}

type loadUniqueIndexPromotionLockFunc func(context.Context, loadUniqueIndexPromotionTarget) error
type loadUniqueIndexPromotionBarrierFunc func(context.Context) (timestamp.Timestamp, error)
type loadUniqueIndexPromotionUpdateSnapshotFunc func(context.Context, timestamp.Timestamp) error
type loadUniqueIndexPromotionSnapshotFunc func() timestamp.Timestamp
type loadUniqueIndexPromotionMetricFunc func(context.Context, context.Context, time.Duration, error)

func executeLoadUniqueIndexPromotion(
	parent context.Context,
	ctx context.Context,
	targets []loadUniqueIndexPromotionTarget,
	lockTable loadUniqueIndexPromotionLockFunc,
	readBarrier loadUniqueIndexPromotionBarrierFunc,
	updateSnapshot loadUniqueIndexPromotionUpdateSnapshotFunc,
	currentSnapshot loadUniqueIndexPromotionSnapshotFunc,
	recordBarrier loadUniqueIndexPromotionMetricFunc,
) (timestamp.Timestamp, timestamp.Timestamp, error) {
	for _, target := range targets {
		if err := lockTable(ctx, target); err != nil {
			return timestamp.Timestamp{}, timestamp.Timestamp{}, err
		}
	}
	start := time.Now()
	frontier, err := readBarrier(ctx)
	recordBarrier(parent, ctx, time.Since(start), err)
	if err != nil {
		return timestamp.Timestamp{}, timestamp.Timestamp{}, err
	}
	if err = updateSnapshot(ctx, frontier); err != nil {
		return timestamp.Timestamp{}, timestamp.Timestamp{}, err
	}
	installed := currentSnapshot()
	if !installed.Greater(frontier) {
		return timestamp.Timestamp{}, timestamp.Timestamp{}, moerr.NewInternalError(
			ctx, "transaction snapshot did not advance after LOAD logtail read barrier")
	}
	return frontier, installed, nil
}

func normalizeLoadUniqueIndexPromotionError(parent, promotion context.Context, err error) error {
	if parentErr := parent.Err(); parentErr != nil {
		return parentErr
	}
	if context.Cause(promotion) == lockservice.ErrLockTimeout {
		return lockservice.ErrLockTimeout
	}
	return err
}

func recordLoadLogtailReadBarrierDuration(
	parent context.Context,
	promotion context.Context,
	duration time.Duration,
	err error,
) {
	seconds := duration.Seconds()
	switch {
	case err == nil:
		v2.TxnLoadLogtailReadBarrierSuccessDurationHistogram.Observe(seconds)
	case errorsIsCanceled(parent, promotion, err):
		v2.TxnLoadLogtailReadBarrierCanceledDurationHistogram.Observe(seconds)
	case errorsIsTimeout(parent, promotion, err):
		v2.TxnLoadLogtailReadBarrierTimeoutDurationHistogram.Observe(seconds)
	default:
		v2.TxnLoadLogtailReadBarrierErrorDurationHistogram.Observe(seconds)
	}
}

func errorsIsCanceled(parent, promotion context.Context, err error) bool {
	return parent.Err() == context.Canceled || context.Cause(promotion) == context.Canceled ||
		errors.Is(err, context.Canceled)
}

func errorsIsTimeout(parent, promotion context.Context, err error) bool {
	return parent.Err() == context.DeadlineExceeded ||
		context.Cause(promotion) == context.DeadlineExceeded ||
		context.Cause(promotion) == lockservice.ErrLockTimeout ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, lockservice.ErrLockTimeout)
}

func (s *loadUniqueIndexPromotionState) validateRetryProof(c *Compile) error {
	if s == nil || s.phase != loadUniqueIndexPromotionFenced || c == nil || c.pn != s.logicalPlan ||
		c.executionGeneration <= s.firstPhysicalGeneration || c.planGenerationReused || c.isInternal ||
		!c.hasPlanSnapshotTS || !c.planSnapshotTS.Equal(s.installedSnapshot) ||
		len(s.targets) == 0 || len(s.txnID) == 0 || !s.installedSnapshot.Greater(s.frontier) {
		return moerr.NewInternalErrorNoCtx("invalid LOAD unique-index promotion retry proof")
	}
	txnOp := c.proc.GetTxnOperator()
	if !loadUniqueIndexPromotionTxnEligible(txnOp) {
		return moerr.NewInternalErrorNoCtx("LOAD unique-index promotion transaction is no longer eligible")
	}
	meta := txnOp.Txn()
	if !bytes.Equal(meta.ID, s.txnID) || !meta.SnapshotTS.Greater(s.frontier) {
		return moerr.NewInternalErrorNoCtx("LOAD unique-index promotion retry proof does not match transaction generation")
	}
	return nil
}

func (s *loadUniqueIndexPromotionState) coversRowTarget(target *plan.LockTarget) bool {
	if s == nil || target == nil {
		return false
	}
	idx, found := slices.BinarySearchFunc(
		s.targets,
		target.TableId,
		func(promoted loadUniqueIndexPromotionTarget, tableID uint64) int {
			switch {
			case promoted.rowTarget == nil || promoted.rowTarget.TableId < tableID:
				return -1
			case promoted.rowTarget.TableId > tableID:
				return 1
			default:
				return 0
			}
		},
	)
	if !found {
		return false
	}
	// Admission rejects duplicate physical table IDs, so one exact protobuf
	// comparison closes the proof without an O(targets*indexes) retry-compile
	// scan on unusually wide index sets.
	promoted := s.targets[idx]
	return promoted.rowTarget != nil && proto.Equal(promoted.rowTarget, target)
}

// onLoadUniqueIndexPromotionRetry disables an incomplete protocol before an
// unrelated retry. A completed proof survives only ordinary physical retries;
// a logical rebuild always invalidates it before rebuilding the plan.
func (c *Compile) onLoadUniqueIndexPromotionRetry(rebuildPlan bool) {
	state := c.loadUniqueIndexPromotion
	if state == nil {
		return
	}
	if rebuildPlan || state.phase != loadUniqueIndexPromotionFenced {
		state.disable()
	}
}
