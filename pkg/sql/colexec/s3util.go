// Copyright 2022 Matrix Origin
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

package colexec

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// WriteS3Threshold when batches'  size of table reaches this, we will
	// trigger write s3
	WriteS3Threshold         = 128 * mpool.MB
	FaultInjectedS3Threshold = 512 * mpool.KB
)

type CNS3Writer struct {
	sinker              *ioutil.Sinker
	isTombstone         bool
	blockInfoBat        *batch.Batch
	memorySizeThreshold int
}

func (w *CNS3Writer) String() string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(fmt.Sprintf("Sinker: %s\n", w.sinker.String()))
	inMemoryThreshold := w.sinker.GetInMemoryThreshold()
	flushOnSync := inMemoryThreshold == math.MaxInt
	var flushOnSyncBatches []*batch.Batch
	if flushOnSync {
		flushOnSyncBatches = w.sinker.GetInMemoryData()
	}

	result, _ := w.sinker.GetResult()

	buf.WriteString(fmt.Sprintf(
		"Others: {result_len=%d, isTombstone=%v, flushOnSync=%v, flushOnSyncBatches_len=%d, blockInfoBat=%v}",
		len(result),
		w.isTombstone,
		flushOnSync,
		len(flushOnSyncBatches),
		common.MoBatchToString(w.blockInfoBat, w.blockInfoBat.RowCount())),
	)

	return buf.String()
}

func NewCNS3TombstoneWriter(
	mp *mpool.MPool,
	fs fileservice.FileService,
	pkType types.Type,
	memoryThreshold int,
	opts ...ioutil.SinkerOption,
) *CNS3Writer {
	return newCNS3TombstoneWriter("", mp, fs, pkType, memoryThreshold, opts...)
}

// NewCNS3TombstoneWriterForService creates a CN writer whose persisted format
// follows the live rollout protocol for exactly one service. Callers without a
// stable service identity must use NewCNS3TombstoneWriter, which is fail-closed.
func NewCNS3TombstoneWriterForService(
	serviceID string,
	mp *mpool.MPool,
	fs fileservice.FileService,
	pkType types.Type,
	memoryThreshold int,
	opts ...ioutil.SinkerOption,
) *CNS3Writer {
	return newCNS3TombstoneWriter(serviceID, mp, fs, pkType, memoryThreshold, opts...)
}

func newCNS3TombstoneWriter(
	serviceID string,
	mp *mpool.MPool,
	fs fileservice.FileService,
	pkType types.Type,
	memoryThreshold int,
	opts ...ioutil.SinkerOption,
) *CNS3Writer {

	writer := &CNS3Writer{
		isTombstone: true,
	}

	if memoryThreshold < 0 {
		memoryThreshold = WriteS3Threshold
	}

	opts = append(opts, ioutil.WithMemorySizeThreshold(memoryThreshold))
	opts = append(opts, ioutil.WithTailSizeCap(0))
	if policy := chunkedColumnPolicyForService(serviceID); policy != nil {
		opts = append(opts, ioutil.WithChunkedColumnPolicy(policy))
	}

	writer.sinker = ioutil.NewTombstoneSinker(
		objectio.HiddenColumnSelection_None,
		pkType,
		mp,
		fs,
		opts...,
	)

	writer.ResetBlockInfoBat()

	return writer
}

func GetSequmsAttrsSortKeyIdxFromTableDef(
	tableDef *plan.TableDef,
) ([]uint16, []types.Type, []string, int, bool) {

	var (
		sequms       []uint16
		sortKeyIdx   = -1
		isPrimaryKey bool

		attrs     []string
		attrTypes []types.Type
	)

	for idx, colDef := range tableDef.Cols {
		if colDef.Name == tableDef.Pkey.PkeyColName && !catalog.IsFakePkName(colDef.Name) {
			sortKeyIdx = idx
			isPrimaryKey = true
			break
		}
	}

	// create table t1(a int primary key) cluster by a ==> not support
	// the `primary key` and `cluster by` cannot both exist.
	// the condition of sortIdx == -1 may be unnecessary.
	if sortKeyIdx == -1 && tableDef.ClusterBy != nil {
		// the rowId column has been excluded from the TableDef of the target table for the insert statements(insert,load).
		// link: pkg/sql/plan/build_constraint_util.go --> func setTableExprToDmlTableInfo,
		// and the sortKeyIdx position can be directly obtained by using a name that matches the sorting key.
		for idx, colDef := range tableDef.Cols {
			if colDef.Name == tableDef.ClusterBy.Name {
				sortKeyIdx = idx
			}
		}
	}

	for i, colDef := range tableDef.Cols {
		if colDef.Name != catalog.Row_ID {
			sequms = append(sequms, uint16(colDef.Seqnum))
			attrs = append(attrs, colDef.Name)

			attrTypes = append(attrTypes, types.MustTypeFromPlan(colDef.Typ))
		} else {
			// check rowid as the last column
			if i != len(tableDef.Cols)-1 {
				logutil.Errorf("bad rowid position for %q, %+v", tableDef.Name, colDef)
			}
		}
	}
	logutil.Debugf("s3 table set from NewS3Writer %q seqnums: %+v", tableDef.Name, sequms)

	return sequms, attrTypes, attrs, sortKeyIdx, isPrimaryKey
}

// `flushOnSync` true means memoryThreshold is math.MaxInt
// `memoryThreshold`
// 1. only effect when `flushOnSync` is false
// 2. < 0 use default threshold
func NewCNS3DataWriter(
	mp *mpool.MPool,
	fs fileservice.FileService,
	tableDef *plan.TableDef,
	memoryThreshold int,
	flushOnSync bool,
	sinkerOpts ...ioutil.SinkerOption,
) *CNS3Writer {
	return newCNS3DataWriter("", mp, fs, tableDef, memoryThreshold, flushOnSync, sinkerOpts...)
}

// NewCNS3DataWriterForService creates a CN writer whose persisted format is
// gated by the service's current minimum deployment protocol at write time.
func NewCNS3DataWriterForService(
	serviceID string,
	mp *mpool.MPool,
	fs fileservice.FileService,
	tableDef *plan.TableDef,
	memoryThreshold int,
	flushOnSync bool,
	sinkerOpts ...ioutil.SinkerOption,
) *CNS3Writer {
	return newCNS3DataWriter(
		serviceID, mp, fs, tableDef, memoryThreshold, flushOnSync, sinkerOpts...,
	)
}

func newCNS3DataWriter(
	serviceID string,
	mp *mpool.MPool,
	fs fileservice.FileService,
	tableDef *plan.TableDef,
	memoryThreshold int,
	flushOnSync bool,
	sinkerOpts ...ioutil.SinkerOption,
) *CNS3Writer {

	writer := new(CNS3Writer)

	sequms, attrTypes, attrs, sortKeyIdx, isPrimaryKey := GetSequmsAttrsSortKeyIdxFromTableDef(tableDef)

	factor := ioutil.NewFSinkerImplFactory(sequms, sortKeyIdx, isPrimaryKey, false, tableDef.Version)
	if memoryThreshold < 0 {
		memoryThreshold = WriteS3Threshold
	}

	if faultInjected, _ := objectio.LogCNFlushSmallObjsInjected(
		tableDef.DbName, tableDef.Name,
	); faultInjected {
		memoryThreshold = FaultInjectedS3Threshold
	}
	if flushOnSync {
		// do not flush on sync, so the threshold is the max int
		memoryThreshold = math.MaxInt
	}
	writer.memorySizeThreshold = memoryThreshold

	sinkerOpts = append(sinkerOpts, ioutil.WithMemorySizeThreshold(memoryThreshold))
	sinkerOpts = append(sinkerOpts, ioutil.WithTailSizeCap(0))
	sinkerOpts = append(sinkerOpts, ioutil.WithOffHeap())
	if policy := chunkedColumnPolicyForService(serviceID); policy != nil {
		sinkerOpts = append(sinkerOpts, ioutil.WithChunkedColumnPolicy(policy))
	}
	writer.sinker = ioutil.NewSinker(
		sortKeyIdx,
		attrs,
		attrTypes,
		factor,
		mp,
		fs,
		sinkerOpts...,
	)

	writer.ResetBlockInfoBat()

	return writer
}

func chunkedColumnPolicyForService(serviceID string) objectio.ChunkedColumnPolicy {
	if serviceID == "" {
		return nil
	}
	return func() bool {
		rt := moruntime.ServiceRuntime(serviceID)
		if rt == nil {
			return false
		}
		value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
		version, valid := value.(int64)
		return ok && valid && version >= defines.MORPCVersion29
	}
}

func (w *CNS3Writer) Write(ctx context.Context, bat *batch.Batch) error {
	return w.sinker.Write(ctx, bat)
}

func (w *CNS3Writer) MemorySizeThreshold() int {
	return w.memorySizeThreshold
}

func (w *CNS3Writer) WriteOwned(ctx context.Context, bat *batch.Batch) (bool, error) {
	return w.sinker.WriteOwned(ctx, bat)
}

func (w *CNS3Writer) Sync(ctx context.Context) (stats []objectio.ObjectStats, err error) {
	if err = w.sinker.Sync(ctx); err != nil {
		return
	}

	stats, _ = w.sinker.GetResult()
	return
}

func (w *CNS3Writer) SyncAndFillBlockInfoBat(ctx context.Context) (*batch.Batch, error) {
	stats, _, err := w.sinker.SyncAndTakeResults(ctx)
	if err != nil {
		return nil, err
	}

	w.ResetBlockInfoBat()
	if len(stats) == 0 {
		return w.blockInfoBat, nil
	}

	if err = ExpandObjectStatsToBatch(
		w.sinker.GetMPool(),
		w.isTombstone,
		w.blockInfoBat,
		true,
		stats...,
	); err != nil {
		return nil, err
	}

	return w.blockInfoBat, nil
}

func (w *CNS3Writer) Close() (err error) {
	var mp *mpool.MPool
	if w.sinker != nil {
		mp = w.sinker.GetMPool()
		if err = w.sinker.Close(); err != nil {
			return
		}
		w.sinker = nil
	}

	if w.blockInfoBat != nil {
		w.blockInfoBat.Clean(mp)
		w.blockInfoBat = nil
	}

	return nil
}

// DeletePersisted deletes every object this writer has persisted, or may
// have persisted after a commit-ambiguous failure, but has not transferred
// out of its lifecycle yet. A failed publication that stops before its
// replacement file entry is recorded leaves the storage objects recorded
// nowhere, so they can only be collected through this explicit ownership
// transfer (issue #25590): the sinker deletes what it can from the file
// service and returns the exact ownership snapshot — on failure the tracked
// references are retained inside the sinker, so the caller must either
// retry or take the returned file list over before closing the writer.
func (w *CNS3Writer) DeletePersisted(ctx context.Context) ([]string, error) {
	if w == nil || w.sinker == nil {
		return nil, nil
	}
	return w.sinker.DeletePersisted(ctx)
}

// PersistedNames hands this writer's persisted / possibly-persisted
// names out without deleting anything (issue #25590 round-15 review):
// the failed-publication paths inside a transaction's statement
// admission transfer the ownership to their asynchronous cleanup owner
// through this method instead of synchronously deleting on a file
// service whose deletion a stalled context cannot move — admission, the
// statements behind this transaction's lock, and rollback stay
// independent of storage fate. The sinker keeps its tracked references;
// closing the writer drops them as before.
func (w *CNS3Writer) PersistedNames() []string {
	if w == nil || w.sinker == nil {
		return nil
	}
	return w.sinker.PersistedNames()
}

// HasUncommittedCapacityHook reports whether a capacity hook guards this
// writer's persistence — the assertion point of the writer-binding
// regressions: a direct-S3 writer bound to its workspace's admission
// reserves one in-flight unit per object BEFORE the object persists, so
// a saturated ledger refuses the spill with nothing half-persisted
// behind it (issue #25590 round-19 review).
func (w *CNS3Writer) HasUncommittedCapacityHook() bool {
	if w == nil || w.sinker == nil {
		return false
	}
	return w.sinker.HasUncommittedCapacityHook()
}

// WorkspaceSinkerCapacityOption resolves the transaction workspace's
// per-object admission option for an S3 writer this proc constructs on
// the workspace's behalf (issue #25590 round-19/20 reviews): through the
// platform-agnostic SinkerOwner surface a bound writer reserves one
// in-flight unit per object BEFORE it persists — so a saturated ledger
// refuses the spill with nothing half-persisted behind it — regardless
// of which operator (insert, multi_update's insert sinkers and
// temporary table writers, the compaction rewriter) built it. A proc
// without a transaction operator, or a workspace (or test double)
// without the owner surface, keeps an unhooked writer — exactly the
// pre-fix behavior.
func WorkspaceSinkerCapacityOption(proc *process.Process) ioutil.SinkerOption {
	if proc == nil {
		return nil
	}
	txnOp := proc.GetTxnOperator()
	if txnOp == nil {
		return nil
	}
	owner, ok := txnOp.GetWorkspace().(ioutil.SinkerOwner)
	if !ok {
		return nil
	}
	return owner.SinkerCapacityOption()
}

// RebindWorkspaceSinkerCapacity re-arms ONE persistent writer's per-object
// capacity admission with the CURRENT proc's transaction workspace (issue
// #25590 round-35 review): a writer a cached plan keeps alive across
// executions — the multi_update delegate's persistent insert sinkers,
// which Prepare may find ALREADY-CREATED with the FIRST transaction's
// hook — enters its next execution still wearing a FINALIZED workspace's
// hook, and every object it spills next would reserve against a workspace
// whose terminal boundaries can never release or convert that
// reservation, permanently consuming engine admission capacity. The
// rebind follows WorkspaceSinkerCapacityOption's own resolution rules: a
// proc without a transaction operator, or a workspace without the owner
// surface, ends up UNHOOKED — exactly the freshly-constructed compat
// shape — never on the previous workspace's stale hook.
func (w *CNS3Writer) RebindWorkspaceSinkerCapacity(proc *process.Process) {
	if w == nil || w.sinker == nil {
		return
	}
	if opt := WorkspaceSinkerCapacityOption(proc); opt != nil {
		opt(w.sinker)
		return
	}
	w.sinker.RebindUncommittedCapacityHook(nil)
}

// ClearWorkspaceSinkerCapacity drops this persistent writer's
// execution-scoped capacity hook unconditionally (issue #25590
// round-54 review): the hook a workspace's SinkerCapacityOption
// installed captures the workspace's TRANSACTION, and a cached plan's
// delegate keeps its sinkers — buffer pools and arenas — alive between
// executions. Between the terminal of the transaction that armed them
// and the next execution's Prepare, that captured completed workspace
// is nothing but retention: its remaining history, backing arrays and
// process references stay reachable through an idle plan even though
// nothing can spill or reserve again before Prepare re-arms the
// binding with the CURRENT statement's workspace. The clear is the
// Reset-side half of the round-35 rebind contract; nil receiver and
// nil sinker stay no-ops, and a writer no execution armed keeps its
// fresh-construction shape.
func (w *CNS3Writer) ClearWorkspaceSinkerCapacity() {
	if w == nil || w.sinker == nil {
		return
	}
	w.sinker.RebindUncommittedCapacityHook(nil)
}

// Reset discards any buffered or staged data accumulated since the last
// Sync, without tearing down the underlying sinker. Call this when the
// enclosing pipeline execution is being reset so that stale data is not
// carried into the next execution. The sinker's buffer pool and arena
// are kept alive for efficient reuse.
func (w *CNS3Writer) Reset() {
	if w.sinker != nil {
		w.sinker.Reset()
	}
	if w.blockInfoBat != nil {
		w.blockInfoBat.CleanOnlyData()
	}
}

func ExpandObjectStatsToBatch(
	mp *mpool.MPool,
	isTombstone bool,
	outBath *batch.Batch,
	isCNCreated bool,
	statsList ...objectio.ObjectStats,
) (err error) {

	if !isTombstone {
		objectio.ForeachBlkInObjStatsList(
			true, nil,
			func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
				if err = vector.AppendBytes(
					outBath.Vecs[0],
					objectio.EncodeBlockInfo(&blk), false, mp); err != nil {
					return false
				}

				return true

			}, statsList...)

		for i := range statsList {
			if isCNCreated {
				objectio.WithCNCreated()(&statsList[i])
			}

			if err = vector.AppendBytes(outBath.Vecs[1],
				statsList[i].Marshal(), false, mp); err != nil {
				return err
			}
		}
	} else {
		for i := range statsList {
			if isCNCreated {
				objectio.WithCNCreated()(&statsList[i])
			}

			if err = vector.AppendBytes(outBath.Vecs[0],
				statsList[i].Marshal(), false, mp); err != nil {
				return err
			}
		}
	}

	outBath.SetRowCount(outBath.Vecs[0].Length())
	return nil
}

func (w *CNS3Writer) FillBlockInfoBat() (*batch.Batch, error) {

	w.ResetBlockInfoBat()

	result, _ := w.sinker.GetResult()

	if err := ExpandObjectStatsToBatch(
		w.sinker.GetMPool(),
		w.isTombstone,
		w.blockInfoBat,
		true,
		result...,
	); err != nil {
		return nil, err
	}

	return w.blockInfoBat, nil
}

func AllocCNS3ResultBat(
	isTombstone bool,
) *batch.Batch {

	var (
		attrs     []string
		attrTypes []types.Type

		blockInfoBat *batch.Batch
	)

	if !isTombstone {
		attrs = []string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
		attrTypes = []types.Type{types.T_text.ToType(), types.T_binary.ToType()}
	} else {
		attrs = []string{catalog.ObjectMeta_ObjectStats}
		attrTypes = []types.Type{types.T_binary.ToType()}
	}

	blockInfoBat = batch.NewWithSize(len(attrs))
	blockInfoBat.Attrs = attrs

	for i := range attrs {
		blockInfoBat.Vecs[i] = vector.NewVec(attrTypes[i])
	}

	return blockInfoBat
}

func (w *CNS3Writer) ResetBlockInfoBat() {

	if w.blockInfoBat != nil {
		w.blockInfoBat.CleanOnlyData()
	} else {
		w.blockInfoBat = AllocCNS3ResultBat(w.isTombstone)
	}
}

// reference to pkg/sql/colexec/order/order.go logic
func SortByKey(
	proc *process.Process,
	bat *batch.Batch,
	sortIndex int,
	allow_null bool,
	m *mpool.MPool,
) error {

	hasNull := false
	// Not-Null Check, notice that cluster by support null value
	if nulls.Any(bat.Vecs[sortIndex].GetNulls()) {
		hasNull = true
		if !allow_null {
			return moerr.NewConstraintViolationf(proc.Ctx,
				"sort key can not be null, sortIndex = %d, sortCol = %s",
				sortIndex, bat.Attrs[sortIndex])
		}
	}
	rowCount := int64(bat.RowCount())
	sels := vector.GetSels()
	defer func() {
		vector.PutSels(sels)
	}()
	for i := int64(0); i < rowCount; i++ {
		sels = append(sels, i)
	}
	ovec := bat.GetVector(int32(sortIndex))
	if allow_null {
		// null last
		sort.Sort(false, true, hasNull, sels, ovec)
	} else {
		sort.Sort(false, false, hasNull, sels, ovec)
	}

	needSort := false
	for i := int64(0); i < int64(rowCount); i++ {
		if sels[i] != i {
			needSort = true
			break
		}
	}
	if needSort {
		return bat.Shuffle(sels, m)
	}
	return nil
}
