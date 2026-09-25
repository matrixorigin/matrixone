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
	"strings"
	"sync"
	"time"

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
	fs                  fileservice.FileService
	serviceID           string
	ownedPersistedNames []string
	cleanupPending      bool
	isTombstone         bool
	blockInfoBat        *batch.Batch
	memorySizeThreshold int
}

// PendingUnpublishedCleanup reports whether a failed writer still owns object
// names that must be deleted. A reusable writer without debt must not enter a
// transaction retry task.
func (w *CNS3Writer) PendingUnpublishedCleanup() bool {
	return w != nil && (w.cleanupPending || len(w.ownedPersistedNames) != 0)
}

func (w *CNS3Writer) ReadyForWrites() bool {
	return w != nil && w.sinker != nil && !w.cleanupPending
}

// UnpublishedS3CleanupRetainer is implemented by workspaces that can own a
// failed cleanup after the operator that created the object is released.
type UnpublishedS3CleanupRetainer interface {
	RetainUnpublishedS3Cleanup(func(context.Context) error)
}

// UnpublishedS3ObjectOwner keeps only the persisted object names that have
// not yet crossed the transaction-workspace registration boundary. It can be
// retained after the writer's buffers are released, then either accept names
// as workspace entries are appended or delete the remainder on abort.
type UnpublishedS3ObjectOwner struct {
	mu        sync.Mutex
	fs        fileservice.FileService
	names     map[string]struct{}
	admission *unpublishedS3Admission
	charged   map[string]struct{}
}

func NewUnpublishedS3ObjectOwner(
	fs fileservice.FileService,
	names ...string,
) (*UnpublishedS3ObjectOwner, error) {
	return newUnpublishedS3ObjectOwner(fs, nil, nil, names...)
}

// NewUnpublishedS3ObjectOwnerForService transfers names already charged by a
// local CN sinker into a lightweight transaction owner.
func NewUnpublishedS3ObjectOwnerForService(
	serviceID string,
	fs fileservice.FileService,
	names ...string,
) (*UnpublishedS3ObjectOwner, error) {
	if serviceID == "" {
		return NewUnpublishedS3ObjectOwner(fs, names...)
	}
	srv := GetServer(serviceID)
	if srv == nil {
		return nil, moerr.NewInvalidStateNoCtx("missing CN unpublished S3 admission service")
	}
	return newUnpublishedS3ObjectOwner(fs, srv.unpublishedS3Admission, names, names...)
}

func newUnpublishedS3ObjectOwner(
	fs fileservice.FileService,
	admission *unpublishedS3Admission,
	charged []string,
	names ...string,
) (*UnpublishedS3ObjectOwner, error) {
	if fs == nil {
		return nil, moerr.NewInternalErrorNoCtx("missing file service for unpublished S3 object owner")
	}
	owner := &UnpublishedS3ObjectOwner{
		fs:        fs,
		names:     make(map[string]struct{}, len(names)),
		admission: admission,
	}
	for _, name := range names {
		if name != "" {
			owner.names[strings.Clone(name)] = struct{}{}
		}
	}
	if len(owner.names) == 0 {
		return nil, nil
	}
	if admission != nil {
		owner.charged = make(map[string]struct{}, len(charged))
		for _, name := range charged {
			if _, exists := owner.names[name]; exists {
				owner.charged[name] = struct{}{}
			}
		}
	}
	return owner, nil
}

func (owner *UnpublishedS3ObjectOwner) Names() []string {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	names := make([]string, 0, len(owner.names))
	for name := range owner.names {
		names = append(names, name)
	}
	return names
}

func (owner *UnpublishedS3ObjectOwner) Accept(names ...string) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	for _, name := range names {
		delete(owner.names, name)
		owner.release(name)
	}
}

func (owner *UnpublishedS3ObjectOwner) AcceptAll() {
	owner.mu.Lock()
	clear(owner.names)
	for name := range owner.charged {
		owner.release(name)
	}
	owner.mu.Unlock()
}

func (owner *UnpublishedS3ObjectOwner) release(name string) {
	if _, charged := owner.charged[name]; charged {
		owner.admission.release(name)
		delete(owner.charged, name)
	}
}

func (owner *UnpublishedS3ObjectOwner) Pending() bool {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return len(owner.names) != 0
}

func (owner *UnpublishedS3ObjectOwner) Cleanup(ctx context.Context) error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if len(owner.names) == 0 {
		return nil
	}
	names := make([]string, 0, len(owner.names))
	for name := range owner.names {
		names = append(names, name)
	}
	completed, _, err := ioutil.DeleteUnpublishedObjectsWithProgress(ctx, owner.fs, names...)
	for _, name := range completed {
		delete(owner.names, name)
		owner.release(name)
	}
	return err
}

type UnpublishedS3ObjectOwnershipWorkspace interface {
	RetainUnpublishedS3ObjectOwner(*UnpublishedS3ObjectOwner)
	AcceptUnpublishedS3ObjectNames(...string)
	HasUnpublishedS3ObjectOwners() bool
	AcceptAllUnpublishedS3ObjectOwners()
}

func RetainUnpublishedS3ObjectOwner(
	proc *process.Process,
	owner *UnpublishedS3ObjectOwner,
) bool {
	if proc == nil || owner == nil || !owner.Pending() || proc.GetTxnOperator() == nil {
		return false
	}
	retainer, ok := proc.GetTxnOperator().GetWorkspace().(UnpublishedS3ObjectOwnershipWorkspace)
	if !ok {
		return false
	}
	retainer.RetainUnpublishedS3ObjectOwner(owner)
	return true
}

// RetainReceivedUnpublishedS3ObjectNames reserves coordinator capacity before
// acknowledging a remote worker's ownership handoff. A failed retention rolls
// back only newly reserved names; existing owners remain charged.
func RetainReceivedUnpublishedS3ObjectNames(
	proc *process.Process,
	fs fileservice.FileService,
	names ...string,
) error {
	if len(names) == 0 {
		return nil
	}
	if proc == nil {
		return moerr.NewInvalidStateNoCtx("missing process for remote S3 ownership")
	}
	srv := GetServer(proc.GetService())
	if srv == nil {
		return moerr.NewInvalidStateNoCtx("missing CN unpublished S3 admission service")
	}
	charged, err := srv.reserveUnpublishedS3Received(names)
	if err != nil {
		return err
	}
	owner, err := newUnpublishedS3ObjectOwner(fs, srv.unpublishedS3Admission, charged, names...)
	if err == nil && !RetainUnpublishedS3ObjectOwner(proc, owner) {
		err = moerr.NewInvalidStateNoCtx("receiving transaction workspace cannot retain remote S3 ownership")
	}
	if err != nil {
		for _, name := range charged {
			srv.unpublishedS3Admission.release(name)
		}
	}
	return err
}

func AcceptUnpublishedS3ObjectNames(proc *process.Process, names ...string) {
	if proc == nil || proc.GetTxnOperator() == nil {
		return
	}
	accepter, ok := proc.GetTxnOperator().GetWorkspace().(UnpublishedS3ObjectOwnershipWorkspace)
	if !ok {
		return
	}
	accepter.AcceptUnpublishedS3ObjectNames(names...)
}

func HasUnpublishedS3ObjectOwners(proc *process.Process) bool {
	if proc == nil || proc.GetTxnOperator() == nil {
		return false
	}
	owners, ok := proc.GetTxnOperator().GetWorkspace().(UnpublishedS3ObjectOwnershipWorkspace)
	return ok && owners.HasUnpublishedS3ObjectOwners()
}

func AcceptAllUnpublishedS3ObjectOwners(proc *process.Process) {
	if proc == nil || proc.GetTxnOperator() == nil {
		return
	}
	owners, ok := proc.GetTxnOperator().GetWorkspace().(UnpublishedS3ObjectOwnershipWorkspace)
	if ok {
		owners.AcceptAllUnpublishedS3ObjectOwners()
	}
}

// RetainUnpublishedS3Cleanup transfers cleanup ownership to the transaction
// workspace when a writer's final operator callback cannot delete its objects.
func RetainUnpublishedS3Cleanup(
	proc *process.Process,
	cleanup func(context.Context) error,
) bool {
	if proc == nil || cleanup == nil || proc.GetTxnOperator() == nil {
		return false
	}
	retainer, ok := proc.GetTxnOperator().GetWorkspace().(UnpublishedS3CleanupRetainer)
	if !ok {
		return false
	}
	retainer.RetainUnpublishedS3Cleanup(cleanup)
	return true
}

// UnpublishedS3CleanupContext ignores request cancellation while preserving an
// existing cleanup deadline. Nested writer cleanup must not restart the outer
// finalizer's budget for each object or writer.
func UnpublishedS3CleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	base := context.WithoutCancel(ctx)
	deadline := time.Now().Add(10 * time.Minute)
	if sharedDeadline, ok := process.PipelineCleanupDeadline(ctx); ok && sharedDeadline.Before(deadline) {
		deadline = sharedDeadline
	}
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	return context.WithDeadlineCause(base, deadline, moerr.CauseCleanUpUselessFiles)
}

// TerminalUnpublishedS3CleanupContext starts a bounded cleanup attempt after
// Commit or Rollback even when the request deadline has already expired. A
// pipeline teardown deadline, when present, is still shared by nested owners.
func TerminalUnpublishedS3CleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(10 * time.Minute)
	if sharedDeadline, ok := process.PipelineCleanupDeadline(ctx); ok && sharedDeadline.Before(deadline) {
		deadline = sharedDeadline
	}
	return context.WithDeadlineCause(context.WithoutCancel(ctx), deadline, moerr.CauseCleanUpUselessFiles)
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
		fs:          fs,
		serviceID:   serviceID,
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
	if serviceID != "" {
		opts = append(opts, UnpublishedS3AdmissionOption(serviceID))
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

			attrTypes = append(attrTypes, types.NewWithCharset(
				types.T(colDef.Typ.Id), colDef.Typ.Width, colDef.Typ.Scale, uint8(colDef.Typ.Charset),
			))
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

// UnpublishedS3AdmissionOption attaches the CN's pre-Sync object-name budget
// to a direct sinker, such as tombstone transfer.
func UnpublishedS3AdmissionOption(serviceID string) ioutil.SinkerOption {
	return ioutil.WithObjectSyncAdmission(
		func(name string) error {
			return GetServer(serviceID).reserveUnpublishedS3Upload(name)
		},
		func(name string) {
			if srv := GetServer(serviceID); srv != nil {
				srv.unpublishedS3Admission.release(name)
			}
		},
	)
}

// ReleaseUnpublishedS3Name retires a ticket after a detached transfer flow
// successfully deletes its last unpublished name.
func ReleaseUnpublishedS3Name(serviceID, name string) {
	if srv := GetServer(serviceID); srv != nil {
		srv.unpublishedS3Admission.release(name)
	}
}

func (w *CNS3Writer) unpublishedS3Admission() (*unpublishedS3Admission, error) {
	if w.serviceID == "" {
		return nil, nil
	}
	srv := GetServer(w.serviceID)
	if srv == nil {
		return nil, moerr.NewInvalidStateNoCtx("missing CN unpublished S3 admission service")
	}
	return srv.unpublishedS3Admission, nil
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
	writer.fs = fs
	writer.serviceID = serviceID

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
	if serviceID != "" {
		sinkerOpts = append(sinkerOpts, UnpublishedS3AdmissionOption(serviceID))
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
	// SyncAndTakeResults transfers the returned stats out of the sinker. Keep
	// their cleanup ownership here until the enclosing transaction accepts the
	// metadata or the failed operation deletes the objects.
	for i := range stats {
		w.ownedPersistedNames = append(w.ownedPersistedNames, stats[i].ObjectName().String())
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

// TransferPersistedObjects hands completed objects to the transaction after the
// caller has copied their output metadata. No more writes may occur between
// Sync and this call. CN writers flush their entire tail (TailSizeCap=0), so
// resetting after retention releases only completed results and reusable state.
// Until retention succeeds, both sinker and detached names remain writer-owned.
func (w *CNS3Writer) TransferPersistedObjects(proc *process.Process) error {
	_, err := w.TransferPersistedObjectsOwner(proc)
	return err
}

// TransferPersistedObjectsOwner returns the lightweight owner for callers
// that must also track a pipeline-level abort before workspace registration.
func (w *CNS3Writer) TransferPersistedObjectsOwner(proc *process.Process) (*UnpublishedS3ObjectOwner, error) {
	stats, _ := w.sinker.GetResult()
	names := append([]string(nil), w.ownedPersistedNames...)
	for i := range stats {
		names = append(names, stats[i].ObjectName().String())
	}
	if len(names) == 0 {
		return nil, nil
	}
	admission, err := w.unpublishedS3Admission()
	if err != nil {
		w.cleanupPending = true
		return nil, err
	}
	owner, err := newUnpublishedS3ObjectOwner(w.fs, admission, names, names...)
	if err != nil {
		w.cleanupPending = true
		return nil, err
	}
	if !RetainUnpublishedS3ObjectOwner(proc, owner) {
		w.cleanupPending = true
		return nil, moerr.NewInternalErrorNoCtx("transaction workspace cannot retain unpublished S3 objects")
	}
	w.sinker.Reset()
	w.ownedPersistedNames = nil
	w.cleanupPending = false
	return owner, nil
}

// DeletePersisted removes objects still owned by this writer, including
// results detached by SyncAndFillBlockInfoBat. On deletion failure the sinker
// and writer retain their exact names so the caller can retry before closing.
func (w *CNS3Writer) DeletePersisted(ctx context.Context) error {
	if w == nil {
		return nil
	}
	w.cleanupPending = true
	admission, err := w.unpublishedS3Admission()
	if err != nil {
		return err
	}
	cleanupCtx, cancel := UnpublishedS3CleanupContext(ctx)
	defer cancel()

	if w.sinker != nil {
		if names, err := w.sinker.DeletePersisted(cleanupCtx); err != nil {
			// The snapshot is complete even after an ambiguous pipeline Sync.
			// Keep only names for the retry; the session mpool and sinker buffers
			// must not outlive the writer's failed execution.
			w.ownedPersistedNames = append(w.ownedPersistedNames, names...)
			if w.blockInfoBat != nil {
				w.blockInfoBat.Clean(w.sinker.GetMPool())
				w.blockInfoBat = nil
			}
			_ = w.sinker.Close()
			w.sinker = nil
			return err
		}
	}
	if len(w.ownedPersistedNames) == 0 {
		w.cleanupPending = false
		return nil
	}
	completed, remaining, err := ioutil.DeleteUnpublishedObjectsWithProgress(
		cleanupCtx, w.fs, w.ownedPersistedNames...)
	if admission != nil {
		for _, name := range completed {
			admission.release(name)
		}
	}
	w.ownedPersistedNames = remaining
	if err != nil {
		// Detached results are the only remaining ownership. Do not let a
		// failed Delete keep the sinker, output batch, or session mpool alive.
		if w.sinker != nil {
			if w.blockInfoBat != nil {
				w.blockInfoBat.Clean(w.sinker.GetMPool())
				w.blockInfoBat = nil
			}
			_ = w.sinker.Close()
			w.sinker = nil
		}
		return err
	}
	w.cleanupPending = false
	return nil
}

// CloseWithCleanup removes still-owned objects after a failed operation, then
// closes the writer. A failed delete retains only file-service names so its
// caller can retry cleanup during the next lifecycle callback.
func (w *CNS3Writer) CloseWithCleanup(ctx context.Context, failed bool) error {
	cleanupRequired := failed || w.cleanupPending
	if cleanupRequired {
		if err := w.DeletePersisted(ctx); err != nil {
			return err
		}
		// Close may return the already-reported pipeline drain error, but it
		// has still released all sinker resources. The unaccepted objects are
		// deleted, so that execution error must not keep this owner alive.
		_ = w.Close()
		return nil
	}
	return w.Close()
}

func (w *CNS3Writer) Close() (err error) {
	if w.cleanupPending {
		return moerr.NewInternalErrorNoCtx("cannot close S3 writer with pending object cleanup")
	}
	// Close on the successful path means the caller has accepted the result
	// metadata. Clone readers use this path instead of TransferPersistedObjects.
	acceptedNames := append([]string(nil), w.ownedPersistedNames...)
	if w.sinker != nil {
		stats, _ := w.sinker.GetResult()
		for i := range stats {
			acceptedNames = append(acceptedNames, stats[i].ObjectName().String())
		}
	}
	admission, admissionErr := w.unpublishedS3Admission()
	if admissionErr != nil {
		return admissionErr
	}
	var mp *mpool.MPool
	if w.sinker != nil {
		mp = w.sinker.GetMPool()
		// Sinker.Close always tears down its buffers and references before
		// returning a pipeline-drain error. Do not retain a closed sinker and
		// make a completed abort cleanup impossible to finish on retry.
		err = w.sinker.Close()
		w.sinker = nil
	}
	w.ownedPersistedNames = nil
	if admission != nil {
		for _, name := range acceptedNames {
			admission.release(name)
		}
	}

	if w.blockInfoBat != nil {
		w.blockInfoBat.Clean(mp)
		w.blockInfoBat = nil
	}

	return err
}

// ResetWithCleanup discards this execution's data after failure and resets the
// sinker only after its persisted objects have been deleted. A successful
// execution treats the results as handed off before resetting reusable state.
func (w *CNS3Writer) ResetWithCleanup(ctx context.Context, failed bool) error {
	if failed || w.cleanupPending {
		if err := w.DeletePersisted(ctx); err != nil {
			return err
		}
	}
	w.Reset()
	return nil
}

// Reset discards any buffered or staged data accumulated since the last
// Sync, without tearing down the underlying sinker. Call this when the
// enclosing pipeline execution is being reset so that stale data is not
// carried into the next execution. The sinker's buffer pool and arena
// are kept alive for efficient reuse.
func (w *CNS3Writer) Reset() {
	if w.cleanupPending {
		return
	}
	// Reset is used after the caller has accepted these completed results.
	// TransferPersistedObjects clears the names only after moving their ticket
	// to the transaction owner, so this release is idempotent there.
	if admission, err := w.unpublishedS3Admission(); err == nil && admission != nil {
		for _, name := range w.ownedPersistedNames {
			admission.release(name)
		}
		if w.sinker != nil {
			stats, _ := w.sinker.GetResult()
			for i := range stats {
				admission.release(stats[i].ObjectName().String())
			}
		}
	}
	if w.sinker != nil {
		w.sinker.Reset()
	}
	w.ownedPersistedNames = nil
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
