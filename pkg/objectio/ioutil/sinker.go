// Copyright 2021 Matrix Origin
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

package ioutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

const DefaultInMemoryStagedSize = mpool.MB * 16

// Cap each sort-key pipeline submission so backfill can produce reasonably
// sized objects without letting pending sorted batches grow without bound.
const pipelineSortKeySubmitThreshold = mpool.MB * 128

const (
	unpublishedObjectDeleteBatchSize = 1000
	unpublishedObjectDeleteTimeout   = 10 * time.Minute
)

type pipelineFlushKeyType struct{}

// PipelineFlushKey is the context key used to enable pipeline flush in Sinker.
// Set the context value to true to enable pipeline flush for INSERT operations
// (e.g., ALTER TABLE ADD PRIMARY KEY).
var PipelineFlushKey = pipelineFlushKeyType{}

type SinkerOption func(*Sinker)

func WithAllMergeSorted() SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.allMergeSorted = true
	}
}

func WithDedupAll() SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.dedupAll = true
	}
}

func WithTailSizeCap(size int) SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.tailSizeCap = size
	}
}

func WithMemorySizeThreshold(size int) SinkerOption {
	return func(sinker *Sinker) {
		sinker.staged.memorySizeThreshold = size
	}
}

// WithBoundedRuns limits the materialized size of an ordinary spill run.
// The factory must expose active object names for commit-ambiguous writes.
func WithBoundedRuns() SinkerOption {
	return func(sinker *Sinker) { sinker.config.boundedRuns = true }
}

func WithOffHeap() SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.offHeap = true
	}
}

func WithBuffer(buffer containers.IBatchBuffer, isOwner bool) SinkerOption {
	return func(sinker *Sinker) {
		sinker.buf.isOwner = isOwner
		sinker.buf.buffers = buffer
	}
}

func WithPipelineFlush() SinkerOption {
	return func(sinker *Sinker) {
		sinker.pipe.enabled = true
	}
}

// WithChunkedColumnPolicy configures a live, service-specific rollout policy
// for object writers created by this Sinker. A nil or unsupported FileSinker
// keeps the legacy persisted-column format.
func WithChunkedColumnPolicy(policy objectio.ChunkedColumnPolicy) SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.chunkedColumnPolicy = policy
	}
}

// WithBeforePersist installs a hook that runs after the active object name is
// known and immediately before Sync can make the object durable. Callers can
// use it to establish restart-safe ownership before a commit-ambiguous write.
func WithBeforePersist(hook func(context.Context, string) error) SinkerOption {
	return func(sinker *Sinker) {
		sinker.beforePersist = hook
	}
}

type FileSinker interface {
	Sink(context.Context, *batch.Batch) error
	Sync(context.Context) (*objectio.ObjectStats, error)
	Reset()
	Close() error
}

// activeObjectNamer exposes the object name reserved by a FileSinker for its
// current Sync attempt. A Sync error is commit-ambiguous: the object may have
// reached storage even though valid publishable stats were not returned. The
// enclosing Sinker captures this name before Reset or Close discards it.
type activeObjectNamer interface {
	ActiveObjectName() string
}

var _ FileSinker = new(FSinkerImpl)

type FSinkerImpl struct {
	writer *BlockWriter
	arena  *objectio.WriteArena
	mp     *mpool.MPool
	fs     fileservice.FileService

	sortKeyPos          int
	isPrimaryKey        bool
	isTombstone         bool
	seqnums             []uint16
	schemaVersion       uint32
	hiddenSelection     objectio.HiddenColumnSelection
	chunkedColumnPolicy objectio.ChunkedColumnPolicy
}

func (s *FSinkerImpl) SetChunkedColumnPolicy(policy objectio.ChunkedColumnPolicy) {
	s.chunkedColumnPolicy = policy
}

func (s *FSinkerImpl) Sink(ctx context.Context, b *batch.Batch) error {
	if s.writer == nil {
		if s.arena == nil {
			s.arena = objectio.GetArena(objectio.ArenaLarge)
		}
		s.arena.Reset()
		if s.isTombstone {
			s.writer = ConstructTombstoneWriterWithArena(
				s.hiddenSelection,
				s.fs,
				s.arena,
			)
		} else {
			s.writer = ConstructWriterWithArena(
				s.schemaVersion,
				s.seqnums,
				s.sortKeyPos,
				s.isPrimaryKey,
				s.isTombstone,
				s.fs,
				s.arena,
			)
		}
		s.writer.SetChunkedColumnPolicy(s.chunkedColumnPolicy)
	}

	_, err := s.writer.WriteBatch(b)
	return err
}

func (s *FSinkerImpl) Sync(ctx context.Context) (*objectio.ObjectStats, error) {
	if _, _, err := s.writer.Sync(ctx); err != nil {
		return nil, err
	}

	var ss objectio.ObjectStats
	if s.sortKeyPos > -1 {
		ss = s.writer.GetObjectStats(objectio.WithSorted(), objectio.WithCNCreated())
	} else {
		ss = s.writer.GetObjectStats(objectio.WithCNCreated())
	}

	// s.writer.Reset
	s.writer = nil

	return &ss, nil
}

func (s *FSinkerImpl) Reset() {
	if s.writer != nil {
		// s.writer.Reset
		s.writer = nil
	}
}

func (s *FSinkerImpl) ActiveObjectName() string {
	if s.writer == nil {
		return ""
	}
	return s.writer.GetName().String()
}

func (s *FSinkerImpl) Close() error {
	s.writer = nil
	if s.arena != nil {
		// Reset grows the arena if needed (adaptive sizing), then return it
		// to the free list so the pre-warmed backing array, serialBuf and
		// compressBuf are reused by the next FSinkerImpl.
		s.arena.Reset()
		// An arena left unused for a while is freed by the pool itself.
		objectio.PutArena(s.arena)
		s.arena = nil
	}
	return nil
}

// FileSinkerFactory is a factory function to create a FileSinker
type FileSinkerFactory func(*mpool.MPool, fileservice.FileService) FileSinker

// This factory is used to create a FileSinker for tombstone
// hiddenSelection: whether to write hidden tombstone
//
//	true: TN created tombstone objects
//	false: CN created tombstone objects
func NewTombstoneFSinkerImplFactory(hidden objectio.HiddenColumnSelection) FileSinkerFactory {
	return func(mp *mpool.MPool, fs fileservice.FileService) FileSinker {
		return NewTombstoneFSinkerImpl(hidden, mp, fs)
	}
}

// This factory is used to create a FileSinker for all kinds of objects
// the user should provide the seqnums, sortKeyPos, isPrimaryKey, isTombstone, schemaVersion
func NewFSinkerImplFactory(
	seqnums []uint16,
	sortKeyPos int,
	isPrimaryKey bool,
	isTombstone bool,
	schemaVersion uint32,
) FileSinkerFactory {
	return func(mp *mpool.MPool, fs fileservice.FileService) FileSinker {
		return NewFSinkerImpl(
			seqnums,
			sortKeyPos,
			isPrimaryKey,
			isTombstone,
			schemaVersion,
			mp,
			fs,
		)
	}
}

func NewTombstoneFSinkerImpl(
	hidden objectio.HiddenColumnSelection,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *FSinkerImpl {
	return &FSinkerImpl{
		fs:              fs,
		mp:              mp,
		isTombstone:     true,
		hiddenSelection: hidden,
	}
}

func NewFSinkerImpl(
	seqnums []uint16,
	sortKeyPos int,
	isPrimaryKey bool,
	isTombstone bool,
	schemaVersion uint32,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *FSinkerImpl {
	return &FSinkerImpl{
		fs:            fs,
		mp:            mp,
		seqnums:       seqnums,
		sortKeyPos:    sortKeyPos,
		isPrimaryKey:  isPrimaryKey,
		isTombstone:   isTombstone,
		schemaVersion: schemaVersion,
	}
}

func NewTombstoneSinker(
	hidden objectio.HiddenColumnSelection,
	pkType types.Type,
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...SinkerOption,
) *Sinker {
	factory := NewTombstoneFSinkerImplFactory(hidden)
	attrs, attrTypes := objectio.GetTombstoneSchema(pkType, hidden)
	return NewSinker(
		objectio.TombstonePrimaryKeyIdx,
		attrs,
		attrTypes,
		factory,
		mp,
		fs,
		opts...,
	)
}

func NewSinker(
	sortKeyIdx int,
	attrs []string,
	attrTypes []types.Type,
	factory FileSinkerFactory,
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...SinkerOption,
) *Sinker {
	sinker := &Sinker{
		schema: struct {
			attrs      []string
			attrTypes  []types.Type
			sortKeyIdx int
		}{
			attrs:      attrs,
			attrTypes:  attrTypes,
			sortKeyIdx: sortKeyIdx,
		},
		fSinker: struct {
			executor FileSinker
			factory  FileSinkerFactory
		}{
			factory: factory,
		},

		mp: mp,
		fs: fs,
	}

	for _, opt := range opts {
		opt(sinker)
	}
	if policy := sinker.config.chunkedColumnPolicy; policy != nil {
		factory := sinker.fSinker.factory
		sinker.fSinker.factory = func(mp *mpool.MPool, fs fileservice.FileService) FileSinker {
			fileSinker := factory(mp, fs)
			if setter, ok := fileSinker.(interface {
				SetChunkedColumnPolicy(objectio.ChunkedColumnPolicy)
			}); ok {
				setter.SetChunkedColumnPolicy(policy)
			}
			return fileSinker
		}
	}

	sinker.fillDefaults()
	return sinker
}

type sinkerStats struct {
	Name               string
	HighWatermarkCnt   uint64
	HighWatermarkBytes uint64
	CurrentCnt         uint64
	CurrentBytes       uint64
}

func (s *sinkerStats) String() string {
	return fmt.Sprintf("%s, high cnt: %d, current cnt: %d, hight bytes: %d, current bytes: %d",
		s.Name, s.HighWatermarkCnt, s.CurrentCnt, s.HighWatermarkBytes, s.CurrentBytes)
}

func (s *sinkerStats) updateCount(n int) {
	if n > 0 {
		s.CurrentCnt += uint64(n)
	} else if n < 0 {
		s.CurrentCnt -= uint64(-n)
	}

	if s.CurrentCnt > s.HighWatermarkCnt {
		s.HighWatermarkCnt = s.CurrentCnt
	}
}

func (s *sinkerStats) updateBytes(n int) {
	if n > 0 {
		s.CurrentBytes += uint64(n)
	} else if n < 0 {
		s.CurrentBytes -= uint64(-n)
	}
	if s.CurrentBytes > s.HighWatermarkBytes {
		s.HighWatermarkBytes = s.CurrentBytes
	}
}

type Sinker struct {
	schema struct {
		attrs      []string
		attrTypes  []types.Type
		sortKeyIdx int
	}
	config struct {
		allMergeSorted      bool
		dedupAll            bool
		tailSizeCap         int
		offHeap             bool
		boundedRuns         bool
		chunkedColumnPolicy objectio.ChunkedColumnPolicy
		// capacityHook gates each object right before it persists; nil
		// in every caller without an engine capacity ledger.
		capacityHook UncommittedCapacityHook
		// The workspace, rather than the sinker, owns unpublished cleanup.
		workspaceOwnedCleanup bool
	}
	fSinker struct {
		executor FileSinker
		factory  FileSinkerFactory
	}
	staged struct {
		inMemStats          sinkerStats
		inMemory            []*batch.Batch
		inMemoryBounds      []int
		persisted           []objectio.ObjectStats
		unpublished         []string
		inMemorySize        int
		memorySizeThreshold int
		aborted             bool
	}
	result struct {
		persisted []objectio.ObjectStats
		tail      []*batch.Batch
	}

	buf struct {
		isOwner  bool
		bufStats sinkerStats
		buffers  containers.IBatchBuffer
	}

	pipe struct {
		enabled bool
		result  *pipelineResult
	}

	timing struct {
		spillCount int64 // atomic
		sortNs     int64 // atomic, nanoseconds
		sinkNs     int64 // atomic, nanoseconds (sync path only)
		syncNs     int64 // atomic, nanoseconds (sync path only)
		waitNs     int64 // atomic, nanoseconds (main goroutine blocked on pool submit)
		spillNs    int64 // atomic, nanoseconds (total wall time in trySpill)
	}

	mp *mpool.MPool
	fs fileservice.FileService

	beforePersist func(context.Context, string) error
}

func (sinker *Sinker) String() string {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(
		"schema:{attrs=%v, types=%v, sortIdx=%d}; ",
		sinker.schema.attrs, sinker.schema.attrTypes, sinker.schema.sortKeyIdx))

	buf.WriteString(fmt.Sprintf(
		"config:{allMergeSorted=%v, dedupAll=%v, tailSizeCap=%d}; ",
		sinker.config.allMergeSorted, sinker.config.dedupAll,
		sinker.config.tailSizeCap))

	return buf.String()
}

func (sinker *Sinker) fillDefaults() {
	if sinker.staged.memorySizeThreshold == 0 {
		sinker.staged.memorySizeThreshold = DefaultInMemoryStagedSize
	}

	sinker.staged.inMemStats.Name = "staged inmem stats"
	sinker.buf.bufStats.Name = "buffer stats"

	if sinker.buf.buffers == nil {
		sinker.buf.isOwner = true
		sinker.buf.buffers = containers.NewBatchFreeList(
			sinker.schema.attrs,
			sinker.schema.attrTypes,
			sinker.config.offHeap,
		)
	}
}

func (sinker *Sinker) runSizeLimit() int {
	if !sinker.config.boundedRuns || sinker.config.allMergeSorted || sinker.config.dedupAll {
		return 0
	}
	return sinker.staged.memorySizeThreshold
}

func (sinker *Sinker) GetResult() ([]objectio.ObjectStats, []*batch.Batch) {
	return sinker.result.persisted, sinker.result.tail
}

// GetUnpublishedFiles returns object names whose Sync may have reached
// storage without producing publishable stats. The snapshot is safe to use
// while transferring cleanup ownership out of the sinker.
func (sinker *Sinker) GetUnpublishedFiles() []string {
	files := make([]string, 0, len(sinker.staged.unpublished))
	files = append(files, sinker.staged.unpublished...)
	if sinker.pipe.result != nil {
		sinker.pipe.result.mu.RLock()
		files = append(files, sinker.pipe.result.unpublished...)
		sinker.pipe.result.mu.RUnlock()
	}
	return normalizeUnpublishedObjectNames(files)
}

// DeleteUnpublishedObjects deletes object files that have not crossed their
// caller's publication boundary. Object stores cap multi-delete requests at
// 1000 keys, so each bounded unit gets the same timeout used by checkpoint GC.
// The caller controls cancellation of the complete cleanup through ctx.
func DeleteUnpublishedObjects(
	ctx context.Context,
	fs fileservice.FileService,
	files ...string,
) (int, error) {
	return deleteUnpublishedObjects(ctx, fs, false, files...)
}

// A bounded writer may stop between requests; other callers retain the
// existing contract of passing even an expired context to the file service.
func deleteUnpublishedObjects(
	ctx context.Context,
	fs fileservice.FileService,
	stopOnCancel bool,
	files ...string,
) (int, error) {
	seen := make(map[string]struct{}, len(files))
	unique := make([]string, 0, len(files))
	for _, file := range files {
		if file == "" {
			continue
		}
		if _, ok := seen[file]; ok {
			continue
		}
		seen[file] = struct{}{}
		unique = append(unique, file)
	}
	for start := 0; start < len(unique); start += unpublishedObjectDeleteBatchSize {
		if stopOnCancel {
			if err := context.Cause(ctx); err != nil {
				return len(unique), err
			}
		}
		end := min(start+unpublishedObjectDeleteBatchSize, len(unique))
		deleteCtx, cancel := context.WithTimeoutCause(
			ctx, unpublishedObjectDeleteTimeout, moerr.CauseCleanUpUselessFiles,
		)
		err := fs.Delete(deleteCtx, unique[start:end]...)
		if stopOnCancel && moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			if cause := context.Cause(deleteCtx); cause != nil {
				cancel()
				return len(unique), cause
			}
		}
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) && end-start > 1 {
			// The file service may stop at the first missing name, leaving the
			// rest untouched. Confirm every member before releasing ownership.
			for _, file := range unique[start:end] {
				if stopOnCancel {
					if cause := context.Cause(deleteCtx); cause != nil {
						cancel()
						return len(unique), cause
					}
				}
				err = fs.Delete(deleteCtx, file)
				if stopOnCancel && moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
					if cause := context.Cause(deleteCtx); cause != nil {
						cancel()
						return len(unique), cause
					}
				}
				if err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
					break
				}
			}
		}
		cancel()
		if err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return len(unique), errors.Join(
				moerr.NewInternalErrorf(
					ctx, "delete unpublished objects [%d:%d]", start, end),
				err,
			)
		}
	}
	return len(unique), nil
}

// PersistedNames takes this sinker's exact ownership snapshot — every
// object it persisted, or may have persisted after a commit-ambiguous
// Sync error, and has not transferred out of its lifecycle yet — WITHOUT
// deleting anything (issue #25590 round-15 review): a caller on a
// lock-holding or user-blocking path (a transaction's statement
// admission) can transfer the names to its asynchronous cleanup owner
// instead of synchronously deleting them on a file service whose
// deletion a stalled context cannot move. The snapshot follows the same
// completeness rule as DeletePersisted: a still-running pipeline is
// drained first so no sibling write's file can be missed, and the tracked
// references stay inside the sinker — closing it drops them exactly as
// DeletePersisted's callers have always done after taking the snapshot.
func (sinker *Sinker) PersistedNames() []string {
	return sinker.persistedSnapshot()
}

// CancelPending stops an abandoned generation without waiting for its workers.
// Owners of several sinkers cancel all of them before waiting for any one.
func (sinker *Sinker) CancelPending() {
	if sinker.pipe.result != nil {
		sinker.pipe.result.cancel()
	}
	if sinker.runSizeLimit() > 0 {
		sinker.staged.aborted = true
	}
}

// AbandonAndRetainableNames stops abandoned work before taking a snapshot.
// A workspace owner receives all names. For an unbound writer, pending runs
// remain under local cleanup and only handed-off results can be retained.
func (sinker *Sinker) AbandonAndRetainableNames() []string {
	sinker.CancelPending()
	files := sinker.persistedSnapshot()
	if sinker.config.workspaceOwnedCleanup {
		return files
	}
	files = files[:0]
	for i := range sinker.result.persisted {
		files = append(files, sinker.result.persisted[i].ObjectName().String())
	}
	return normalizeUnpublishedObjectNames(files)
}

// persistedSnapshot assembles the persisted/possibly-persisted name
// snapshot shared by DeletePersisted (which deletes it) and
// PersistedNames (which only hands it out).
func (sinker *Sinker) persistedSnapshot() []string {
	if sinker.pipe.enabled && sinker.pipe.result != nil {
		// A failed pipeline may still have successful sibling writes. Wait for
		// all of them before taking the ownership snapshot.
		_ = sinker.drainPipeline()
	}

	files := make([]string, 0,
		len(sinker.staged.persisted)+len(sinker.result.persisted)+
			len(sinker.staged.unpublished))
	files = append(files, sinker.staged.unpublished...)
	appendStats := func(stats []objectio.ObjectStats) {
		for i := range stats {
			files = append(files, stats[i].ObjectName().String())
		}
	}
	appendStats(sinker.staged.persisted)
	appendStats(sinker.result.persisted)
	if sinker.pipe.result != nil {
		sinker.pipe.result.mu.RLock()
		appendStats(sinker.pipe.result.persisted)
		files = append(files, sinker.pipe.result.unpublished...)
		sinker.pipe.result.mu.RUnlock()
	}
	return normalizeUnpublishedObjectNames(files)
}

// DeletePersisted deletes every object that this sinker has persisted, or may
// have persisted after a commit-ambiguous Sync error, but has not transferred
// out of its lifecycle yet. It returns the exact ownership snapshot on both
// success and failure. The tracked references are retained when deletion fails
// so the caller may retry or hand them off before closing the sinker.
func (sinker *Sinker) DeletePersisted(ctx context.Context) ([]string, error) {
	if sinker.config.workspaceOwnedCleanup {
		return nil, moerr.NewInternalErrorNoCtx("workspace owns unpublished spill cleanup")
	}
	files := sinker.persistedSnapshot()
	_, err := DeleteUnpublishedObjects(ctx, sinker.fs, files...)
	if err != nil {
		return files, err
	}

	sinker.staged.persisted = sinker.staged.persisted[:0]
	sinker.result.persisted = sinker.result.persisted[:0]
	sinker.staged.unpublished = sinker.staged.unpublished[:0]
	if sinker.pipe.result != nil {
		sinker.pipe.result.mu.Lock()
		sinker.pipe.result.persisted = sinker.pipe.result.persisted[:0]
		sinker.pipe.result.unpublished = sinker.pipe.result.unpublished[:0]
		sinker.pipe.result.mu.Unlock()
	}
	return files, nil
}

// PendingUnpublishedRuns snapshots the objects that have not crossed Sync's
// result handoff. In-flight pipeline jobs can add names until they are drained.
func (sinker *Sinker) PendingUnpublishedRuns() []string {
	files := append([]string(nil), sinker.staged.unpublished...)
	appendStats := func(stats []objectio.ObjectStats) {
		for i := range stats {
			files = append(files, stats[i].ObjectName().String())
		}
	}
	appendStats(sinker.staged.persisted)
	if sinker.pipe.result != nil {
		sinker.pipe.result.mu.RLock()
		appendStats(sinker.pipe.result.persisted)
		files = append(files, sinker.pipe.result.unpublished...)
		sinker.pipe.result.mu.RUnlock()
	}
	return normalizeUnpublishedObjectNames(files)
}

// hasPendingUnpublishedRuns is used after pipeline drain, when the owner only
// needs to decide whether Reset may discard the generation.
func (sinker *Sinker) hasPendingUnpublishedRuns() bool {
	if len(sinker.staged.persisted) != 0 || len(sinker.staged.unpublished) != 0 {
		return true
	}
	if r := sinker.pipe.result; r != nil {
		r.mu.RLock()
		pending := len(r.persisted) != 0 || len(r.unpublished) != 0
		r.mu.RUnlock()
		return pending
	}
	return false
}

// RetryUnpublishedRuns stops the current generation and deletes only its
// pre-handoff objects. On failure their names remain available for retry.
func (sinker *Sinker) RetryUnpublishedRuns(ctx context.Context) ([]string, error) {
	if sinker.config.workspaceOwnedCleanup {
		return nil, moerr.NewInternalErrorNoCtx("workspace owns unpublished spill cleanup")
	}
	if sinker.runSizeLimit() == 0 {
		return nil, nil
	}
	sinker.staged.aborted = true
	if sinker.pipe.result != nil {
		sinker.pipe.result.cancel()
		_ = sinker.drainPipeline()
	}
	sinker.cleanupInMemoryStaged()
	files := sinker.PendingUnpublishedRuns()
	if _, err := deleteUnpublishedObjects(ctx, sinker.fs, true, files...); err != nil {
		return files, err
	}
	sinker.staged.persisted = sinker.staged.persisted[:0]
	sinker.staged.unpublished = sinker.staged.unpublished[:0]
	if sinker.pipe.result != nil {
		sinker.pipe.result.mu.Lock()
		sinker.pipe.result.persisted = sinker.pipe.result.persisted[:0]
		sinker.pipe.result.unpublished = sinker.pipe.result.unpublished[:0]
		sinker.pipe.result.mu.Unlock()
	}
	return files, nil
}

func (sinker *Sinker) cleanupRunOnFailure(ctx context.Context, operationErr *error) {
	if *operationErr == nil || sinker.staged.aborted {
		return
	}
	if r := sinker.pipe.result; r != nil {
		// Select the first terminal cause under the same lock used by workers
		// and context callbacks. In particular, a later cancellation must not
		// mask a worker error already selected before this operation failed.
		r.mu.Lock()
		if r.err != nil {
			*operationErr = r.err
		} else {
			r.err = *operationErr
		}
		r.mu.Unlock()
		r.cancel()
	}
	if sinker.config.workspaceOwnedCleanup {
		// The transaction owns the reservations and must take the names.
		// Never delete or call back into it from a possibly lock-held write.
		sinker.staged.aborted = true
		if sinker.pipe.result != nil {
			_ = sinker.drainPipeline()
		}
		sinker.cleanupInMemoryStaged()
		return
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	files, cleanupErr := sinker.RetryUnpublishedRuns(cleanupCtx)
	if cleanupErr != nil {
		logutil.Error("failed to clean up unpublished CN spill objects",
			zap.Strings("objects", files), zap.Error(cleanupErr))
	}
}

func (sinker *Sinker) fetchBuffer() (*batch.Batch, error) {
	x := sinker.buf.buffers.Len()
	bat := sinker.buf.buffers.FetchWithSchema(sinker.schema.attrs, sinker.schema.attrTypes)
	y := sinker.buf.buffers.Len()

	if x < y {
		sinker.buf.bufStats.updateCount(-1)
		sinker.buf.bufStats.updateBytes(-bat.Size())
	}

	if err := bat.PreExtend(sinker.mp, objectio.BlockMaxRows); err != nil {
		// Earlier vectors may already have allocated before a later one failed.
		// The fetched batch is no longer owned by the freelist, so release it.
		bat.Clean(sinker.mp)
		return nil, err
	}
	return bat, nil
}

func (sinker *Sinker) putBackBuffer(bat *batch.Batch) {
	if limit := sinker.runSizeLimit(); limit > 0 && bat.Allocated() > limit {
		bat.Clean(sinker.mp)
		return
	}
	x := sinker.buf.buffers.Len()
	sinker.buf.buffers.Putback(bat, sinker.mp)
	y := sinker.buf.buffers.Len()

	if x > y {
		sinker.buf.bufStats.updateCount(1)
		sinker.buf.bufStats.updateBytes(bat.Size())
	}
}

func (sinker *Sinker) putBackOneInMemory(idx int) {
	bat := sinker.staged.inMemory[idx]
	sinker.staged.inMemory[idx] = nil
	if sinker.runSizeLimit() > 0 {
		sinker.staged.inMemorySize -= sinker.staged.inMemoryBounds[idx]
		sinker.staged.inMemoryBounds[idx] = 0
	} else {
		sinker.staged.inMemorySize -= bat.Size()
	}
	sinker.staged.inMemStats.updateCount(-1)
	sinker.staged.inMemStats.updateBytes(-bat.Size())
	sinker.putBackBuffer(bat)
}

func (sinker *Sinker) popStaged() (*batch.Batch, int) {
	if len(sinker.staged.inMemory) == 0 {
		return nil, 0
	}

	ret := sinker.staged.inMemory[len(sinker.staged.inMemory)-1]
	sinker.staged.inMemory = sinker.staged.inMemory[:len(sinker.staged.inMemory)-1]
	bound := ret.Size()
	if sinker.runSizeLimit() > 0 {
		last := len(sinker.staged.inMemoryBounds) - 1
		bound = sinker.staged.inMemoryBounds[last]
		sinker.staged.inMemoryBounds = sinker.staged.inMemoryBounds[:last]
	}
	sinker.staged.inMemorySize -= bound

	sinker.staged.inMemStats.updateCount(-1)
	sinker.staged.inMemStats.updateBytes(-ret.Size())

	return ret, bound
}

// pushStaged take the ownership of the bat
func (sinker *Sinker) pushStaged(
	ctx context.Context, bat *batch.Batch, bound int,
) error {

	sinker.staged.inMemStats.updateCount(1)
	sinker.staged.inMemStats.updateBytes(bat.Size())

	sinker.staged.inMemory = append(sinker.staged.inMemory, bat)
	if sinker.runSizeLimit() > 0 {
		sinker.staged.inMemoryBounds = append(sinker.staged.inMemoryBounds, bound)
		sinker.staged.inMemorySize += bound
	} else {
		sinker.staged.inMemorySize += bat.Size()
	}
	if sinker.staged.inMemorySize >= sinker.staged.memorySizeThreshold {
		return sinker.trySpill(ctx)
	}
	return nil
}

func (sinker *Sinker) clearInMemoryStaged() {
	sinker.staged.inMemory = sinker.staged.inMemory[:0]
	sinker.staged.inMemoryBounds = sinker.staged.inMemoryBounds[:0]
	sinker.staged.inMemorySize = 0
}

func (sinker *Sinker) cleanupInMemoryStaged() {
	for i, bat := range sinker.staged.inMemory {
		if bat != nil {
			sinker.putBackBuffer(bat)
			sinker.staged.inMemory[i] = nil
		}
	}
	sinker.staged.inMemory = sinker.staged.inMemory[:0]
	sinker.staged.inMemoryBounds = sinker.staged.inMemoryBounds[:0]
	sinker.staged.inMemorySize = 0
}

func (sinker *Sinker) trySortInMemoryStaged(ctx context.Context) error {
	if sinker.schema.sortKeyIdx == -1 {
		return nil
	}
	var idxBuf []int64
	var shuffleBuf []byte
	for _, bat := range sinker.staged.inMemory {
		if sinker.runSizeLimit() > 0 {
			if err := context.Cause(ctx); err != nil {
				return err
			}
		}
		if err := mergeutil.SortColumnsByIndexWithBuf(
			bat.Vecs,
			sinker.schema.sortKeyIdx,
			sinker.mp,
			&idxBuf,
			&shuffleBuf,
		); err != nil {
			return err
		}
	}
	return nil
}

// pipelineSubmit submits sorted batches to the global sink pool for async
// serialization + IO. Transfers batch ownership to the pool workers.
//
// The pipelineResult context is derived from the first caller's ctx. This
// assumes all Write calls within a single Sinker lifecycle share the same
// parent context (e.g., ALTER TABLE's process context). If the parent
// context is cancelled, all in-flight pipeline operations will be aborted.
func (sinker *Sinker) pipelineSubmit(ctx context.Context, data []*batch.Batch) error {
	if sinker.pipe.result == nil {
		pctx, cancel := context.WithCancel(ctx)
		sinker.pipe.result = &pipelineResult{
			ctx:    pctx,
			cancel: cancel,
		}
	}
	r := sinker.pipe.result
	var stop func() bool
	active := true // protected by r.mu
	if sinker.runSizeLimit() > 0 {
		stop = context.AfterFunc(ctx, func() {
			r.mu.Lock()
			if active && r.err == nil {
				r.err = context.Cause(ctx)
				r.cancel()
			}
			r.mu.Unlock()
		})
	}
	waitStart := time.Now()
	err := GetDefaultSinkPool().Submit(&poolSinkJob{
		data:         data,
		factory:      sinker.fSinker.factory,
		mp:           sinker.mp,
		fs:           sinker.fs,
		result:       r,
		capacityHook: sinker.config.capacityHook,
	})
	atomic.AddInt64(&sinker.timing.waitNs, int64(time.Since(waitStart)))
	if stop != nil {
		r.mu.Lock()
		if r.err == nil {
			if cause := context.Cause(ctx); cause != nil {
				r.err = cause
				r.cancel()
			}
		}
		active = false
		if r.err != nil {
			err = r.err
		}
		r.mu.Unlock()
		stop()
	}
	return err
}

// drainPipeline waits for all in-flight pipeline jobs to complete and returns
// any accumulated error.
func (sinker *Sinker) drainPipeline() error {
	if sinker.pipe.result == nil {
		return nil
	}
	sinker.pipe.result.pending.Wait()
	return sinker.pipe.result.getError()
}

func (sinker *Sinker) trySpill(ctx context.Context) error {
	spillStart := time.Now()
	defer func() {
		atomic.AddInt64(&sinker.timing.spillCount, 1)
		atomic.AddInt64(&sinker.timing.spillNs, int64(time.Since(spillStart)))
	}()

	// sort all in memory data
	sortStart := time.Now()
	if err := sinker.trySortInMemoryStaged(ctx); err != nil {
		return err
	}
	defer sinker.cleanupInMemoryStaged()

	// merge sort
	if sinker.schema.sortKeyIdx != -1 {
		if sinker.config.dedupAll {
			fSinker := sinker.getStageFileSinker()
			err := sinker.trySpillMergeSortAccumulate(ctx, fSinker)
			if err != nil {
				sinker.resetFileSinker()
				return err
			}
			atomic.AddInt64(&sinker.timing.sortNs, int64(time.Since(sortStart)))
			err = sinker.syncFileSinker(ctx, fSinker)
			sinker.resetFileSinker()
			return err
		}

		if sinker.pipe.enabled {
			err := sinker.trySpillMergeSortPipeline(ctx)
			atomic.AddInt64(&sinker.timing.sortNs, int64(time.Since(sortStart)))
			return err
		}

		fSinker := sinker.getStageFileSinker()
		err := sinker.trySpillMergeSortStreaming(ctx, fSinker)
		if err != nil {
			sinker.resetFileSinker()
			return err
		}
		atomic.AddInt64(&sinker.timing.sortNs, int64(time.Since(sortStart)))
		err = sinker.syncFileSinker(ctx, fSinker)
		sinker.resetFileSinker()
		return err
	}

	atomic.AddInt64(&sinker.timing.sortNs, int64(time.Since(sortStart)))

	// no sort key: pipeline path — steal in-memory staged batches
	if sinker.pipe.enabled {
		jobData := make([]*batch.Batch, len(sinker.staged.inMemory))
		copy(jobData, sinker.staged.inMemory)
		for i := range sinker.staged.inMemory {
			sinker.staged.inMemory[i] = nil
		}
		sinker.staged.inMemory = sinker.staged.inMemory[:0]
		sinker.staged.inMemoryBounds = sinker.staged.inMemoryBounds[:0]
		sinker.staged.inMemorySize = 0
		return sinker.pipelineSubmit(ctx, jobData)
	}

	// no sort key: spill directly (sync path)
	fSinker := sinker.getStageFileSinker()
	sinkStart := time.Now()
	for _, bat := range sinker.staged.inMemory {
		if err := fSinker.Sink(ctx, bat); err != nil {
			sinker.resetFileSinker()
			return err
		}
	}
	atomic.AddInt64(&sinker.timing.sinkNs, int64(time.Since(sinkStart)))
	err := sinker.syncFileSinker(ctx, fSinker)
	sinker.resetFileSinker()
	return err
}

// UncommittedCapacityHook gates one object sitting on the edge of
// persistence: the file sinker calls it right BEFORE its Sync would make
// the object durable, with the object's name, and a returned error aborts
// that spill before anything persists. The engine uses this hook to
// reserve capacity for each unpublished object as it is created, in
// object units rather than writer units, so a workspace dump whose
// writer spills several objects consumes one reservation per object and
// a refusal stops the spilling writer with no partially-persisted
// object behind it (issue #25590 round-18 review).
type UncommittedCapacityHook func(name string) error

// WithWorkspaceOwnedCleanup leaves unpublished names for the transaction's
// existing cleanup owner instead of deleting them during a sinker operation.
func WithWorkspaceOwnedCleanup() SinkerOption {
	return func(sinker *Sinker) { sinker.config.workspaceOwnedCleanup = true }
}

func (sinker *Sinker) WorkspaceOwnsCleanup() bool {
	return sinker != nil && sinker.config.workspaceOwnedCleanup
}

// ClearWorkspaceOwnedCleanup removes the binding after the owner took the names.
func (sinker *Sinker) ClearWorkspaceOwnedCleanup() {
	sinker.config.workspaceOwnedCleanup = false
}

// WithUncommittedCapacityHook installs the capacity hook consulted
// before each object persists. It is nil in production unless the
// caller's persistence is bounded by an engine admission ledger.
func WithUncommittedCapacityHook(hook UncommittedCapacityHook) SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.capacityHook = hook
	}
}

// SinkerOwner is the structural interface to a workspace that owns the
// uncommitted-capacity admission for the S3 writers its statements build:
// the pipeline's direct-S3 insert operator resolves the transaction
// operator's workspace through it and appends the option to its writer's
// options, binding the SAME per-object reservation-before-persistence
// admission the workspace dump carries — WITHOUT the operator's package
// importing the workspace's engine (issue #25590 round-19 review: an
// unbound direct-S3 writer persisted objects at saturation whose
// metadata could never be recorded, and the pipeline's teardown dropped
// them with no cleanup owner).
type SinkerOwner interface {
	SinkerCapacityOption() SinkerOption
}

// UnrecordedSinkerObjectsRetainer is the structural interface through
// which a writer being unwound, after its metadata publication failed or
// the pipeline broke down between persistence and recording, transfers
// its persisted-or-possibly-persisted names to the workspace's
// asynchronous cleanup obligation: those names are the only surviving
// reference to physical objects whose metadata record was refused, and
// closing a writer always dropped it (issue #25590 round-19 review).
// The owner already counts every recorded name through its own registries,
// so it filters those out and takes over only the UNRECORDED remainder.
type UnrecordedSinkerObjectsRetainer interface {
	RetainUnrecordedS3Objects(persisted []string, reason string)
}

// HasUncommittedCapacityHook reports whether a capacity hook guards this
// sinker's persistence — the assertion point of the writer-binding
// regressions (issue #25590 round-19 review).
func (sinker *Sinker) HasUncommittedCapacityHook() bool {
	return sinker != nil && sinker.config.capacityHook != nil
}

// RebindUncommittedCapacityHook installs hook as the per-object admission
// this sinker consults before each object persists, replacing whatever
// earlier hook (or none) it carried; nil removes it. Writers that
// outlive their binding transaction — a cached plan keeps the
// multi_update delegate's persistent insert sinkers alive exactly for
// their buffer pools and arenas — must rebind to the CURRENT execution's
// workspace when they are reused: the workspace that built them settles
// its own reservations at its terminal, and the next execution's objects
// reserve under a workspace whose release path can still reach them
// (issue #25590 round-35 review: without the rebind, every reused
// writer's next spill reserved against the finalized first workspace,
// stranding engine-wide admission capacity permanently).
func (sinker *Sinker) RebindUncommittedCapacityHook(hook UncommittedCapacityHook) {
	sinker.config.capacityHook = hook
}

// syncFileSinker persists the file sinker's data. On error, the caller is
// responsible for cleanup via resetFileSinker(); the executor is eventually
// closed in Sinker.Close().
func (sinker *Sinker) syncFileSinker(ctx context.Context, fSinker FileSinker) error {
	if sinker.beforePersist != nil {
		if name := activeFileSinkerObjectName(fSinker); name != "" {
			if err := sinker.beforePersist(ctx, name); err != nil {
				return err
			}
		}
	}
	syncStart := time.Now()
	if sinker.config.capacityHook != nil {
		if name := activeFileSinkerObjectName(fSinker); name != "" {
			if err := sinker.config.capacityHook(name); err != nil {
				return err
			}
		}
	}
	stats, err := fSinker.Sync(ctx)
	atomic.AddInt64(&sinker.timing.syncNs, int64(time.Since(syncStart)))
	if err != nil {
		if name := activeFileSinkerObjectName(fSinker); name != "" {
			sinker.staged.unpublished = append(sinker.staged.unpublished, name)
		}
		return err
	}
	sinker.staged.persisted = append(sinker.staged.persisted, *stats)
	return nil
}

func activeFileSinkerObjectName(sinker FileSinker) string {
	namer, ok := sinker.(activeObjectNamer)
	if !ok {
		return ""
	}
	return namer.ActiveObjectName()
}

// trySpillMergeSortStreaming merge-sorts staged data and streams each full
// block directly to the file sinker, reusing a single buffer. This avoids
// accumulating all sorted blocks in memory and eliminates PreExtend churn.
func (sinker *Sinker) trySpillMergeSortStreaming(
	ctx context.Context, fSinker FileSinker,
) error {
	buffer, err := sinker.fetchBuffer()
	if err != nil {
		return err
	}
	streamSinker := func(data *batch.Batch) (*batch.Batch, error) {
		sinkStart := time.Now()
		if err := fSinker.Sink(ctx, data); err != nil {
			// Keep ownership of the merge buffer so the caller can recycle it.
			return data, err
		}
		atomic.AddInt64(&sinker.timing.sinkNs, int64(time.Since(sinkStart)))
		data.CleanOnlyData()
		return data, nil
	}
	buffer, err = mergeutil.MergeSortBatches(
		sinker.staged.inMemory,
		sinker.schema.sortKeyIdx,
		buffer,
		streamSinker,
		sinker.mp,
		sinker.putBackOneInMemory,
	)
	sinker.putBackBuffer(buffer)
	return err
}

// trySpillMergeSortAccumulate merge-sorts staged data, accumulates all sorted
// blocks, runs dedup, then writes to the file sinker.
func (sinker *Sinker) trySpillMergeSortAccumulate(
	ctx context.Context, fSinker FileSinker,
) error {
	var sorted []*batch.Batch
	innersinker := func(data *batch.Batch) (*batch.Batch, error) {
		sorted = append(sorted, data)
		return sinker.fetchBuffer()
	}
	defer func() {
		for i, bat := range sorted {
			sinker.putBackBuffer(bat)
			sorted[i] = nil
		}
		sorted = sorted[:0]
	}()

	buffer, err := sinker.fetchBuffer()
	if err != nil {
		return err
	}
	buffer, err = mergeutil.MergeSortBatches(
		sinker.staged.inMemory,
		sinker.schema.sortKeyIdx,
		buffer,
		innersinker,
		sinker.mp,
		sinker.putBackOneInMemory,
	)
	sinker.putBackBuffer(buffer)
	if err != nil {
		return err
	}

	// dedup
	if err := containers.DedupSortedBatches(
		sinker.schema.sortKeyIdx,
		sorted,
		sinker.mp,
	); err != nil {
		return err
	}

	sinkStart := time.Now()
	for _, bat := range sorted {
		if err := fSinker.Sink(ctx, bat); err != nil {
			return err
		}
	}
	atomic.AddInt64(&sinker.timing.sinkNs, int64(time.Since(sinkStart)))
	return nil
}

// trySpillMergeSortPipeline merge-sorts staged data and submits sorted blocks
// to the pipeline in bounded groups. Grouping keeps the pipeline async path from
// turning every BlockMaxRows output block into its own tiny persisted object.
func (sinker *Sinker) trySpillMergeSortPipeline(ctx context.Context) (err error) {
	submitThreshold := sinker.staged.memorySizeThreshold
	if submitThreshold <= 0 {
		submitThreshold = DefaultInMemoryStagedSize
	}
	if submitThreshold > pipelineSortKeySubmitThreshold {
		submitThreshold = pipelineSortKeySubmitThreshold
	}

	var (
		pending     []*batch.Batch
		pendingSize int
	)
	cleanupPending := func() {
		for _, bat := range pending {
			if bat != nil {
				sinker.putBackBuffer(bat)
			}
		}
		pending = nil
		pendingSize = 0
	}
	defer func() {
		if err != nil {
			cleanupPending()
		}
	}()

	submitPending := func() error {
		if len(pending) == 0 {
			return nil
		}
		jobData := pending
		pending = nil
		pendingSize = 0
		return sinker.pipelineSubmit(ctx, jobData)
	}

	streamSubmit := func(data *batch.Batch) (*batch.Batch, error) {
		pending = append(pending, data)
		pendingSize += data.Size()
		if pendingSize >= submitThreshold {
			if err := submitPending(); err != nil {
				return nil, err
			}
		}
		return sinker.fetchBuffer()
	}

	buffer, err := sinker.fetchBuffer()
	if err != nil {
		return err
	}
	buffer, err = mergeutil.MergeSortBatches(
		sinker.staged.inMemory,
		sinker.schema.sortKeyIdx,
		buffer,
		streamSubmit,
		sinker.mp,
		sinker.putBackOneInMemory,
	)
	if buffer != nil {
		sinker.putBackBuffer(buffer)
	}
	if err != nil {
		return err
	}
	if err := submitPending(); err != nil {
		return err
	}
	return nil
}

func (sinker *Sinker) resetFileSinker() {
	sinker.fSinker.executor.Reset()
}
func (sinker *Sinker) getStageFileSinker() FileSinker {
	if sinker.fSinker.executor == nil {
		sinker.fSinker.executor = sinker.fSinker.factory(sinker.mp, sinker.fs)
	}
	return sinker.fSinker.executor
}

func (sinker *Sinker) GetInMemoryData() []*batch.Batch {
	return sinker.staged.inMemory
}

func (sinker *Sinker) GetMPool() *mpool.MPool {
	return sinker.mp
}

// Write copies data into staged spill runs; the caller retains ownership.
func (sinker *Sinker) Write(ctx context.Context, data *batch.Batch) (err error) {
	if sinker.runSizeLimit() > 0 {
		if sinker.staged.aborted {
			return moerr.NewInternalErrorNoCtx("cannot write an aborted spill run")
		}
		err = sinker.write(ctx, data)
		if err != nil {
			sinker.cleanupRunOnFailure(ctx, &err)
		}
		return err
	}
	return sinker.write(ctx, data)
}

func (sinker *Sinker) write(ctx context.Context, data *batch.Batch) (err error) {
	var curr *batch.Batch
	currBound := 0
	limit := sinker.runSizeLimit()
	inputSize := 0
	wholeInputFits := false
	fastAdmission := false
	if limit > 0 {
		if err = context.Cause(ctx); err != nil {
			return err
		}
		if data == nil {
			return moerr.NewInvalidInput(ctx, "Sinker.Write requires a batch")
		}
		if data.RowCount() == 0 {
			return nil
		}
		for i, vec := range data.Vecs {
			if vec == nil || !vec.CoversLogicalRows(0, data.RowCount()) {
				return moerr.NewInvalidInputf(ctx, "invalid spill vector at column %d", i)
			}
		}
		var fits bool
		inputSize, fits, fastAdmission = fastBatchAdmission(data, limit-sinker.staged.inMemorySize)
		if !fits {
			inputSize = materializedBatchSize(data)
			fastAdmission = false
		}
		wholeInputFits = inputSize != math.MaxInt && sinker.staged.inMemorySize <= limit &&
			inputSize <= limit-sinker.staged.inMemorySize
	}
	defer func() {
		if curr != nil {
			sinker.putBackBuffer(curr)
		}
	}()
	offset, left := 0, data.RowCount()
	for left > 0 {
		if limit > 0 && offset > 0 {
			if err = context.Cause(ctx); err != nil {
				return err
			}
		}
		if curr == nil {
			curr, currBound = sinker.popStaged()
			if curr == nil {
				if curr, err = sinker.fetchBuffer(); err != nil {
					return err
				}
			} else if curr.RowCount() == objectio.BlockMaxRows {
				staged := curr
				curr = nil // pushStaged owns the batch even if spilling fails.
				if err = sinker.pushStaged(ctx, staged, currBound); err != nil {
					return err
				}
				currBound = 0
				if curr, err = sinker.fetchBuffer(); err != nil {
					return err
				}
			}
		}
		toAdd := min(left, objectio.BlockMaxRows-curr.RowCount())
		useOne, measureAppend := false, false
		addedBound := 0
		if limit > 0 {
			remaining := limit - sinker.staged.inMemorySize - currBound
			if wholeInputFits {
				if offset == 0 && toAdd == data.RowCount() && !fastAdmission {
					addedBound = inputSize
				} else {
					measureAppend = true
				}
			} else {
				toAdd, addedBound = boundedRunRows(data, offset, toAdd, remaining)
				if toAdd == 0 {
					if curr.RowCount() > 0 || len(sinker.staged.inMemory) > 0 {
						if curr.RowCount() > 0 {
							staged := curr
							curr = nil
							if err = sinker.pushStaged(ctx, staged, currBound); err != nil {
								return err
							}
							currBound = 0
						}
						if len(sinker.staged.inMemory) > 0 {
							if err = sinker.trySpill(ctx); err != nil {
								return err
							}
						}
						if err = context.Cause(ctx); err != nil {
							return err
						}
						continue
					}
					toAdd = 1 // one indivisible oversized row makes progress alone
					_, addedBound = boundedRunRows(data, offset, 1, math.MaxInt)
					useOne = true
				}
				// A whole disjoint input can include dead area. Copy a proper
				// window, or a single row, to avoid retaining that dead area.
				if offset == 0 && toAdd == data.RowCount() && inputSize > addedBound {
					if inputSize <= remaining {
						addedBound = inputSize
					} else if toAdd == 1 {
						useOne = true
					} else {
						toAdd--
						_, addedBound = boundedRunRows(data, offset, toAdd, remaining)
					}
				}
			}
			if addedBound == math.MaxInt || currBound > math.MaxInt-addedBound ||
				sinker.staged.inMemorySize > math.MaxInt-currBound-addedBound {
				return moerr.NewInternalErrorNoCtx("spill size exceeds supported range")
			}
		}
		if measureAppend {
			addedBound, err = sinker.copyAndMeasureRun(curr, data, offset, toAdd)
		} else if useOne {
			err = curr.UnionOne(data, int64(offset), sinker.mp)
		} else {
			err = curr.UnionWindow(data, offset, toAdd, sinker.mp)
		}
		if err != nil {
			return err
		}
		if measureAppend {
			if currBound > math.MaxInt-addedBound ||
				sinker.staged.inMemorySize > math.MaxInt-currBound-addedBound {
				return moerr.NewInternalErrorNoCtx("spill size exceeds supported range")
			}
		}
		currBound += addedBound
		if curr.RowCount() == objectio.BlockMaxRows ||
			(limit > 0 && currBound >= limit-sinker.staged.inMemorySize) {
			staged := curr
			curr = nil
			if err = sinker.pushStaged(ctx, staged, currBound); err != nil {
				return err
			}
			currBound = 0
		}
		left -= toAdd
		offset += toAdd
	}
	if curr != nil && curr.RowCount() > 0 {
		staged := curr
		curr = nil
		if err = sinker.pushStaged(ctx, staged, currBound); err != nil {
			return err
		}
	}
	return nil
}

func (sinker *Sinker) copyAndMeasureRun(dst, src *batch.Batch, offset, rows int) (int, error) {
	var storage [128]int
	var oldAreas []int
	if len(dst.Vecs) <= len(storage) {
		oldAreas = storage[:len(dst.Vecs)]
	} else {
		oldAreas = make([]int, len(dst.Vecs))
	}
	for i, vec := range dst.Vecs {
		oldAreas[i] = len(vec.GetArea())
	}
	if err := dst.UnionWindow(src, offset, rows, sinker.mp); err != nil {
		return 0, err
	}
	bound, ok := copiedRunBytes(dst, src, oldAreas, rows)
	if !ok {
		return 0, moerr.NewInternalErrorNoCtx("spill size exceeds supported range")
	}
	return bound, nil
}

// WriteOwned transfers a unique, full-block batch when it fits; otherwise it copies.
// A true ownership result applies even if spilling returns an error.
func (sinker *Sinker) WriteOwned(ctx context.Context, data *batch.Batch) (owned bool, err error) {
	limit := sinker.runSizeLimit()
	if limit > 0 {
		if sinker.staged.aborted {
			return false, moerr.NewInternalErrorNoCtx("cannot write an aborted spill run")
		}
		defer sinker.cleanupRunOnFailure(ctx, &err)
		if err = context.Cause(ctx); err != nil {
			return false, err
		}
	}
	if data == nil {
		return false, moerr.NewInvalidInput(ctx, "Sinker.WriteOwned requires a batch")
	}
	copyRequired := data.RowCount() != objectio.BlockMaxRows
	for i, vec := range data.Vecs {
		if vec == nil {
			return false, moerr.NewInvalidInputf(ctx, "Sinker.WriteOwned batch has a nil vector at column %d", i)
		}
		if vec.HasBorrowedBacking() || vec.NeedDup() {
			return false, moerr.NewInvalidInputf(ctx, "Sinker.WriteOwned requires unique-owned vector backing at column %d", i)
		}
		if limit > 0 {
			if !vec.CoversLogicalRows(0, data.RowCount()) {
				return false, moerr.NewInvalidInputf(ctx, "spill column %d has %d rows, batch has %d", i, vec.Length(), data.RowCount())
			}
			copyRequired = copyRequired || vec.IsConst() || vec.Length() != data.RowCount()
		}
	}
	if n := len(sinker.staged.inMemory); n > 0 && sinker.staged.inMemory[n-1].RowCount() < objectio.BlockMaxRows {
		copyRequired = true
	}
	if copyRequired {
		return false, sinker.write(ctx, data)
	}
	bound := 0
	if limit > 0 {
		bound = materializedBatchSize(data)
		if bound == math.MaxInt || bound > limit-sinker.staged.inMemorySize {
			return false, sinker.write(ctx, data)
		}
	}
	return true, sinker.pushStaged(ctx, data, bound)
}

func (sinker *Sinker) Sync(ctx context.Context) (err error) {
	if sinker.runSizeLimit() > 0 {
		if sinker.staged.aborted {
			return moerr.NewInternalErrorNoCtx("cannot sync an aborted spill run")
		}
		defer sinker.cleanupRunOnFailure(ctx, &err)
	}
	return sinker.sync(ctx)
}

func (sinker *Sinker) sync(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	if sinker.pipe.enabled && sinker.pipe.result != nil {
		if err := sinker.pipe.result.getError(); err != nil {
			return err
		}
	}
	if len(sinker.staged.persisted) == 0 && len(sinker.staged.inMemory) == 0 &&
		(!sinker.pipe.enabled || sinker.pipe.result == nil) {
		return nil
	}
	// spill the remaining data
	if sinker.staged.inMemorySize > 0 &&
		sinker.staged.inMemorySize >= sinker.config.tailSizeCap {
		if err := sinker.trySpill(ctx); err != nil {
			return err
		}
	} else {
		if err := sinker.trySortInMemoryStaged(ctx); err != nil {
			return err
		}
		if sinker.config.dedupAll {
			if err := containers.DedupSortedBatches(
				sinker.schema.sortKeyIdx,
				sinker.staged.inMemory,
				sinker.mp,
			); err != nil {
				return err
			}
		}
		sinker.result.tail = sinker.staged.inMemory
		sinker.clearInMemoryStaged()
	}

	// drain pipeline and collect results
	if sinker.pipe.enabled && sinker.pipe.result != nil {
		if sinker.runSizeLimit() > 0 {
			if err := sinker.collectPipeline(ctx); err != nil {
				return err
			}
		} else {
			if err := sinker.drainPipeline(); err != nil {
				return err
			}
			sinker.pipe.result.mu.Lock()
			sinker.staged.persisted = append(sinker.staged.persisted, sinker.pipe.result.persisted...)
			sinker.pipe.result.persisted = nil
			sinker.pipe.result.mu.Unlock()
		}
	} else if sinker.runSizeLimit() > 0 {
		if err := context.Cause(ctx); err != nil {
			return err
		}
	}

	defer func() {
		sinker.staged.persisted = sinker.staged.persisted[:0]
	}()

	spillCount := atomic.LoadInt64(&sinker.timing.spillCount)
	if spillCount > 0 {
		var poolSinkNs, poolSyncNs int64
		if sinker.pipe.result != nil {
			poolSinkNs = atomic.LoadInt64(&sinker.pipe.result.sinkNs)
			poolSyncNs = atomic.LoadInt64(&sinker.pipe.result.syncNs)
		}
		logutil.Debug("Sinker flush stats",
			zap.Bool("pipeline", sinker.pipe.enabled),
			zap.Int64("spills", spillCount),
			zap.Duration("sortTime", time.Duration(atomic.LoadInt64(&sinker.timing.sortNs))),
			zap.Duration("serializeTime", time.Duration(atomic.LoadInt64(&sinker.timing.sinkNs)+poolSinkNs)),
			zap.Duration("ioTime", time.Duration(atomic.LoadInt64(&sinker.timing.syncNs)+poolSyncNs)),
			zap.Duration("submitWaitTime", time.Duration(atomic.LoadInt64(&sinker.timing.waitNs))),
			zap.Duration("totalSpillTime", time.Duration(atomic.LoadInt64(&sinker.timing.spillNs))),
			zap.Int("objects", len(sinker.staged.persisted)),
		)
	}

	// if there is only one file, it is sorted an deduped
	if len(sinker.staged.persisted) == 1 {
		sinker.result.persisted = append(sinker.result.persisted, sinker.staged.persisted[0])
		return nil
	}

	if !sinker.config.allMergeSorted && !sinker.config.dedupAll {
		sinker.result.persisted = append(sinker.result.persisted, sinker.staged.persisted...)
		return nil
	}
	panic("not implemented")
}

// collectPipeline selects cancellation, the first worker error, or success
// under the same lock before handing completed results to Sync.
func (sinker *Sinker) collectPipeline(ctx context.Context) error {
	r := sinker.pipe.result
	active := true // protected by r.mu
	stop := context.AfterFunc(ctx, func() {
		r.mu.Lock()
		if active && r.err == nil {
			r.err = context.Cause(ctx)
			r.cancel()
		}
		r.mu.Unlock()
	})
	r.pending.Wait()
	r.mu.Lock()
	if r.err == nil {
		if cause := context.Cause(ctx); cause != nil {
			r.err = cause
			r.cancel()
		}
	}
	active = false
	err := r.err
	if err == nil {
		sinker.staged.persisted = append(sinker.staged.persisted, r.persisted...)
		r.persisted = nil
	}
	r.mu.Unlock()
	stop()
	return err
}

func (sinker *Sinker) GetInMemoryThreshold() int {
	return sinker.staged.memorySizeThreshold
}

// SyncAndTakeResults calls Sync, then extracts and returns the accumulated
// results, resetting the sinker so it can accept more Write calls.
func (sinker *Sinker) SyncAndTakeResults(ctx context.Context) ([]objectio.ObjectStats, []*batch.Batch, error) {
	if err := sinker.Sync(ctx); err != nil {
		return nil, nil, err
	}
	persisted := sinker.result.persisted
	tail := sinker.result.tail
	sinker.result.persisted = nil
	sinker.result.tail = nil
	return persisted, tail, nil
}

// TakeStagedBatches extracts the un-synced in-memory staged batches,
// transferring ownership to the caller. The sinker's staged area is cleared.
func (sinker *Sinker) TakeStagedBatches() []*batch.Batch {
	bats := sinker.staged.inMemory
	sinker.staged.inMemory = nil
	sinker.staged.inMemoryBounds = nil
	sinker.staged.inMemorySize = 0
	return bats
}

// StagedSize returns the current size of in-memory staged data.
func (sinker *Sinker) StagedSize() int {
	return sinker.staged.inMemorySize
}

func (sinker *Sinker) Close() error {
	var drainErr error
	if sinker.pipe.enabled && sinker.pipe.result != nil {
		if sinker.runSizeLimit() > 0 {
			sinker.pipe.result.cancel()
		}
		drainErr = sinker.drainPipeline()
		// No more jobs will use this context after Close. Release its
		// registration with the parent even when every job succeeded.
		sinker.pipe.result.cancel()
	}
	sinker.cleanupInMemoryStaged()
	if sinker.buf.buffers != nil {
		if sinker.buf.isOwner {
			// it's not safe to free a shared buffer
			sinker.buf.buffers.Close(sinker.mp)
		}

		sinker.buf.buffers = nil
	}
	for i := range sinker.result.tail {
		if sinker.result.tail[i] != nil {
			sinker.result.tail[i].Clean(sinker.mp)
			sinker.result.tail[i] = nil
		}
	}
	sinker.result.tail = nil
	sinker.staged.persisted = nil
	sinker.staged.unpublished = nil
	if sinker.fSinker.executor != nil {
		sinker.fSinker.executor.Close()
		sinker.fSinker.executor = nil
	}
	sinker.fSinker.factory = nil
	sinker.mp = nil
	sinker.fs = nil
	return drainErr
}

// Reset discards all staged and result data without tearing down the sinker.
// It is safe to call after an aborted pipeline execution so that the next
// Write/Sync cycle starts from a clean state. The buf pool, file-sinker
// executor (and its arena), mpool, and fileservice references are all kept
// alive so they can be reused without reallocation. Callers must publish,
// delete, or transfer cleanup ownership of persisted objects before Reset.
func (sinker *Sinker) Reset() {
	// Drain any in-flight pipeline jobs before clearing state, so pool
	// workers don't race with the cleanup below. Clearing pipe.result
	// ensures the next Write/Sync cycle gets a fresh pipelineResult and
	// doesn't inherit stale errors or persisted stats.
	if sinker.pipe.result != nil {
		if sinker.runSizeLimit() > 0 {
			sinker.pipe.result.cancel()
		}
		sinker.drainPipeline()
		sinker.pipe.result.cancel()
	}

	sinker.cleanupInMemoryStaged()
	// Workspace callers transfer names before Reset; unbound callers must retain
	// an unsuccessful local cleanup generation until it can be retried.
	pending := sinker.runSizeLimit() > 0 && !sinker.config.workspaceOwnedCleanup && sinker.hasPendingUnpublishedRuns()
	if pending {
		sinker.staged.aborted = true
	} else {
		sinker.pipe.result = nil
		sinker.staged.aborted = false
		sinker.staged.persisted = sinker.staged.persisted[:0]
		sinker.staged.unpublished = nil
	}

	for i := range sinker.result.tail {
		if sinker.result.tail[i] != nil {
			sinker.result.tail[i].Clean(sinker.mp)
			sinker.result.tail[i] = nil
		}
	}
	sinker.result.tail = sinker.result.tail[:0]
	sinker.result.persisted = sinker.result.persisted[:0]

	if sinker.fSinker.executor != nil {
		sinker.fSinker.executor.Reset()
	}
}
