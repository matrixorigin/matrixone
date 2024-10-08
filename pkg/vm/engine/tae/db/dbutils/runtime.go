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

package dbutils

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	fcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
)

type RuntimeOption func(*Runtime)

func WithRuntimeSmallPool(vp *containers.VectorPool) RuntimeOption {
	return func(r *Runtime) {
		r.VectorPool.Small = vp
	}
}

func WithRuntimeTransientPool(vp *containers.VectorPool) RuntimeOption {
	return func(r *Runtime) {
		r.VectorPool.Transient = vp
	}
}

func WithRuntimeObjectFS(fs *objectio.ObjectFS) RuntimeOption {
	return func(r *Runtime) {
		r.Fs = fs
	}
}

func WithRuntimeLocalFS(fs *objectio.ObjectFS) RuntimeOption {
	return func(r *Runtime) {
		r.LocalFs = fs
	}
}

func WithRuntimeTransferTable(tt *model.HashPageTable) RuntimeOption {
	return func(r *Runtime) {
		r.TransferTable = tt
	}
}

func WithRuntimeScheduler(s tasks.TaskScheduler) RuntimeOption {
	return func(r *Runtime) {
		r.Scheduler = s
	}
}

func WithRuntimeOptions(opts *options.Options) RuntimeOption {
	return func(r *Runtime) {
		r.Options = opts
	}
}

type lockedTableInfo struct {
	lockedAt   time.Time // user txn lock time
	lockStatus int       // 0 no lock. above 1 user txn lock count
	indexes    []string

	lockedByNonReentrantLock bool
}

// Indexes only for tests
func (l *lockedTableInfo) Indexes() []string {
	return l.indexes
}

type LockMergeService struct {
	rwlock sync.RWMutex

	locked  map[uint64]*lockedTableInfo // map table id to lockedTableInfo
	indexes map[string]struct{}
}

// LockedInfos Only for tests
func (l *LockMergeService) LockedInfos() map[uint64]*lockedTableInfo {
	return l.locked
}

// LockedInfos Only for tests
func (l *LockMergeService) Indexes() map[string]struct{} {
	return l.indexes
}

func NewLockMergeService() *LockMergeService {
	return &LockMergeService{
		locked:  make(map[uint64]*lockedTableInfo),
		indexes: make(map[string]struct{}),
	}
}

func (l *LockMergeService) IsLockedByUser(id uint64, tblName string) (isLocked bool) {
	l.rwlock.RLock()
	defer l.rwlock.RUnlock()

	return l.isLockedByUser(id, tblName)
}

func (l *LockMergeService) isLockedByUser(id uint64, tblName string) bool {
	if strings.HasPrefix(tblName, fcatalog.PrefixIndexTableName) {
		_, ok := l.indexes[tblName]
		return ok
	}

	if info, ok := l.locked[id]; ok {
		return info.lockedByNonReentrantLock || info.lockStatus > 0
	}
	return false
}
func (l *LockMergeService) LockFromUser(id uint64, tblName string, reentrant bool, indexTableNames ...string) error {
	if strings.HasPrefix(tblName, fcatalog.PrefixIndexTableName) {
		if reentrant {
			return nil
		}
		return moerr.NewInternalErrorNoCtx("lock on index")
	}

	l.rwlock.Lock()
	defer l.rwlock.Unlock()

	if l.isLockedByUser(id, tblName) {
		if !reentrant {
			return moerr.NewInternalErrorNoCtxf("%s is already locked", tblName)
		}

		l.locked[id].lockStatus++
		return nil
	}

	lockInfo := &lockedTableInfo{
		lockedAt: time.Now(),
		indexes:  indexTableNames,
	}
	if reentrant {
		lockInfo.lockStatus = 1
	} else {
		lockInfo.lockedByNonReentrantLock = true
	}

	l.locked[id] = lockInfo
	for _, indexTableName := range indexTableNames {
		l.indexes[indexTableName] = struct{}{}
	}
	return nil
}

func (l *LockMergeService) UnlockFromUser(id uint64, reentrant bool) error {
	l.rwlock.Lock()
	defer l.rwlock.Unlock()

	info, ok := l.locked[id]
	if !ok {
		return moerr.NewInternalErrorNoCtxf("table %d is not locked", id)
	}

	if reentrant {
		if info.lockStatus <= 0 {
			panic("bad lock status")
		}
		info.lockStatus--
		return nil
	}

	if !info.lockedByNonReentrantLock {
		return moerr.NewInternalErrorNoCtxf("table %d is not locked by non-reentrant lock", id)
	}
	info.lockedByNonReentrantLock = false

	if !info.lockedByNonReentrantLock && info.lockStatus == 0 {
		for _, index := range info.indexes {
			delete(l.indexes, index)
		}
		delete(l.locked, id)
	}
	return nil
}

func (l *LockMergeService) PruneStale(id uint64) {
	l.rwlock.RLock()
	info := l.locked[id]
	l.rwlock.RUnlock()

	// The table is index table or locked by non-reentrant lock.
	if info == nil || info.lockedByNonReentrantLock {
		return
	}

	if !info.lockedAt.IsZero() && time.Since(info.lockedAt) > time.Minute*10 {
		l.rwlock.Lock()
		delete(l.locked, id)
		for _, index := range info.indexes {
			delete(l.indexes, index)
		}
		l.rwlock.Unlock()
	}
}

func (l *LockMergeService) Prune() {
	l.rwlock.RLock()
	pruned := make([]uint64, 0)
	for id, info := range l.locked {
		if !info.lockedAt.IsZero() && time.Since(info.lockedAt) > time.Minute*10 {
			if info.lockStatus == 0 && !info.lockedByNonReentrantLock {
				pruned = append(pruned, id)
			} else {
				logutil.Warn(
					"LockMerge abnormally stale",
					zap.Uint64("tableId", id),
					zap.Duration("ago", time.Since(info.lockedAt)),
					zap.Int("lockStatus", info.lockStatus),
					zap.Bool("non-reentrant", info.lockedByNonReentrantLock),
				)
			}
		}
	}
	l.rwlock.RUnlock()

	if len(pruned) > 0 {
		logutil.Info("LockMerge prune", zap.Int("count", len(pruned)))
		l.rwlock.Lock()
		defer l.rwlock.Unlock()
		for _, id := range pruned {
			info := l.locked[id]
			delete(l.locked, id)
			for _, index := range info.indexes {
				delete(l.indexes, index)
			}
		}
	}
}

type Runtime struct {
	Now        func() types.TS
	VectorPool struct {
		Small     *containers.VectorPool
		Transient *containers.VectorPool
	}

	Fs      *objectio.ObjectFS
	LocalFs *objectio.ObjectFS

	TransferTable   *model.HashPageTable
	TransferDelsMap *model.TransDelsForBlks
	Scheduler       tasks.TaskScheduler

	LockMergeService *LockMergeService

	Options *options.Options

	Logtail struct {
		CompactStats stats.Counter
	}
}

func NewRuntime(opts ...RuntimeOption) *Runtime {
	r := new(Runtime)
	r.TransferDelsMap = model.NewTransDelsForBlks()
	r.LockMergeService = NewLockMergeService()
	for _, opt := range opts {
		opt(r)
	}
	r.fillDefaults()
	return r
}

func (r *Runtime) fillDefaults() {
	if r.VectorPool.Small == nil {
		r.VectorPool.Small = MakeDefaultSmallPool("small-vector-pool")
	}
	if r.VectorPool.Transient == nil {
		r.VectorPool.Transient = MakeDefaultTransientPool("trasient-vector-pool")
	}
}

func (r *Runtime) SID() string {
	if r == nil {
		return ""
	}
	return r.Options.SID
}

func (r *Runtime) PrintVectorPoolUsage() {
	var w bytes.Buffer
	w.WriteString(r.VectorPool.Transient.String())
	w.WriteByte('\n')
	w.WriteString(r.VectorPool.Small.String())
	logutil.Info(w.String())
}

func (r *Runtime) ExportLogtailStats() string {
	return fmt.Sprintf(
		"LogtailStats: Compact[%d|%d]",
		r.Logtail.CompactStats.SwapW(0),
		r.Logtail.CompactStats.Load(),
	)
}
