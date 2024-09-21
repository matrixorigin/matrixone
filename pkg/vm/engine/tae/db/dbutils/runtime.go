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
	"sync"
	"time"

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
}

type LockMergeService struct {
	rwlock sync.RWMutex
	locked map[uint64]*lockedTableInfo // map table id to lockedTableInfo
}

func NewLockMergeService() *LockMergeService {
	return &LockMergeService{
		locked: make(map[uint64]*lockedTableInfo),
	}
}

func (l *LockMergeService) IsLockedByUser(id uint64) (isLocked bool) {
	l.rwlock.RLock()
	defer l.rwlock.RUnlock()
	info, existed := l.locked[id]
	return existed && info.lockStatus > 0
}

func (l *LockMergeService) LockFromUser(id uint64) {
	l.rwlock.Lock()
	defer l.rwlock.Unlock()
	if info, ok := l.locked[id]; ok {
		info.lockStatus++
	} else {
		l.locked[id] = &lockedTableInfo{
			lockStatus: 1,
			lockedAt:   time.Now(),
		}
	}
}

func (l *LockMergeService) UnlockFromUser(id uint64) {
	l.rwlock.Lock()
	defer l.rwlock.Unlock()
	if info, ok := l.locked[id]; ok {
		if info.lockStatus <= 0 {
			panic("bad lock status")
		}
		info.lockStatus--
	} else {
		panic("Before UnlockFromUser, call LockFromUser")
	}
}

func (l *LockMergeService) Prune() {
	l.rwlock.RLock()
	pruned := make([]uint64, 0)
	for id, info := range l.locked {
		if !info.lockedAt.IsZero() && time.Since(info.lockedAt) > time.Minute*10 {
			if info.lockStatus == 0 {
				pruned = append(pruned, id)
			} else {
				logutil.Warn(
					"LockMerge abnormally stale",
					zap.Uint64("tableId", id),
					zap.Duration("ago", time.Since(info.lockedAt)),
					zap.Int("lockStatus", info.lockStatus),
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
			delete(l.locked, id)
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
