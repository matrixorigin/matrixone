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
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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

func WithRuntimeObjectFS(fs fileservice.FileService) RuntimeOption {
	return func(r *Runtime) {
		r.Fs = fs
	}
}

func WithRuntimeLocalFS(fs fileservice.FileService) RuntimeOption {
	return func(r *Runtime) {
		r.LocalFs = fs
	}
}

func WithRuntimeTmpFS(fs *fileservice.TmpFileService) RuntimeOption {
	return func(r *Runtime) {
		r.TmpFS = fs
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

type bigDelInfo struct {
	recordAt time.Time // user txn record time
	commitAt types.TS  // user txn commit time
}

type BDRecordService struct {
	rwlock sync.RWMutex
	info   map[uint64]bigDelInfo // map table id to lockedTableInfo
}

func (l *BDRecordService) HasBigDelAfter(tid uint64, ts *types.TS) bool {
	l.rwlock.RLock()
	defer l.rwlock.RUnlock()
	info, ok := l.info[tid]
	if !ok {
		return false
	}
	return ts.LE(&info.commitAt)
}

func (l *BDRecordService) RecordBigDel(tid []uint64, commitAt types.TS) {
	l.rwlock.Lock()
	defer l.rwlock.Unlock()
	now := time.Now()
	for _, id := range tid {
		l.info[id] = bigDelInfo{recordAt: now, commitAt: commitAt}
	}
}

// for test
func (l *BDRecordService) DeleteBigDel(tid uint64) {
	l.rwlock.Lock()
	defer l.rwlock.Unlock()
	delete(l.info, tid)
}

func NewBDRecordService() *BDRecordService {
	return &BDRecordService{
		info: make(map[uint64]bigDelInfo),
	}
}

func (l *BDRecordService) Prune() {
	l.rwlock.RLock()
	pruned := make([]uint64, 0)
	now := time.Now()
	for id, info := range l.info {
		if now.Sub(info.recordAt) > time.Minute*10 {
			pruned = append(pruned, id)
		}
	}
	l.rwlock.RUnlock()

	if len(pruned) > 0 {
		logutil.Info("BDRecord prune", zap.Int("count", len(pruned)))
		l.rwlock.Lock()
		defer l.rwlock.Unlock()
		for _, id := range pruned {
			delete(l.info, id)
		}
	}
}

type Runtime struct {
	Now        func() types.TS
	VectorPool struct {
		Small     *containers.VectorPool
		Transient *containers.VectorPool
	}

	Fs      fileservice.FileService
	LocalFs fileservice.FileService
	TmpFS   *fileservice.TmpFileService

	TransferTable   *model.HashPageTable
	TransferDelsMap *model.TransDelsForBlks
	Scheduler       tasks.TaskScheduler

	BigDeleteHinter *BDRecordService

	Options *options.Options

	Logtail struct {
		CompactStats stats.Counter
	}
}

func NewRuntime(opts ...RuntimeOption) *Runtime {
	r := new(Runtime)
	r.TransferDelsMap = model.NewTransDelsForBlks()
	r.BigDeleteHinter = NewBDRecordService()
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

func (r *Runtime) PoolUsageReport() {
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
