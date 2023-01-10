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

package checkpoint

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type TestRunner interface {
	EnableCheckpoint()
	DisableCheckpoint()

	CleanPenddingCheckpoint()
	ForceGlobalCheckpoint(end types.TS, versionInterval time.Duration) error
	ForceIncrementalCheckpoint(end types.TS) error
	IsAllChangesFlushed(start, end types.TS, printTree bool) bool
	MaxLSNInRange(end types.TS) uint64

	ExistPendingEntryToGC() bool
	MaxGlobalCheckpoint() *CheckpointEntry
	ForceFlush(ts types.TS, ctx context.Context) (err error)
}

// DisableCheckpoint stops generating checkpoint
func (r *runner) DisableCheckpoint() {
	r.disabled.Store(true)
}

func (r *runner) EnableCheckpoint() {
	r.disabled.Store(false)
}

func (r *runner) CleanPenddingCheckpoint() {
	prev := r.MaxCheckpoint()
	if prev == nil {
		return
	}
	if !prev.IsFinished() {
		r.storage.Lock()
		r.storage.entries.Delete(prev)
		r.storage.Unlock()
	}
	if prev.IsRunning() {
		logutil.Warnf("Delete a running checkpoint entry")
	}
	prev = r.MaxGlobalCheckpoint()
	if prev == nil {
		return
	}
	if !prev.IsFinished() {
		r.storage.Lock()
		r.storage.entries.Delete(prev)
		r.storage.Unlock()
	}
	if prev.IsRunning() {
		logutil.Warnf("Delete a running checkpoint entry")
	}
}

func (r *runner) ForceGlobalCheckpoint(end types.TS, versionInterval time.Duration) error {
	if r.GetPenddingIncrementalCount() == 0 {
		err := r.ForceIncrementalCheckpoint(end)
		if err != nil {
			return err
		}
	} else {
		end = r.MaxCheckpoint().GetEnd()
	}
	r.globalCheckpointQueue.Enqueue(&globalCheckpointContext{
		force:    true,
		end:      end,
		interval: versionInterval,
	})
	return nil
}
func (r *runner) ForceFlush(ts types.TS, ctx context.Context) (err error) {
	makeCtx := func() *DirtyCtx {
		tree := r.source.ScanInRangePruned(types.TS{}, ts)
		tree.GetTree().Compact()
		if tree.IsEmpty() {
			return nil
		}
		entry := logtail.NewDirtyTreeEntry(types.TS{}, ts, tree.GetTree())
		dirtyCtx := new(DirtyCtx)
		dirtyCtx.tree = entry
		dirtyCtx.force = true
		// logutil.Infof("try flush %v",tree.String())
		return dirtyCtx
	}
	op := func() (ok bool, err error) {
		dirtyCtx := makeCtx()
		if dirtyCtx == nil {
			return true, nil
		}
		if _, err = r.dirtyEntryQueue.Enqueue(dirtyCtx); err != nil {
			return true, nil
		}
		return false, nil
	}

	err = common.RetryWithIntervalAndTimeout(
		op,
		r.options.forceFlushTimeout,
		r.options.forceFlushCheckInterval, false)
	return
}

func (r *runner) ForceIncrementalCheckpoint(end types.TS) error {
	prev := r.MaxCheckpoint()
	if prev != nil && !prev.IsFinished() {
		return moerr.NewInternalError(context.Background(), "prev checkpoint not finished")
	}
	start := types.TS{}
	if prev != nil {
		start = prev.end.Next()
	}
	entry := NewCheckpointEntry(start, end, ET_Incremental)
	r.storage.Lock()
	r.storage.entries.Set(entry)
	now := time.Now()
	r.storage.Unlock()
	r.doIncrementalCheckpoint(entry)
	if err := r.saveCheckpoint(entry.start, entry.end); err != nil {
		return err
	}
	entry.SetState(ST_Finished)
	logutil.Infof("%s is done, takes %s", entry.String(), time.Since(now))
	return nil
}

func (r *runner) IsAllChangesFlushed(start, end types.TS, printTree bool) bool {
	tree := r.source.ScanInRangePruned(start, end)
	tree.GetTree().Compact()
	if printTree && !tree.IsEmpty() {
		logutil.Infof("%v", tree.String())
	}
	return tree.IsEmpty()
}
