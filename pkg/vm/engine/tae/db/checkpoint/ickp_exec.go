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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"go.uber.org/zap"
)

func (executor *checkpointExecutor) TryScheduleCheckpoint(
	ts types.TS, force bool,
) (ret Intent, err error) {
	if !force {
		return executor.softScheduleCheckpoint(&ts)
	}

	intent, updated := executor.runner.store.UpdateICKPIntent(&ts, true, true)
	if intent == nil {
		return
	}

	now := time.Now()
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}

		logger(
			"ICKP-Schedule-Force",
			zap.String("intent", intent.String()),
			zap.String("ts", ts.ToString()),
			zap.Bool("updated", updated),
			zap.Duration("cost", time.Since(now)),
			zap.Error(err),
		)
	}()

	executor.TriggerExecutingICKP()

	if intent.end.LT(&ts) {
		err = ErrPendingCheckpoint
		return
	}

	return intent, nil
}

func (executor *checkpointExecutor) softScheduleCheckpoint(
	ts *types.TS,
) (ret *CheckpointEntry, err error) {
	var (
		updated bool
	)
	intent := executor.runner.store.GetICKPIntent()

	check := func() (done bool) {
		start, end := intent.GetStart(), intent.GetEnd()
		ready := IsAllDirtyFlushed(executor.runner.source, executor.runner.catalog, start, end, intent.TooOld())
		if !ready {
			intent.DeferRetirement()
		}
		return ready
	}

	now := time.Now()

	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		} else {
			ret = intent
		}
		intentInfo := "nil"
		ageStr := ""
		if intent != nil {
			intentInfo = intent.String()
			ageStr = intent.Age().String()
		}
		if (err != nil && err != ErrPendingCheckpoint) || (intent != nil && intent.TooOld()) {
			logger(
				"ICKP-Schedule-Soft",
				zap.String("intent", intentInfo),
				zap.String("ts", ts.ToString()),
				zap.Duration("cost", time.Since(now)),
				zap.String("age", ageStr),
				zap.Error(err),
			)
		}
	}()

	if intent == nil {
		start := executor.runner.store.GetCheckpointed()
		if ts.LT(&start) {
			return
		}
		if !executor.incrementalPolicy.Check(start) {
			return
		}
		_, count := executor.runner.source.ScanInRange(start, *ts)
		if count < int(executor.cfg.MinCount) {
			return
		}
		intent, updated = executor.runner.store.UpdateICKPIntent(ts, true, false)
		if updated {
			logutil.Info(
				"ICKP-Schedule-Soft-Updated",
				zap.String("intent", intent.String()),
				zap.String("ts", ts.ToString()),
			)
		}
	}

	// [intent == nil]
	// if intent is nil, it means no need to do checkpoint
	if intent == nil {
		return
	}

	// [intent != nil]

	var (
		policyChecked  bool
		flushedChecked bool
	)
	policyChecked = intent.IsPolicyChecked()
	if !policyChecked {
		if !executor.incrementalPolicy.Check(intent.GetStart()) {
			return
		}
		_, count := executor.runner.source.ScanInRange(intent.GetStart(), intent.GetEnd())
		if count < int(executor.cfg.MinCount) {
			return
		}
		policyChecked = true
	}

	flushedChecked = intent.IsFlushChecked()
	if !flushedChecked && check() {
		flushedChecked = true
	}

	if policyChecked != intent.IsPolicyChecked() || flushedChecked != intent.IsFlushChecked() {
		endTS := intent.GetEnd()
		intent, updated = executor.runner.store.UpdateICKPIntent(&endTS, policyChecked, flushedChecked)

		if updated {
			logutil.Info(
				"ICKP-Schedule-Soft-Updated",
				zap.String("intent", intent.String()),
				zap.String("endTS", ts.ToString()),
			)
		}
	}

	// no need to do checkpoint
	if intent == nil {
		return
	}

	if intent.end.LT(ts) {
		err = ErrPendingCheckpoint
		executor.TriggerExecutingICKP()
		return
	}

	if intent.AllChecked() {
		executor.TriggerExecutingICKP()
	}
	return
}

func (executor *checkpointExecutor) TriggerExecutingICKP() (err error) {
	if !executor.active.Load() {
		err = ErrExecutorClosed
		return
	}
	_, err = executor.ickpQueue.Enqueue(struct{}{})
	return
}

func (executor *checkpointExecutor) RunICKP() (err error) {
	if !executor.active.Load() {
		err = ErrCheckpointDisabled
		return
	}
	if executor.runningICKP.Load() != nil {
		err = ErrPendingCheckpoint
	}
	job := &checkpointJob{
		doneCh:      make(chan struct{}),
		executor:    executor,
		runICKPFunc: executor.runICKPFunc,
	}
	if !executor.runningICKP.CompareAndSwap(nil, job) {
		err = ErrPendingCheckpoint
		return
	}
	defer func() {
		job.Done(err)
		executor.runningICKP.Store(nil)
	}()
	err = job.RunICKP(executor.ctx)
	return
}

func (executor *checkpointExecutor) onICKPEntries(items ...any) {
	executor.RunICKP()
}

func (executor *checkpointExecutor) doIncrementalCheckpoint(
	entry *CheckpointEntry,
) (fields []zap.Field, files []string, err error) {
	factory := logtail.IncrementalCheckpointDataFactory(
		executor.runner.rt.SID(), entry.start, entry.end, true,
	)
	data, err := factory(executor.runner.catalog)
	if err != nil {
		return
	}
	fields = data.ExportStats("")
	defer data.Close()
	var cnLocation, tnLocation objectio.Location
	cnLocation, tnLocation, files, err = data.WriteTo(
		executor.ctx,
		executor.cfg.BlockMaxRowsHint,
		executor.cfg.SizeHint,
		executor.runner.rt.Fs.Service,
	)
	if err != nil {
		return
	}
	files = append(files, cnLocation.Name().String())
	entry.SetLocation(cnLocation, tnLocation)

	perfcounter.Update(executor.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.CheckPoint.DoIncrementalCheckpoint.Add(1)
	})
	return
}
