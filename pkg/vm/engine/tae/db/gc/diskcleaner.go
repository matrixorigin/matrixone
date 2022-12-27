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

package gc

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

const (
	MessgeReplay = iota
	MessgeNormal
)

// diskCleaner is the main structure of gc operation,
// and provides "JobFactory" to let tae notify itself
// to perform a gc
type diskCleaner struct {
	fs *objectio.ObjectFS

	// ckpClient is used to get the instance of the specified checkpoint
	ckpClient checkpoint.RunnerReader

	// Parsing checkpoint needs to use catalog instance
	catalog *catalog.Catalog

	// maxConsumed is to mark which checkpoint the current diskCleaner has processed,
	// through which you can get the next checkpoint to be processed
	maxConsumed atomic.Pointer[checkpoint.CheckpointEntry]

	// inputs is to record the currently valid GCTable
	inputs struct {
		sync.RWMutex
		tables []*GCTable
	}

	// outputs is a list of files that have been deleted
	outputs struct {
		sync.RWMutex
		files []string
	}
	// delTask is a worker that deletes s3‘s objects or local
	// files, and only one worker will run
	delTask *GCTask

	processQueue sm.Queue

	onceStart sync.Once
	onceStop  sync.Once
}

func NewDiskCleaner(
	fs *objectio.ObjectFS,
	ckpClient checkpoint.RunnerReader,
	catalog *catalog.Catalog,
) *diskCleaner {
	cleaner := &diskCleaner{
		fs:        fs,
		ckpClient: ckpClient,
		catalog:   catalog,
	}
	cleaner.delTask = NewGCTask(fs, cleaner)
	cleaner.processQueue = sm.NewSafeQueue(10000, 1000, cleaner.process)
	return cleaner
}

func (cleaner *diskCleaner) JobFactory(ctx context.Context) (err error) {
	return cleaner.tryClean(ctx)
}

// Replay is an interface provided for testing
func (cleaner *diskCleaner) Replay() {
	cleaner.tryReplay()
}

func (cleaner *diskCleaner) tryReplay() {
	if _, err := cleaner.processQueue.Enqueue(MessgeReplay); err != nil {
		panic(err)
	}
}

func (cleaner *diskCleaner) tryClean(ctx context.Context) (err error) {
	_, err = cleaner.processQueue.Enqueue(MessgeNormal)
	return
}

func (cleaner *diskCleaner) replay() error {
	dirs, err := cleaner.fs.ListDir(GCMetaDir)
	if err != nil {
		return err
	}
	if len(dirs) == 0 {
		return nil
	}
	jobs := make([]*tasks.Job, len(dirs))
	jobScheduler := tasks.NewParallelJobScheduler(100)
	defer jobScheduler.Stop()
	maxConsumed := types.TS{}
	for _, dir := range dirs {
		_, end := blockio.DecodeCheckpointMetadataFileName(dir.Name)
		if maxConsumed.IsEmpty() || maxConsumed.Less(end) {
			maxConsumed = end
			continue
		}
	}
	makeJob := func(i int) (job *tasks.Job) {
		dir := dirs[i]
		exec := func(ctx context.Context) (result *tasks.JobResult) {
			result = &tasks.JobResult{}
			table := NewGCTable()
			err := table.ReadTable(ctx, GCMetaDir+dir.Name, dir.Size, cleaner.fs)
			if err != nil {
				result.Err = err
				return
			}
			cleaner.updateInputs(table)
			return
		}
		job = tasks.NewJob(
			fmt.Sprintf("load-%s", dir.Name),
			context.Background(),
			exec)
		return
	}

	for i := range dirs {
		jobs[i] = makeJob(i)
		if err = jobScheduler.Schedule(jobs[i]); err != nil {
			return err
		}
	}

	for _, job := range jobs {
		result := job.WaitDone()
		if err = result.Err; err != nil {
			return err
		}
	}
	ckp := checkpoint.NewCheckpointEntry(types.TS{}, maxConsumed, checkpoint.ET_Incremental)
	cleaner.updateMaxConsumed(ckp)
	return nil
}

func (cleaner *diskCleaner) process(items ...any) {
	if items[0].(int) == MessgeReplay {
		err := cleaner.replay()
		if err != nil {
			panic(err)
		}
		// TODO:
		cleaner.tryGC()
		if len(items) == 1 {
			return
		}
	}

	var ts types.TS
	maxConsumed := cleaner.maxConsumed.Load()
	if maxConsumed != nil {
		ts = maxConsumed.GetEnd()
	}

	candidates := cleaner.ckpClient.ICKPSeekLT(ts, 10)

	if len(candidates) == 0 {
		return
	}

	var input *GCTable
	var err error
	if input, err = cleaner.createNewInput(candidates); err != nil {
		logutil.Errorf("processing clean %s: %v", candidates[0].String(), err)
		// TODO
		return
	}
	cleaner.updateInputs(input)
	cleaner.updateMaxConsumed(candidates[len(candidates)-1])

	// TODO:
	cleaner.tryGC()

}

func (cleaner *diskCleaner) collectCkpData(
	ckp *checkpoint.CheckpointEntry,
) (data *logtail.CheckpointData, err error) {
	factory := logtail.IncrementalCheckpointDataFactory(
		ckp.GetStart(),
		ckp.GetEnd(),
	)
	data, err = factory(cleaner.catalog)
	return
}

func (cleaner *diskCleaner) createNewInput(
	ckps []*checkpoint.CheckpointEntry) (input *GCTable, err error) {
	input = NewGCTable()
	var data *logtail.CheckpointData
	for _, candidate := range ckps {
		data, err = cleaner.collectCkpData(candidate)
		if err != nil {
			logutil.Errorf("processing clean %s: %v", candidate.String(), err)
			// TODO
			return
		}
		defer data.Close()
		input.UpdateTable(data)
	}
	files := cleaner.GetAndClearOutputs()
	_, err = input.SaveTable(
		ckps[0].GetStart(),
		ckps[len(ckps)-1].GetEnd(),
		cleaner.fs,
		files,
	)
	if err != nil {
		return
	}

	return
}

func (cleaner *diskCleaner) tryGC() {
	if cleaner.delTask.GetState() == Running {
		return
	}
	gc := cleaner.softGC()
	if len(gc) == 0 {
		return
	}
	// Delete files after softGC
	// TODO:Requires Physical Removal Policy
	go cleaner.delTask.ExecDelete(gc)
}

func (cleaner *diskCleaner) softGC() []string {
	cleaner.inputs.Lock()
	defer cleaner.inputs.Unlock()
	if len(cleaner.inputs.tables) == 0 {
		return nil
	}
	mergeTable := NewGCTable()
	for _, table := range cleaner.inputs.tables {
		mergeTable.Merge(table)
	}
	gc := mergeTable.SoftGC()
	cleaner.inputs.tables = make([]*GCTable, 0)
	cleaner.inputs.tables = append(cleaner.inputs.tables, mergeTable)
	return gc
}

func (cleaner *diskCleaner) updateMaxConsumed(e *checkpoint.CheckpointEntry) {
	cleaner.maxConsumed.Store(e)
}

func (cleaner *diskCleaner) updateInputs(input *GCTable) {
	cleaner.inputs.Lock()
	defer cleaner.inputs.Unlock()
	cleaner.inputs.tables = append(cleaner.inputs.tables, input)
}

func (cleaner *diskCleaner) updateOutputs(files []string) {
	cleaner.outputs.Lock()
	defer cleaner.outputs.Unlock()
	cleaner.outputs.files = append(cleaner.outputs.files, files...)
}

func (cleaner *diskCleaner) GetMaxConsumed() *checkpoint.CheckpointEntry {
	return cleaner.maxConsumed.Load()
}

func (cleaner *diskCleaner) GetInputs() *GCTable {
	cleaner.inputs.RLock()
	defer cleaner.inputs.RUnlock()
	return cleaner.inputs.tables[0]
}

func (cleaner *diskCleaner) GetAndClearOutputs() []string {
	cleaner.outputs.RLock()
	defer cleaner.outputs.RUnlock()
	files := cleaner.outputs.files
	//Empty the array, in order to store the next file list
	cleaner.outputs.files = make([]string, 0)
	return files
}

func (cleaner *diskCleaner) Start() {
	cleaner.onceStart.Do(func() {
		cleaner.processQueue.Start()
		cleaner.tryReplay()
	})
}

func (cleaner *diskCleaner) Stop() {
	cleaner.onceStop.Do(func() {
		cleaner.processQueue.Stop()
	})
}
