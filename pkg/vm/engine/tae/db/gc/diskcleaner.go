package gc

import (
	"context"
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

// TODO
// TO BE REMOVED
func tempSeekLT(
	client checkpoint.Client,
	startTs types.TS,
	_ int,
) (entries []*checkpoint.CheckpointEntry) {
	all := client.GetAllCheckpoints()
	if len(all) == 0 {
		return
	}
	if startTs.IsEmpty() {
		entries = append(entries, all[0])
		return
	}
	for _, ckp := range all {
		if ckp.GetStart().Greater(startTs) {
			entries = append(entries, ckp)
			return
		}
	}
	return
}

const (
	MessgeReplay = iota
	MessgeNormal
)

type diskCleaner struct {
	fs        *objectio.ObjectFS
	ckpClient checkpoint.Client
	catalog   *catalog.Catalog

	maxConsumed atomic.Pointer[checkpoint.CheckpointEntry]
	inputs      struct {
		sync.RWMutex
		tables []GCTable
	}

	processQueue sm.Queue

	onceStart sync.Once
	onceStop  sync.Once
}

func newDiskCleaner(
	fs *objectio.ObjectFS,
	ckpClient checkpoint.Client,
	catalog *catalog.Catalog,
) *diskCleaner {
	cleaner := &diskCleaner{
		fs:        fs,
		ckpClient: ckpClient,
		catalog:   catalog,
	}
	cleaner.processQueue = sm.NewSafeQueue(10000, 1000, cleaner.process)
	return cleaner
}

func (cleaner *diskCleaner) JobFactory(ctx context.Context) (err error) {
	return cleaner.tryClean(ctx)
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

func (cleaner *diskCleaner) replay() {
	// TODO
}

func (cleaner *diskCleaner) process(items ...any) {
	if items[0].(int) == MessgeReplay {
		cleaner.replay()
		if len(items) == 1 {
			return
		}
	}

	var ts types.TS
	maxConsumed := cleaner.maxConsumed.Load()
	if maxConsumed != nil {
		ts = maxConsumed.GetStart()
	}

	// TODO: implemnet ICKPSeekLT
	// candidates := cleaner.ckpClient.ICKPSeekLT(ts, 1)
	candidates := tempSeekLT(cleaner.ckpClient, ts, 1)

	if len(candidates) == 0 {
		return
	}
	candidate := candidates[0]

	data, err := cleaner.collectCkpData(candidate)
	if err != nil {
		logutil.Errorf("processing clean %s: %v", candidate.String(), err)
		// TODO
		return
	}
	defer data.Close()

	var input GCTable

	if input, err = cleaner.createNewInput(candidate, data); err != nil {
		logutil.Errorf("processing clean %s: %v", candidate.String(), err)
		// TODO
		return
	}
	cleaner.updateInputs(input)
	cleaner.updateMaxConsumed(candidate)
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
	ckp *checkpoint.CheckpointEntry,
	data *logtail.CheckpointData) (input GCTable, err error) {
	input = NewGCTable()
	input.UpdateTable(data)
	_, err = input.SaveTable(
		ckp.GetStart(),
		ckp.GetEnd(),
		cleaner.fs,
	)
	return
}

func (cleaner *diskCleaner) updateMaxConsumed(e *checkpoint.CheckpointEntry) {
	cleaner.maxConsumed.Store(e)
}

func (cleaner *diskCleaner) updateInputs(input GCTable) {
	cleaner.inputs.Lock()
	defer cleaner.inputs.Unlock()
	cleaner.inputs.tables = append(cleaner.inputs.tables, input)
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
