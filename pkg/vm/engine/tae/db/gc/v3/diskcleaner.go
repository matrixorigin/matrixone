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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

const (
	MessgeReplay = iota
	MessgeNormal
)

// DiskCleaner is the main structure of v2 operation,
// and provides "JobFactory" to let tae notify itself
// to perform a v2
type DiskCleaner struct {
	cleaner Cleaner

	processQueue sm.Queue

	onceStart sync.Once
	onceStop  sync.Once
}

func NewDiskCleaner(
	diskCleaner Cleaner,
) *DiskCleaner {
	cleaner := &DiskCleaner{
		cleaner: diskCleaner,
	}
	cleaner.processQueue = sm.NewSafeQueue(10000, 1000, cleaner.process)
	return cleaner
}

func (cleaner *DiskCleaner) GC(ctx context.Context) (err error) {
	logutil.Info("JobFactory is start")
	return cleaner.tryClean(ctx)
}

func (cleaner *DiskCleaner) GetCleaner() Cleaner {
	return cleaner.cleaner
}

func (cleaner *DiskCleaner) tryReplay() {
	if _, err := cleaner.processQueue.Enqueue(MessgeReplay); err != nil {
		panic(err)
	}
}

func (cleaner *DiskCleaner) tryClean(ctx context.Context) (err error) {
	_, err = cleaner.processQueue.Enqueue(MessgeNormal)
	return
}

func (cleaner *DiskCleaner) replay() error {
	return cleaner.cleaner.Replay()
}

func (cleaner *DiskCleaner) process(items ...any) {
	if items[0].(int) == MessgeReplay {
		err := cleaner.replay()
		if err != nil {
			panic(err)
		}
		cleaner.cleaner.TryGC()
		if len(items) == 1 {
			return
		}
	}

	cleaner.cleaner.Process()

}

func (cleaner *DiskCleaner) Start() {
	cleaner.onceStart.Do(func() {
		cleaner.processQueue.Start()
		cleaner.tryReplay()
	})
}

func (cleaner *DiskCleaner) Stop() {
	cleaner.onceStop.Do(func() {
		cleaner.processQueue.Stop()
		cleaner.cleaner.Stop()
	})
}
