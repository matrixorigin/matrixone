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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type MockCleanerOption func(*MockCleaner)

func WithReplayFunc(f func(context.Context) error) MockCleanerOption {
	return func(c *MockCleaner) {
		c.repalyFunc = f
	}
}

func WithProcessFunc(f func(context.Context) error) MockCleanerOption {
	return func(c *MockCleaner) {
		c.processFunc = f
	}
}

func WithTryGCFunc(f func(context.Context) error) MockCleanerOption {
	return func(c *MockCleaner) {
		c.tryGC = f
	}
}

type MockCleaner struct {
	repalyFunc  func(context.Context) error
	processFunc func(context.Context) error
	tryGC       func(context.Context) error
}

func NewMockCleaner(opts ...MockCleanerOption) MockCleaner {
	cleaner := MockCleaner{}
	for _, opt := range opts {
		opt(&cleaner)
	}
	return cleaner
}

func (c *MockCleaner) Replay(ctx context.Context) error {
	if c.repalyFunc != nil {
		return c.repalyFunc(ctx)
	}
	return nil
}

func (c *MockCleaner) Process(ctx context.Context) error {
	if c.processFunc != nil {
		return c.processFunc(ctx)
	}
	return nil
}

func (c *MockCleaner) TryGC(ctx context.Context) error {
	if c.tryGC != nil {
		return c.tryGC(ctx)
	}
	return nil
}

func (c *MockCleaner) AddChecker(checker func(item any) bool, key string) int {
	return 0
}

func (c *MockCleaner) Close() error {
	return nil
}

func (c *MockCleaner) GetChecker(key string) func(item any) bool {
	return nil
}

func (c *MockCleaner) RemoveChecker(key string) error {
	return nil
}

func (c *MockCleaner) GetScanWaterMark() *checkpoint.CheckpointEntry {
	return nil
}

func (c *MockCleaner) GetCheckpointGCWaterMark() *types.TS {
	return nil
}

func (c *MockCleaner) GetScannedWindow() *GCWindow {
	return nil
}

func (c *MockCleaner) Stop() {
}

func (c *MockCleaner) GetMinMerged() *checkpoint.CheckpointEntry {
	return nil
}

func (c *MockCleaner) DoCheck() error {
	return nil
}

func (c *MockCleaner) GetPITRs() (*logtail.PitrInfo, error) {
	return nil, nil
}

func (c *MockCleaner) SetTid(tid uint64) {
}

func (c *MockCleaner) EnableGC() {
}

func (c *MockCleaner) DisableGC() {
}

func (c *MockCleaner) GCEnabled() bool {
	return false
}

func (c *MockCleaner) GetMPool() *mpool.MPool {
	return nil
}

func (c *MockCleaner) GetSnapshots() (map[uint32]containers.Vector, error) {
	return nil, nil
}

func (c *MockCleaner) GetTablePK(tableId uint64) string {
	return ""
}
