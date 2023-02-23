// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"go.uber.org/zap"
)

type lockTableKeeper struct {
	logger   *log.MOLogger
	sender   KeepaliveSender
	stopper  *stopper.Stopper
	interval time.Duration
	changedC chan pb.LockTable
	mu       struct {
		sync.Mutex
		lockTables map[uint64]pb.LockTable
		changed    bool
	}
}

// NewLockTableKeeper create a locktable keeper, an internal timer is started
// to send a keepalive request to the lockTableAllocator every interval, so this
// interval needs to be much smaller than the real lockTableAllocator's timeout.
func NewLockTableKeeper(
	sender KeepaliveSender,
	interval time.Duration) LockTableKeeper {
	logger := runtime.ProcessLevelRuntime().Logger()
	tag := "lock-table-keeper"
	s := &lockTableKeeper{
		logger:   logger.Named(tag),
		sender:   sender,
		changedC: make(chan pb.LockTable, 1024),
		stopper: stopper.NewStopper(tag,
			stopper.WithLogger(logger.RawLogger().Named(tag))),
	}
	s.mu.lockTables = make(map[uint64]pb.LockTable)
	if err := s.stopper.RunTask(s.send); err != nil {
		panic(err)
	}
	return s
}

func (k *lockTableKeeper) Add(value pb.LockTable) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := k.mu.lockTables[value.Table]; ok {
		if k.logger.Enabled(zap.DebugLevel) {
			k.logger.Debug("lock table already added",
				zap.String("lock-table", value.DebugString()))
		}
		return
	}
	k.mu.lockTables[value.Table] = value
	k.mu.changed = true
	if k.logger.Enabled(zap.DebugLevel) {
		k.logger.Debug("lock table added",
			zap.String("lock-table", value.DebugString()))
	}
}

func (k *lockTableKeeper) Close() error {
	k.stopper.Stop()
	close(k.changedC)
	return k.sender.Close()
}

func (k *lockTableKeeper) Changed() chan pb.LockTable {
	return k.changedC
}

func (k *lockTableKeeper) send(ctx context.Context) {
	timer := time.NewTimer(k.interval)
	defer timer.Stop()

	var values []pb.LockTable
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			values = k.doSend(ctx, values)
			timer.Reset(k.interval)
		}
	}
}

func (k *lockTableKeeper) doSend(ctx context.Context, values []pb.LockTable) []pb.LockTable {
	values = k.getLockTables(values)
	if len(values) > 0 {
		ctx, cancel := context.WithTimeout(ctx, k.interval)
		defer cancel()
		changed, err := k.sender.Keep(ctx, values)
		if err != nil {
			k.logger.Error("failed to send keepalive request", zap.Error(err))
			return values
		}
		if len(changed) > 0 {
			k.lockTablesChanged(changed)
		}
	}
	return values
}

func (k *lockTableKeeper) getLockTables(values []pb.LockTable) []pb.LockTable {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.mu.changed {
		return values
	}
	values = values[:0]
	for _, v := range k.mu.lockTables {
		values = append(values, v)
	}
	k.mu.changed = false
	return values
}

func (k *lockTableKeeper) lockTablesChanged(changed []pb.LockTable) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, v := range changed {
		o := k.mu.lockTables[v.Table]
		delete(k.mu.lockTables, v.Table)
		if k.logger.Enabled(zap.DebugLevel) {
			k.logger.Debug("lock table changed",
				zap.String("old", v.DebugString()),
				zap.String("new", o.DebugString()))
		}
	}
	k.mu.changed = true
}
