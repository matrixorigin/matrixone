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

type lockTableAllocator struct {
	logger  *log.MOLogger
	stoper  *stopper.Stopper
	timeout time.Duration

	mu struct {
		sync.RWMutex
		services   map[string]*serviceBinds
		lockTables map[uint64]pb.LockTable
	}
}

// NewLockTableAllocator create a memory based lock table allocator.
func NewLockTableAllocator(timeout time.Duration) LockTableAllocator {
	if timeout == 0 {
		panic("invalid lock table bind timeout")
	}

	logger := runtime.ProcessLevelRuntime().Logger()
	tag := "lock-table-allocator"
	la := &lockTableAllocator{
		logger: logger.Named(tag),
		stoper: stopper.NewStopper(tag,
			stopper.WithLogger(logger.RawLogger().Named(tag))),
		timeout: timeout,
	}
	la.mu.lockTables = make(map[uint64]pb.LockTable, 10240)
	la.mu.services = make(map[string]*serviceBinds)
	if err := la.stoper.RunTask(la.checkInvalidBinds); err != nil {
		panic(err)
	}
	return la
}

func (l *lockTableAllocator) Get(
	serviceID string,
	tableID uint64) pb.LockTable {
	binds := l.getServiceBinds(serviceID)
	if binds == nil {
		binds = l.registerService(serviceID, tableID)
	}
	binds.active()
	return l.registerBind(binds, tableID)
}

func (l *lockTableAllocator) Keepalive(serviceID string) bool {
	b := l.getServiceBinds(serviceID)
	if b == nil {
		return false
	}
	return b.active()
}

func (l *lockTableAllocator) Valid(binds []pb.LockTable) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, b := range binds {
		if !b.Valid {
			panic("BUG")
		}
		current, ok := l.mu.lockTables[b.Table]
		if !ok ||
			current.Changed(b) {
			if l.logger.Enabled(zap.DebugLevel) {
				l.logger.Debug("table and service bind changed",
					zap.String("current", current.DebugString()),
					zap.String("received", b.DebugString()))
			}
			return false
		}
	}
	return true
}

func (l *lockTableAllocator) Close() error {
	l.stoper.Stop()
	l.logger.Debug("lock service allocator closed")
	return nil
}

func (l *lockTableAllocator) disableTableBinds(b *serviceBinds) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// we can't just delete the LockTable's effectiveness binding directly, we
	// need to keep the binding version.
	for table := range b.tables {
		if old, ok := l.mu.lockTables[table]; ok &&
			old.ServiceID == b.serviceID {
			old.Valid = false
			l.mu.lockTables[table] = old
		}
	}
	// service need deleted, because this service may never restart
	delete(l.mu.services, b.serviceID)
	l.logger.Debug("lock service disabled",
		zap.String("service", b.serviceID))
}

func (l *lockTableAllocator) getServiceBinds(serviceID string) *serviceBinds {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.mu.services[serviceID]
}

func (l *lockTableAllocator) getTimeoutBinds(now time.Time) []*serviceBinds {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var values []*serviceBinds
	for _, b := range l.mu.services {
		if b.timeout(now, l.timeout) {
			values = append(values, b)
		}
	}
	return values
}

func (l *lockTableAllocator) registerService(
	serviceID string,
	tableID uint64) *serviceBinds {
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.mu.services[serviceID]
	if ok {
		return b
	}
	b = newServiceBinds(serviceID, l.logger.With(zap.String("lock-service", serviceID)))
	l.mu.services[serviceID] = b

	if l.logger.Enabled(zap.DebugLevel) {
		l.logger.Debug("lock service registered",
			zap.String("service", serviceID))
	}
	return b
}

func (l *lockTableAllocator) registerBind(
	binds *serviceBinds,
	tableID uint64) pb.LockTable {
	l.mu.Lock()
	defer l.mu.Unlock()

	if old, ok := l.mu.lockTables[tableID]; ok {
		return l.tryRebindLocked(binds, old, tableID)
	}
	return l.createBindLocked(binds, tableID)
}

func (l *lockTableAllocator) tryRebindLocked(
	binds *serviceBinds,
	old pb.LockTable,
	tableID uint64) pb.LockTable {
	// find a valid table and service bind
	if old.Valid {
		return old
	}
	// reaches here, it means that the original table and service bindings have
	// been invalidated, and the current service has also been invalidated, so
	// there is no need for any re-bind operation here, and the invalid bind
	// information is directly returned to the service.
	if !binds.bind(tableID) {
		return old
	}

	// current service get the bind
	old.ServiceID = binds.serviceID
	old.Version++
	old.Valid = true
	l.mu.lockTables[tableID] = old
	return old
}

func (l *lockTableAllocator) createBindLocked(
	binds *serviceBinds,
	tableID uint64) pb.LockTable {
	// current service is invalid
	if !binds.bind(tableID) {
		return pb.LockTable{}
	}

	// create new table and service bind
	b := pb.LockTable{
		Table:     tableID,
		ServiceID: binds.serviceID,
		Version:   1,
		Valid:     true,
	}
	l.mu.lockTables[tableID] = b
	return b
}

func (l *lockTableAllocator) checkInvalidBinds(ctx context.Context) {
	timer := time.NewTimer(l.timeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			l.logger.Debug("check lock service invalid task stopped")
			return
		case <-timer.C:
			timeoutBinds := l.getTimeoutBinds(time.Now())
			l.logger.Debug("get timeout services",
				zap.Int("count", len(timeoutBinds)))
			for _, b := range timeoutBinds {
				b.disable()
				l.disableTableBinds(b)
			}
			timer.Reset(l.timeout)
		}
	}
}

// serviceBinds an instance of serviceBinds, recording the bindings of a lockservice
// and the locktable it manages
type serviceBinds struct {
	sync.RWMutex
	logger            *log.MOLogger
	serviceID         string
	tables            map[uint64]struct{}
	lastKeepaliveTime time.Time
	disabled          bool
}

func newServiceBinds(
	serviceID string,
	logger *log.MOLogger) *serviceBinds {
	return &serviceBinds{
		serviceID:         serviceID,
		logger:            logger,
		tables:            make(map[uint64]struct{}, 1024),
		lastKeepaliveTime: time.Now(),
	}
}

func (b *serviceBinds) active() bool {
	b.Lock()
	defer b.Unlock()
	if b.disabled {
		return false
	}
	b.lastKeepaliveTime = time.Now()
	b.logger.Debug("lock service binds actived")
	return true
}

func (b *serviceBinds) bind(tableID uint64) bool {
	b.Lock()
	defer b.Unlock()
	if b.disabled {
		return false
	}
	b.tables[tableID] = struct{}{}
	b.logger.Debug("table binded",
		zap.Uint64("table", tableID))
	return true
}

func (b *serviceBinds) timeout(
	now time.Time,
	timeout time.Duration) bool {
	b.RLock()
	defer b.RUnlock()
	return now.Sub(b.lastKeepaliveTime) >= timeout
}

func (b *serviceBinds) disable() {
	b.Lock()
	defer b.Unlock()
	b.disabled = true
	b.logger.Debug("lock service binds disabled")
}
