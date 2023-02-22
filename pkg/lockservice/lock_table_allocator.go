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

func (l *lockTableAllocator) disableService(b *serviceBinds) {
	b.disable()
}

func (l *lockTableAllocator) getServiceBinds(serviceID string) *serviceBinds {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.mu.services[serviceID]
}

func (l *lockTableAllocator) getTimeoutService(now time.Time) []*serviceBinds {
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
		l.logger.Debug("lock service registed",
			zap.String("service", serviceID))
	}
	return b
}

func (l *lockTableAllocator) registerBind(
	binds *serviceBinds,
	tableID uint64) pb.LockTable {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.mu.lockTables[tableID]
	if ok {
		if b.Valid {
			return b
		}

		// the service and table's bind is invalid, rebind here
		b.ServiceID = binds.serviceID
		b.Version++
		b.Valid =
	}

	if !ok {
		if binds.bind(tableID) {
			b = pb.LockTable{Table: tableID, ServiceID: binds.serviceID, Version: 1}
			l.mu.lockTables[tableID] = b
		}
	}
	return b
}

func (l *lockTableAllocator) checkTimeoutService(ctx context.Context) {
	timer := time.NewTimer(l.timeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			timeoutServices := l.getTimeoutService(time.Now())
			for _, b := range timeoutServices {
				l.disableService(b)
			}
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
