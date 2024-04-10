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
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"go.uber.org/zap"
)

type lockTableAllocator struct {
	logger          *log.MOLogger
	stopper         *stopper.Stopper
	keepBindTimeout time.Duration
	address         string
	server          Server
	client          Client

	mu struct {
		sync.RWMutex
		services   map[string]*serviceBinds
		lockTables map[uint64]pb.LockTable
	}
}

// NewLockTableAllocator create a memory based lock table allocator.
func NewLockTableAllocator(
	address string,
	keepBindTimeout time.Duration,
	cfg morpc.Config) LockTableAllocator {
	if keepBindTimeout == 0 {
		panic("invalid lock table bind timeout")
	}

	rpcClient, err := NewClient(cfg)
	if err != nil {
		panic(err)
	}

	logger := runtime.ProcessLevelRuntime().Logger()
	tag := "lockservice.allocator"
	la := &lockTableAllocator{
		address: address,
		logger:  logger.Named(tag),
		stopper: stopper.NewStopper(tag,
			stopper.WithLogger(logger.RawLogger().Named(tag))),
		keepBindTimeout: keepBindTimeout,
		client:          rpcClient,
	}
	la.mu.lockTables = make(map[uint64]pb.LockTable, 10240)
	la.mu.services = make(map[string]*serviceBinds)
	if err := la.stopper.RunTask(la.checkInvalidBinds); err != nil {
		panic(err)
	}

	la.initServer(cfg)
	return la
}

func (l *lockTableAllocator) Get(
	serviceID string,
	tableID uint64) pb.LockTable {
	binds := l.getServiceBinds(serviceID)
	if binds == nil {
		binds = l.registerService(serviceID, tableID)
	}
	return l.registerBind(binds, tableID)
}

func (l *lockTableAllocator) KeepLockTableBind(serviceID string) bool {
	b := l.getServiceBinds(serviceID)
	if b == nil {
		return false
	}
	return b.active()
}

func (l *lockTableAllocator) Valid(binds []pb.LockTable) []uint64 {
	var invalid []uint64
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, b := range binds {
		if !b.Valid {
			panic("BUG")
		}

		// For upgrade, we must abort all old cn version's transactions.
		// Because the previous version does not support shared Table
		// segregation by tenant, then during the upgrade process there will
		// be 2 versions of CN at the same time, resulting in WW conflicts
		// that may be missed to be detected.
		// FIXME(fagongzi): remove this logic in next version.
		if isSharedTable(b.Table) {
			_, _, ok := decodeSharedTableID(b.Table)
			if !ok {
				invalid = append(invalid, b.Table)
				continue
			}
		}

		current, ok := l.mu.lockTables[b.Table]
		if !ok ||
			current.Changed(b) {
			l.logger.Info("table and service bind changed",
				zap.String("current", current.DebugString()),
				zap.String("received", b.DebugString()))
			invalid = append(invalid, b.Table)
		}
	}
	return invalid
}

func (l *lockTableAllocator) Close() error {
	l.stopper.Stop()
	var err error
	err1 := l.server.Close()
	l.logger.Debug("lock service allocator server closed",
		zap.Error(err))
	if err1 != nil {
		err = err1
	}
	err2 := l.client.Close()
	l.logger.Debug("lock service allocator client closed",
		zap.Error(err))
	if err2 != nil {
		err = err2
	}
	return err
}

func (l *lockTableAllocator) GetLatest(tableID uint64) pb.LockTable {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if old, ok := l.mu.lockTables[tableID]; ok {
		return old
	}
	return pb.LockTable{}
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
	l.logger.Info("service removed",
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
		if b.timeout(now, l.keepBindTimeout) {
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
	b = newServiceBinds(serviceID, l.logger.With(zap.String("lockservice", serviceID)))
	l.mu.services[serviceID] = b
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

	l.logger.Info("bind changed",
		zap.Uint64("table", tableID),
		zap.Uint64("version", old.Version),
		zap.String("service", binds.serviceID))
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
	l.logger.Info("bind created",
		zap.Uint64("table", tableID),
		zap.String("service", binds.serviceID))
	return b
}

func (l *lockTableAllocator) checkInvalidBinds(ctx context.Context) {
	timer := time.NewTimer(l.keepBindTimeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			l.logger.Debug("check lock service invalid task stopped")
			return
		case <-timer.C:
			timeoutBinds := l.getTimeoutBinds(time.Now())
			if len(timeoutBinds) > 0 {
				l.logger.Info("get timeout services",
					zap.Duration("timeout", l.keepBindTimeout),
					zap.Int("count", len(timeoutBinds)))
			}
			for _, b := range timeoutBinds {
				if !l.validateService(b.getServiceID(), ctx) {
					b.disable()
					l.disableTableBinds(b)
				}
			}
			timer.Reset(l.keepBindTimeout)
		}
	}
}

func (l *lockTableAllocator) validateService(serviceID string, ctx context.Context) bool {
	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_ValidateService
	req.ValidateService.ServiceID = serviceID

	ctx, cancel := context.WithTimeout(ctx, l.keepBindTimeout)
	defer cancel()
	resp, err := l.client.Send(ctx, req)
	if err != nil {
		logPingFailed(serviceID, err)
		return false
	}
	defer releaseResponse(resp)

	return resp.ValidateService.OK
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
	return true
}

func (b *serviceBinds) getServiceID() string {
	b.RLock()
	defer b.RUnlock()
	return b.serviceID
}

func (b *serviceBinds) timeout(
	now time.Time,
	timeout time.Duration) bool {
	b.RLock()
	defer b.RUnlock()
	v := now.Sub(b.lastKeepaliveTime)
	return v >= timeout
}

func (b *serviceBinds) disable() {
	b.Lock()
	defer b.Unlock()
	b.disabled = true
	b.logger.Info("bind disabled",
		zap.Int("tables", len(b.tables)),
		zap.String("service", b.serviceID))
}

func (l *lockTableAllocator) initServer(cfg morpc.Config) {
	s, err := NewServer(l.address, cfg)
	if err != nil {
		panic(err)
	}
	l.server = s
	l.initHandler()

	if err := l.server.Start(); err != nil {
		panic(err)
	}
}

func (l *lockTableAllocator) initHandler() {
	l.server.RegisterMethodHandler(
		pb.Method_GetBind,
		l.handleGetBind)

	l.server.RegisterMethodHandler(
		pb.Method_KeepLockTableBind,
		l.handleKeepLockTableBind,
	)
}

func (l *lockTableAllocator) handleGetBind(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	resp.GetBind.LockTable = l.Get(req.GetBind.ServiceID,
		req.GetBind.Table)
	writeResponse(ctx, cancel, resp, nil, cs)
}

func (l *lockTableAllocator) handleKeepLockTableBind(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	resp.KeepLockTableBind.OK = l.KeepLockTableBind(req.KeepLockTableBind.ServiceID)
	writeResponse(ctx, cancel, resp, nil, cs)
}
