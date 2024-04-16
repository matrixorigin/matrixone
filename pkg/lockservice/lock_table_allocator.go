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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/common/util"
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
		services     map[string]*serviceBinds
		lockTables   map[uint32]map[uint64]pb.LockTable
		cannotCommit map[string]*cannotCommit
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
	la.mu.lockTables = make(map[uint32]map[uint64]pb.LockTable)
	la.mu.services = make(map[string]*serviceBinds)
	la.mu.cannotCommit = make(map[string]*cannotCommit)
	if err := la.stopper.RunTask(la.checkInvalidBinds); err != nil {
		panic(err)
	}
	if err := la.stopper.RunTask(la.cleanCannotCommit); err != nil {
		panic(err)
	}

	la.initServer(cfg)
	logLockAllocatorStartSucc()
	return la
}

func (l *lockTableAllocator) Get(
	serviceID string,
	group uint32,
	tableID uint64,
	originTableID uint64,
	sharding pb.Sharding) pb.LockTable {
	binds := l.getServiceBinds(serviceID)
	if binds == nil {
		binds = l.registerService(serviceID, tableID)
	}
	return l.registerBind(binds, group, tableID, originTableID, sharding)
}

func (l *lockTableAllocator) KeepLockTableBind(serviceID string) bool {
	b := l.getServiceBinds(serviceID)
	if b == nil {
		return false
	}
	return b.active()
}

func (l *lockTableAllocator) AddCannotCommit(values []pb.OrphanTxn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, v := range values {
		service := getUUIDFromServiceIdentifier(v.Service)
		if _, ok := l.mu.cannotCommit[service]; !ok {
			l.mu.cannotCommit[service] = &cannotCommit{txn: map[string]struct{}{}, serviceID: v.Service}
		}
		for _, txn := range v.Txn {
			l.mu.cannotCommit[service].txn[util.UnsafeBytesToString(txn)] = struct{}{}
		}
	}
}

func (l *lockTableAllocator) Valid(
	serviceID string,
	txnID []byte,
	binds []pb.LockTable,
) ([]uint64, error) {
	var invalid []uint64
	l.mu.RLock()
	defer l.mu.RUnlock()

	fn := func() error {
		service := getUUIDFromServiceIdentifier(serviceID)
		if c, ok := l.mu.cannotCommit[service]; ok {
			// means cn restart, all cannot commit txn can remove.
			if c.serviceID != serviceID {
				return nil
			}

			if _, ok := c.txn[util.UnsafeBytesToString(txnID)]; ok {
				return moerr.NewCannotCommitOrphanNoCtx()
			}
		}
		return nil
	}

	if err := fn(); err != nil {
		return nil, err
	}

	for _, b := range binds {
		if !b.Valid {
			panic("BUG")
		}

		current, ok := l.getLockTablesLocked(b.Group)[b.Table]
		if !ok ||
			current.Changed(b) {
			l.logger.Info("table and service bind changed",
				zap.String("current", current.DebugString()),
				zap.String("received", b.DebugString()))
			invalid = append(invalid, b.Table)
		}
	}
	return invalid, nil
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

func (l *lockTableAllocator) GetLatest(groupID uint32, tableID uint64) pb.LockTable {
	l.mu.RLock()
	defer l.mu.RUnlock()

	m := l.getLockTablesLocked(groupID)
	if old, ok := m[tableID]; ok {
		return old
	}
	return pb.LockTable{}
}

func (l *lockTableAllocator) setRestartService(serviceID string) {
	b := l.getServiceBindsWithoutPrefix(serviceID)
	if b == nil {
		getLogger().Error("not found restart lock service",
			zap.String("serviceID", serviceID))
		return
	}
	b.setStatus(pb.Status_ServiceLockWaiting)
}

func (l *lockTableAllocator) remainTxnInService(serviceID string) int32 {
	b := l.getServiceBindsWithoutPrefix(serviceID)
	if b == nil {
		getLogger().Error("not found restart lock service",
			zap.String("serviceID", serviceID))
		return 0
	}
	txnIDs := b.getTxnIds()
	getLogger().Error("remain txn in restart service",
		bytesArrayField("txnIDs", txnIDs),
		zap.String("serviceID", serviceID))

	c := len(txnIDs)
	if c == 0 {
		b := l.getServiceBindsWithoutPrefix(serviceID)
		if b == nil ||
			!b.isStatus(pb.Status_ServiceCanRestart) ||
			!b.isStatus(pb.Status_ServiceCanRestart) {
			// -1 means can not get right remain txn in restart lock service
			c = -1
			getLogger().Error("can not get right remain txn in restart lock service",
				zap.String("serviceID", serviceID))
		}

	}
	return int32(c)
}

func (l *lockTableAllocator) validLockTable(group uint32, table uint64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	t, ok := l.mu.lockTables[group][table]
	if !ok {
		return false
	}
	return t.Valid
}

func (l *lockTableAllocator) canRestartService(serviceID string) bool {
	b := l.getServiceBindsWithoutPrefix(serviceID)
	if b == nil {
		getLogger().Error("not found restart lock service",
			zap.String("serviceID", serviceID))
		return true
	}
	return b.isStatus(pb.Status_ServiceCanRestart)
}

func (l *lockTableAllocator) disableTableBinds(b *serviceBinds) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// we can't just delete the LockTable's effectiveness binding directly, we
	// need to keep the binding version.
	for g, tables := range b.groupTables {
		for table := range tables {
			if old, ok := l.getLockTablesLocked(g)[table]; ok &&
				old.ServiceID == b.serviceID {
				old.Valid = false
				l.getLockTablesLocked(g)[table] = old
			}
		}
	}

	// service need deleted, because this service may never restart
	delete(l.mu.services, b.serviceID)
	l.logger.Info("service removed",
		zap.String("service", b.serviceID))
}

func (l *lockTableAllocator) disableGroupTables(groupTables []pb.LockTable, b *serviceBinds) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, t := range groupTables {
		if old, ok := l.getLockTablesLocked(t.Group)[t.Table]; ok &&
			old.ServiceID == b.serviceID {
			old.Valid = false
			l.getLockTablesLocked(t.Group)[t.Table] = old
		}
	}
}

func (l *lockTableAllocator) getServiceBinds(serviceID string) *serviceBinds {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.mu.services[serviceID]
}

func (l *lockTableAllocator) getServiceBindsWithoutPrefix(serviceID string) *serviceBinds {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for k, v := range l.mu.services {
		id := getUUIDFromServiceIdentifier(k)
		if serviceID == id {
			return v
		}
	}
	return nil
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
	group uint32,
	tableID uint64,
	originTableID uint64,
	sharding pb.Sharding) pb.LockTable {
	l.mu.Lock()
	defer l.mu.Unlock()

	if old, ok := l.getLockTablesLocked(group)[tableID]; ok {
		return l.tryRebindLocked(binds, group, old, tableID)
	}
	return l.createBindLocked(binds, group, tableID, originTableID, sharding)
}

func (l *lockTableAllocator) tryRebindLocked(
	binds *serviceBinds,
	group uint32,
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
	if !binds.bind(group, tableID) {
		return old
	}

	// current service get the bind
	old.ServiceID = binds.serviceID
	old.Version++
	old.Valid = true
	l.getLockTablesLocked(group)[tableID] = old

	l.logger.Info("bind changed",
		zap.Uint64("table", tableID),
		zap.Uint64("version", old.Version),
		zap.String("service", binds.serviceID))
	return old
}

func (l *lockTableAllocator) createBindLocked(
	binds *serviceBinds,
	group uint32,
	tableID uint64,
	originTableID uint64,
	sharding pb.Sharding) pb.LockTable {
	// current service is invalid
	if !binds.bind(group, tableID) {
		return pb.LockTable{}
	}

	if originTableID == 0 {
		if sharding == pb.Sharding_ByRow {
			panic("invalid sharding origin table id")
		}
		originTableID = tableID
	}

	// create new table and service bind
	b := pb.LockTable{
		Table:       tableID,
		OriginTable: originTableID,
		ServiceID:   binds.serviceID,
		Version:     1,
		Valid:       true,
		Sharding:    sharding,
		Group:       group,
	}
	l.getLockTablesLocked(group)[tableID] = b
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
				valid, err := validateService(
					l.keepBindTimeout,
					b.getServiceID(),
					l.client,
				)
				if !valid || !isRetryError(err) {
					b.disable()
					l.disableTableBinds(b)
				}
			}
			timer.Reset(l.keepBindTimeout)
		}
	}
}

func (l *lockTableAllocator) cleanCannotCommit(ctx context.Context) {
	defer l.logger.InfoAction("clean cannot commit task")()

	timer := time.NewTimer(l.keepBindTimeout * 2)
	defer timer.Stop()

	getActiveTxn := func(sid string) ([][]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
		defer cancel()

		req := acquireRequest()
		defer releaseRequest(req)

		req.Method = pb.Method_GetActiveTxn
		req.GetActiveTxn.ServiceID = sid

		resp, err := l.client.Send(ctx, req)
		if err != nil {
			return nil, err
		}
		defer releaseResponse(resp)

		if !resp.GetActiveTxn.Valid {
			return nil, nil
		}

		return resp.GetActiveTxn.Txn, nil
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			var services []string
			var invalidServices []string
			activesMap := make(map[string]map[string]struct{})

			l.mu.RLock()
			for _, c := range l.mu.cannotCommit {
				services = append(services, c.serviceID)
			}
			l.mu.RUnlock()

			for _, sid := range services {
				actives, err := getActiveTxn(sid)
				if err == nil {
					activesMap[getUUIDFromServiceIdentifier(sid)] = make(map[string]struct{}, len(actives))
					if len(actives) == 0 {
						invalidServices = append(invalidServices, sid)
					} else {
						for _, txn := range actives {
							activesMap[getUUIDFromServiceIdentifier(sid)][util.UnsafeBytesToString(txn)] = struct{}{}
						}
					}
				}
			}

			l.mu.Lock()
			for _, sid := range invalidServices {
				delete(l.mu.cannotCommit, sid)
			}
			for sid, c := range l.mu.cannotCommit {
				if m, ok := activesMap[sid]; ok {
					for k := range c.txn {
						if _, ok := m[k]; !ok {
							delete(c.txn, k)
						}
					}

					if len(c.txn) == 0 {
						delete(l.mu.cannotCommit, sid)
					}
				}
			}
			l.mu.Unlock()

			timer.Reset(l.keepBindTimeout * 2)
		}
	}
}

// serviceBinds an instance of serviceBinds, recording the bindings of a lockservice
// and the locktable it manages
type serviceBinds struct {
	sync.RWMutex
	logger            *log.MOLogger
	serviceID         string
	groupTables       map[uint32]map[uint64]struct{}
	lastKeepaliveTime time.Time
	disabled          bool
	status            pb.Status
	txnIDs            [][]byte
}

func newServiceBinds(
	serviceID string,
	logger *log.MOLogger) *serviceBinds {
	return &serviceBinds{
		serviceID:         serviceID,
		logger:            logger,
		groupTables:       make(map[uint32]map[uint64]struct{}),
		lastKeepaliveTime: time.Now(),
	}
}

func (b *serviceBinds) getTxnIds() [][]byte {
	b.RLock()
	defer b.RUnlock()
	return b.txnIDs
}

func (b *serviceBinds) setTxnIds(txnIDs [][]byte) {
	b.Lock()
	defer b.Unlock()
	b.txnIDs = txnIDs
}

func (b *serviceBinds) isStatus(status pb.Status) bool {
	b.RLock()
	defer b.RUnlock()
	return b.status == status
}

func (b *serviceBinds) setStatus(status pb.Status) {
	b.Lock()
	defer b.Unlock()
	b.status = status
}

func (b *serviceBinds) active() bool {
	b.Lock()
	defer b.Unlock()
	if b.disabled {
		return false
	}
	b.lastKeepaliveTime = time.Now()
	b.logger.Debug("lock service binds active")
	return true
}

func (b *serviceBinds) bind(
	group uint32,
	tableID uint64) bool {
	b.Lock()
	defer b.Unlock()
	if b.disabled {
		return false
	}
	b.getTablesLocked(group)[tableID] = struct{}{}
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
		zap.String("service", b.serviceID))
}

func (b *serviceBinds) getTablesLocked(group uint32) map[uint64]struct{} {
	m, ok := b.groupTables[group]
	if ok {
		return m
	}
	m = make(map[uint64]struct{}, 1024)
	b.groupTables[group] = m
	return m
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

	l.server.RegisterMethodHandler(
		pb.Method_CanRestartService,
		l.handleCanRestartService,
	)

	l.server.RegisterMethodHandler(
		pb.Method_SetRestartService,
		l.handleSetRestartService,
	)

	l.server.RegisterMethodHandler(
		pb.Method_RemainTxnInService,
		l.handleRemainTxnInService,
	)

	l.server.RegisterMethodHandler(
		pb.Method_CannotCommit,
		l.handleCannotCommit)
}

func (l *lockTableAllocator) handleGetBind(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	if !l.canGetBind(req.GetBind.Group, req.GetBind.Table) {
		writeResponse(ctx, cancel, resp, moerr.NewRetryForCNRollingRestart(), cs)
		return
	}
	resp.GetBind.LockTable = l.Get(
		req.GetBind.ServiceID,
		req.GetBind.Group,
		req.GetBind.Table,
		req.GetBind.OriginTable,
		req.GetBind.Sharding)
	writeResponse(ctx, cancel, resp, nil, cs)
}

func (l *lockTableAllocator) handleKeepLockTableBind(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	resp.KeepLockTableBind.OK = l.KeepLockTableBind(req.KeepLockTableBind.ServiceID)
	if !resp.KeepLockTableBind.OK {
		writeResponse(ctx, cancel, resp, nil, cs)
		return
	}
	b := l.getServiceBinds(req.KeepLockTableBind.ServiceID)
	if b.isStatus(pb.Status_ServiceLockEnable) {
		writeResponse(ctx, cancel, resp, nil, cs)
		return
	}
	b.setTxnIds(req.KeepLockTableBind.TxnIDs)
	switch req.KeepLockTableBind.Status {
	case pb.Status_ServiceLockEnable:
		if b.isStatus(pb.Status_ServiceLockWaiting) {
			resp.KeepLockTableBind.Status = pb.Status_ServiceLockWaiting
		}
	case pb.Status_ServiceUnLockSucc:
		b.disable()
		l.disableTableBinds(b)
		b.setStatus(pb.Status_ServiceCanRestart)
		resp.KeepLockTableBind.Status = pb.Status_ServiceCanRestart
	default:
		b.setStatus(req.KeepLockTableBind.Status)
		resp.KeepLockTableBind.Status = req.KeepLockTableBind.Status
	}
	l.disableGroupTables(req.KeepLockTableBind.LockTables, b)
	writeResponse(ctx, cancel, resp, nil, cs)
}

func (l *lockTableAllocator) handleSetRestartService(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	l.setRestartService(req.SetRestartService.ServiceID)
	resp.SetRestartService.OK = true
	writeResponse(ctx, cancel, resp, nil, cs)
}

func (l *lockTableAllocator) handleCanRestartService(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	resp.CanRestartService.OK = l.canRestartService(req.CanRestartService.ServiceID)
	writeResponse(ctx, cancel, resp, nil, cs)
}

func (l *lockTableAllocator) handleRemainTxnInService(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	resp.RemainTxnInService.RemainTxn = l.remainTxnInService(req.RemainTxnInService.ServiceID)
	writeResponse(ctx, cancel, resp, nil, cs)
}

func (l *lockTableAllocator) getLockTablesLocked(group uint32) map[uint64]pb.LockTable {
	m, ok := l.mu.lockTables[group]
	if ok {
		return m
	}
	m = make(map[uint64]pb.LockTable, 10240)
	l.mu.lockTables[group] = m
	return m
}

func (l *lockTableAllocator) canGetBind(group uint32, tableID uint64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	t, ok := l.mu.lockTables[group][tableID]
	if !ok || !t.Valid {
		return true
	}
	b := l.mu.services[t.ServiceID]
	if b != nil &&
		!b.disabled &&
		!b.isStatus(pb.Status_ServiceLockEnable) {
		return false
	}
	return true
}

func (l *lockTableAllocator) handleCannotCommit(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {

	l.AddCannotCommit(req.CannotCommit.OrphanTxnList)

	writeResponse(ctx, cancel, resp, nil, cs)
}

func validateService(
	timeout time.Duration,
	serviceID string,
	client Client,
) (bool, error) {
	if timeout < defaultRPCTimeout {
		timeout = defaultRPCTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_ValidateService
	req.ValidateService.ServiceID = serviceID

	resp, err := client.Send(ctx, req)
	if err != nil {
		logPingFailed(serviceID, err)
		return false, err
	}
	defer releaseResponse(resp)

	return resp.ValidateService.OK, nil
}

type cannotCommit struct {
	serviceID string
	txn       map[string]struct{}
}
