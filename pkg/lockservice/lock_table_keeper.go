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
	"math/rand"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

const keepRemoteLockBatchSize = 64
const keeperIntervalJitterFraction = 10 // +/- 10%
const maxKeepRemoteLockFailureSummaries = 16
const maxKeepRemoteLockRefreshes = 64

type lockTableKeeper struct {
	serviceID                   string
	client                      Client
	stopper                     *stopper.Stopper
	keepLockTableBindInterval   time.Duration
	keepRemoteLockInterval      time.Duration
	groupTables                 *lockTableHolders
	service                     *service
	keepRemoteLockPeerOffset    uint64
	keepRemoteLockRefreshOffset uint64
	keepRemoteLockBindOffsets   map[string]uint64
}

// NewLockTableKeeper create a locktable keeper, an internal timer is started
// to send a keepalive request to the lockTableAllocator every interval, so this
// interval needs to be much smaller than the real lockTableAllocator's timeout.
func NewLockTableKeeper(
	serviceID string,
	client Client,
	keepLockTableBindInterval time.Duration,
	keepRemoteLockInterval time.Duration,
	groupTables *lockTableHolders,
	service *service,
) LockTableKeeper {
	s := &lockTableKeeper{
		serviceID:                 serviceID,
		client:                    client,
		groupTables:               groupTables,
		keepLockTableBindInterval: keepLockTableBindInterval,
		keepRemoteLockInterval:    keepRemoteLockInterval,
		service:                   service,
		stopper: stopper.NewStopper("lock-table-keeper",
			stopper.WithLogger(service.logger.RawLogger())),
	}
	if err := s.stopper.RunTask(s.keepLockTableBind); err != nil {
		panic(err)
	}
	if err := s.stopper.RunTask(s.keepRemoteLock); err != nil {
		panic(err)
	}
	return s
}

func (k *lockTableKeeper) Close() error {
	k.stopper.Stop()
	return nil
}

func (k *lockTableKeeper) keepLockTableBind(ctx context.Context) {
	defer k.service.logger.InfoAction("keep lock table bind task")()

	timer := time.NewTimer(jitterKeeperInterval(k.keepLockTableBindInterval))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			k.doKeepLockTableBind(ctx)
			timer.Reset(jitterKeeperInterval(k.keepLockTableBindInterval))
		}
	}
}

func (k *lockTableKeeper) keepRemoteLock(ctx context.Context) {
	defer k.service.logger.InfoAction("keep remote locks task")()

	timer := time.NewTimer(jitterKeeperInterval(k.keepRemoteLockInterval))
	defer timer.Stop()

	var futures []*morpc.Future
	var binds []pb.LockTable
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			futures, binds = k.doKeepRemoteLock(
				ctx,
				futures,
				binds)
			timer.Reset(jitterKeeperInterval(k.keepRemoteLockInterval))
		}
	}
}

func jitterKeeperInterval(interval time.Duration) time.Duration {
	if interval <= 0 {
		return interval
	}
	window := interval / keeperIntervalJitterFraction
	if window == 0 {
		return interval
	}
	return interval - window + time.Duration(rand.Int63n(int64(2*window)+1))
}

func (k *lockTableKeeper) doKeepRemoteLock(
	ctx context.Context,
	futures []*morpc.Future,
	binds []pb.LockTable) ([]*morpc.Future, []pb.LockTable) {
	type bindKey struct {
		group     uint32
		table     uint64
		serviceID string
		version   uint64
	}
	type failureSummary struct {
		bind  pb.LockTable
		err   error
		count int
	}
	type failureKey struct {
		code     uint16
		message  string
		overflow bool
	}
	type keepResult struct {
		bind                       pb.LockTable
		err                        error
		refresh                    bool
		invalidateOnRefreshFailure bool
		remove                     bool
	}
	type peerWork struct {
		serviceID string
		binds     []pb.LockTable
		next      int
		start     int
		launched  int
	}
	type keepCompletion struct {
		result keepResult
	}

	allBinds := make([]pb.LockTable, 0, cap(binds))
	binds = binds[:0]
	futures = futures[:0]
	checkedBinds := make(map[bindKey]struct{}, maxKeepRemoteLockRefreshes)
	refreshEligible := make(map[bindKey]struct{}, maxKeepRemoteLockRefreshes)
	refreshes := make([]keepResult, 0, maxKeepRemoteLockRefreshes)
	failures := make(map[failureKey]*failureSummary)
	bindKeyOf := func(bind pb.LockTable) bindKey {
		return bindKey{
			group:     bind.Group,
			table:     bind.Table,
			serviceID: bind.ServiceID,
			version:   bind.Version,
		}
	}
	recordFailure := func(bind pb.LockTable, err error) {
		key := failureKey{}
		if code, ok := moerr.GetMoErrCode(err); ok {
			key.code = code
		} else {
			key.message = err.Error()
		}
		if _, exists := failures[key]; !exists &&
			len(failures) >= maxKeepRemoteLockFailureSummaries {
			key = failureKey{overflow: true}
		}
		if summary, ok := failures[key]; ok {
			summary.count++
			return
		}
		failures[key] = &failureSummary{bind: bind, err: err, count: 1}
	}
	defer func() {
		for _, summary := range failures {
			logKeepRemoteLocksFailed(
				k.service.logger,
				summary.bind,
				summary.err,
				summary.count,
			)
		}
	}()
	recordRefresh := func(result keepResult) {
		key := bindKeyOf(result.bind)
		if _, ok := refreshEligible[key]; !ok {
			return
		}
		if _, ok := checkedBinds[key]; ok {
			return
		}
		if len(refreshes) == maxKeepRemoteLockRefreshes {
			return
		}
		checkedBinds[key] = struct{}{}
		refreshes = append(refreshes, result)
	}

	k.groupTables.iter(func(_ uint64, v lockTable) bool {
		bind := v.getBind()
		if bind.ServiceID != k.serviceID {
			allBinds = append(allBinds, bind)
		}
		return true
	})
	if len(allBinds) == 0 {
		return futures[:0], binds[:0]
	}
	sort.Slice(allBinds, func(i, j int) bool {
		left, right := allBinds[i], allBinds[j]
		if left.ServiceID != right.ServiceID {
			return left.ServiceID < right.ServiceID
		}
		if left.Group != right.Group {
			return left.Group < right.Group
		}
		if left.Table != right.Table {
			return left.Table < right.Table
		}
		return left.Version < right.Version
	})
	refreshStart := int(k.keepRemoteLockRefreshOffset % uint64(len(allBinds)))
	refreshCount := min(maxKeepRemoteLockRefreshes, len(allBinds))
	for i := 0; i < refreshCount; i++ {
		refreshEligible[bindKeyOf(allBinds[(refreshStart+i)%len(allBinds)])] = struct{}{}
	}
	k.keepRemoteLockRefreshOffset = uint64(
		(refreshStart + refreshCount) % len(allBinds),
	)

	roundCtx, cancel := context.WithTimeoutCause(
		ctx,
		defaultRPCTimeout,
		moerr.CauseDoKeepRemoteLock,
	)
	defer cancel()

	// Give every peer one slot before distributing additional slots
	// round-robin. This avoids cross-peer head-of-line blocking while keeping a
	// hard global cap on goroutines, Futures, requests, and completions.
	peerBinds := make(map[string]*peerWork)
	for _, bind := range allBinds {
		peer := peerBinds[bind.ServiceID]
		if peer == nil {
			peer = &peerWork{serviceID: bind.ServiceID}
			peerBinds[bind.ServiceID] = peer
		}
		peer.binds = append(peer.binds, bind)
	}
	if k.keepRemoteLockBindOffsets == nil {
		k.keepRemoteLockBindOffsets = make(map[string]uint64)
	}
	for serviceID, peer := range peerBinds {
		peer.start = int(k.keepRemoteLockBindOffsets[serviceID] % uint64(len(peer.binds)))
		if peer.start > 0 {
			rotated := append([]pb.LockTable(nil), peer.binds[peer.start:]...)
			peer.binds = append(rotated, peer.binds[:peer.start]...)
		}
	}
	for serviceID := range k.keepRemoteLockBindOffsets {
		if _, ok := peerBinds[serviceID]; !ok {
			delete(k.keepRemoteLockBindOffsets, serviceID)
		}
	}
	peerIDs := make([]string, 0, len(peerBinds))
	for serviceID := range peerBinds {
		peerIDs = append(peerIDs, serviceID)
	}
	sort.Strings(peerIDs)
	start := int(k.keepRemoteLockPeerOffset % uint64(len(peerIDs)))
	peers := make([]*peerWork, 0, len(peerIDs))
	for i := range peerIDs {
		peers = append(peers, peerBinds[peerIDs[(start+i)%len(peerIDs)]])
	}

	completions := make(chan keepCompletion, keepRemoteLockBatchSize)
	active := 0
	uniqueStarted := 0
	nextNewPeer := 0
	nextPeer := 0
	launch := func(peer *peerWork) {
		bind := peer.binds[peer.next]
		peer.next++
		peer.launched++
		if peer.next == 1 {
			uniqueStarted++
		}
		active++
		go func() {
			req := acquireRequest()
			req.Method = pb.Method_KeepRemoteLock
			req.LockTable = bind
			req.KeepRemoteLock.ServiceID = k.serviceID

			f, err := k.client.AsyncSend(roundCtx, req)
			if err != nil {
				err = moerr.AttachCause(roundCtx, err)
				completions <- keepCompletion{
					result: keepResult{
						bind:    bind,
						err:     err,
						refresh: true,
						remove:  !isRetryError(err),
					},
				}
				return
			}

			v, err := f.Get()
			result := keepResult{bind: bind}
			if err == nil {
				resp := v.(*pb.Response)
				if err = resp.UnwrapError(); err != nil {
					result.err = err
					result.refresh = canRefreshRemoteBindOnKeepError(err)
					result.invalidateOnRefreshFailure = result.refresh
				} else if resp.NewBind != nil {
					// A late response must not republish a superseded bind.
					result.refresh = true
					result.invalidateOnRefreshFailure = true
				}
				releaseResponse(resp)
			} else {
				result.err = err
				result.refresh = true
			}
			f.Close()
			completions <- keepCompletion{result: result}
		}()
	}
	findReadyPeer := func() *peerWork {
		for nextNewPeer < len(peers) {
			peer := peers[nextNewPeer]
			nextNewPeer++
			if peer.next < len(peer.binds) {
				return peer
			}
		}
		for checked := 0; checked < len(peers); checked++ {
			peer := peers[nextPeer]
			nextPeer = (nextPeer + 1) % len(peers)
			if peer.next < len(peer.binds) {
				return peer
			}
		}
		return nil
	}
	handleResult := func(result keepResult) {
		if result.err != nil {
			recordFailure(result.bind, result.err)
		}
		if result.refresh {
			recordRefresh(result)
		}
		if result.remove {
			bind := result.bind
			k.groupTables.removeWithFilter(func(_ uint64, v lockTable) bool {
				return !v.getBind().Changed(bind)
			}, closeReasonKeeperFailed)
		}
	}

	for {
		for active < keepRemoteLockBatchSize && roundCtx.Err() == nil {
			peer := findReadyPeer()
			if peer == nil {
				break
			}
			launch(peer)
		}
		if active == 0 {
			break
		}
		completion := <-completions
		active--
		handleResult(completion.result)
	}
	if uniqueStarted < len(peers) {
		k.keepRemoteLockPeerOffset = uint64((start + uniqueStarted) % len(peers))
	} else {
		k.keepRemoteLockPeerOffset = uint64((start + 1) % len(peers))
	}
	for _, peer := range peers {
		k.keepRemoteLockBindOffsets[peer.serviceID] = uint64(
			(peer.start + peer.launched) % len(peer.binds),
		)
	}

	for _, result := range refreshes {
		k.maybeHandleRemoteBindChanged(
			roundCtx,
			result.bind,
			result.invalidateOnRefreshFailure,
		)
	}
	return futures[:0], binds[:0]
}

func canRefreshRemoteBindOnKeepError(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) ||
		moerr.IsMoErrCode(err, moerr.ErrLockTableNotFound)
}

func (k *lockTableKeeper) maybeHandleRemoteBindChanged(
	ctx context.Context,
	bind pb.LockTable,
	invalidateOnRefreshFailure bool,
) {
	requestAllocator := k.service.allocatorStateSnapshot()
	newBind, allocator, err := getLockTableBindWithContext(
		ctx,
		k.client,
		bind.Group,
		bind.Table,
		bind.OriginTable,
		k.serviceID,
		bind.Sharding,
	)
	if err != nil {
		logGetRemoteBindFailed(k.service.logger, bind.Table, err)
		if invalidateOnRefreshFailure {
			k.invalidateRemoteBind(bind, requestAllocator)
		}
		return
	}
	if newBind.Changed(bind) {
		if err := k.service.handleBindChangedFromAllocator(
			"keep-remote-refresh",
			bind,
			newBind,
			allocator,
			requestAllocator); err != nil {
			if !moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) {
				logGetRemoteBindFailed(k.service.logger, bind.Table, err)
			}
		}
	}
}

func (k *lockTableKeeper) invalidateRemoteBind(
	bind pb.LockTable,
	allocator allocatorState,
) {
	k.service.bindChangeMu.Lock()
	defer k.service.bindChangeMu.Unlock()

	k.service.removeLockTablesWithFence(
		k.groupTables,
		func(candidate pb.LockTable) bool {
			return candidate.Group == bind.Group &&
				candidate.Table == bind.Table &&
				!candidate.Changed(bind)
		},
		allocator,
	)
}

func (k *lockTableKeeper) doKeepLockTableBind(ctx context.Context) {
	k.service.tryCompleteDrain()

	req := acquireRequest()
	defer releaseRequest(req)

	oldVersion := k.groupTables.getVersion()
	req.Method = pb.Method_KeepLockTableBind
	req.KeepLockTableBind.ServiceID = k.serviceID
	req.KeepLockTableBind.Status = k.service.getStatus()
	if !k.service.isStatus(pb.Status_ServiceLockEnable) {
		req.KeepLockTableBind.LockTables = k.service.topGroupTables()
		req.KeepLockTableBind.TxnIDs = k.service.activeTxnHolder.getAllTxnID()
	}

	timeout := k.keepLockTableBindInterval
	if keepBindTimeout := k.service.cfg.KeepBindTimeout.Duration; keepBindTimeout > 0 {
		if v := keepBindTimeout / 2; v > timeout {
			timeout = v
		}
	}
	if timeout > defaultRPCTimeout {
		timeout = defaultRPCTimeout
	}
	requestAllocator := k.service.allocatorStateSnapshot()
	ctx, cancel := context.WithTimeoutCause(ctx, timeout, moerr.CauseDoKeepLockTableBind)
	defer cancel()
	resp, err := k.client.Send(ctx, req)
	if err != nil {
		err = moerr.AttachCause(ctx, err)
		logKeepBindFailed(k.service.logger, err)
		return
	}
	defer releaseResponse(resp)

	if resp.KeepLockTableBind.OK {
		if _, accepted := k.service.observeAllocatorStateWithHoldersFromSnapshot(
			"keepalive",
			allocatorState{
				id:      resp.KeepLockTableBind.AllocatorID,
				version: resp.KeepLockTableBind.AllocatorVersion,
			},
			requestAllocator,
			true,
			k.groupTables); !accepted {
			return
		}
		switch resp.KeepLockTableBind.Status {
		case pb.Status_ServiceLockEnable:
			if !k.service.isStatus(pb.Status_ServiceLockEnable) {
				k.service.logger.Error("tn has abnormal lock service status",
					zap.String("serviceID", k.serviceID),
					zap.String("status", k.service.getStatus().String()))
			}
			return
		case pb.Status_ServiceLockWaiting:
			// maybe pb.Status_ServiceUnLockSucc
			if k.service.isStatus(pb.Status_ServiceLockEnable) {
				go k.service.checkCanMoveGroupTables()
			}
		default:
			k.service.setStatus(resp.KeepLockTableBind.Status)
		}
		if len(req.KeepLockTableBind.LockTables) > 0 {
			logBindsMove(k.service.logger, k.service.popGroupTables())
			logStatus(k.service.logger, k.service.getStatus())
		}
		return
	}

	n := k.service.handleKeepBindFailed(
		k.serviceID,
		k.groupTables,
		oldVersion,
		allocatorState{
			id:      resp.KeepLockTableBind.AllocatorID,
			version: resp.KeepLockTableBind.AllocatorVersion,
		},
		requestAllocator)

	if n > 0 {
		// Keep bind receiving an explicit failure means that all the binds of the local
		// lock table are invalid. We just need to remove it from the map, and the next
		// time we access it, we will automatically get the latest bind from allocate.
		logLocalBindsInvalid(k.service.logger)
	}
}
