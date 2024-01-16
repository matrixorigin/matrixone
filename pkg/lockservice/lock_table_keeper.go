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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

type lockTableKeeper struct {
	serviceID                 string
	client                    Client
	stopper                   *stopper.Stopper
	keepLockTableBindInterval time.Duration
	keepRemoteLockInterval    time.Duration
	tables                    *lockTableHolder
	canDoKeep                 bool
}

// NewLockTableKeeper create a locktable keeper, an internal timer is started
// to send a keepalive request to the lockTableAllocator every interval, so this
// interval needs to be much smaller than the real lockTableAllocator's timeout.
func NewLockTableKeeper(
	serviceID string,
	client Client,
	keepLockTableBindInterval time.Duration,
	keepRemoteLockInterval time.Duration,
	tables *lockTableHolder) LockTableKeeper {
	s := &lockTableKeeper{
		serviceID:                 serviceID,
		client:                    client,
		tables:                    tables,
		keepLockTableBindInterval: keepLockTableBindInterval,
		keepRemoteLockInterval:    keepRemoteLockInterval,
		stopper: stopper.NewStopper("lock-table-keeper",
			stopper.WithLogger(getLogger().RawLogger())),
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
	defer getLogger().InfoAction("keep lock table bind task")()

	timer := time.NewTimer(k.keepLockTableBindInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			k.doKeepLockTableBind(ctx)
			timer.Reset(k.keepLockTableBindInterval)
		}
	}
}

func (k *lockTableKeeper) keepRemoteLock(ctx context.Context) {
	defer getLogger().InfoAction("keep remote locks task")()

	timer := time.NewTimer(k.keepRemoteLockInterval)
	defer timer.Stop()

	services := make(map[string]pb.LockTable)
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
				services,
				binds)
			timer.Reset(k.keepRemoteLockInterval)
		}
	}
}

func (k *lockTableKeeper) doKeepRemoteLock(
	ctx context.Context,
	futures []*morpc.Future,
	services map[string]pb.LockTable,
	binds []pb.LockTable) ([]*morpc.Future, []pb.LockTable) {
	for k := range services {
		delete(services, k)
	}
	binds = binds[:0]
	futures = futures[:0]

	k.tables.iter(func(key uint64, value lockTable) bool {
		bind := value.getBind()
		if bind.ServiceID != k.serviceID {
			services[bind.ServiceID] = bind
		}
		return true
	})
	if len(services) == 0 {
		return futures[:0], binds[:0]
	}

	ctx, cancel := context.WithTimeout(ctx, defaultRPCTimeout)
	defer cancel()
	for _, bind := range services {
		req := acquireRequest()
		defer releaseRequest(req)

		req.Method = pb.Method_KeepRemoteLock
		req.LockTable = bind
		req.KeepRemoteLock.ServiceID = k.serviceID

		f, err := k.client.AsyncSend(ctx, req)
		if err == nil {
			futures = append(futures, f)
			binds = append(binds, bind)
			continue
		}
		logKeepRemoteLocksFailed(bind, err)
	}

	for idx, f := range futures {
		v, err := f.Get()
		if err == nil {
			releaseResponse(v.(*pb.Response))
		} else {
			logKeepRemoteLocksFailed(binds[idx], err)
		}
		f.Close()
		futures[idx] = nil // gc
	}
	return futures[:0], binds[:0]
}

func (k *lockTableKeeper) doKeepLockTableBind(ctx context.Context) {
	if !k.canDoKeep {
		k.tables.iter(func(key uint64, value lockTable) bool {
			bind := value.getBind()
			if bind.ServiceID == k.serviceID {
				k.canDoKeep = true
			}
			return true
		})
	}
	if !k.canDoKeep {
		return
	}

	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_KeepLockTableBind
	req.KeepLockTableBind.ServiceID = k.serviceID

	ctx, cancel := context.WithTimeout(ctx, k.keepLockTableBindInterval)
	defer cancel()
	resp, err := k.client.Send(ctx, req)
	if err != nil {
		logKeepBindFailed(err)
		return
	}
	defer releaseResponse(resp)

	if resp.KeepLockTableBind.OK {
		return
	}

	n := 0
	k.tables.removeWithFilter(func(key uint64, value lockTable) bool {
		bind := value.getBind()
		if bind.ServiceID == k.serviceID {
			return true
		}
		n++
		return false
	})
	if n > 0 {
		// Keep bind receiving an explicit failure means that all the binds of the local
		// locktable are invalid. We just need to remove it from the map, and the next
		// time we access it, we will automatically get the latest bind from allocate.
		logLocalBindsInvalid()
	}
}
