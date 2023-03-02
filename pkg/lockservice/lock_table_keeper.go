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

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"go.uber.org/zap"
)

type lockTableKeeper struct {
	logger    *log.MOLogger
	serviceID string
	sender    Client
	stopper   *stopper.Stopper
	interval  time.Duration
}

// NewLockTableKeeper create a locktable keeper, an internal timer is started
// to send a keepalive request to the lockTableAllocator every interval, so this
// interval needs to be much smaller than the real lockTableAllocator's timeout.
func NewLockTableKeeper(
	serviceID string,
	sender Client,
	interval time.Duration) LockTableKeeper {
	logger := runtime.ProcessLevelRuntime().Logger()
	tag := "lock-table-keeper"
	s := &lockTableKeeper{
		logger:    logger.Named(tag),
		serviceID: serviceID,
		sender:    sender,
		stopper: stopper.NewStopper(tag,
			stopper.WithLogger(logger.RawLogger().Named(tag))),
	}
	if err := s.stopper.RunTask(s.send); err != nil {
		panic(err)
	}
	return s
}

func (k *lockTableKeeper) Close() error {
	k.stopper.Stop()
	return nil
}

func (k *lockTableKeeper) send(ctx context.Context) {
	timer := time.NewTimer(k.interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			k.doSend(ctx)
			timer.Reset(k.interval)
		}
	}
}

func (k *lockTableKeeper) doSend(ctx context.Context) {
	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_Keepalive
	req.Keepalive.ServiceID = k.serviceID

	ctx, cancel := context.WithTimeout(ctx, k.interval)
	defer cancel()
	_, err := k.sender.Send(ctx, req)
	if err != nil {
		k.logger.Error("failed to send keepalive request",
			zap.Error(err))
	}
}
