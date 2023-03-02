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
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

// remoteLockTable the lock corresponding to the Table is managed by a remote LockTable.
// And the remoteLockTable acts as a proxy for this LockTable locally.
type remoteLockTable struct {
	logger            *log.MOLogger
	binding           pb.LockTable
	client            Client
	stopper           *stopper.Stopper
	retryC            chan []byte
	heartbeatInterval time.Duration
}

func newRemoteLockTable(
	binding pb.LockTable,
	client Client) *remoteLockTable {
	tag := "remote-lock-table"
	logger := runtime.ProcessLevelRuntime().Logger().
		Named(tag).
		With(zap.String("binding", binding.DebugString()))
	l := &remoteLockTable{
		logger:  logger,
		binding: binding,
		stopper: stopper.NewStopper(tag,
			stopper.WithLogger(logger.RawLogger())),
		retryC: make(chan []byte, 1024),
	}
	return l
}

func (l *remoteLockTable) lock(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	options LockOptions) error {
	ctx, span := trace.Debug(ctx, "lockservice.remote.lock")
	defer span.End()

	req := acquireRequest()
	defer releaseRequest(req)

	req.ServiceID = l.binding.ServiceID
	req.Method = pb.Method_Lock
	req.Lock.Options = options
	req.Lock.LockTable = l.binding
	req.Lock.TxnID = txn.txnID
	req.Lock.Rows = rows

	resp, err := l.client.Send(ctx, req)
	if err == nil {
		// TODO: handle bind changed
		releaseResponse(resp)
		return nil
	}
	return err
}

func (l *remoteLockTable) unlock(
	ctx context.Context,
	txn *activeTxn,
	ls *cowSlice) error {
	return l.doUnlock(ctx, txn.txnID)
}

func (l *remoteLockTable) addToRetry(id []byte) {
	l.retryC <- id
}

// backgroundTask once a remote local table has been created, a periodic heartbeat needs to
// be enabled to maintain communication so that the remote lockservice does not think the
// current lockservice instance is down and release all the locks held by the transactions
// created on the current instance.
func (l *remoteLockTable) backgroundTask(ctx context.Context) {
	timer := time.NewTimer(l.heartbeatInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			l.doHeartbeat()
			timer.Reset(l.heartbeatInterval)
		case txnID := <-l.retryC:
			ctx2, cancel := context.WithTimeout(ctx, l.heartbeatInterval)
			if err := l.doUnlock(ctx2, txnID); err != nil {
				if l.logger.Enabled(zap.DebugLevel) {
					l.logger.Debug("failed to retry unlock txn",
						zap.ByteString("txn-id", txnID))
				}
			}
			cancel()
		}
	}
}

func (l *remoteLockTable) doHeartbeat() {

}

func (l *remoteLockTable) doUnlock(ctx context.Context, id []byte) error {
	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_Unlock
	req.ServiceID = l.binding.ServiceID
	req.Unlock.TxnID = id

	resp, err := l.client.Send(ctx, req)
	if err == nil {
		// TODO: handle bind changed
		releaseResponse(resp)
		return nil
	}

	l.addToRetry(id)
	return err
}

type lockTableServer struct {
	logger        *log.MOLogger
	lockTableFunc func(uint64) lockTable
	mu            struct {
		sync.RWMutex
		remoteActiveTxns map[string]string
	}
}

func newLockTableServer(server Server) *lockTableServer {
	return nil
}

func (s *lockTableServer) handleRemoteLock(context.Context, *pb.Request, *pb.Response) error {

	return nil
}
