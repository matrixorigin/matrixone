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
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
)

func TestLockRemote(t *testing.T) {
	runRemoteLockTableTests(
		t,
		pb.LockTable{ServiceID: "s1"},
		func(s Server) {
			s.RegisterMethodHandler(
				pb.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					writeResponse(ctx, cancel, resp, nil, cs)
				},
			)
		},
		func(l *remoteLockTable, s Server) {
			txnID := []byte("txn1")
			txn := newActiveTxn(txnID, string(txnID), newFixedSlicePool(32), "")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			txn.Lock()
			defer txn.Unlock()
			l.lock(ctx, txn, [][]byte{{1}}, LockOptions{}, func(r pb.Result, err error) {
				assert.NoError(t, err)
			})
			reuse.Free(txn, nil)
		},
		func(lt pb.LockTable) {},
	)
}

func TestUnlockRemote(t *testing.T) {
	runRemoteLockTableTests(
		t,
		pb.LockTable{ServiceID: "s1"},
		func(s Server) {
			s.RegisterMethodHandler(
				pb.Method_Unlock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					writeResponse(ctx, cancel, resp, nil, cs)
				},
			)
		},
		func(l *remoteLockTable, s Server) {
			txnID := []byte("txn1")
			txn := newActiveTxn(txnID, string(txnID), newFixedSlicePool(32), "")
			l.unlock(txn, nil, timestamp.Timestamp{})
			reuse.Free(txn, nil)
		},
		func(lt pb.LockTable) {},
	)
}

func TestUnlockRemoteWithRetry(t *testing.T) {
	n := 0
	c := make(chan struct{})
	runRemoteLockTableTests(
		t,
		pb.LockTable{ServiceID: "s1"},
		func(s Server) {
			s.RegisterMethodHandler(
				pb.Method_Unlock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					n++
					if n == 1 {
						writeResponse(ctx, cancel, resp, moerr.NewRPCTimeout(ctx), cs)
						return
					}
					close(c)
					writeResponse(ctx, cancel, resp, nil, cs)
				},
			)
			s.RegisterMethodHandler(
				pb.Method_GetBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.GetBind.LockTable = pb.LockTable{
						ServiceID: "s1",
						Valid:     true,
					}
					writeResponse(ctx, cancel, resp, nil, cs)
				},
			)
		},
		func(l *remoteLockTable, s Server) {
			txnID := []byte("txn1")
			txn := newActiveTxn(txnID, string(txnID), newFixedSlicePool(32), "")
			l.unlock(txn, nil, timestamp.Timestamp{})
			<-c
			reuse.Free(txn, nil)
		},
		func(lt pb.LockTable) {},
	)
}

func TestRemoteWithBindChanged(t *testing.T) {
	newBind := pb.LockTable{
		ServiceID: "s2",
		Table:     1,
		Version:   2,
	}

	c := make(chan pb.LockTable, 1)
	defer close(c)
	runRemoteLockTableTests(
		t,
		pb.LockTable{ServiceID: "s1", Table: 1, Version: 1},
		func(s Server) {
			s.RegisterMethodHandler(
				pb.Method_Lock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.NewBind = &newBind
					writeResponse(ctx, cancel, resp, nil, cs)
				},
			)

			s.RegisterMethodHandler(
				pb.Method_Unlock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.NewBind = &newBind
					writeResponse(ctx, cancel, resp, nil, cs)
				},
			)

			s.RegisterMethodHandler(
				pb.Method_GetTxnLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.NewBind = &newBind
					writeResponse(ctx, cancel, resp, nil, cs)
				},
			)
		},
		func(l *remoteLockTable, s Server) {
			txnID := []byte("txn1")
			txn := newActiveTxn(txnID, string(txnID), newFixedSlicePool(32), "")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			txn.Lock()
			l.lock(ctx, txn, [][]byte{{1}}, LockOptions{}, func(r pb.Result, err error) {
				assert.Error(t, ErrLockTableBindChanged, err)
			})
			assert.Equal(t, newBind, <-c)
			txn.Unlock()

			l.unlock(txn, nil, timestamp.Timestamp{})
			assert.Equal(t, newBind, <-c)

			l.getLock(txnID, pb.WaitTxn{TxnID: []byte{1}}, nil)
			assert.Equal(t, newBind, <-c)
			reuse.Free(txn, nil)
		},
		func(bind pb.LockTable) {
			c <- bind
		},
	)
}

func runRemoteLockTableTests(
	t *testing.T,
	binding pb.LockTable,
	register func(s Server),
	fn func(l *remoteLockTable, s Server),
	changed func(pb.LockTable)) {
	defer leaktest.AfterTest(t)()
	runRPCTests(t, func(c Client, s Server) {
		register(s)
		l := newRemoteLockTable(
			"",
			time.Second,
			binding,
			c,
			changed)
		defer l.close()
		fn(l, s)
	})
}
