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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeeper(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			n1 := atomic.Uint64{}
			n2 := atomic.Uint64{}
			c1 := make(chan struct{})
			c2 := make(chan struct{})
			s.RegisterMethodHandler(
				pb.Method_KeepLockTableBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {

					if n1.Add(1) == 10 {
						close(c1)
					}
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})
			s.RegisterMethodHandler(
				pb.Method_KeepRemoteLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {

					if n2.Add(1) == 10 {
						close(c2)
					}
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})
			m := &lockTableHolders{service: "s1", holders: map[uint32]*lockTableHolder{}}
			m.set(
				0,
				0,
				newRemoteLockTable(
					"s1",
					time.Second,
					pb.LockTable{ServiceID: "s2"},
					c,
					func(lt pb.LockTable) {},
					getLogger(""),
				),
			)
			m.set(
				0,
				1,
				newRemoteLockTable(
					"s1",
					time.Second,
					pb.LockTable{ServiceID: "s1"},
					c,
					func(lt pb.LockTable) {},
					getLogger(""),
				),
			)
			k := NewLockTableKeeper(
				"s1",
				c,
				time.Millisecond*10,
				time.Millisecond*10,
				m,
				&service{logger: getLogger("")})
			defer func() {
				assert.NoError(t, k.Close())
			}()
			<-c1
			<-c2
		},
	)
}

func TestKeepBindFailedWillRemoveAllLocalLockTable(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			events := newWaiterEvents(1, nil, nil, time.Second, nil, getLogger(""))
			defer events.close()

			s.RegisterMethodHandler(
				pb.Method_KeepLockTableBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.KeepLockTableBind.OK = false
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			s.RegisterMethodHandler(
				pb.Method_KeepRemoteLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			m := &lockTableHolders{service: "s1", holders: map[uint32]*lockTableHolder{}}
			m.set(
				0,
				1,
				newLocalLockTable(
					pb.LockTable{ServiceID: "s1"},
					nil,
					events,
					runtime.DefaultRuntime().Clock(),
					nil,
					getLogger(""),
				),
			)
			m.set(
				0,
				2,
				newLocalLockTable(
					pb.LockTable{ServiceID: "s1"},
					nil,
					events,
					runtime.DefaultRuntime().Clock(),
					nil,
					getLogger(""),
				),
			)
			m.set(
				0,
				3,
				newRemoteLockTable(
					"s1",
					time.Second,
					pb.LockTable{ServiceID: "s2"},
					c,
					func(lt pb.LockTable) {},
					getLogger(""),
				),
			)
			k := NewLockTableKeeper(
				"s1",
				c,
				time.Millisecond*10,
				time.Millisecond*10,
				m,
				&service{
					logger: getLogger(""),
				})
			defer func() {
				assert.NoError(t, k.Close())
			}()

			for {
				v := 0
				m.iter(func(key uint64, value lockTable) bool {
					v++
					return true
				})
				if v == 1 {
					v := m.get(0, 3)
					assert.NotNil(t, v)
					return
				}
				time.Sleep(time.Millisecond * 100)
			}
		},
	)
}

func TestKeepRemoteLockBindChangedFencesActiveTxn(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, server Server) {
			oldBind := pb.LockTable{Group: 0, Table: 1, OriginTable: 1, ServiceID: "s2", Version: 1, Valid: true}
			newBind := oldBind
			newBind.ServiceID = "s3"
			newBind.Version = 2

			server.RegisterMethodHandler(
				pb.Method_KeepRemoteLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.NewBind = &newBind
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})

			logger := getLogger("")
			fsp := newFixedSlicePool(2)
			svc := &service{
				serviceID: "s1",
				fsp:       fsp,
				logger:    logger,
			}
			svc.remote.client = c
			svc.tableGroups = &lockTableHolders{service: svc.serviceID, logger: logger, holders: map[uint32]*lockTableHolder{}}
			svc.activeTxnHolder = newMapBasedTxnHandler(
				svc.serviceID,
				logger,
				fsp,
				func(string) (bool, error) { return true, nil },
				func([]pb.OrphanTxn) ([][]byte, error) { return nil, nil },
				func(pb.WaitTxn) (bool, error) { return true, nil },
			)
			svc.tableGroups.set(
				oldBind.Group,
				oldBind.Table,
				newRemoteLockTable(svc.serviceID, time.Second, oldBind, c, svc.handleBindChanged, logger),
			)

			txnID := []byte("txn1")
			txn := svc.activeTxnHolder.getActiveTxn(txnID, true, "")
			txn.Lock()
			require.NoError(t, txn.lockAdded(oldBind.Group, oldBind, [][]byte{{1}}, logger))
			txn.Unlock()

			keeper := &lockTableKeeper{
				serviceID:   svc.serviceID,
				client:      c,
				groupTables: svc.tableGroups,
				service:     svc,
			}
			keeper.doKeepRemoteLock(context.Background(), nil, nil)

			txn.Lock()
			require.True(t, txn.bindChanged)
			txn.Unlock()
			require.Equal(t, newBind, svc.tableGroups.get(newBind.Group, newBind.Table).getBind())

			require.Equal(t, txn, svc.activeTxnHolder.deleteActiveTxn(txnID))
			txn.Lock()
			err := txn.close(txnID, timestamp.Timestamp{}, func(uint32, uint64) (lockTable, error) {
				return nil, nil
			}, logger)
			txn.Unlock()
			require.NoError(t, err)
		},
	)
}

func TestKeepRemoteLockFailureFetchesNewBindAndFencesActiveTxn(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, server Server) {
			oldBind := pb.LockTable{Group: 0, Table: 1, OriginTable: 1, ServiceID: "s2", Version: 1, Valid: true}
			newBind := oldBind
			newBind.ServiceID = "s3"
			newBind.Version = 2

			server.RegisterMethodHandler(
				pb.Method_GetBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.GetBind.LockTable = newBind
					writeResponse(getLogger(""), cancel, resp, nil, cs)
				})
			server.RegisterMethodHandler(
				pb.Method_KeepRemoteLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					writeResponse(getLogger(""), cancel, resp, ErrLockTableNotFound, cs)
				})

			logger := getLogger("")
			fsp := newFixedSlicePool(2)
			svc := &service{
				serviceID: "s1",
				fsp:       fsp,
				logger:    logger,
			}
			svc.remote.client = c
			svc.tableGroups = &lockTableHolders{service: svc.serviceID, logger: logger, holders: map[uint32]*lockTableHolder{}}
			svc.activeTxnHolder = newMapBasedTxnHandler(
				svc.serviceID,
				logger,
				fsp,
				func(string) (bool, error) { return true, nil },
				func([]pb.OrphanTxn) ([][]byte, error) { return nil, nil },
				func(pb.WaitTxn) (bool, error) { return true, nil },
			)
			svc.tableGroups.set(
				oldBind.Group,
				oldBind.Table,
				newRemoteLockTable(svc.serviceID, time.Second, oldBind, c, svc.handleBindChanged, logger),
			)

			txnID := []byte("txn1")
			txn := svc.activeTxnHolder.getActiveTxn(txnID, true, "")
			txn.Lock()
			require.NoError(t, txn.lockAdded(oldBind.Group, oldBind, [][]byte{{1}}, logger))
			txn.Unlock()

			keeper := &lockTableKeeper{
				serviceID:   svc.serviceID,
				client:      c,
				groupTables: svc.tableGroups,
				service:     svc,
			}
			keeper.doKeepRemoteLock(context.Background(), nil, nil)

			txn.Lock()
			require.True(t, txn.bindChanged)
			txn.Unlock()
			require.Equal(t, newBind, svc.tableGroups.get(newBind.Group, newBind.Table).getBind())

			require.Equal(t, txn, svc.activeTxnHolder.deleteActiveTxn(txnID))
			txn.Lock()
			err := txn.close(txnID, timestamp.Timestamp{}, func(uint32, uint64) (lockTable, error) {
				return nil, nil
			}, logger)
			txn.Unlock()
			require.NoError(t, err)
		},
	)
}
