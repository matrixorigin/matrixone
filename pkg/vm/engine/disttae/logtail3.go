// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"fmt"
	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"sync"
	"time"
)

// subscribedTable records cn table subscribe status.
type subscribedTable struct {
	m     map[subscribeID]bool
	mutex sync.RWMutex
}

func (s *subscribedTable) initTableSubscribeRecord() {
	s.mutex.Lock()
	s.m = make(map[subscribeID]bool)
	s.mutex.Unlock()
}

func (s *subscribedTable) getTableSubscribe(dbId, tblId uint64) bool {
	s.mutex.RLock()
	_, ok := s.m[subscribeID{dbId, tblId}]
	s.mutex.RUnlock()
	return ok
}

func (s *subscribedTable) setTableSubscribe(dbId, tblId uint64) {
	s.mutex.Lock()
	s.m[subscribeID{dbId, tblId}] = true
	s.mutex.Unlock()
}

type syncLogTailTimestamp struct {
	t timestamp.Timestamp
	sync.RWMutex
}

func (r *syncLogTailTimestamp) initLogTailTimestamp() {
	r.updateTimestamp(timestamp.Timestamp{})
}

func (r *syncLogTailTimestamp) getTimestamp() timestamp.Timestamp {
	r.RLock()
	t := r.t
	r.RUnlock()
	return t
}

func (r *syncLogTailTimestamp) updateTimestamp(newTimestamp timestamp.Timestamp) {
	r.Lock()
	r.t = newTimestamp
	r.Unlock()
}

func (r *syncLogTailTimestamp) greatEq(txnTime timestamp.Timestamp) bool {
	r.RLock()
	t := r.t
	r.RUnlock()
	return txnTime.LessEq(t)
}

func (r *syncLogTailTimestamp) blockUntilTxnTimeIsLegal(
	ctx context.Context, txnTime timestamp.Timestamp) error {
	// if block time is too long, return error.
	maxBlockTime := maxTimeToCheckTxnTimestamp
	for {
		if maxBlockTime < 0 {
			return moerr.NewTxnError(ctx,
				"new txn failed. please retry.")
		}
		if r.greatEq(txnTime) {
			return nil
		}
		time.Sleep(periodToCheckTxnTimestamp)
		maxBlockTime -= periodToCheckTxnTimestamp
	}
}

type logTailSubscriber struct {
	dnNodeID      int
	logTailClient *service.LogtailClient
}

func (s *logTailSubscriber) subscribeTable(
	ctx context.Context, tblId api.TableID) error {
	// set a default deadline for ctx if it doesn't have.
	if _, ok := ctx.Deadline(); !ok {
		newCtx, _ := context.WithTimeout(ctx, defaultTimeOutToSubscribeTable)
		return s.logTailClient.Subscribe(newCtx, tblId)
	}
	return s.logTailClient.Subscribe(ctx, tblId)
}

func (s *logTailSubscriber) unSubscribeTable(
	ctx context.Context, tblId api.TableID) error {
	// set a default deadline for ctx if it doesn't have.
	if _, ok := ctx.Deadline(); !ok {
		newCtx, _ := context.WithTimeout(ctx, defaultTimeOutToSubscribeTable)
		return s.logTailClient.Unsubscribe(newCtx, tblId)
	}
	return s.logTailClient.Unsubscribe(ctx, tblId)
}

func (e *Engine) InitLogTailPushModel(
	ctx context.Context) error {
	e.receiveLogTailTime.initLogTailTimestamp()
	e.subscribed.initTableSubscribeRecord()
	if err := e.initTableLogTailSubscriber(); err != nil {
		return err
	}
	e.StartToReceiveTableLogTail()
	if err := e.connectToLogTailServer(ctx); err != nil {
		return err
	}
	return nil
}

func (e *Engine) initTableLogTailSubscriber() error {
	// close the old rpc client.
	if e.subscriber != nil {
		if err := e.subscriber.logTailClient.Close(); err != nil {
			return err
		}
	}
	e.subscriber = new(logTailSubscriber)
	cluster, err := e.getClusterDetails()
	if err != nil {
		return err
	}
	// XXX we assume that we have only 1 dn now.
	e.subscriber.dnNodeID = 0
	dnLogTailServerBackend := cluster.DNStores[0].LogtailServerAddress
	// XXX generate a rpc client and new a stream.
	// we should hide these code into NewClient method next day.
	codec := morpc.NewMessageCodec(func() morpc.Message {
		return &service.LogtailResponseSegment{}
	})
	factory := morpc.NewGoettyBasedBackendFactory(codec,
		morpc.WithBackendGoettyOptions(
			goetty.WithSessionRWBUfferSize(1<<20, 1<<20),
		),
		morpc.WithBackendLogger(logutil.GetGlobalLogger().Named("cn-log-tail-client-backend")),
	)

	c, err := morpc.NewClient(factory,
		morpc.WithClientMaxBackendPerHost(10000),
		morpc.WithClientTag("cn-log-tail-client"),
	)

	s, err := c.NewStream(dnLogTailServerBackend, true)
	if err != nil {
		return err
	}
	// new the log tail client.
	e.subscriber.logTailClient, err = service.NewLogtailClient(s,
		service.WithClientRequestPerSecond(maxSubscribeRequestPerSecond))
	return nil
}

func (e *Engine) connectToLogTailServer(
	ctx context.Context) error {
	var err error
	// push subscription to Table `mo_database`, `mo_table`, `mo_column` of mo_catalog.
	databaseId := uint64(catalog.MO_CATALOG_ID)
	tableIds := []uint64{catalog.MO_DATABASE_ID, catalog.MO_TABLES_ID, catalog.MO_COLUMNS_ID}

	ch := make(chan error)
	go func() {
		for _, ti := range tableIds {
			er := e.subscriber.subscribeTable(ctx,
				api.TableID{DbId: databaseId, TbId: ti})
			if err != nil {
				ch <- er
				return
			}
		}
		ch <- nil
	}()

	select {
	case <-ctx.Done():
		return moerr.NewInternalError(ctx, "connect to dn log tail server failed")
	case err = <-ch:
		return err
	}
}

func (e *Engine) tryToGetTableLogTail(
	ctx context.Context,
	dbId, tblId uint64) error {
	// subscribe table if it's never subscribed.
	// and poll to check if we receive the log.
	if !e.subscribed.getTableSubscribe(dbId, tblId) {
		if err := e.subscriber.subscribeTable(ctx,
			api.TableID{DbId: dbId, TbId: tblId}); err != nil {
			return err
		}
		// poll until table was subscribed.
		for {
			if e.subscribed.getTableSubscribe(dbId, tblId) {
				break
			}
			time.Sleep(periodToCheckLogTailReady)
		}
	}
	// XXX we can move the subscribe-status-check here.
	return nil
}

func (e *Engine) StartToReceiveTableLogTail() {
	type response struct {
		r   *service.LogtailResponse
		err error
	}
	generateResponse := func(ltR *service.LogtailResponse, err error) response {
		return response{r: ltR, err: err}
	}
	// set up a background routine to receive table log.
	// if fail to connect log tail server.
	// it should reconnect.
	go func() {
		ctx := context.TODO()
		for {
			ch := make(chan response, 1)
			reconect := false
			for {
				if reconect {
					break
				}

				deadline, cancel := context.WithTimeout(ctx, reconnectDeadTime)
				select {
				case <-deadline.Done():
					reconect = true
					continue
				case ch <- generateResponse(e.subscriber.logTailClient.Receive()):
					cancel()
				}

				resp := <-ch
				if resp.err != nil {
					// get a rpc err from log tail server. and should reconnect soon.
					logutil.Error(
						fmt.Sprintf("receive a error from dn log tail server, err is %s",
							resp.err.Error()))
					reconect = true
				} else {
					subscriber := e.subscriber
					switch {
					case resp.r.GetSubscribeResponse() != nil:
						lt := resp.r.GetSubscribeResponse().GetLogtail()
						logTs := lt.GetTs()
						if err := updatePartition2(ctx, subscriber.dnNodeID, e, &lt, *logTs); err != nil {
							logutil.Errorf("update partition failed. err is %s", err)
							reconect = true
							break
						}
						tbl := lt.GetTable()
						e.subscribed.setTableSubscribe(tbl.DbId, tbl.TbId)
						e.receiveLogTailTime.updateTimestamp(*logTs)

					case resp.r.GetUpdateResponse() != nil:
						logLists := resp.r.GetUpdateResponse().GetLogtailList()
						to := resp.r.GetUpdateResponse().GetTo()

						logList(logLists).Sort()
						for _, l := range logLists {
							if err := updatePartition2(ctx, subscriber.dnNodeID, e, &l, *l.Ts); err != nil {
								logutil.Errorf("update partition failed. err is %s", err)
								reconect = true
								break
							}
						}
						e.receiveLogTailTime.updateTimestamp(*to)

					default:
						// errResponse and unSubscribeResponse.
						// we have no need to handle these now.
						//case resp.r.GetError() != nil:
						//case resp.r.GetUnsubscribeResponse() != nil:
					}
				}
			}

			// reconnect to log tail server.
			e.receiveLogTailTime.initLogTailTimestamp()
			e.subscribed.initTableSubscribeRecord()
			for {
				if err := e.initTableLogTailSubscriber(); err != nil {
					logutil.Error("rebuild the cn log tail client failed")
					continue
				}
				if err := e.connectToLogTailServer(ctx); err == nil {
					logutil.Info("reconnect to dn log tail server success.")
					break
				}
				logutil.Error("reconnect to dn log tail server failed.")
				time.Sleep(periodToReconnectDnLogServer)
			}
		}
	}()
}
