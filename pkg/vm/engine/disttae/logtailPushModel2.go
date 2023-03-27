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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"time"
)

type pushClient struct {
	subscriber *logTailSubscriber

	// the timestamp of last log received by CN.
	receivedLogTailTime syncLogTailTimestamp

	// the record of subscribed table.
	subscribed subscribedTable
}

func (client *pushClient) init(serviceAddr string) error {
	client.receivedLogTailTime.initLogTailTimestamp()
	client.subscribed.initTableSubscribeRecord()

	err := client.subscriber.init(serviceAddr)
	if err != nil {
		return err
	}
	return nil
}

// checkTxnTimeIsLegal will block the process until log tail time of pushClient >= txn time.
func (client *pushClient) checkTxnTimeIsLegal(
	ctx context.Context, txnTime timestamp.Timestamp) error {
	ticker := time.NewTicker(periodToCheckTxnTimestamp)
	defer ticker.Stop()

	if client.receivedLogTailTime.greatEq(txnTime) {
		return nil
	}

	for i := maxTimeToNewTransaction; i > 0; i -= periodToCheckTxnTimestamp {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if client.receivedLogTailTime.greatEq(txnTime) {
				return nil
			}
		}
	}
	logutil.Errorf("new txn failed because lack of enough log tail. txn time is [%s]", txnTime)
	return moerr.NewTxnError(ctx, "new txn failed. please retry")
}

func (client *pushClient) getLatestLogTailTimestamp() timestamp.Timestamp {
	return client.receivedLogTailTime.getTimestamp()
}

// subscriber related
func (client *pushClient) subscribeTable(
	ctx context.Context, tblId api.TableID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case subscriber := <-client.subscriber.lockSubscriber:
		client.subscriber.lockSubscriber <- subscriber
		return subscriber(ctx, tblId)
	}
}

func (client *pushClient) unsubscribeTable(
	ctx context.Context, tblId api.TableID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case unsubscriber := <-client.subscriber.lockUnSubscriber:
		client.subscriber.lockUnSubscriber <- unsubscriber
		return unsubscriber(ctx, tblId)
	}
}

// subscribed related
