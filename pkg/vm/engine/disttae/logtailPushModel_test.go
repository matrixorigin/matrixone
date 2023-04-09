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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/stretchr/testify/require"
)

// should ensure that subscribe and unsubscribe methods are effective.
func TestSubscribedTable(t *testing.T) {
	var subscribeRecord subscribedTable
	subscribeRecord.initTableSubscribeRecord()
	require.Equal(t, 0, len(subscribeRecord.m))

	tbls := []struct {
		db uint64
		tb uint64
	}{
		{db: 0, tb: 1},
		{db: 0, tb: 2},
		{db: 0, tb: 3},
		{db: 0, tb: 4},
		{db: 0, tb: 1},
	}
	for _, tbl := range tbls {
		subscribeRecord.setTableSubscribe(tbl.db, tbl.tb)
	}
	require.Equal(t, 4, len(subscribeRecord.m))
	require.Equal(t, true, subscribeRecord.getTableSubscribe(tbls[0].db, tbls[0].tb))
	for _, tbl := range tbls {
		subscribeRecord.setTableUnsubscribe(tbl.db, tbl.tb)
	}
	require.Equal(t, 0, len(subscribeRecord.m))
}

func TestReconnectRace(t *testing.T) {
	pClient := pushClient{
		receivedLogTailTime: syncLogTailTimestamp{},
		subscribed:          subscribedTable{},
		subscriber: &logTailSubscriber{
			lockSubscriber: make(chan func(context.Context, api.TableID) error, 1),
		},
	}
	pClient.receivedLogTailTime.initLogTailTimestamp()
	pClient.subscribed.initTableSubscribeRecord()
	pClient.subscriber.lockSubscriber <- func(ctx context.Context, id api.TableID) error {
		return nil
	}

	// mock reconnect
	mockReconnect := func(pc *pushClient) {
		<-pc.subscriber.lockSubscriber
		time.Sleep(time.Millisecond * 2)
		pc.subscriber.lockSubscriber <- func(ctx context.Context, id api.TableID) error {
			return nil
		}
	}

	go func() {
		for i := 0; i < 100; i++ {
			mockReconnect(&pClient)
		}
	}()
	go func() {
		for i := 0; i < 1000; i++ {
			err := pClient.subscribeTable(context.TODO(), api.TableID{DbId: 0, TbId: 0})
			require.NoError(t, err)
		}
	}()
	go func() {
		for i := 0; i < 1000; i++ {
			err := pClient.subscribeTable(context.TODO(), api.TableID{DbId: 0, TbId: 0})
			require.NoError(t, err)
		}
	}()
}

var _ = debugToPrintLogList

func debugToPrintLogList(ls []logtail.TableLogtail) string {
	if len(ls) == 0 {
		return ""
	}
	str := "log list are:\n"
	for i, l := range ls {
		did, tid := l.Table.DbId, l.Table.TbId
		str += fmt.Sprintf("\t log: %d, dn: %d, tbl: %d\n", i, did, tid)
		if len(l.Commands) > 0 {
			str += "\tcommands are :\n"
		}
		for j, command := range l.Commands {
			str += fmt.Sprintf("\t\t %d: [dnName: %s, tableName: %s, typ: %s]\n",
				j, command.DatabaseName, command.TableName, command.EntryType.String())
		}
	}
	return str
}
