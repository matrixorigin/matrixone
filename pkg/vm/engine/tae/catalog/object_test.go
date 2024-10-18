// Copyright 2021 Matrix Origin
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

package catalog

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPrintPrepareCompactDebugLog(t *testing.T) {
	cat := MockCatalog()
	defer cat.Close()
	txnMgr := txnbase.NewTxnManager(
		MockTxnStoreFactory(cat),
		MockTxnFactory(cat),
		types.NewMockHLCClock(1),
	)
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	txn, _ := txnMgr.StartTxn(nil)
	dbEntry, err := cat.CreateDBEntry("test", "", "", txn)
	require.NoError(t, err)

	schema := MockSchema(1, 0)
	schema.Name = "test"
	tb, err := dbEntry.CreateTableEntry(schema, txn, nil)
	require.NoError(t, err)

	stats := objectio.NewObjectStatsWithObjectID(objectio.NewObjectid(), true, false, false)
	object, err := tb.CreateObject(txn, &objectio.CreateObjOpt{Stats: stats, IsTombstone: false}, nil)
	require.NoError(t, err)
	object.PrintPrepareCompactDebugLog()
	object.PrintPrepareCompactDebugLog()
}
