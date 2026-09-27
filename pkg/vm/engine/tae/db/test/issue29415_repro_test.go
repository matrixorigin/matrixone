package test

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

type inspectPromotionServer struct {
	rpc.TxnServer
	inspect func()
}

func (s *inspectPromotionServer) SwitchTxnHandleStateTo(_ int, _ ...rpc.ServerOption) error {
	s.inspect()
	return context.Canceled
}

func TestIssue29415ReplayPromotionLateTable(t *testing.T) {
	ctx := context.Background()
	writeOpts := config.WithLongScanAndCKPOpts(nil, options.WithWalClientFactory(nil))
	writer := testutil.NewTestEngine(ctx, ModuleName, t, writeOpts)
	writerClosed := false
	t.Cleanup(func() {
		if !writerClosed {
			require.NoError(t, writer.DB.Close())
		}
		ioutil.Stop("")
	})

	replayOpts := config.WithLongScanAndCKPOpts(nil,
		options.WithWalClientFactory(writeOpts.WalClientFactory))
	replay := testutil.NewReplayTestEngine(ctx, ModuleName, t, replayOpts)
	t.Cleanup(func() { replay.Close() })

	schema := catalog.MockSchemaAll(2, 1)
	schema.Name = "late_replay_table"
	txn, err := writer.StartTxn(nil)
	require.NoError(t, err)
	database, err := txn.CreateDatabase("late_replay_db", "", "")
	require.NoError(t, err)
	_, err = database.CreateRelation(schema)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctx))

	var replayTable *catalog.TableEntry
	require.Eventually(t, func() bool {
		readTxn, err := replay.StartTxn(nil)
		if err != nil {
			return false
		}
		defer readTxn.Commit(ctx)
		entry, err := readTxn.GetDatabase("late_replay_db")
		if err != nil {
			return false
		}
		table, err := entry.GetRelationByName(schema.Name)
		if err != nil {
			return false
		}
		replayTable = table.GetMeta().(*catalog.TableEntry)
		return true
	}, 10*time.Second, time.Millisecond)
	require.NoError(t, writer.DB.Close())
	writerClosed = true

	var exists bool
	var queryErr error
	replay.TxnServer = &inspectPromotionServer{
		TxnServer: newTestTxnServer(t),
		inspect: func() {
			answer, err := replay.MergeScheduler.Query(ctx, catalog.ToMergeTable(replayTable))
			queryErr = err
			if queryErr == nil {
				exists = !answer.NotExists
			}
		},
	}
	_ = replay.Controller.SwitchTxnMode(ctx, 2, "")
	require.NoError(t, queryErr)
	require.True(t, exists, "scheduler missed a table committed by WAL replay")
}
