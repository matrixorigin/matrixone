// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"context"
	"math"
	"testing"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

type replayPromotionCase struct {
	name                 string
	createSettings       bool
	settingsJSON         string
	expectTrigger        bool
	expectError          string
	cancelBeforeCall     bool
	preexistingLockMerge bool
	deleteSettingsRow    bool
	futureWriterClock    bool
	disableGC            bool
	ignoreClockUpdate    bool
}

type promotionUnresponsiveClock struct{ *types.MockHLCClock }

func (promotionUnresponsiveClock) Update(timestamp.Timestamp) {}

func TestIssue29415ReplayPromotionLateTableAndSettings(t *testing.T) {
	setting := merge.DefaultMergeSettings.Clone()
	setting.VacuumTopK++
	shortPoints := setting.Clone()
	shortPoints.L0MaxCountDecayControl = shortPoints.L0MaxCountDecayControl[:3]
	extraPoints := setting.Clone()
	extraPoints.L0MaxCountDecayControl = append(extraPoints.L0MaxCountDecayControl, 0.9)
	negativeCount := setting.Clone()
	negativeCount.TombstoneL1Count = -1
	tooManyL1 := setting.Clone()
	tooManyL1.TombstoneL1Count = math.MaxInt
	tooManyL2 := setting.Clone()
	tooManyL2.TombstoneL2Count = math.MaxInt
	for _, tc := range []replayPromotionCase{
		{name: "late setting", createSettings: true, settingsJSON: setting.String(), expectTrigger: true, cancelBeforeCall: true},
		{name: "future WAL timestamp", createSettings: true, settingsJSON: setting.String(), expectTrigger: true, futureWriterClock: true},
		{name: "settings table absent"},
		{name: "disabled disk GC", disableGC: true},
		{name: "clock cannot advance", futureWriterClock: true, ignoreClockUpdate: true, expectError: "promotion clock did not advance"},
		{name: "negative tombstone count", createSettings: true, settingsJSON: negativeCount.String(), expectError: "invalid merge settings counts"},
		{name: "oversized L1 count", createSettings: true, settingsJSON: tooManyL1.String(), expectError: "invalid merge settings counts"},
		{name: "oversized L2 count", createSettings: true, settingsJSON: tooManyL2.String(), expectError: "invalid merge settings counts"},
		{name: "settings row absent", createSettings: true},
		{name: "settings row deleted", createSettings: true, settingsJSON: setting.String(), deleteSettingsRow: true},
		{name: "invalid setting", createSettings: true, settingsJSON: `{"bad_settings":100}`, expectError: "probable corrupted merge settings"},
		{name: "short decay points", createSettings: true, settingsJSON: shortPoints.String(), expectError: "invalid merge settings decay points"},
		{name: "extra decay points", createSettings: true, settingsJSON: extraPoints.String(), expectError: "invalid merge settings decay points"},
		{name: "optional replay lock merge job", preexistingLockMerge: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runReplayPromotionLateSettings(t, tc)
		})
	}
}

func runReplayPromotionLateSettings(t *testing.T, tc replayPromotionCase) {
	t.Helper()
	ctx := context.Background()
	writeOpts := config.WithLongScanAndCKPOpts(nil, options.WithWalClientFactory(nil))
	if tc.futureWriterClock {
		writeOpts.Clock = types.NewMockHLCClock(time.Now().Add(time.Hour).UnixNano())
	}
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
	if tc.disableGC {
		replayOpts.GCCfg = &options.GCCfg{DisableGC: true}
	}
	if tc.ignoreClockUpdate {
		replayOpts.Clock = promotionUnresponsiveClock{types.NewMockHLCClock(time.Now().UnixNano())}
	}
	replay := testutil.NewReplayTestEngine(ctx, ModuleName, t, replayOpts)
	t.Cleanup(func() { replay.Close() })

	schema := catalog.MockSchemaAll(2, 1)
	schema.Name = "late_replay_table"
	txn, err := writer.StartTxn(nil)
	require.NoError(t, err)
	database, err := txn.CreateDatabase("late_replay_db", "", "")
	require.NoError(t, err)
	rel, err := database.CreateRelation(schema)
	require.NoError(t, err)
	if tc.createSettings {
		settingsSchema := catalog.NewEmptySchema(pkgcatalog.MO_MERGE_SETTINGS)
		require.NoError(t, settingsSchema.AppendCol("account_id", types.T_uint32.ToType()))
		require.NoError(t, settingsSchema.AppendPKCol("tid", types.T_uint64.ToType(), 0))
		require.NoError(t, settingsSchema.AppendCol("version", types.T_uint32.ToType()))
		require.NoError(t, settingsSchema.AppendCol("settings", types.T_json.ToType()))
		require.NoError(t, settingsSchema.AppendCol("extra_info", types.T_varchar.ToType()))
		require.NoError(t, settingsSchema.Finalize(false))
		settingsRel, err := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
		require.NoError(t, err)
		settingsTable, err := settingsRel.CreateRelation(settingsSchema)
		require.NoError(t, err)
		if tc.settingsJSON != "" {
			jsonValue, err := types.ParseStringToByteJson(tc.settingsJSON)
			require.NoError(t, err)
			encoded, err := types.EncodeJson(jsonValue)
			require.NoError(t, err)
			bat := containers.BuildBatch(
				[]string{"account_id", "tid", "version", "settings", "extra_info"},
				[]types.Type{types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_uint32.ToType(), types.T_json.ToType(), types.T_varchar.ToType()},
				containers.Options{},
			)
			t.Cleanup(bat.Close)
			bat.Vecs[0].Append(uint32(0), false)
			bat.Vecs[1].Append(rel.ID(), false)
			bat.Vecs[2].Append(uint32(merge.MergeSettingsVersion_Curr), false)
			bat.Vecs[3].Append(encoded, false)
			bat.Vecs[4].Append([]byte(""), false)
			require.NoError(t, settingsTable.Append(ctx, bat))
		}
	}
	require.NoError(t, txn.Commit(ctx))
	if tc.deleteSettingsRow {
		deleteTxn, err := writer.StartTxn(nil)
		require.NoError(t, err)
		settingsDB, err := deleteTxn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
		require.NoError(t, err)
		settingsTable, err := settingsDB.GetRelationByName(pkgcatalog.MO_MERGE_SETTINGS)
		require.NoError(t, err)
		require.NoError(t, settingsTable.DeleteByFilter(ctx, handle.NewEQFilter(rel.ID())))
		require.NoError(t, deleteTxn.Commit(ctx))
	}

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
	if tc.futureWriterClock {
		maxCommitted := replay.TxnMgr.MaxCommittedTS.Load()
		require.Greater(t, maxCommitted.Physical(), time.Now().UnixNano(), "WAL commit must be ahead of the replay TN wall clock")
		now := replay.TxnMgr.Now()
		require.True(t, now.LT(maxCommitted), "promotion must use the replayed commit timestamp when the local clock is behind")
	}

	if tc.cancelBeforeCall {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		require.ErrorIs(t, replay.Controller.SwitchTxnMode(canceled, 2, ""), context.Canceled)
	}
	if tc.preexistingLockMerge {
		require.NoError(t, db.AddCronJob(replay.DB, db.CronJobs_Name_GCLockMerge, false))
		require.ErrorContains(t, replay.Controller.SwitchTxnMode(ctx, 2, ""), "already exists before promotion")
		require.Nil(t, replay.CronJobs.GetJob(db.CronJobs_Name_GCCheckpoint))
		db.RemoveCronJob(replay.DB, db.CronJobs_Name_GCLockMerge)
	}
	replayedCommit := *replay.TxnMgr.MaxCommittedTS.Load()
	err = replay.Controller.SwitchTxnMode(ctx, 2, "")
	if tc.expectError != "" {
		require.ErrorContains(t, err, tc.expectError)
		require.True(t, replay.IsReplayMode())
		require.EqualError(t, replay.Controller.SwitchTxnMode(ctx, 2, ""), err.Error(), "failed handoff must not retry")
		_, startErr := replay.StartTxn(nil)
		require.EqualError(t, startErr, err.Error(), "terminal handoff must reject new transactions")
		require.Nil(t, replay.CronJobs.GetJob(db.CronJobs_Name_GCCheckpoint))
		require.Nil(t, replay.CronJobs.GetJob(db.CronJobs_Name_GCLockMerge))
		require.Nil(t, replay.CronJobs.GetJob(db.CronJobs_Name_GCDisk))
		return
	}
	require.NoError(t, err)
	require.True(t, replay.IsWriteMode())
	if tc.disableGC {
		require.Nil(t, replay.CronJobs.GetJob(db.CronJobs_Name_GCDisk))
		require.NotNil(t, replay.CronJobs.GetJob(db.CronJobs_Name_GCCheckpoint))
		require.NotNil(t, replay.CronJobs.GetJob(db.CronJobs_Name_GCLockMerge))
	} else {
		require.NoError(t, db.CheckCronJobs(replay.DB, db.DBTxnMode_Write))
	}
	// Prove write admission is usable, including a WAL clock ahead of local time.
	writeTxn, err := replay.StartTxn(nil)
	require.NoError(t, err)
	_, err = writeTxn.CreateDatabase("after_promotion", "", "")
	require.NoError(t, err)
	require.NoError(t, writeTxn.Commit(ctx))
	commitTS := writeTxn.GetCommitTS()
	require.True(t, commitTS.GT(&replayedCommit))
	answer, err := replay.MergeScheduler.Query(ctx, catalog.ToMergeTable(replayTable))
	require.NoError(t, err)
	require.False(t, answer.NotExists, "scheduler missed a table committed by WAL replay")
	if tc.expectTrigger {
		expected := merge.DefaultMergeSettings.Clone()
		expected.VacuumTopK++
		trigger, err := expected.ToMMsgTaskTrigger()
		require.NoError(t, err)
		require.Equal(t, trigger.String(), answer.BaseTrigger, "scheduler must recover the exact nondefault settings")
	} else {
		require.Empty(t, answer.BaseTrigger)
	}
}
