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

package merge

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/require"
)

func promotionTable(id uint64) catalog.MergeTable {
	db := catalog.MockDBEntryWithAccInfo(1, id)
	return catalog.ToMergeTable(catalog.MockTableEntryWithDB(db, id))
}

func TestPromotionSettingsDomains(t *testing.T) {
	decode := func(t *testing.T, setting *MergeSettings) *MergeSettings {
		t.Helper()
		jsonValue, err := types.ParseStringToByteJson(setting.String())
		require.NoError(t, err)
		encoded, err := types.EncodeJson(jsonValue)
		require.NoError(t, err)
		decoded, err := DecodeMergeSettings(MergeSettingsVersion_Curr, encoded)
		require.NoError(t, err)
		return decoded
	}
	for _, tc := range []struct {
		name      string
		edit      func(*MergeSettings)
		roundTrip bool
	}{
		{"negative L1 count", func(s *MergeSettings) { s.TombstoneL1Count = -1 }, false},
		{"zero L1 count", func(s *MergeSettings) { s.TombstoneL1Count = 0 }, false},
		{"negative L2 count", func(s *MergeSettings) { s.TombstoneL2Count = -1 }, false},
		{"zero L2 count", func(s *MergeSettings) { s.TombstoneL2Count = 0 }, false},
		{"L1 above budget", func(s *MergeSettings) { s.TombstoneL1Count = 65537 }, true},
		{"L2 above budget", func(s *MergeSettings) { s.TombstoneL2Count = 65537 }, true},
		{"L1 integer maximum", func(s *MergeSettings) { s.TombstoneL1Count = math.MaxInt }, true},
		{"L2 integer maximum", func(s *MergeSettings) { s.TombstoneL2Count = math.MaxInt }, true},
		{"negative overlap depth", func(s *MergeSettings) { s.LNMinPointDepthPerCluster = -1 }, false},
		{"zero overlap depth", func(s *MergeSettings) { s.LNMinPointDepthPerCluster = 0 }, false},
		{"zero vacuum duration", func(s *MergeSettings) { s.VacuumScoreDecayDuration = "0s" }, false},
		{"negative vacuum duration", func(s *MergeSettings) { s.VacuumScoreDecayDuration = "-1s" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := DefaultMergeSettings.Clone()
			tc.edit(s)
			if tc.roundTrip {
				s = decode(t, s)
			}
			trigger, err := s.toPromotionTrigger()
			require.ErrorContains(t, err, "invalid merge settings")
			require.Nil(t, trigger)
		})
	}

	// The smallest positive capacities and duration must still be schedulable.
	s := DefaultMergeSettings.Clone()
	s.TombstoneL1Count, s.TombstoneL2Count = 1, 1
	s.LNMinPointDepthPerCluster = 1
	s.VacuumScoreDecayDuration = "1ns"
	trigger, err := s.toPromotionTrigger()
	require.NoError(t, err)
	require.Equal(t, s.VacuumScoreStart, trigger.vacuum.CalcScore(0))
	require.Empty(t, GatherTombstoneTasks(t.Context(), IterStats(nil), trigger.tomb, 0))

	// The largest accepted settings must also be usable by their consumer.
	s.TombstoneL1Count, s.TombstoneL2Count = 65536, 65536
	trigger, err = decode(t, s).toPromotionTrigger()
	require.NoError(t, err)
	require.Empty(t, GatherTombstoneTasks(t.Context(), IterStats(nil), trigger.tomb, 0))
}

func TestPreparePromotionReconcilesStoppedScheduler(t *testing.T) {
	keep := promotionTable(1001)
	drop := promotionTable(1002)
	late := promotionTable(1003)
	sched := NewMergeScheduler(time.Hour,
		&dummyCatalogSource{initTables: []catalog.MergeTable{keep, drop}},
		&dummyExecutor{}, NewStdClock())
	require.NoError(t, sched.CheckPromotionReady())
	oldSupporter := sched.supps[keep.ID()]
	oldSupporter.AddTask()
	removedSupporter := sched.supps[drop.ID()]
	removedSupporter.AddTask()
	removedObserver := sched.taskObserverFactory(removedSupporter, 0, sched.rc)
	removedObserver.Admit()
	trigger, err := DefaultMergeSettings.ToMMsgTaskTrigger()
	require.NoError(t, err)
	oldSupporter.baseTrigger = trigger
	require.NoError(t, sched.PreparePromotion(
		&dummyCatalogSource{initTables: []catalog.MergeTable{keep, late}},
		map[uint64]*MMsgTaskTrigger{late.ID(): trigger},
	))
	require.Same(t, oldSupporter, sched.supps[keep.ID()])
	require.EqualValues(t, 1, oldSupporter.mergingTaskCnt.Load())
	oldSupporter.DoneTask()
	require.Nil(t, sched.supps[drop.ID()])
	removedObserver.OnExecDone(nil)
	require.EqualValues(t, 0, removedSupporter.mergingTaskCnt.Load())
	require.NotNil(t, sched.supps[late.ID()])
	require.Nil(t, sched.supps[keep.ID()].baseTrigger)
	require.Same(t, trigger, sched.supps[late.ID()].baseTrigger)
	require.Len(t, sched.pq, 2)
	require.Nil(t, sched.bootstrapMsg)
	require.True(t, sched.allPaused)

	sched.Start()
	t.Cleanup(sched.Stop)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, sched.ResumePromotion(ctx))
	answer, err := sched.Query(ctx, late)
	require.NoError(t, err)
	require.False(t, answer.NotExists)
	require.NotEmpty(t, answer.BaseTrigger)
	answer, err = sched.Query(ctx, drop)
	require.NoError(t, err)
	require.True(t, answer.NotExists)
}

func TestPromotionPreflightRejectsQueuedOrPreviouslyRunScheduler(t *testing.T) {
	table := promotionTable(1004)
	sched := NewMergeScheduler(time.Hour,
		&dummyCatalogSource{initTables: []catalog.MergeTable{table}},
		&dummyExecutor{}, NewStdClock())
	require.NoError(t, sched.SendConfig(table.ID(), DefaultMergeSettings, types.TS{}))
	require.ErrorContains(t, sched.CheckPromotionReady(), "queued messages")
	require.Len(t, sched.msgChan, 1)

	sched = NewMergeScheduler(time.Hour,
		&dummyCatalogSource{initTables: []catalog.MergeTable{table}},
		&dummyExecutor{}, NewStdClock())
	sched.Start()
	sched.Stop()
	require.ErrorContains(t, sched.CheckPromotionReady(), "already run")
}

func TestResumePromotionCancellationAfterCommitPoint(t *testing.T) {
	sched := NewMergeScheduler(time.Hour,
		&dummyCatalogSource{}, &dummyExecutor{}, NewStdClock())
	// Drive the private queue by hand so cancellation lands after the resume
	// message is sent and before the query barrier is answered.
	sched.stopped.Store(false)
	generation := newMergeSchedulerGeneration()
	sched.generation.Store(generation)
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		close(generation.stopCh)
		<-done
	})
	go func() {
		defer close(done)
		result <- sched.ResumePromotion(ctx)
	}()
	select {
	case msg := <-sched.msgChan:
		require.Equal(t, MMsgKindSwitch, msg.Kind)
		require.True(t, msg.Value.(MMsgSwitch).On)
	case <-time.After(time.Second):
		t.Fatal("resume message was not sent")
	}
	cancel()
	select {
	case msg := <-sched.msgChan:
		require.Equal(t, MMsgKindQuery, msg.Kind)
		msg.Value.(MMsgQuery).Answer <- &QueryAnswer{}
	case <-time.After(time.Second):
		t.Fatal("resume query barrier was not sent")
	}
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("resume did not complete after the barrier")
	}
}
