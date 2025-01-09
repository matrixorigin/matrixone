// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoint

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func TestCkpCheck(t *testing.T) {
	ioutil.RunPipelineTest(
		func() {
			defer testutils.AfterTest(t)()
			r := NewRunner(context.Background(), nil, nil, nil, nil, nil)

			for i := 0; i < 100; i += 10 {
				r.store.incrementals.Set(&CheckpointEntry{
					start:      types.BuildTS(int64(i), 0),
					end:        types.BuildTS(int64(i+9), 0),
					state:      ST_Finished,
					cnLocation: objectio.Location(fmt.Sprintf("loc-%d", i)),
					version:    1,
					entryType:  ET_Incremental,
				})
			}

			r.store.incrementals.Set(&CheckpointEntry{
				start:      types.BuildTS(int64(100), 0),
				end:        types.BuildTS(int64(109), 0),
				state:      ST_Running,
				cnLocation: objectio.Location("loc-100"),
				version:    1,
				entryType:  ET_Incremental,
			})

			ctx := context.Background()

			loc, e, err := r.CollectCheckpointsInRange(ctx, types.BuildTS(4, 0), types.BuildTS(5, 0))
			assert.NoError(t, err)
			assert.True(t, e.Equal(types.BuildTSForTest(9, 0)))
			assert.Equal(t, "loc-0;1;[0-0_9-0]", loc)

			loc, e, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(12, 0), types.BuildTS(25, 0))
			assert.NoError(t, err)
			assert.True(t, e.Equal(types.BuildTSForTest(29, 0)))
			assert.Equal(t, "loc-10;1;loc-20;1;[10-0_29-0]", loc)
		},
	)
}

func TestGetCheckpoints1(t *testing.T) {
	defer testutils.AfterTest(t)()
	r := NewRunner(context.Background(), nil, nil, nil, nil, nil)

	// ckp0[0,10]
	// ckp1[10,20]
	// ckp2[20,30]
	// ckp3[30,40]
	// ckp4[40,50(unfinished)]
	timestamps := make([]types.TS, 0)
	for i := 0; i < 6; i++ {
		ts := types.BuildTS(int64(i*10), 0)
		timestamps = append(timestamps, ts)
	}
	for i := 0; i < 5; i++ {
		entry := &CheckpointEntry{
			start:      timestamps[i].Next(),
			end:        timestamps[i+1],
			state:      ST_Finished,
			cnLocation: objectio.Location(fmt.Sprintf("ckp%d", i)),
			version:    1,
			entryType:  ET_Incremental,
		}
		if i == 4 {
			entry.state = ST_Pending
		}
		r.store.incrementals.Set(entry)
	}

	ctx := context.Background()
	// [0,10]
	location, checkpointed, err := r.CollectCheckpointsInRange(ctx, types.BuildTS(0, 1), types.BuildTS(10, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp0;1;[0-1_10-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(10, 0)))

	// [45,50]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(45, 0), types.BuildTS(50, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "", location)
	assert.True(t, checkpointed.IsEmpty())

	// [30,45]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(30, 1), types.BuildTS(45, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp3;1;[30-1_40-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(40, 0)))

	// [25,45]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(25, 1), types.BuildTS(45, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp2;1;ckp3;1;[20-1_40-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(40, 0)))

	// [22,25]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(25, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp2;1;[20-1_30-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(30, 0)))

	// [22,35]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(35, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp2;1;ckp3;1;[20-1_40-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(40, 0)))
}
func TestGetCheckpoints2(t *testing.T) {
	defer testutils.AfterTest(t)()
	r := NewRunner(context.Background(), nil, nil, nil, nil, nil)

	// ckp0[0,10]
	// ckp1[10,20]
	// ckp2[20,30]
	// global3[0,30]
	// ckp3[30,40]
	// ckp4[40,50(unfinished)]
	timestamps := make([]types.TS, 0)
	for i := 0; i < 6; i++ {
		ts := types.BuildTS(int64(i*10), 0)
		timestamps = append(timestamps, ts)
	}
	for i := 0; i < 5; i++ {
		addGlobal := false
		if i == 3 {
			addGlobal = true
		}
		if addGlobal {
			entry := &CheckpointEntry{
				start:      types.TS{},
				end:        timestamps[i].Next(),
				state:      ST_Finished,
				cnLocation: objectio.Location(fmt.Sprintf("global%d", i)),
				version:    100,
				entryType:  ET_Global,
			}
			r.store.globals.Set(entry)
		}
		start := timestamps[i].Next()
		if addGlobal {
			start = start.Next()
		}
		entry := &CheckpointEntry{
			start:      start,
			end:        timestamps[i+1],
			state:      ST_Finished,
			cnLocation: objectio.Location(fmt.Sprintf("ckp%d", i)),
			version:    uint32(i),
			entryType:  ET_Incremental,
		}
		if i == 4 {
			entry.state = ST_Pending
		}
		r.store.incrementals.Set(entry)
	}

	ctx := context.Background()
	// [0,10]
	location, checkpointed, err := r.CollectCheckpointsInRange(ctx, types.BuildTS(0, 1), types.BuildTS(10, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;100;[30-1_30-1]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(30, 1)))

	// [45,50]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(45, 0), types.BuildTS(50, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "", location)
	assert.True(t, checkpointed.IsEmpty())

	// [30,45]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(30, 2), types.BuildTS(45, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp3;3;[30-2_40-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(40, 0)))

	// [25,45]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(25, 1), types.BuildTS(45, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;100;ckp3;3;[30-1_40-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(40, 0)))

	// [22,25]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(25, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;100;[30-1_30-1]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(30, 1)))

	// [22,35]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(35, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;100;ckp3;3;[30-1_40-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(40, 0)))

	// [22,29]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(29, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;100;[30-1_30-1]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(30, 1)))
}
func TestICKPSeekLT(t *testing.T) {
	defer testutils.AfterTest(t)()
	r := NewRunner(context.Background(), nil, nil, nil, nil, nil)

	// ckp0[0,10]
	// ckp1[10,20]
	// ckp2[20,30]
	// ckp3[30,40]
	// ckp4[40,50(unfinished)]
	timestamps := make([]types.TS, 0)
	for i := 0; i < 6; i++ {
		ts := types.BuildTS(int64(i*10), 0)
		timestamps = append(timestamps, ts)
	}
	for i := 0; i < 5; i++ {
		entry := &CheckpointEntry{
			start:      timestamps[i].Next(),
			end:        timestamps[i+1],
			state:      ST_Finished,
			cnLocation: objectio.Location(fmt.Sprintf("ckp%d", i)),
			version:    uint32(i),
		}
		if i == 4 {
			entry.state = ST_Pending
		}
		r.store.incrementals.Set(entry)
	}

	// 0, 1
	ckps := r.ICKPSeekLT(types.BuildTS(0, 0), 1)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 1, len(ckps))
	assert.Equal(t, "ckp0", ckps[0].cnLocation.String())

	// 0, 0
	ckps = r.ICKPSeekLT(types.BuildTS(0, 0), 0)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 0, 2
	ckps = r.ICKPSeekLT(types.BuildTS(0, 0), 4)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 4, len(ckps))
	assert.Equal(t, "ckp0", ckps[0].cnLocation.String())
	assert.Equal(t, "ckp1", ckps[1].cnLocation.String())

	// 0, 4
	ckps = r.ICKPSeekLT(types.BuildTS(0, 0), 4)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 4, len(ckps))
	assert.Equal(t, "ckp0", ckps[0].cnLocation.String())
	assert.Equal(t, "ckp1", ckps[1].cnLocation.String())
	assert.Equal(t, "ckp2", ckps[2].cnLocation.String())
	assert.Equal(t, "ckp3", ckps[3].cnLocation.String())

	// 0,10
	ckps = r.ICKPSeekLT(types.BuildTS(0, 0), 10)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 4, len(ckps))
	assert.Equal(t, "ckp0", ckps[0].cnLocation.String())
	assert.Equal(t, "ckp1", ckps[1].cnLocation.String())
	assert.Equal(t, "ckp2", ckps[2].cnLocation.String())
	assert.Equal(t, "ckp3", ckps[3].cnLocation.String())

	// 5,1
	ckps = r.ICKPSeekLT(types.BuildTS(5, 0), 1)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 1, len(ckps))
	assert.Equal(t, "ckp1", ckps[0].cnLocation.String())

	// 50,1
	ckps = r.ICKPSeekLT(types.BuildTS(50, 0), 1)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 55,3
	ckps = r.ICKPSeekLT(types.BuildTS(55, 0), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 40,3
	ckps = r.ICKPSeekLT(types.BuildTS(40, 0), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 35,3
	ckps = r.ICKPSeekLT(types.BuildTS(35, 0), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 30,3
	ckps = r.ICKPSeekLT(types.BuildTS(30, 0), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 1, len(ckps))
	assert.Equal(t, "ckp3", ckps[0].cnLocation.String())

	// 30-2,3
	ckps = r.ICKPSeekLT(types.BuildTS(30, 2), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))
}

func Test_RunnerStore1(t *testing.T) {
	store := newRunnerStore("", time.Second, time.Second*1000)
	_ = types.NextGlobalTsForTest()
	_ = types.NextGlobalTsForTest()
	t3 := types.NextGlobalTsForTest()
	intent, updated := store.UpdateICKPIntent(&t3, true, true)
	assert.True(t, updated)
	assert.True(t, intent.IsPolicyChecked())
	assert.True(t, intent.IsFlushChecked())
	assert.True(t, intent.IsPendding())
	assert.True(t, intent.AllChecked())
	assert.True(t, intent.end.EQ(&t3))

	taken, rollback := store.TakeICKPIntent()
	assert.NotNil(t, taken)
	assert.NotNil(t, rollback)
	assert.True(t, taken.IsRunning())
	assert.True(t, taken.end.EQ(&t3))

	prepared := store.PrepareCommitICKPIntent(taken)
	assert.True(t, prepared)

	store.CommitICKPIntent(taken)

	intent, updated = store.UpdateICKPIntent(&t3, true, true)
	assert.False(t, updated)
	assert.Nilf(t, intent, intent.String())
}

func Test_RunnerStore2(t *testing.T) {
	store := newRunnerStore("", time.Second, time.Second*1000)
	t1 := types.NextGlobalTsForTest()
	intent, updated := store.UpdateICKPIntent(&t1, false, false)
	assert.True(t, updated)
	assert.True(t, intent.start.IsEmpty())
	assert.True(t, intent.end.EQ(&t1))
	assert.True(t, intent.IsPendding())
	assert.False(t, intent.AllChecked())

	intent, updated = store.UpdateICKPIntent(&t1, true, false)
	assert.True(t, updated)
	assert.True(t, intent.IsPolicyChecked())
	assert.False(t, intent.IsFlushChecked())
	assert.True(t, intent.IsPendding())
	assert.False(t, intent.AllChecked())

	intent, updated = store.UpdateICKPIntent(&t1, false, true)
	assert.False(t, updated)
	assert.True(t, intent.IsPolicyChecked())
	assert.False(t, intent.IsFlushChecked())
	assert.True(t, intent.IsPendding())
	assert.False(t, intent.AllChecked())
	bornTime := intent.bornTime

	intent, updated = store.UpdateICKPIntent(&t1, true, true)
	assert.True(t, updated)
	assert.True(t, intent.IsPolicyChecked())
	assert.True(t, intent.IsFlushChecked())
	assert.True(t, intent.IsPendding())
	assert.True(t, intent.AllChecked())
	assert.True(t, intent.end.EQ(&t1))
	assert.True(t, bornTime.Equal(intent.bornTime))

	t2 := types.NextGlobalTsForTest()
	intent2, updated := store.UpdateICKPIntent(&t2, true, false)
	assert.False(t, updated)
	assert.Equal(t, intent, intent2)
	intent2, updated = store.UpdateICKPIntent(&t2, false, true)
	assert.False(t, updated)
	assert.Equal(t, intent, intent2)

	intent2, updated = store.UpdateICKPIntent(&t2, true, true)
	assert.True(t, updated)
	assert.True(t, intent2.IsPolicyChecked())
	assert.True(t, intent2.IsFlushChecked())
	assert.True(t, intent2.IsPendding())
	assert.True(t, intent2.AllChecked())
	assert.True(t, intent2.end.EQ(&t2))
}

func Test_RunnerStore3(t *testing.T) {
	store := newRunnerStore("", time.Second, time.Second*1000)

	t1 := types.NextGlobalTsForTest()
	intent, updated := store.UpdateICKPIntent(&t1, false, false)
	assert.True(t, updated)
	assert.True(t, intent.start.IsEmpty())
	assert.True(t, intent.end.EQ(&t1))
	assert.True(t, intent.IsPendding())

	intent2 := store.incrementalIntent.Load()
	assert.Equal(t, intent, intent2)

	t2 := types.NextGlobalTsForTest()
	intent3, updated := store.UpdateICKPIntent(&t2, true, true)
	assert.True(t, updated)
	assert.True(t, intent3.start.IsEmpty())
	assert.True(t, intent3.end.EQ(&t2))
	assert.True(t, intent3.IsPendding())
	intent4 := store.incrementalIntent.Load()
	assert.Equal(t, intent3, intent4)

	ii, updated := store.UpdateICKPIntent(&t1, true, true)
	assert.False(t, updated)
	assert.Equal(t, ii, intent4)

	taken, rollback := store.TakeICKPIntent()
	assert.NotNil(t, taken)
	assert.NotNil(t, rollback)
	assert.True(t, taken.IsRunning())
	intent5 := store.incrementalIntent.Load()
	assert.Equal(t, intent5, taken)

	taken2, rollback2 := store.TakeICKPIntent()
	assert.Nil(t, taken2)
	assert.Nil(t, rollback2)

	t3 := types.NextGlobalTsForTest()
	ii2, updated := store.UpdateICKPIntent(&t3, true, true)
	assert.False(t, updated)
	assert.Equal(t, ii2, intent5)

	rollback()
	intent6 := store.incrementalIntent.Load()
	assert.Nil(t, intent6)
	// assert.True(t, intent6.IsPendding())
	// assert.True(t, intent6.end.EQ(&t2))
	// assert.True(t, intent6.start.IsEmpty())
	// assert.Equal(t, intent6.bornTime, intent5.bornTime)
	// assert.Equal(t, intent6.refreshCnt, intent5.refreshCnt)
	// assert.Equal(t, intent6.policyChecked, intent5.policyChecked)
	// assert.Equal(t, intent6.flushChecked, intent5.flushChecked)

	ii2, updated = store.UpdateICKPIntent(&t3, true, true)
	assert.True(t, updated)
	assert.True(t, ii2.IsPendding())
	assert.True(t, ii2.end.EQ(&t3))
	assert.True(t, ii2.start.IsEmpty())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ii2.Wait()
	}()

	taken, rollback = store.TakeICKPIntent()
	assert.NotNil(t, taken)
	assert.NotNil(t, rollback)
	assert.True(t, taken.IsRunning())
	intent7 := store.incrementalIntent.Load()
	assert.Equal(t, intent7, taken)

	maxEntry := store.MaxIncrementalCheckpoint()
	assert.Nil(t, maxEntry)

	prepared := store.PrepareCommitICKPIntent(taken)
	assert.True(t, prepared)
	store.CommitICKPIntent(taken)
	assert.True(t, taken.IsFinished())

	wg.Wait()

	maxEntry = store.MaxIncrementalCheckpoint()
	assert.Equal(t, maxEntry, taken)

	intent8 := store.incrementalIntent.Load()
	assert.Nil(t, intent8)

	// UpdateICKPIntent with a smaller ts than the finished one
	intent9, updated := store.UpdateICKPIntent(&t3, true, true)
	assert.False(t, updated)
	assert.Nil(t, intent9)

	t4 := types.NextGlobalTsForTest()
	t4 = t4.Next()

	// UpdateICKPIntent with a larger ts than the finished one
	// check if the intent is updated
	// check if the start ts is equal to the last end ts
	intent10, updated := store.UpdateICKPIntent(&t4, true, true)
	assert.True(t, updated)
	assert.True(t, intent10.IsPendding())
	assert.True(t, intent10.end.EQ(&t4))
	prev := intent10.start.Prev()
	assert.True(t, prev.EQ(&t3))

	taken2, rollback2 = store.TakeICKPIntent()
	assert.NotNil(t, taken2)
	assert.NotNil(t, rollback2)
	assert.True(t, taken2.IsRunning())
	intent11 := store.incrementalIntent.Load()
	assert.Equal(t, intent11, taken2)
	t.Logf("taken2: %s", taken2.String())
	entries := store.incrementals.Items()
	for i, entry := range entries {
		t.Logf("entry[%d]: %s", i, entry.String())
	}

	// cannot commit a different intent with the incremental intent
	t5 := types.NextGlobalTsForTest()
	taken2_1 := InheritCheckpointEntry(taken2, WithEndEntryOption(t5))
	prepared = store.PrepareCommitICKPIntent(taken2_1)
	assert.False(t, prepared)

	prepared = store.PrepareCommitICKPIntent(taken2)
	assert.True(t, prepared)

	store.CommitICKPIntent(taken2)
	assert.True(t, taken2.IsFinished())
	entries = store.incrementals.Items()
	assert.Equal(t, 2, len(entries))
	for _, entry := range entries {
		assert.True(t, entry.IsFinished())
	}
	assert.Equalf(
		t,
		taken2.end.Next(),
		store.GetCheckpointed(),
		"%s:%s",
		taken2.end.ToString(),
		store.GetCheckpointed().ToString(),
	)

	timer := time.After(time.Second * 10)
	select {
	case <-intent10.Wait():
	case <-timer:
		assert.Equal(t, 1, 0)
	}
}

func Test_RunnerStore4(t *testing.T) {
	store := newRunnerStore("", time.Second, time.Second*1000)

	t1 := types.NextGlobalTsForTest()
	intent, updated := store.UpdateICKPIntent(&t1, true, false)
	assert.True(t, updated)
	assert.True(t, intent.start.IsEmpty())
	assert.True(t, intent.end.EQ(&t1))
	assert.True(t, intent.IsPendding())

	t2 := types.NextGlobalTsForTest()
	intent2, updated := store.UpdateICKPIntent(&t2, true, true)
	assert.True(t, updated)
	assert.True(t, intent2.start.IsEmpty())
	assert.True(t, intent2.end.EQ(&t2))
	assert.True(t, intent2.IsPendding())
	assert.True(t, intent2.AllChecked())

	taken, rollback := store.TakeICKPIntent()
	assert.NotNil(t, taken)
	assert.NotNil(t, rollback)

	t3 := types.NextGlobalTsForTest()
	intent3, updated := store.UpdateICKPIntent(&t3, true, true)
	assert.False(t, updated)
	assert.True(t, intent3.IsRunning())
	assert.True(t, intent3.end.EQ(&t2))

	rollback()
	intent4 := store.incrementalIntent.Load()
	assert.Nil(t, intent4)
	<-taken.Wait()
}

func Test_RunnerStore5(t *testing.T) {
	store := newRunnerStore("", time.Second, time.Second*1000)

	t1 := types.NextGlobalTsForTest()
	t2 := types.NextGlobalTsForTest()
	intent, updated := store.UpdateICKPIntent(&t2, true, false)
	assert.True(t, updated)
	assert.True(t, intent.start.IsEmpty())
	assert.True(t, intent.end.EQ(&t2))
	t.Log(intent.String())

	intent2, updated := store.UpdateICKPIntent(&t1, true, true)
	assert.True(t, updated)
	assert.True(t, intent2.end.EQ(&t1))
	assert.True(t, intent2.start.IsEmpty())
	assert.True(t, intent2.IsPendding())
	assert.True(t, intent2.AllChecked())
	assert.True(t, intent2.bornTime.After(intent.bornTime))
	t.Log(intent2.String())

	// sid string, start, end types.TS, typ EntryType, opts ...EntryOption,
	intent3 := NewCheckpointEntry(
		"", types.TS{}, types.NextGlobalTsForTest(), ET_Global,
	)
	assert.False(t, store.AddGCKPIntent(intent2))
	assert.False(t, store.AddGCKPIntent(nil))
	intent3.SetState(ST_Finished)
	assert.False(t, store.AddGCKPIntent(intent3))
	intent3.state = ST_Pending
	assert.True(t, store.AddGCKPIntent(intent3))
	assert.False(t, store.AddGCKPIntent(intent3))
	intent3.state = ST_Finished
	assert.False(t, store.RemoveGCKPIntent())
	intent3.state = ST_Pending
	assert.True(t, store.RemoveGCKPIntent())

}
func Test_RunnerStore6(t *testing.T) {
	store := newRunnerStore("", time.Second, time.Second*1000)

	t1 := types.NextGlobalTsForTest()
	entry1 := NewCheckpointEntry("", types.TS{}, t1, ET_Global)
	entry1.SetState(ST_Running)

	assert.True(t, store.AddGCKPIntent(entry1))

	locations, checkpointed, err := store.CollectCheckpointsInRange(
		context.Background(), types.TS{}, types.NextGlobalTsForTest(),
	)
	assert.NoError(t, err)

	// obj1 := objectio.MockObjectName()
	// loc1 := objectio.BuildLocation(obj1, objectio.NewExtent(1, 1, 1, 1), 1, 1)
	t.Log(locations)
	assert.Equalf(t, checkpointed, types.TS{}, checkpointed.ToString())
	words := strings.Split(locations, ";")
	_, err = objectio.StringToLocation(words[0])
	t.Log(err)
}

func Test_RunnerStore7(t *testing.T) {
	store := newRunnerStore("", time.Second, time.Second*1000)

	t1 := types.NextGlobalTsForTest()
	entry1 := NewCheckpointEntry("", types.TS{}, t1, ET_Global)
	entry1.SetState(ST_Running)

	entry2 := NewCheckpointEntry("", types.TS{}, t1, ET_Incremental)
	entry2.SetState(ST_Finished)
	objName := objectio.BuildObjectNameWithObjectID(objectio.NewObjectid())
	entry2.SetLocation(objectio.MockLocation(objName), objectio.MockLocation(objName))

	assert.True(t, store.AddGCKPIntent(entry1))
	assert.True(t, store.AddICKPFinishedEntry(entry2))

	locations, checkpointed, err := store.CollectCheckpointsInRange(
		context.Background(), types.TS{}, types.NextGlobalTsForTest(),
	)
	assert.NoError(t, err)

	// obj1 := objectio.MockObjectName()
	// loc1 := objectio.BuildLocation(obj1, objectio.NewExtent(1, 1, 1, 1), 1, 1)
	t.Log(locations)
	assert.Equalf(t, checkpointed, t1, checkpointed.ToString())
	words := strings.Split(locations, ";")
	_, err = objectio.StringToLocation(words[0])
	assert.NoError(t, err)

}
func Test_Executor1(t *testing.T) {
	var (
		gctx1, gctx2 gckpContext
	)
	gctx1.force = true
	gctx1.end = types.NextGlobalTsForTest()
	gctx1.histroyRetention = time.Duration(2)
	gctx2.end = types.NextGlobalTsForTest()
	gctx2.histroyRetention = time.Duration(1)
	gctx2.ckpLSN = 100
	gctx2.truncateLSN = 10
	gctx1.Merge(&gctx2)
	assert.True(t, gctx1.force)
	assert.Equal(t, gctx2.end, gctx1.end)
	assert.Equal(t, gctx2.histroyRetention, gctx1.histroyRetention)
	assert.Equal(t, gctx2.ckpLSN, gctx1.ckpLSN)
	assert.Equal(t, gctx2.truncateLSN, gctx1.truncateLSN)

	executor := newCheckpointExecutor(nil, nil)
	assert.True(t, executor.active.Load())

	done := make(chan struct{})
	running := make(chan struct{})
	mockRunICKP := func(ctx context.Context, _ *runner) (err error) {
		close(running)
		<-ctx.Done()
		close(done)
		err = context.Cause(ctx)
		return
	}
	executor.runICKPFunc = mockRunICKP
	go func() {
		err := executor.RunICKP()
		assert.Equal(t, err, ErrPendingCheckpoint)
	}()
	// wait running
	<-running
	// stop executor
	executor.StopWithCause(ErrPendingCheckpoint)
	<-done

	assert.False(t, executor.active.Load())
	err := executor.RunICKP()
	assert.Equal(t, err, ErrCheckpointDisabled)
	executor.StopWithCause(nil)
}
