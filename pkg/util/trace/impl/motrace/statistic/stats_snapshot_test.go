// Copyright 2026 Matrix Origin
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

package statistic

import (
	"encoding/json"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStatsInfoSnapshot(t *testing.T) {
	var absent *StatsInfo
	require.Equal(t, StatsInfo{}, absent.Snapshot())
	require.Equal(t, StatsInfo{}, new(StatsInfo).Snapshot())

	stats := NewStatsInfo()
	// Seed every exported leaf independently of Snapshot so newly added fields
	// cannot silently disappear from diagnostic exports.
	var seed func(reflect.Value)
	next := int64(1)
	seed = func(value reflect.Value) {
		if value.Type() == reflect.TypeOf(time.Time{}) {
			value.Set(reflect.ValueOf(time.Unix(next, next).UTC()))
			next++
			return
		}
		switch value.Kind() {
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				if value.Field(i).CanSet() {
					seed(value.Field(i))
				}
			}
		case reflect.Array:
			for i := 0; i < value.Len(); i++ {
				seed(value.Index(i))
			}
		case reflect.Int64:
			value.SetInt(next)
			next++
		case reflect.Float64:
			value.SetFloat(float64(next))
			next++
		default:
			t.Fatalf("unhandled StatsInfo field type %s", value.Type())
		}
	}
	seed(reflect.ValueOf(stats).Elem())
	stats.resourceClaimed = 1
	want := *stats // All writers are quiescent here.
	snapshot := stats.Snapshot()
	require.Equal(t, want, snapshot)
	wantJSON, err := json.Marshal(want)
	require.NoError(t, err)
	gotJSON, err := json.Marshal(snapshot)
	require.NoError(t, err)
	require.JSONEq(t, string(wantJSON), string(gotJSON))
	stats.Reset()
	require.Equal(t, want, snapshot)
}

func TestStatsInfoSnapshotConcurrentCounters(t *testing.T) {
	stats := NewStatsInfo()
	start := make(chan struct{})
	var worker sync.WaitGroup
	worker.Add(1)
	go func() {
		defer worker.Done()
		<-start
		atomic.CompareAndSwapUint32(&stats.resourceClaimed, 0, 1)
		for i := 0; i < 100; i++ {
			stats.ResetIOMergerTimeConsumption()
			stats.ResetIOAccessTimeConsumption()
			stats.ResetBuildReaderTimeConsumption()
			stats.ResetCompilePreRunOnceDuration()
			stats.ResetCompilePreRunOnceWaitLock()
			stats.ResetScopePrepareDuration()
			stats.AddOutputTimeConsumption(1)
			stats.AddBuildReaderTimeConsumption(2)
			stats.AddIOAccessTimeConsumption(3)
			stats.AddLocalFSReadIOMergerTimeConsumption(4)
			stats.AddS3FSPrefetchFileIOMergerTimeConsumption(5)
			stats.AddS3FSReadIOMergerTimeConsumption(6)
			stats.AddBuildPlanStatsConsumption(7)
			stats.AddBuildPlanStatsIOConsumption(8)
			stats.AddStatsStatsInCacheDuration(9)
			stats.AddBuildPlanResolveVarConsumption(10)
			stats.AddCompileTableScanConsumption(11)
			request := S3Request{List: 1, Head: 2, Put: 3, Get: 4, Delete: 5, DeleteMul: 6}
			stats.AddBuildPlanS3Request(request)
			stats.AddBuildPlanStatsS3Request(request)
			stats.AddCompileS3Request(request)
			stats.AddCompileIOConsumption(12)
			stats.CompileExpandRangesS3Request(request)
			stats.AddScopePrepareS3Request(request)
			stats.AddTxnIncrStatementS3Request(request)
			stats.AddScopePrepareDuration(13)
			stats.StoreCompilePreRunOnceDuration(14)
			stats.AddPreRunOnceWaitLockDuration(15)
		}
	}()
	defer worker.Wait()
	close(start)
	for i := 0; i < 100; i++ {
		snapshot := stats.Snapshot()
		_, err := json.Marshal(snapshot)
		require.NoError(t, err)
	}
	worker.Wait()
	require.Equal(t, *stats, stats.Snapshot())
	require.Equal(t, int64(400), stats.Snapshot().PlanStage.BuildPlanStatsS3.Get)
	require.Equal(t, int64(100), stats.Snapshot().ExecuteStage.OutputDuration)
	require.Equal(t, uint32(1), stats.Snapshot().resourceClaimed)
}
