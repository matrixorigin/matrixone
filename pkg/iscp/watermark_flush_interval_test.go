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

package iscp

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func jobEntryForFlush(t *testing.T, consumerType ConsumerType, opt *ISCPExecutorOption) *JobEntry {
	t.Helper()
	table := &TableEntry{exec: &ISCPTaskExecutor{option: opt}}
	spec := &JobSpec{ConsumerInfo: ConsumerInfo{ConsumerType: int8(consumerType)}}
	return NewJobEntry(table, "index_x", spec, 1, types.BuildTS(1, 0), ISCPJobState_Completed, 0)
}

// An index job's watermark is READ by the optimizer, so it flushes on its own
// short threshold; every other consumer keeps the general one, which exists to
// bound restart work and is deliberately coarse.
func TestFlushThresholdIsPerConsumerClass(t *testing.T) {
	const general = time.Hour
	opt := &ISCPExecutorOption{IndexFlushWatermarkInterval: 5 * time.Second}

	idx := jobEntryForFlush(t, ConsumerType_IndexSync, opt)
	require.True(t, idx.isIndexJob)
	require.Equal(t, 5*time.Second, idx.flushThreshold(general))

	cn := jobEntryForFlush(t, ConsumerType_CNConsumer, opt)
	require.False(t, cn.isIndexJob)
	require.Equal(t, general, cn.flushThreshold(general))
}

// Missing configuration must never SHORTEN or lengthen the flush by accident:
// an unset interval and an unreachable executor both fall back to the general
// threshold rather than to zero (which would flush on every tick).
func TestFlushThresholdFallsBackToGeneral(t *testing.T) {
	const general = time.Hour

	unset := jobEntryForFlush(t, ConsumerType_IndexSync, &ISCPExecutorOption{})
	require.Equal(t, general, unset.flushThreshold(general))

	orphan := jobEntryForFlush(t, ConsumerType_IndexSync, nil)
	orphan.tableInfo.exec = nil
	require.Equal(t, general, orphan.flushThreshold(general))

	orphan2 := jobEntryForFlush(t, ConsumerType_IndexSync, nil)
	orphan2.tableInfo = nil
	require.Equal(t, general, orphan2.flushThreshold(general))

	// exec present but never configured
	noOpt := jobEntryForFlush(t, ConsumerType_IndexSync, nil)
	require.Equal(t, general, noOpt.flushThreshold(general))
}

// The executor defaults must leave the index interval short and well below the
// general one; if they ever converge, index watermarks silently go stale again.
func TestIndexFlushIntervalDefault(t *testing.T) {
	opt := ISCPExecutorOption{}
	filled := fillDefaultOption(&opt)
	require.Equal(t, DefaultIndexFlushWatermarkInterval, filled.IndexFlushWatermarkInterval)
	require.Less(t, filled.IndexFlushWatermarkInterval, filled.FlushWatermarkInterval)
	require.Less(t, filled.IndexFlushWatermarkInterval, filled.FlushWatermarkTTL)

	// an explicit setting is honored
	custom := ISCPExecutorOption{IndexFlushWatermarkInterval: time.Second}
	require.Equal(t, time.Second, fillDefaultOption(&custom).IndexFlushWatermarkInterval)
}
