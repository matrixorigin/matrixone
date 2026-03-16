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

package publication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetJobStats(t *testing.T) {
	stats := GetJobStats()
	require.NotNil(t, stats)
	// Should return the same global instance
	assert.Equal(t, stats, GetJobStats())
}

func TestJobStats_FilterObjectCounters(t *testing.T) {
	s := &JobStats{}
	s.IncrementFilterObjectPending()
	assert.Equal(t, int64(1), s.FilterObjectPending.Load())

	s.IncrementFilterObjectRunning()
	assert.Equal(t, int64(1), s.FilterObjectRunning.Load())

	s.DecrementFilterObjectRunning()
	assert.Equal(t, int64(0), s.FilterObjectRunning.Load())
}

func TestJobStats_GetChunkCounters(t *testing.T) {
	s := &JobStats{}
	s.IncrementGetChunkPending()
	assert.Equal(t, int64(1), s.GetChunkPending.Load())

	s.IncrementGetChunkRunning()
	assert.Equal(t, int64(1), s.GetChunkRunning.Load())

	s.DecrementGetChunkRunning()
	assert.Equal(t, int64(0), s.GetChunkRunning.Load())
}

func TestJobStats_GetMetaCounters(t *testing.T) {
	s := &JobStats{}
	s.IncrementGetMetaPending()
	assert.Equal(t, int64(1), s.GetMetaPending.Load())

	s.IncrementGetMetaRunning()
	assert.Equal(t, int64(1), s.GetMetaRunning.Load())

	s.DecrementGetMetaRunning()
	assert.Equal(t, int64(0), s.GetMetaRunning.Load())
}

func TestJobStats_WriteObjectCounters(t *testing.T) {
	s := &JobStats{}
	s.IncrementWriteObjectPending()
	assert.Equal(t, int64(1), s.WriteObjectPending.Load())

	s.IncrementWriteObjectRunning()
	assert.Equal(t, int64(1), s.WriteObjectRunning.Load())

	s.DecrementWriteObjectRunning()
	assert.Equal(t, int64(0), s.WriteObjectRunning.Load())
}

func TestJobStats_RecordGetChunkDuration(t *testing.T) {
	s := &JobStats{}

	s.RecordGetChunkDuration("obj1", 0, 100*time.Millisecond)
	s.RecordGetChunkDuration("obj2", 1, 300*time.Millisecond)
	s.RecordGetChunkDuration("obj3", 2, 200*time.Millisecond)
	s.RecordGetChunkDuration("obj4", 3, 50*time.Millisecond)

	top := s.GetTopGetChunkDurations()
	require.Len(t, top, 3)
	// Should be sorted descending
	assert.Equal(t, 300*time.Millisecond, top[0].Duration)
	assert.Equal(t, "obj2", top[0].ObjectName)
	assert.Equal(t, 200*time.Millisecond, top[1].Duration)
	assert.Equal(t, 100*time.Millisecond, top[2].Duration)
}

func TestJobStats_RecordWriteObjectDuration(t *testing.T) {
	s := &JobStats{}

	s.RecordWriteObjectDuration("obj1", 1024, 100*time.Millisecond)
	s.RecordWriteObjectDuration("obj2", 2048, 300*time.Millisecond)
	s.RecordWriteObjectDuration("obj3", 512, 200*time.Millisecond)
	s.RecordWriteObjectDuration("obj4", 4096, 50*time.Millisecond)

	top := s.GetTopWriteObjectDurations()
	require.Len(t, top, 3)
	assert.Equal(t, 300*time.Millisecond, top[0].Duration)
	assert.Equal(t, "obj2", top[0].ObjectName)
	assert.Equal(t, int64(2048), top[0].Size)
}

func TestJobStats_ResetTopGetChunkDurations(t *testing.T) {
	s := &JobStats{}
	s.RecordGetChunkDuration("obj1", 0, 100*time.Millisecond)
	require.Len(t, s.GetTopGetChunkDurations(), 1)

	s.ResetTopGetChunkDurations()
	assert.Empty(t, s.GetTopGetChunkDurations())
}

func TestJobStats_ResetTopWriteObjectDurations(t *testing.T) {
	s := &JobStats{}
	s.RecordWriteObjectDuration("obj1", 1024, 100*time.Millisecond)
	require.Len(t, s.GetTopWriteObjectDurations(), 1)

	s.ResetTopWriteObjectDurations()
	assert.Empty(t, s.GetTopWriteObjectDurations())
}

func TestJobStats_GetTopGetChunkDurations_Empty(t *testing.T) {
	s := &JobStats{}
	assert.Empty(t, s.GetTopGetChunkDurations())
}

func TestJobStats_GetTopWriteObjectDurations_Empty(t *testing.T) {
	s := &JobStats{}
	assert.Empty(t, s.GetTopWriteObjectDurations())
}
