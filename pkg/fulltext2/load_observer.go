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

package fulltext2

import (
	"encoding/json"
	"os"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

// LoadMissReason identifies the event which caused a cached FULLTEXT2 load.
// It is intentionally a small, bounded vocabulary: these values are suitable
// for diagnostics and must not contain query or primary-key data.
type LoadMissReason string

const (
	LoadMissProcessStart     LoadMissReason = "process_start"
	LoadMissTTLExpired       LoadMissReason = "ttl_expired"
	LoadMissCDCFlush         LoadMissReason = "cdc_flush"
	LoadMissMerge            LoadMissReason = "merge"
	LoadMissRebuild          LoadMissReason = "rebuild"
	LoadMissGenerationChange LoadMissReason = "generation_changed"
)

// LoadEvent is emitted once for one Fulltext2Search.Load attempt when the
// internal observer is installed or MO_FULLTEXT2_LOAD_TRACE=1 is set. The
// default process has no observer and does not sample any phase timestamps.
type LoadEvent struct {
	Index                 string         `json:"index"`
	MissReason            LoadMissReason `json:"miss_reason"`
	BaseGeneration        int64          `json:"base_generation"`
	TailGeneration        int64          `json:"tail_generation"`
	BaseBytes             int64          `json:"base_bytes"`
	TailBytes             int64          `json:"tail_bytes"`
	InternalSQLTimeMicros int64          `json:"internal_sql_time_us"`
	TempFileWriteMicros   int64          `json:"temp_file_write_time_us"`
	MmapMicros            int64          `json:"mmap_time_us"`
	ChecksumMicros        int64          `json:"checksum_time_us"`
	TotalLoadMicros       int64          `json:"total_load_time_us"`
	SingleflightWaiters   int64          `json:"singleflight_waiters"`
	LoadSuccess           bool           `json:"load_success"`
	LoadError             bool           `json:"load_error"`
	LoadCancel            bool           `json:"load_cancel"`
}

type loadObserverHolder struct {
	observe func(LoadEvent)
}

var loadObserver atomic.Pointer[loadObserverHolder]

// loadTraceEnabled is read once at process startup. The environment switch is
// diagnostic-only; it is not a SQL/session setting and remains off by default.
var loadTraceEnabled = os.Getenv("MO_FULLTEXT2_LOAD_TRACE") == "1"

// setLoadObserver is intentionally package-private. Tests and an in-process
// diagnostic harness can install a bounded observer without adding a public
// FULLTEXT2 configuration surface.
func setLoadObserver(observe func(LoadEvent)) func() {
	if observe == nil {
		loadObserver.Store(nil)
		return func() {}
	}
	loadObserver.Store(&loadObserverHolder{observe: observe})
	return func() { loadObserver.Store(nil) }
}

func loadObservationEnabled() bool {
	return loadObserver.Load() != nil || loadTraceEnabled
}

func emitLoadEvent(event LoadEvent) {
	if h := loadObserver.Load(); h != nil && h.observe != nil {
		h.observe(event)
	}
	if !loadTraceEnabled {
		return
	}
	b, err := json.Marshal(event)
	if err == nil {
		logutil.Debugf("[ft2-load] %s", b)
	}
}

type loadTrace struct {
	event LoadEvent
	start time.Time
	phase phaseTimes
	ended atomic.Bool
}

type phaseTimes struct {
	internalSQL time.Duration
	tempWrite   time.Duration
	mmap        time.Duration
	checksum    time.Duration
}

func newLoadTrace(index string, reason LoadMissReason) *loadTrace {
	if !loadObservationEnabled() {
		return nil
	}
	return &loadTrace{event: LoadEvent{Index: index, MissReason: reason}, start: time.Now()}
}

func (t *loadTrace) addInternalSQL(d time.Duration) {
	if t != nil {
		t.phase.internalSQL += d
	}
}

func (t *loadTrace) addTempWrite(d time.Duration) {
	if t != nil {
		t.phase.tempWrite += d
	}
}

func (t *loadTrace) addMmap(d time.Duration) {
	if t != nil {
		t.phase.mmap += d
	}
}

func (t *loadTrace) addChecksum(d time.Duration) {
	if t != nil {
		t.phase.checksum += d
	}
}

func (t *loadTrace) addBaseBytes(n int64) {
	if t != nil {
		t.event.BaseBytes += n
	}
}

func (t *loadTrace) addTailBytes(n int64) {
	if t != nil {
		t.event.TailBytes += n
	}
}

func (t *loadTrace) setGeneration(base, tail int64) {
	if t != nil {
		t.event.BaseGeneration = base
		t.event.TailGeneration = tail
	}
}

func (t *loadTrace) finish(err error, canceled bool, waiters int64) {
	if t == nil || !t.ended.CompareAndSwap(false, true) {
		return
	}
	t.event.InternalSQLTimeMicros = t.phase.internalSQL.Microseconds()
	t.event.TempFileWriteMicros = t.phase.tempWrite.Microseconds()
	t.event.MmapMicros = t.phase.mmap.Microseconds()
	t.event.ChecksumMicros = t.phase.checksum.Microseconds()
	t.event.TotalLoadMicros = time.Since(t.start).Microseconds()
	t.event.SingleflightWaiters = waiters
	t.event.LoadError = err != nil && !canceled
	t.event.LoadCancel = canceled
	t.event.LoadSuccess = err == nil && !canceled
	emitLoadEvent(t.event)
}
