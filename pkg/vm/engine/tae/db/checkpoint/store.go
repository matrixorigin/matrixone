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

package checkpoint

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/tidwall/btree"
)

type runnerStore struct {
	sync.RWMutex

	options struct {
		globalHistoryDuration time.Duration
	}

	incrementals *btree.BTreeG[*CheckpointEntry]
	globals      *btree.BTreeG[*CheckpointEntry]
	compacted    atomic.Pointer[CheckpointEntry]
	metaFiles    map[string]struct{}

	gcIntent    types.TS
	gcCount     int
	gcTime      time.Time
	gcWatermark atomic.Value
}

func (s *runnerStore) IsStale(ts *types.TS) bool {
	waterMark := s.gcWatermark.Load()
	if waterMark == nil {
		return false
	}
	wm := waterMark.(types.TS)
	minPhysical := wm.Physical() - s.options.globalHistoryDuration.Nanoseconds()
	return ts.Physical() < minPhysical
}

func (s *runnerStore) TryGC() (gdeleted, ideleted int) {
	s.Lock()
	defer s.Unlock()
	// if there's no intent, no need to GC
	if s.gcIntent.IsEmpty() {
		return
	}
	intent := s.gcIntent

	minTS := s.minTSLocked()
	// no need to GC if the minTS is larger than the gcIntent
	//          gcIntent	      minTS
	// ----------+-----------------+--------------------------->
	if minTS.GE(&intent) {
		return
	}

	safeTS := s.getSafeGCTSLocked()
	if intent.GT(&safeTS) {
		intent = safeTS
	}
	return s.doGC(&intent)
}

func (s *runnerStore) GCNeeded() bool {
	s.RLock()
	defer s.RUnlock()
	// no gc intent, no need to GC
	if s.gcIntent.IsEmpty() {
		return false
	}
	intent := s.gcIntent
	safeTS := s.getSafeGCTSLocked()
	// if the safeTS is less than the intent, use the safeTS as the intent
	if safeTS.LT(&intent) {
		intent = safeTS
	}
	minTS := s.minTSLocked()
	return intent.LT(&minTS)
}

// -----------------------------------------------------------------------
// the following are internal apis
// -----------------------------------------------------------------------

// minTSLocked returns the minimum timestamp that is not garbage collected
func (s *runnerStore) minTSLocked() types.TS {
	minGlobal, _ := s.globals.Min()
	minIncremental, _ := s.incrementals.Min()

	// no global checkpoint yet. no gc executed.
	if minGlobal == nil {
		return types.TS{}
	}
	if minIncremental == nil || minIncremental.AllGE(minGlobal) {
		return minGlobal.GetEnd()
	}
	return minIncremental.GetStart()
}

// here we only consider the global checkpoints as the safe GC timestamp
func (s *runnerStore) getSafeGCTSLocked() (ts types.TS) {
	if s.globals.Len() <= 1 {
		return
	}
	maxGlobal, _ := s.globals.Max()
	// if there's no global checkpoint, no need to GC
	if maxGlobal == nil {
		return
	}
	// if the max global checkpoint is finished, we can GC checkpoints before it
	if maxGlobal.IsFinished() {
		ts = maxGlobal.GetEnd()
		ts = ts.Prev()
		return
	}
	// only one non-finished global checkpoint, no need to GC
	if s.globals.Len() == 1 {
		return
	}
	items := s.globals.Items()
	maxGlobal = items[len(items)-1]
	ts = maxGlobal.GetEnd()
	ts = ts.Prev()
	return
}

func (s *runnerStore) doGC(ts *types.TS) (gdeleted, ideleted int) {
	if ts.IsEmpty() {
		return
	}
	gloabls := s.globals.Items()
	for _, e := range gloabls {
		if e.LessEq(ts) {
			s.globals.Delete(e)
			gdeleted++
		}
	}
	incrementals := s.incrementals.Items()
	for _, e := range incrementals {
		if e.LessEq(ts) {
			s.incrementals.Delete(e)
			ideleted++
		}
	}
	s.gcCount++
	s.gcTime = time.Now()
	s.gcWatermark.Store(*ts)
	return
}
