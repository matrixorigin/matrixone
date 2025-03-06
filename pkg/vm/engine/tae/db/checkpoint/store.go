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
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/tidwall/btree"
	"go.uber.org/zap"
)

const (
	CKPProtocolVersion_V1 uint8 = 1
	CKPProtocolVersion_V2 uint8 = 2

	CKPProtocolVersion_Curr = CKPProtocolVersion_V2
)

func newRunnerStore(
	sid string,
	globalHistoryDuration time.Duration,
	intentOldAge time.Duration,
) *runnerStore {
	s := new(runnerStore)
	s.sid = sid
	s.globalHistoryDuration = globalHistoryDuration
	s.intentOldAge = intentOldAge
	s.incrementals = btree.NewBTreeGOptions(
		func(a, b *CheckpointEntry) bool {
			return a.end.LT(&b.end)
		}, btree.Options{
			NoLocks: true,
		},
	)
	s.globals = btree.NewBTreeGOptions(
		func(a, b *CheckpointEntry) bool {
			return a.end.LT(&b.end)
		}, btree.Options{
			NoLocks: true,
		},
	)
	s.metaFiles = make(map[string]struct{})
	return s
}

type runnerStore struct {
	sync.RWMutex
	sid string

	globalHistoryDuration time.Duration
	intentOldAge          time.Duration

	incrementalIntent atomic.Pointer[CheckpointEntry]

	incrementals *btree.BTreeG[*CheckpointEntry]
	globals      *btree.BTreeG[*CheckpointEntry]
	compacted    atomic.Pointer[CheckpointEntry]
	metaFiles    map[string]struct{}

	gcIntent    types.TS
	gcCount     int
	gcTime      time.Time
	gcWatermark atomic.Value
}

func (s *runnerStore) GetICKPIntent() *CheckpointEntry {
	return s.incrementalIntent.Load()
}

func (s *runnerStore) GetCheckpointed() types.TS {
	s.RLock()
	defer s.RUnlock()
	return s.GetCheckpointedLocked()
}

func (s *runnerStore) MaxIncrementalCheckpoint() *CheckpointEntry {
	s.RLock()
	maxEntry, _ := s.incrementals.Max()
	s.RUnlock()
	if maxEntry == nil || maxEntry.IsFinished() {
		return maxEntry
	}
	entries := s.GetAllIncrementalCheckpoints()
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].IsFinished() {
			return entries[i]
		}
	}
	return nil
}

func (s *runnerStore) GetCheckpointedLocked() types.TS {
	var ret types.TS
	maxICKP, _ := s.incrementals.Max()
	maxGCKP, _ := s.globals.Max()
	if maxICKP == nil {
		// no ickp and no gckp, it's the first ickp
		if maxGCKP == nil {
			ret = types.TS{}
		} else {
			ret = maxGCKP.end
		}
	} else {
		if maxICKP.IsFinished() {
			ret = maxICKP.end.Next()
		} else {
			entries := s.incrementals.Items()
			for i := len(entries) - 1; i >= 0; i-- {
				if entries[i].IsFinished() {
					ret = entries[i].end.Next()
					break
				}
			}
		}
	}
	return ret
}

// updated:
// true:  updated and intent must contain the updated ts
// false: not updated and intent is the old intent
// policyChecked, flushChecked:
// it cannot update the intent if the intent is checked by policy or flush
func (s *runnerStore) UpdateICKPIntent(
	ts *types.TS, policyChecked, flushChecked bool,
) (intent *CheckpointEntry, updated bool) {
	for {
		old := s.incrementalIntent.Load()
		// in the case we will decrease the end ts of the old intent
		if old != nil && !old.AllChecked() && policyChecked && flushChecked {
			checkpointed := s.GetCheckpointed()
			// no need to do checkpoint
			if checkpointed.GT(&old.end) {
				s.incrementalIntent.CompareAndSwap(old, nil)
				continue
			}
			if checkpointed.GE(ts) {
				intent = nil
				return
			}
			newIntent := InheritCheckpointEntry(
				old,
				WithEndEntryOption(*ts),
				WithCheckedEntryOption(policyChecked, flushChecked),
			)
			if old.end.GT(ts) {
				newIntent.ResetAge()
			}
			if s.incrementalIntent.CompareAndSwap(old, newIntent) {
				intent = newIntent
				updated = true
				return
			}
			continue
		}
		// Scenario 1:
		// there is already an intent meets one of the following conditions:
		// 1. the range of the old intent contains the ts, no need to update
		// 2. the intent is not pendding: Running or Finished, cannot update
		if old != nil && (old.end.GT(ts) || !old.IsPendding() || old.Age() > s.intentOldAge) {
			intent = old
			return
		}

		// Here
		// 1. old == nil
		// 2. old.end <= ts && old.IsPendding() && old.Age() <= s.intentOldAge

		if old != nil {
			// if the old intent is checked by policy and the incoming intent is not checked by policy
			// incoming vs old: false vs true
			// it cannot update the intent in this case

			if !policyChecked && old.IsPolicyChecked() {
				intent = old
				return
			}
			if !flushChecked && old.IsFlushChecked() {
				intent = old
				return
			}
		}

		var start types.TS
		if old != nil {
			// Scenario 2:
			// there is an pendding intent with smaller end ts. we need to update
			// the intent to extend the end ts to the given ts
			start = old.start
		} else {
			// Scenario 3:
			// there is no intent, we need to create a new intent
			// start-ts:
			// 1. if there is no ickp and no gckp, it's the first ickp, start ts is empty
			// 2. if there is no ickp but has gckp, start ts is the end ts of the max gckp
			// 3. if there is ickp, start ts is the end ts of the max ickp
			// end-ts: the given ts
			start = s.GetCheckpointed()
		}

		if old != nil && old.end.EQ(ts) {
			if old.IsPolicyChecked() == policyChecked && old.IsFlushChecked() == flushChecked {
				intent = old
				return
			}
		}

		// if the start ts is larger equal to the given ts, no need to update
		if start.GE(ts) {
			intent = old
			return
		}
		var newIntent *CheckpointEntry
		if old == nil {
			newIntent = NewCheckpointEntry(
				s.sid,
				start,
				*ts,
				ET_Incremental,
				WithCheckedEntryOption(policyChecked, flushChecked),
			)
		} else {
			// the incoming checked status can override the old status
			// it is impossible that the old is checked and the incoming is not checked here
			// false -> true: impossible here
			newIntent = InheritCheckpointEntry(
				old,
				WithEndEntryOption(*ts),
				WithCheckedEntryOption(policyChecked, flushChecked),
			)
		}
		if s.incrementalIntent.CompareAndSwap(old, newIntent) {
			intent = newIntent
			updated = true
			return
		}
	}
}

func (s *runnerStore) TakeICKPIntent() (taken *CheckpointEntry, rollback func()) {
	for {
		old := s.incrementalIntent.Load()
		if old == nil || !old.IsPendding() || !old.AllChecked() {
			return
		}
		taken = InheritCheckpointEntry(
			old,
			WithStateEntryOption(ST_Running),
		)
		if s.incrementalIntent.CompareAndSwap(old, taken) {
			rollback = func() {
				// clear the intent and notify the intent is done
				s.incrementalIntent.Store(nil)
				taken.Done()
			}
			break
		}
		taken = nil
		rollback = nil
	}
	return
}

func (s *runnerStore) PrepareCommitICKPIntent(
	intent *CheckpointEntry,
) (ok bool) {
	expect := s.incrementalIntent.Load()
	// should not happen
	if intent != expect || !intent.IsRunning() {
		logutil.Error(
			"ICKP-PrepareCommit",
			zap.Any("expected", expect),
			zap.Any("actual", intent),
		)
		return
	}
	s.Lock()
	defer s.Unlock()
	s.incrementals.Set(intent)
	ok = true
	return
}

func (s *runnerStore) RollbackICKPIntent(
	intent *CheckpointEntry,
) {
	expect := s.incrementalIntent.Load()
	// should not happen
	if intent != expect || !intent.IsRunning() {
		logutil.Fatal(
			"ICKP-Rollback",
			zap.Any("expected", expect),
			zap.Any("actual", intent),
		)
	}
	s.Lock()
	defer s.Unlock()
	s.incrementals.Delete(intent)
}

// intent must be in Running state
func (s *runnerStore) CommitICKPIntent(
	intent *CheckpointEntry,
) {
	defer intent.Done()
	old := s.incrementalIntent.Load()
	// should not happen
	if old != intent {
		logutil.Fatal(
			"CommitICKPIntent-Error",
			zap.String("intent", intent.String()),
			zap.String("expected", old.String()),
		)
	}
	intent.SetState(ST_Finished)
	s.incrementalIntent.Store(nil)
}

func (s *runnerStore) AddMetaFile(name string) {
	s.Lock()
	defer s.Unlock()
	s.metaFiles[name] = struct{}{}
}

func (s *runnerStore) RemoveMetaFile(name string) {
	s.Lock()
	defer s.Unlock()
	delete(s.metaFiles, name)
}

func (s *runnerStore) GetMetaFiles() map[string]struct{} {
	s.RLock()
	defer s.RUnlock()
	files := make(map[string]struct{})
	for k, v := range s.metaFiles {
		files[k] = v
	}
	return files
}

func (s *runnerStore) ExportStatsLocked() []zap.Field {
	fields := make([]zap.Field, 0, 8)
	fields = append(fields, zap.Int("gc-count", s.gcCount))
	fields = append(fields, zap.Time("gc-time", s.gcTime))
	wm := s.gcWatermark.Load()
	if wm != nil {
		fields = append(fields, zap.String("gc-watermark", wm.(types.TS).ToString()))
	}
	fields = append(fields, zap.Int("global-count", s.globals.Len()))
	fields = append(fields, zap.Int("incremental-count", s.incrementals.Len()))
	return fields
}

func (s *runnerStore) AddNewIncrementalEntry(entry *CheckpointEntry) {
	s.Lock()
	defer s.Unlock()
	s.incrementals.Set(entry)
}

func (s *runnerStore) AddICKPFinishedEntry(entry *CheckpointEntry) (success bool) {
	if !entry.IsFinished() {
		return false
	}
	s.Lock()
	defer s.Unlock()
	maxEntry, _ := s.incrementals.Max()

	// if it's the first entry, add it
	if maxEntry == nil {
		s.incrementals.Set(entry)
		success = true
		return
	}

	// if it is not the right candidate, skip this request
	// [startTs, endTs] --> [endTs+1, ?]
	if !maxEntry.IsYoungNeighbor(entry) {
		success = false
		return
	}

	// if the max entry is not finished, skip this request
	if !maxEntry.IsFinished() {
		success = false
		return
	}

	s.incrementals.Set(entry)

	success = true
	return
}

// Since there is no wal after recovery, the checkpoint lsn before backup must be set to 0.
func (s *runnerStore) AddBackupCKPEntry(entry *CheckpointEntry) (success bool) {
	entry.entryType = ET_Incremental
	success = s.AddICKPFinishedEntry(entry)
	if !success {
		return
	}
	s.Lock()
	defer s.Unlock()
	it := s.incrementals.Iter()
	for it.Next() {
		e := it.Item()
		e.ckpLSN = 0
		e.truncateLSN = 0
	}
	return
}

func (s *runnerStore) AddGCKPIntent(
	intent *CheckpointEntry,
) (success bool) {
	if intent == nil || intent.entryType != ET_Global || intent.IsFinished() {
		return false
	}

	s.Lock()
	defer s.Unlock()

	maxEntry, _ := s.globals.Max()
	if maxEntry != nil && (maxEntry.end.GE(&intent.end) || !maxEntry.IsFinished()) {
		return false
	}

	s.globals.Set(intent)
	return true
}

func (s *runnerStore) RemoveGCKPIntent() (ok bool) {
	s.Lock()
	defer s.Unlock()
	intent, _ := s.globals.Max()
	if intent == nil || intent.IsFinished() {
		return false
	}
	s.globals.Delete(intent)
	return true
}

func (s *runnerStore) AddGCKPFinishedEntry(
	entry *CheckpointEntry,
) (success bool) {
	s.Lock()
	defer s.Unlock()
	s.globals.Set(entry)
	return true
}

func (s *runnerStore) GetAllGlobalCheckpoints() []*CheckpointEntry {
	s.Lock()
	snapshot := s.globals.Copy()
	s.Unlock()
	return snapshot.Items()
}

func (s *runnerStore) GetAllCheckpointsForBackup(compact *CheckpointEntry) []*CheckpointEntry {
	ckps := make([]*CheckpointEntry, 0)
	var ts types.TS
	if compact != nil {
		ts = compact.GetEnd()
		ckps = append(ckps, compact)
	}
	s.Lock()
	g := s.MaxFinishedGlobalCheckpointLocked()
	tree := s.incrementals.Copy()
	s.Unlock()
	if g != nil {
		if ts.IsEmpty() {
			ts = g.GetEnd()
		}
		ckps = append(ckps, g)
	}
	pivot := NewCheckpointEntry(s.sid, ts.Next(), ts.Next(), ET_Incremental)
	iter := tree.Iter()
	defer iter.Release()
	if ok := iter.Seek(pivot); ok {
		for {
			e := iter.Item()
			if !e.IsFinished() {
				break
			}
			ckps = append(ckps, e)
			if !iter.Next() {
				break
			}
		}
	}
	return ckps
}

func (s *runnerStore) GetAllCheckpoints() []*CheckpointEntry {
	ckps := make([]*CheckpointEntry, 0)
	var ts types.TS
	s.Lock()
	g := s.MaxFinishedGlobalCheckpointLocked()
	tree := s.incrementals.Copy()
	s.Unlock()
	if g != nil {
		ts = g.GetEnd()
		ckps = append(ckps, g)
	}
	pivot := NewCheckpointEntry(s.sid, ts.Next(), ts.Next(), ET_Incremental)
	iter := tree.Iter()
	defer iter.Release()
	if ok := iter.Seek(pivot); ok {
		for {
			e := iter.Item()
			if !e.IsFinished() {
				break
			}
			ckps = append(ckps, e)
			if !iter.Next() {
				break
			}
		}
	}
	return ckps
}

func (s *runnerStore) MaxFinishedGlobalCheckpointLocked() *CheckpointEntry {
	g, ok := s.globals.Max()
	if !ok {
		return nil
	}
	if g.IsFinished() {
		return g
	}
	it := s.globals.Iter()
	it.Seek(g)
	defer it.Release()
	if !it.Prev() {
		return nil
	}
	return it.Item()
}

func (s *runnerStore) GetPenddingIncrementalCount() int {
	entries := s.GetAllIncrementalCheckpoints()
	global := s.MaxGlobalCheckpoint()

	count := 0
	for i := len(entries) - 1; i >= 0; i-- {
		if global != nil && entries[i].end.LE(&global.end) {
			break
		}
		if !entries[i].IsFinished() {
			continue
		}
		count++
	}
	return count
}

func (s *runnerStore) GetAllIncrementalCheckpoints() []*CheckpointEntry {
	s.Lock()
	snapshot := s.incrementals.Copy()
	s.Unlock()
	return snapshot.Items()
}

func (s *runnerStore) GetLowWaterMark() types.TS {
	s.RLock()
	defer s.RUnlock()
	global, okG := s.globals.Min()
	incremental, okI := s.incrementals.Min()
	if !okG && !okI {
		return types.TS{}
	}
	if !okG {
		return incremental.start
	}
	if !okI {
		return global.start
	}
	if global.end.LT(&incremental.start) {
		return global.end
	}
	return incremental.start
}

func (s *runnerStore) GetCompacted() *CheckpointEntry {
	return s.compacted.Load()
}

func (s *runnerStore) UpdateCompacted(entry *CheckpointEntry) (updated bool) {
	for {
		old := s.compacted.Load()
		if old != nil {
			newEnd := entry.GetEnd()
			oldEnd := old.GetEnd()
			if newEnd.LE(&oldEnd) {
				return
			}
		}
		if s.compacted.CompareAndSwap(old, entry) {
			updated = true
			return
		}
	}
}

func (s *runnerStore) ICKPRange(
	start, end *types.TS, cnt int,
) []*CheckpointEntry {
	s.Lock()
	tree := s.incrementals.Copy()
	s.Unlock()
	it := tree.Iter()
	ok := it.Seek(NewCheckpointEntry(s.sid, *start, *start, ET_Incremental))
	incrementals := make([]*CheckpointEntry, 0)
	if ok {
		for len(incrementals) < cnt {
			e := it.Item()
			if !e.IsFinished() {
				break
			}
			if e.start.GE(start) && e.start.LT(end) {
				incrementals = append(incrementals, e)
			}
			if !it.Next() {
				break
			}
		}
	}
	return incrementals
}

func (s *runnerStore) ICKPSeekLT(ts types.TS, cnt int) []*CheckpointEntry {
	s.Lock()
	tree := s.incrementals.Copy()
	s.Unlock()
	it := tree.Iter()
	ok := it.Seek(NewCheckpointEntry(s.sid, ts, ts, ET_Incremental))
	incrementals := make([]*CheckpointEntry, 0)
	if ok {
		for len(incrementals) < cnt {
			e := it.Item()
			if !e.IsFinished() {
				break
			}
			if e.start.LT(&ts) {
				if !it.Next() {
					break
				}
				continue
			}
			incrementals = append(incrementals, e)
			if !it.Next() {
				break
			}
		}
	}
	return incrementals
}

// return the max finished global checkpoint
func (s *runnerStore) MaxGlobalCheckpoint() *CheckpointEntry {
	s.RLock()
	global, _ := s.globals.Max()
	s.RUnlock()
	if global == nil || global.IsFinished() {
		return global
	}
	s.Lock()
	items := s.globals.Items()
	s.Unlock()
	for i := len(items) - 1; i >= 0; i-- {
		if items[i].IsFinished() {
			return items[i]
		}
	}
	return nil
}

func (s *runnerStore) IsStale(ts *types.TS) bool {
	waterMark := s.gcWatermark.Load()
	if waterMark == nil {
		return false
	}
	wm := waterMark.(types.TS)
	minPhysical := wm.Physical() - s.globalHistoryDuration.Nanoseconds()
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

func (s *runnerStore) UpdateGCIntent(newIntent *types.TS) (oldIntent types.TS, updated bool) {
	s.Lock()
	defer s.Unlock()
	oldIntent = s.gcIntent
	if s.gcIntent.LT(newIntent) {
		s.gcIntent = *newIntent
		updated = true
	}
	return
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
	return minTS.LT(&intent)
}

func (s *runnerStore) CollectCheckpointsInRange(
	ctx context.Context, start, end types.TS,
) (locations string, checkpointed types.TS, err error) {
	if s.IsStale(&end) {
		return "", types.TS{}, moerr.NewInternalErrorf(ctx, "ts %v is staled", end.ToString())
	}
	s.Lock()
	tree := s.incrementals.Copy()
	globaltree := s.globals.Copy()
	s.Unlock()

	locs := make([]string, 0)
	ckpStart := types.MaxTs()
	newStart := start

	defer func() {
		if len(locs) == 0 {
			return
		}
		duration := fmt.Sprintf("[%s_%s]",
			ckpStart.ToString(),
			checkpointed.ToString())
		locs = append(locs, duration)
		locs = append([]string{strconv.Itoa(int(CKPProtocolVersion_Curr))}, locs...)
		locations = strings.Join(locs, ";")
	}()

	collectEntryFn := func(entry *CheckpointEntry) {
		locs = append(locs, entry.GetLocation().String())
		locs = append(locs, strconv.Itoa(int(entry.version)))
		if checkpointed.LT(&entry.end) {
			checkpointed = entry.GetEnd()
		}
		if entry.IsGlobal() {
			ckpStart = entry.end
			newStart = entry.end.Next()
		}
		if entry.IsIncremental() {
			start := entry.start
			if start.LT(&ckpStart) {
				ckpStart = start
			}
		}
		// checkpoints = append(checkpoints, entry)
	}

	globalIter := globaltree.Iter()
	ok := globalIter.Last()
	for ok {
		ckp := globalIter.Item()
		if ckp.IsCommitted() && ckp.HasOverlap(start, end) {
			collectEntryFn(ckp)
			break
		}
		ok = globalIter.Prev()
	}
	pivot := NewCheckpointEntry(s.sid, newStart, newStart, ET_Incremental)

	// For debug
	// checkpoints := make([]*CheckpointEntry, 0)
	// defer func() {
	// 	items := tree.Items()
	// 	logutil.Infof("CollectCheckpointsInRange: Pivot: %s", pivot.String())
	// 	for i, item := range items {
	// 		logutil.Infof("CollectCheckpointsInRange: Source[%d]: %s", i, item.String())
	// 	}
	// 	for i, ckp := range checkpoints {
	// 		logutil.Infof("CollectCheckpointsInRange: Found[%d]:%s", i, ckp.String())
	// 	}
	// 	logutil.Infof("CollectCheckpointsInRange: Checkpointed=%s", checkpointed.ToString())
	// }()

	iter := tree.Iter()
	defer iter.Release()

	if ok := iter.Seek(pivot); ok {
		if ok = iter.Prev(); ok {
			e := iter.Item()
			if !e.IsCommitted() {
				return
			}
			if e.HasOverlap(newStart, end) {
				collectEntryFn(e)
			}
			iter.Next()
		}
		for {
			e := iter.Item()
			if !e.IsCommitted() || !e.HasOverlap(newStart, end) {
				break
			}
			collectEntryFn(e)
			if ok = iter.Next(); !ok {
				break
			}
		}
	} else {
		// if it is empty, quick quit
		if ok = iter.Last(); !ok {
			return
		}
		// get last entry
		e := iter.Item()
		// if it is committed and visible, quick quit
		if !e.IsCommitted() || !e.HasOverlap(newStart, end) {
			return
		}
		collectEntryFn(e)
	}
	return
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
		if e.LEByTS(ts) {
			s.globals.Delete(e)
			gdeleted++
		}
	}
	incrementals := s.incrementals.Items()
	for _, e := range incrementals {
		if e.LEByTS(ts) {
			s.incrementals.Delete(e)
			ideleted++
		}
	}
	s.gcCount++
	s.gcTime = time.Now()
	s.gcWatermark.Store(*ts)
	fields := s.ExportStatsLocked()
	fields = append(fields, zap.Int("this-g-deleted", gdeleted))
	fields = append(fields, zap.Int("this-i-deleted", ideleted))
	logutil.Info(
		"GC-Inmemory-Checkpoints",
		fields...,
	)
	return
}
