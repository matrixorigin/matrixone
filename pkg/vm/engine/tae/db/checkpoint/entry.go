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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type Intent interface {
	String() string
	Wait() <-chan struct{}
}

type EntryOption func(*CheckpointEntry)

func WithEndEntryOption(end types.TS) EntryOption {
	return func(e *CheckpointEntry) {
		e.end = end
	}
}

func WithStateEntryOption(state State) EntryOption {
	return func(e *CheckpointEntry) {
		e.state = state
	}
}

func WithCheckedEntryOption(policyChecked, flushedChecked bool) EntryOption {
	return func(e *CheckpointEntry) {
		e.policyChecked = policyChecked
		e.flushChecked = flushedChecked
	}
}

type CheckpointEntry struct {
	sync.RWMutex
	sid        string
	start, end types.TS
	state      State
	entryType  EntryType
	cnLocation objectio.Location
	tnLocation objectio.Location
	version    uint32

	ckpLSN      uint64
	truncateLSN uint64

	policyChecked bool
	flushChecked  bool

	// only for new entry logic procedure
	bornTime   time.Time
	refreshCnt uint32

	doneC chan struct{}
}

func NewCheckpointEntry(
	sid string, start, end types.TS, typ EntryType, opts ...EntryOption,
) *CheckpointEntry {
	e := &CheckpointEntry{
		sid:       sid,
		start:     start,
		end:       end,
		state:     ST_Pending,
		entryType: typ,
		version:   logtail.CheckpointCurrentVersion,
		bornTime:  time.Now(),
		doneC:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

func InheritCheckpointEntry(
	from *CheckpointEntry,
	replaceOpts ...EntryOption,
) *CheckpointEntry {
	from.RLock()
	defer from.RUnlock()
	e := &CheckpointEntry{
		sid:           from.sid,
		start:         from.start,
		end:           from.end,
		state:         from.state,
		entryType:     from.entryType,
		version:       from.version,
		bornTime:      from.bornTime,
		refreshCnt:    from.refreshCnt,
		policyChecked: from.policyChecked,
		flushChecked:  from.flushChecked,
		doneC:         from.doneC,
	}
	for _, opt := range replaceOpts {
		opt(e)
	}
	return e
}

// ================================================================
// ts comparison related
// ================================================================

// compare with other entry
// e.start >= o.end
func (e *CheckpointEntry) AllGE(o *CheckpointEntry) bool {
	return e.start.GE(&o.end)
}

// e.end <= ts
// it means that e is before ts
func (e *CheckpointEntry) LEByTS(ts *types.TS) bool {
	return e.end.LE(ts)
}

// false: e.start > to or e.end < from
// true: otherwise
func (e *CheckpointEntry) HasOverlap(from, to types.TS) bool {
	if e.start.GT(&to) || e.end.LT(&from) {
		return false
	}
	return true
}

// true: o.start == e.end + 1
func (e *CheckpointEntry) IsYoungNeighbor(o *CheckpointEntry) bool {
	next := e.end.Next()
	return o.start.Equal(&next)
}

//===============================================================
// execution related
//===============================================================

// it can be called multiple times
// it will block until Done() is called
func (e *CheckpointEntry) Wait() <-chan struct{} {
	return e.doneC
}

// it can only be called at most once
// it will unblock all Wait() calls
func (e *CheckpointEntry) Done() {
	close(e.doneC)
}

//===============================================================
// getter related
//===============================================================

func (e *CheckpointEntry) IsPolicyChecked() bool {
	e.RLock()
	defer e.RUnlock()
	return e.policyChecked
}

func (e *CheckpointEntry) IsFlushChecked() bool {
	e.RLock()
	defer e.RUnlock()
	return e.flushChecked
}

func (e *CheckpointEntry) AllChecked() bool {
	e.RLock()
	defer e.RUnlock()
	return e.policyChecked && e.flushChecked
}

func (e *CheckpointEntry) LSN() uint64 {
	e.RLock()
	defer e.RUnlock()
	return e.ckpLSN
}

func (e *CheckpointEntry) GetStart() types.TS { return e.start }
func (e *CheckpointEntry) GetEnd() types.TS   { return e.end }
func (e *CheckpointEntry) GetState() State {
	e.RLock()
	defer e.RUnlock()
	return e.state
}

func (e *CheckpointEntry) IsCommitted() bool {
	e.RLock()
	defer e.RUnlock()
	return e.state == ST_Finished
}

func (e *CheckpointEntry) GetLocation() objectio.Location {
	e.RLock()
	defer e.RUnlock()
	return e.cnLocation
}

func (e *CheckpointEntry) GetTNLocation() objectio.Location {
	e.RLock()
	defer e.RUnlock()
	return e.tnLocation
}

func (e *CheckpointEntry) GetVersion() uint32 {
	e.RLock()
	defer e.RUnlock()
	return e.version
}

func (e *CheckpointEntry) GetTruncateLsn() uint64 {
	e.RLock()
	defer e.RUnlock()
	return e.truncateLSN
}

func (e *CheckpointEntry) IsIncremental() bool {
	return e.entryType == ET_Incremental
}

func (e *CheckpointEntry) IsGlobal() bool {
	return e.entryType == ET_Global
}

func (e *CheckpointEntry) IsBackup() bool {
	return e.entryType == ET_Backup
}

func (e *CheckpointEntry) IsCompact() bool {
	return e.entryType == ET_Compacted
}

func (e *CheckpointEntry) GetType() EntryType {
	return e.entryType
}

//===============================================================
// setter related
//===============================================================

func (e *CheckpointEntry) SetPolicyChecked() {
	e.Lock()
	defer e.Unlock()
	e.policyChecked = true
}

func (e *CheckpointEntry) SetFlushChecked() {
	e.Lock()
	defer e.Unlock()
	e.flushChecked = true
}

func (e *CheckpointEntry) SetVersion(version uint32) {
	e.Lock()
	defer e.Unlock()
	e.version = version
}

func (e *CheckpointEntry) SetLSN(ckpLSN, truncateLSN uint64) {
	e.Lock()
	defer e.Unlock()
	e.ckpLSN = ckpLSN
	e.truncateLSN = truncateLSN
}

func (e *CheckpointEntry) SetLocation(cn, tn objectio.Location) {
	e.Lock()
	defer e.Unlock()
	e.cnLocation = cn.Clone()
	e.tnLocation = tn.Clone()
}

//===============================================================
// born time related
//===============================================================

func (e *CheckpointEntry) DeferRetirement() {
	e.Lock()
	defer e.Unlock()
	e.refreshCnt++
}

func (e *CheckpointEntry) Age() time.Duration {
	e.RLock()
	defer e.RUnlock()
	return time.Since(e.bornTime)
}

func (e *CheckpointEntry) ResetAge() {
	e.Lock()
	defer e.Unlock()
	e.bornTime = time.Now()
	e.refreshCnt = 0
}

func (e *CheckpointEntry) TooOld() bool {
	e.RLock()
	defer e.RUnlock()
	return time.Since(e.bornTime) > time.Minute*3*time.Duration(e.refreshCnt+1)
}

//===============================================================
// state related
//===============================================================

func (e *CheckpointEntry) IsRunning() bool {
	e.RLock()
	defer e.RUnlock()
	return e.state == ST_Running
}

func (e *CheckpointEntry) IsPendding() bool {
	e.RLock()
	defer e.RUnlock()
	return e.state == ST_Pending
}

func (e *CheckpointEntry) IsFinished() bool {
	e.RLock()
	defer e.RUnlock()
	return e.state == ST_Finished
}

func (e *CheckpointEntry) SetState(state State) (ok bool) {
	e.Lock()
	defer e.Unlock()
	// entry is already finished
	if e.state == ST_Finished {
		return
	}
	// entry is already running
	if state == ST_Running && e.state == ST_Running {
		return
	}
	e.state = state
	ok = true
	return
}

//===============================================================
// string related
//===============================================================

func (e *CheckpointEntry) LSNString() string {
	e.RLock()
	defer e.RUnlock()
	return fmt.Sprintf("%d-%d", e.ckpLSN, e.truncateLSN)
}

func (e *CheckpointEntry) String() string {
	if e == nil {
		return "nil"
	}
	t := "I"
	if !e.IsIncremental() {
		t = "G"
	}
	state := e.GetState()
	return fmt.Sprintf(
		"CKP[%s][%v][%v:%v][%s](%s->%s)",
		t,
		state,
		e.IsPolicyChecked(),
		e.IsFlushChecked(),
		e.LSNString(),
		e.start.ToString(),
		e.end.ToString(),
	)
}

//===============================================================
// read related
//===============================================================

func (e *CheckpointEntry) Prefetch(
	ctx context.Context,
	fs *objectio.ObjectFS,
	data *logtail.CheckpointData,
) (err error) {
	if err = data.PrefetchFrom(
		fs.Service,
	); err != nil {
		return
	}
	return
}

func (e *CheckpointEntry) Read(
	ctx context.Context,
	fs *objectio.ObjectFS,
	data *logtail.CheckpointData,
) (err error) {
	reader, err := ioutil.NewObjectReader(fs.Service, e.tnLocation)
	if err != nil {
		return
	}

	if err = data.ReadFrom(
		ctx,
		e.version,
		e.tnLocation,
		reader,
		fs.Service,
	); err != nil {
		return
	}
	return
}

func (e *CheckpointEntry) PrefetchMetaIdx(
	ctx context.Context,
	fs *objectio.ObjectFS,
) (data *logtail.CheckpointData, err error) {
	data = logtail.NewCheckpointData(e.sid, common.CheckpointAllocator)
	if err = data.PrefetchMeta(
		fs.Service,
		e.tnLocation,
	); err != nil {
		return
	}
	return
}

func (e *CheckpointEntry) ReadMetaIdx(
	ctx context.Context,
	fs *objectio.ObjectFS,
	data *logtail.CheckpointData,
) (err error) {
	reader, err := ioutil.NewObjectReader(fs.Service, e.tnLocation)
	if err != nil {
		return
	}
	return data.ReadTNMetaBatch(ctx, e.version, e.tnLocation, reader)
}

func (e *CheckpointEntry) GetTableByID(
	ctx context.Context, fs *objectio.ObjectFS, tid uint64,
) (ins, del, dataObject, tombstoneObject *api.Batch, err error) {
	reader, err := ioutil.NewObjectReader(fs.Service, e.cnLocation)
	if err != nil {
		return
	}
	data := logtail.NewCNCheckpointData(e.sid)
	err = ioutil.PrefetchMeta(e.sid, fs.Service, e.cnLocation)
	if err != nil {
		return
	}

	err = data.PrefetchMetaIdx(ctx, e.version, logtail.GetMetaIdxesByVersion(e.version), e.cnLocation, fs.Service)
	if err != nil {
		return
	}
	err = data.InitMetaIdx(ctx, e.version, reader, e.cnLocation, common.CheckpointAllocator)
	if err != nil {
		return
	}
	err = data.PrefetchMetaFrom(ctx, e.version, e.cnLocation, fs.Service, tid)
	if err != nil {
		return
	}
	err = data.PrefetchFrom(ctx, e.version, fs.Service, e.cnLocation, tid)
	if err != nil {
		return
	}
	var bats []*batch.Batch
	if bats, err = data.ReadFromData(ctx, tid, e.cnLocation, reader, e.version, common.CheckpointAllocator); err != nil {
		return
	}
	ins, del, dataObject, tombstoneObject, err = data.GetTableDataFromBats(tid, bats)
	return
}
