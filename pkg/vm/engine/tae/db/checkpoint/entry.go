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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type CheckpointEntry struct {
	sync.RWMutex
	sid        string
	start, end types.TS
	state      State
	entryType  EntryType
	cnLocation objectio.Location
	tnLocation objectio.Location
	lastPrint  time.Time
	waterLine  time.Duration
	version    uint32

	ckpLSN      uint64
	truncateLSN uint64
}

func NewCheckpointEntry(sid string, start, end types.TS, typ EntryType) *CheckpointEntry {
	return &CheckpointEntry{
		sid:       sid,
		start:     start,
		end:       end,
		state:     ST_Pending,
		entryType: typ,
		lastPrint: time.Now(),
		waterLine: time.Minute * 4,
		version:   logtail.CheckpointCurrentVersion,
	}
}

func (e *CheckpointEntry) SetVersion(version uint32) {
	e.Lock()
	defer e.Unlock()
	e.version = version
}

func (e *CheckpointEntry) IncrWaterLine() {
	e.Lock()
	defer e.Unlock()
	e.waterLine += time.Minute * 4
}
func (e *CheckpointEntry) SetLSN(ckpLSN, truncateLSN uint64) {
	e.ckpLSN = ckpLSN
	e.truncateLSN = truncateLSN
}
func (e *CheckpointEntry) CheckPrintTime() bool {
	e.RLock()
	defer e.RUnlock()
	return time.Since(e.lastPrint) > e.waterLine
}
func (e *CheckpointEntry) LSNString() string {
	return fmt.Sprintf("ckp %d, truncate %d", e.ckpLSN, e.truncateLSN)
}

func (e *CheckpointEntry) LSN() uint64 {
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
func (e *CheckpointEntry) HasOverlap(from, to types.TS) bool {
	if e.start.Greater(&to) || e.end.Less(&from) {
		return false
	}
	return true
}
func (e *CheckpointEntry) LessEq(ts types.TS) bool {
	return e.end.LessEq(&ts)
}
func (e *CheckpointEntry) SetLocation(cn, tn objectio.Location) {
	e.Lock()
	defer e.Unlock()
	e.cnLocation = cn
	e.tnLocation = tn
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
	return e.version
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

func (e *CheckpointEntry) IsIncremental() bool {
	return e.entryType == ET_Incremental
}

func (e *CheckpointEntry) GetType() EntryType {
	return e.entryType
}

func (e *CheckpointEntry) String() string {
	t := "I"
	if !e.IsIncremental() {
		t = "G"
	}
	state := e.GetState()
	return fmt.Sprintf("CKP[%s][%v][%s](%s->%s)", t, state, e.LSNString(), e.start.ToString(), e.end.ToString())
}

func (e *CheckpointEntry) Prefetch(
	ctx context.Context,
	fs *objectio.ObjectFS,
	data *logtail.CheckpointData,
) (err error) {
	if err = data.PrefetchFrom(
		ctx,
		e.version,
		fs.Service,
		e.tnLocation,
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
	reader, err := blockio.NewObjectReader(e.sid, fs.Service, e.tnLocation)
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
		ctx,
		e.version,
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
	reader, err := blockio.NewObjectReader(e.sid, fs.Service, e.tnLocation)
	if err != nil {
		return
	}
	return data.ReadTNMetaBatch(ctx, e.version, e.tnLocation, reader)
}

func (e *CheckpointEntry) GetByTableID(ctx context.Context, fs *objectio.ObjectFS, tid uint64) (ins, del, dataObject, tombstoneObject *api.Batch, err error) {
	reader, err := blockio.NewObjectReader(e.sid, fs.Service, e.cnLocation)
	if err != nil {
		return
	}
	data := logtail.NewCNCheckpointData(e.sid)
	err = blockio.PrefetchMeta(e.sid, fs.Service, e.cnLocation)
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

func (e *CheckpointEntry) GCMetadata(fs *objectio.ObjectFS) error {
	name := blockio.EncodeCheckpointMetadataFileName(CheckpointDir, PrefixMetadata, e.start, e.end)
	err := fs.Delete(name)
	logutil.Infof("GC checkpoint metadata %v, err %v", e.String(), err)
	return err
}

func (e *CheckpointEntry) GCEntry(fs *objectio.ObjectFS) error {
	err := fs.Delete(e.cnLocation.Name().String())
	defer logutil.Infof("GC checkpoint entry %v, err %v", e.String(), err)
	return err
}

type MetaFile struct {
	index int
	start types.TS
	end   types.TS
	name  string
}

func (m *MetaFile) String() string {
	return fmt.Sprintf("MetaFile[%d][%s->%s][%s]", m.index, m.start.ToString(), m.end.ToString(), m.name)
}

func (m *MetaFile) GetIndex() int {
	return m.index
}

func (m *MetaFile) GetStart() types.TS {
	return m.start
}

func (m *MetaFile) GetEnd() types.TS {
	return m.end
}

func (m *MetaFile) GetName() string {
	return m.name
}

func NewMetaFile(index int, start, end types.TS, name string) *MetaFile {
	return &MetaFile{
		index: index,
		start: start,
		end:   end,
		name:  name,
	}

}
