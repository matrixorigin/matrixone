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

package catalog

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type LogEntry = entry.Entry

var EmptyCheckpoint = new(Checkpoint)

const (
	ETCatalogCheckpoint = entry.ETCustomizedStart + 100 + iota
)

type CheckpointItem interface {
	Clone() CheckpointItem
	CloneCreate() CheckpointItem
	MakeLogEntry() *EntryCommand
	StringLocked() string
}

func CheckpointSelectOp(entry *BaseEntry, minTs, maxTs uint64) bool {
	entry.RLock()
	defer entry.RUnlock()
	if entry.InTxnOrRollbacked() {
		return false
	}
	// 1. entry was created after maxTs. Skip it
	if entry.CreateAfter(maxTs) {
		return false
	}
	// 2. entry was deleted before minTs. Skip it
	if entry.DeleteBefore(minTs) {
		return false
	}
	// 3. entry was created in (0, minTs)
	if entry.CreateBefore(minTs) {
		// 3.1 entry was not deleted. Skip it
		if !entry.HasDropped() {
			return false
		}
		// 3.2 entry was deleted in (maxTs, +inf). Skip it
		if entry.DeleteAfter(maxTs) {
			return false
		}
	}
	return true
}

func CheckpointOp(ckpEntry *CheckpointEntry, entry *BaseEntry, item CheckpointItem, minTs, maxTs uint64) {
	entry.RLock()
	if entry.InTxnOrRollbacked() {
		entry.RUnlock()
		return
	}
	// 1. entry was created in (maxTs, +inf). Skip it
	if entry.CreateAfter(maxTs) {
		entry.RUnlock()
		return
	}
	// 2. entry was deleted in (0, minTs). Skip it
	if entry.DeleteBefore(minTs) {
		entry.RUnlock()
		return
	}
	// 3. entry was created in (0, minTs)
	if entry.CreateBefore(minTs) {
		// 3.1 entry was not deleted. skip it
		if !entry.HasDropped() {
			entry.RUnlock()
			return
		}
		// 3.2 entry was deleted (maxTs, inf). Skip it
		if entry.DeleteAfter(maxTs) {
			entry.RUnlock()
			return
		}
		// 3.3 entry was deleted in [minTs, maxTs]
		ckpEntry.AddIndex(entry.LogIndex)
		cloned := item.Clone()
		entry.RUnlock()
		ckpEntry.AddCommand(cloned.MakeLogEntry())
		return
	}
	// 4. entry was created at|after minTs
	// 4.1 entry was deleted at|before maxTs
	if entry.DeleteBefore(maxTs + 1) {
		ckpEntry.AddIndex(entry.LogIndex)
		ckpEntry.AddIndex(entry.PrevCommit.LogIndex)
		cloned := item.Clone()
		entry.RUnlock()
		ckpEntry.AddCommand(cloned.MakeLogEntry())
		return
	}
	// 4.2 entry was not deleted
	if !entry.HasDropped() {
		ckpEntry.AddIndex(entry.LogIndex)
		cloned := item.Clone()
		entry.RUnlock()
		ckpEntry.AddCommand(cloned.MakeLogEntry())
		return
	}
	// 4.3 entry was deleted after maxTs
	ckpEntry.AddIndex(entry.PrevCommit.LogIndex)
	cloned := item.CloneCreate()
	entry.RUnlock()
	ckpEntry.AddCommand(cloned.MakeLogEntry())
}

type Checkpoint struct {
	MaxTS    uint64
	LSN      uint64
	CommitId uint64
}

func (ckp *Checkpoint) String() string {
	if ckp == nil {
		return "CommitId=0,MaxTS=0,LSN=0"
	}
	return fmt.Sprintf("CommitId=%d,MaxTS=%d,LSN=%d", ckp.CommitId, ckp.MaxTS, ckp.LSN)
}

type CheckpointEntry struct {
	MinTS, MaxTS uint64
	LogIndexes   []*wal.Index
	MaxIndex     wal.Index
	Entries      []*EntryCommand
}

func NewEmptyCheckpointEntry() *CheckpointEntry {
	return &CheckpointEntry{
		Entries: make([]*EntryCommand, 0, 16),
	}
}

func NewCheckpointEntry(minTs, maxTs uint64) *CheckpointEntry {
	return &CheckpointEntry{
		MinTS:      minTs,
		MaxTS:      maxTs,
		LogIndexes: make([]*wal.Index, 0, 16),
		Entries:    make([]*EntryCommand, 0, 16),
	}
}

func (e *CheckpointEntry) AddCommand(cmd *EntryCommand) {
	e.Entries = append(e.Entries, cmd)
}

func (e *CheckpointEntry) AddIndex(index *wal.Index) {
	if index == nil {
		return
	}
	if e.MaxIndex.Compare(index) < 0 {
		e.MaxIndex = *index
	}
	e.LogIndexes = append(e.LogIndexes, index)
}

func (e *CheckpointEntry) GetMaxIndex() *wal.Index {
	return &e.MaxIndex
}

func (e *CheckpointEntry) Marshal() (buf []byte, err error) {
	var w bytes.Buffer
	if _, err = e.MaxIndex.WriteTo(&w); err != nil {
		return
	}
	if err = binary.Write(&w, binary.BigEndian, e.MinTS); err != nil {
		return
	}
	if err = binary.Write(&w, binary.BigEndian, e.MaxTS); err != nil {
		return
	}

	if err = binary.Write(&w, binary.BigEndian, uint32(len(e.Entries))); err != nil {
		return
	}
	for _, cmd := range e.Entries {
		if _, err = cmd.WriteTo(&w); err != nil {
			return
		}
	}

	buf = w.Bytes()
	return
}

func (e *CheckpointEntry) Unmarshal(buf []byte) (err error) {
	r := bytes.NewBuffer(buf)
	if _, err = e.MaxIndex.ReadFrom(r); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &e.MinTS); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &e.MaxTS); err != nil {
		return
	}
	length := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	for i := 0; i < int(length); i++ {
		txnEntry, _, err := txnbase.BuildCommandFrom(r)
		if err != nil {
			return err
		}
		e.Entries = append(e.Entries, txnEntry.(*EntryCommand))
	}
	return
}

func (e *CheckpointEntry) MakeLogEntry() (logEntry LogEntry, err error) {
	var buf []byte
	if buf, err = e.Marshal(); err != nil {
		return
	}
	logEntry = entry.GetBase()
	logEntry.SetType(ETCatalogCheckpoint)
	err = logEntry.Unmarshal(buf)
	return
}

func (e *CheckpointEntry) PrintItems() {
	for _, cmd := range e.Entries {
		if cmd.Block != nil {
			logutil.Infof("%s", cmd.Block.StringLocked())
		} else if cmd.Segment != nil {
			logutil.Infof("%s", cmd.Segment.StringLocked())
		} else if cmd.Table != nil {
			logutil.Infof("%s", cmd.Table.StringLocked())
		} else if cmd.DB != nil {
			logutil.Infof("%s", cmd.DB.StringLocked())
		}
	}
}
