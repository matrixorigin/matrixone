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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

type LogEntry = entry.Entry

// var EmptyCheckpoint = new(Checkpoint)

// const (
// 	ETCatalogCheckpoint = entry.ETCustomizedStart + 100 + iota
// )

// type CheckpointItem interface {
// 	Clone() CheckpointItem
// 	CloneCreate() CheckpointItem
// 	MakeLogEntry() *EntryCommand
// 	StringLocked() string
// }

// func CheckpointSelectOp(entry BaseEntry, minTs, maxTs types.TS) bool {
// 	entry.RLock()
// 	defer entry.RUnlock()
// 	return entry.HasCommittedNodeInRange(minTs, maxTs)
// }

// type CatalogEntry interface {
// 	GetCheckpointItems(start, end types.TS) CheckpointItems
// 	RLock()
// 	RUnlock()
// }

// type CheckpointItems interface {
// 	GetIndexes() []*wal.Index
// 	MakeLogEntry() *EntryCommand
// }

// func CheckpointOp(ckpEntry *CheckpointEntry, entry CatalogEntry, minTs, maxTs types.TS) {
// 	entry.RLock()
// 	ckpItem := entry.GetCheckpointItems(minTs, maxTs)
// 	entry.RUnlock()
// 	if ckpItem == nil {
// 		return
// 	}
// 	ckpEntry.AddCommand(ckpItem.MakeLogEntry())
// 	ckpEntry.AddIndexes(ckpItem.GetIndexes())
// }

// type Checkpoint struct {
// 	MaxTS    types.TS
// 	LSN      uint64
// 	CommitId uint64
// }

// func (ckp *Checkpoint) String() string {
// 	if ckp == nil {
// 		return "CommitId=0,MaxTS=0-0,LSN=0"
// 	}
// 	return fmt.Sprintf("CommitId=%d,MaxTS=%s,LSN=%d", ckp.CommitId, ckp.MaxTS.ToString(), ckp.LSN)
// }

// type CheckpointEntry struct {
// 	MinTS, MaxTS types.TS
// 	LogIndexes   []*wal.Index
// 	MaxIndex     wal.Index
// 	Entries      []*EntryCommand
// }

// func NewEmptyCheckpointEntry() *CheckpointEntry {
// 	return &CheckpointEntry{
// 		Entries: make([]*EntryCommand, 0, 16),
// 	}
// }

// func NewCheckpointEntry(minTs, maxTs types.TS) *CheckpointEntry {
// 	return &CheckpointEntry{
// 		MinTS:      minTs,
// 		MaxTS:      maxTs,
// 		LogIndexes: make([]*wal.Index, 0, 16),
// 		Entries:    make([]*EntryCommand, 0, 16),
// 	}
// }

// func (e *CheckpointEntry) AddCommand(cmd *EntryCommand) {
// 	e.Entries = append(e.Entries, cmd)
// }

// func (e *CheckpointEntry) AddIndex(index *wal.Index) {
// 	if index == nil {
// 		return
// 	}
// 	if e.MaxIndex.Compare(index) < 0 {
// 		e.MaxIndex = *index
// 	}
// 	e.LogIndexes = append(e.LogIndexes, index)
// }

// func (e *CheckpointEntry) AddIndexes(indexes []*wal.Index) {
// 	for _, idx := range indexes {
// 		e.AddIndex(idx)
// 	}
// }

// func (e *CheckpointEntry) GetMaxIndex() *wal.Index {
// 	return &e.MaxIndex
// }

// func (e *CheckpointEntry) Marshal() (buf []byte, err error) {
// 	var w bytes.Buffer
// 	if _, err = e.MaxIndex.WriteTo(&w); err != nil {
// 		return
// 	}
// 	if err = binary.Write(&w, binary.BigEndian, e.MinTS); err != nil {
// 		return
// 	}
// 	if err = binary.Write(&w, binary.BigEndian, e.MaxTS); err != nil {
// 		return
// 	}

// 	if err = binary.Write(&w, binary.BigEndian, uint32(len(e.Entries))); err != nil {
// 		return
// 	}
// 	for _, cmd := range e.Entries {
// 		if _, err = cmd.WriteTo(&w); err != nil {
// 			return
// 		}
// 	}

// 	buf = w.Bytes()
// 	return
// }

// func (e *CheckpointEntry) Unmarshal(buf []byte) (err error) {
// 	r := bytes.NewBuffer(buf)
// 	if _, err = e.MaxIndex.ReadFrom(r); err != nil {
// 		return
// 	}
// 	if err = binary.Read(r, binary.BigEndian, &e.MinTS); err != nil {
// 		return
// 	}
// 	if err = binary.Read(r, binary.BigEndian, &e.MaxTS); err != nil {
// 		return
// 	}
// 	length := uint32(0)
// 	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
// 		return
// 	}
// 	for i := 0; i < int(length); i++ {
// 		txnEntry, _, err := txnbase.BuildCommandFrom(r)
// 		if err != nil {
// 			return err
// 		}
// 		e.Entries = append(e.Entries, txnEntry.(*EntryCommand))
// 	}
// 	return
// }

// func (e *CheckpointEntry) MakeLogEntry() (logEntry LogEntry, err error) {
// 	var buf []byte
// 	if buf, err = e.Marshal(); err != nil {
// 		return
// 	}
// 	logEntry = entry.GetBase()
// 	logEntry.SetType(ETCatalogCheckpoint)
// 	err = logEntry.SetPayload(buf)
// 	return
// }

// func (e *CheckpointEntry) PrintItems() {
// 	for _, cmd := range e.Entries {
// 		if cmd.Block != nil {
// 			logutil.Infof("%s", cmd.Block.StringLocked())
// 		} else if cmd.Segment != nil {
// 			logutil.Infof("%s", cmd.Segment.StringLocked())
// 		} else if cmd.Table != nil {
// 			logutil.Infof("%s", cmd.Table.StringLocked())
// 		} else if cmd.DB != nil {
// 			logutil.Infof("%s", cmd.DB.StringLocked())
// 		}
// 	}
// }
