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

package shard

import (
	"encoding/binary"
	"matrixone/pkg/vm/engine/aoe/storage/logstore"
	"matrixone/pkg/vm/engine/aoe/storage/wal"
	"unsafe"
)

type Entry = wal.Entry

type LogEntryType = logstore.EntryType
type LogEntry = logstore.AsyncEntry
type LogEntryMeta = logstore.EntryMeta

const (
	ETShardWalStart = uint16(30)
)

const (
	ETShardWalSafeId LogEntryType = iota + ETShardWalStart
)

type SafeId struct {
	ShardId, Id uint64
}

func (id *SafeId) Marshal() ([]byte, error) {
	buf := make([]byte, unsafe.Sizeof(id))
	binary.BigEndian.PutUint64(buf[:unsafe.Sizeof(id.ShardId)], id.ShardId)
	binary.BigEndian.PutUint64(buf[unsafe.Sizeof(id.ShardId):], id.Id)
	return buf, nil
}

func (id *SafeId) Unmarshal(buf []byte) error {
	id.ShardId = binary.BigEndian.Uint64(buf[:unsafe.Sizeof(id.ShardId)])
	id.Id = binary.BigEndian.Uint64(buf[unsafe.Sizeof(id.ShardId):])
	return nil
}

func SafeIdToEntry(id SafeId) LogEntry {
	entry := logstore.NewAsyncBaseEntry()
	entry.Meta.SetType(ETShardWalSafeId)
	buf := make([]byte, unsafe.Sizeof(id))
	binary.BigEndian.PutUint64(buf[:unsafe.Sizeof(id.ShardId)], id.ShardId)
	binary.BigEndian.PutUint64(buf[unsafe.Sizeof(id.ShardId):], id.Id)
	if err := entry.Unmarshal(buf); err != nil {
		panic(err)
	}
	return entry
}

func EntryToSafeId(entry LogEntry) (id SafeId, err error) {
	payload := entry.GetPayload()
	err = id.Unmarshal(payload)
	return
}
