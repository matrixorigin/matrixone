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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
)

type Entry = wal.Entry

type LogEntryType = logstore.EntryType
type LogEntry = logstore.AsyncEntry
type LogEntryMeta = logstore.EntryMeta

const (
	SafeIdSize = int(unsafe.Sizeof(SafeId{}))
)

const (
	ETShardWalStart = uint16(30)
)

const (
	ETShardWalSafeId LogEntryType = iota + ETShardWalStart
	ETShardWalCheckpoint
)

type SafeId struct {
	ShardId, Id uint64
}

func (id *SafeId) Marshal() ([]byte, error) {
	buf := make([]byte, SafeIdSize)
	id.MarshalTo(buf)
	return buf, nil
}

func (id *SafeId) MarshalTo(buf []byte) error {
	binary.BigEndian.PutUint64(buf[:unsafe.Sizeof(id.ShardId)], id.ShardId)
	binary.BigEndian.PutUint64(buf[unsafe.Sizeof(id.ShardId):], id.Id)
	return nil
}

func (id *SafeId) Unmarshal(buf []byte) error {
	id.ShardId = binary.BigEndian.Uint64(buf[:unsafe.Sizeof(id.ShardId)])
	id.Id = binary.BigEndian.Uint64(buf[unsafe.Sizeof(id.ShardId):])
	return nil
}

type SafeIds struct {
	Ids []SafeId
}

func (ids *SafeIds) Marshal() ([]byte, error) {
	size := len(ids.Ids) * SafeIdSize
	buf := make([]byte, size)
	for i, id := range ids.Ids {
		id.MarshalTo(buf[i*SafeIdSize : (i+1)*SafeIdSize])
	}
	return buf, nil
}

func (ids *SafeIds) Unmarshal(buf []byte) error {
	ids.Ids = make([]SafeId, len(buf)/SafeIdSize)
	for i, id := range ids.Ids {
		id.Unmarshal(buf[i*SafeIdSize : (i+1)*SafeIdSize])
	}
	return nil
}

func (ids *SafeIds) Append(shardId, id uint64) {
	ids.Ids = append(ids.Ids, SafeId{ShardId: shardId, Id: id})
}

func SafeIdToEntry(id SafeId) LogEntry {
	entry := logstore.NewAsyncBaseEntry()
	entry.Meta.SetType(ETShardWalSafeId)
	buf, _ := id.Marshal()
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

func SafeIdsToEntry(ids SafeIds) LogEntry {
	entry := logstore.NewAsyncBaseEntry()
	entry.Meta.SetType(ETShardWalCheckpoint)
	buf, _ := ids.Marshal()
	if err := entry.Unmarshal(buf); err != nil {
		panic(err)
	}
	return entry
}

func EntryToSafeIds(entry LogEntry) (ids SafeIds, err error) {
	payload := entry.GetPayload()
	err = ids.Unmarshal(payload)
	return
}
