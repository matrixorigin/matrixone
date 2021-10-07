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

package logstore

import (
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	mockETDDL = ETCustomizeStart + 1
)

type mockDDLOp uint8

const (
	mockCreateOp mockDDLOp = iota
	mockDropOp
)

type mockDDLEntry struct {
	BaseEntry
}

func newMockDDLEntry(op mockDDLOp, data []byte) *mockDDLEntry {
	payload := make([]byte, len(data)+1)
	payload[0] = byte(op)
	copy(payload[1:], data)
	entry := newEmptyDDLEntry(nil)
	entry.Unmarshal(payload)
	entry.Meta.SetType(mockETDDL)
	entry.Meta.SetPayloadSize(uint32(len(payload)))
	return entry
}

func newEmptyDDLEntry(meta *EntryMeta) *mockDDLEntry {
	entry := new(mockDDLEntry)
	if meta == nil {
		entry.BaseEntry = *newBaseEntry()
		entry.Meta.SetType(mockETDDL)
	} else {
		entry.BaseEntry = *NewBaseEntryWithMeta(meta)
	}
	return entry
}

func mockETDDLHandler(r io.Reader, meta *EntryMeta) (Entry, int64, error) {
	entry := newEmptyDDLEntry(meta)
	n, err := entry.ReadFrom(r)
	if err != nil {
		return nil, int64(n), err
	}
	return entry, int64(n), err
}

func TestStore(t *testing.T) {
	dir := "/tmp/teststore"
	os.RemoveAll(dir)
	name := "sstore"
	roCfg := &RotationCfg{}
	store, err := New(dir, name, roCfg)
	assert.Nil(t, err)

	buf := make([]byte, 8)
	step := 5
	uncommitted := make([]Entry, 0)
	committed := make([]Entry, 0)
	for i := 0; i < 13; i++ {
		binary.BigEndian.PutUint64(buf, uint64(i))
		e := newMockDDLEntry(mockCreateOp, buf)
		err = store.AppendEntry(e)
		assert.Nil(t, err)
		uncommitted = append(uncommitted, e)
		if i%step == step-1 {
			err = store.Sync()
			assert.Nil(t, err)
			committed = append(committed, uncommitted...)
			uncommitted = uncommitted[:0]
		}
	}
	store.Close()

	roCfg = &RotationCfg{}
	store, err = New(dir, name, roCfg)
	assert.Nil(t, err)
	defer store.Close()

	replayer := NewSimpleReplayer()
	err = replayer.RegisterEntryHandler(mockETDDL, mockETDDLHandler)
	assert.Nil(t, err)
	err = replayer.Replay(store)
	assert.Nil(t, err)
	t.Log(replayer.GetOffset())
	assert.Equal(t, len(committed), len(replayer.committed))

	err = replayer.Truncate(store)
	assert.Nil(t, err)
}
