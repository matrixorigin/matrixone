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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

var (
	moduleName = "logstore"
	mockETDDL  = ETCustomizeStart + 1
)

type mockDDLOp uint8

const (
	mockCreateOp mockDDLOp = iota
	// mockDropOp // Unused
)

type mockDDLEntry struct {
	BaseEntry
}

func newMockDDLEntry(op mockDDLOp, data []byte) *mockDDLEntry {
	payload := make([]byte, len(data)+1)
	payload[0] = byte(op)
	copy(payload[1:], data)
	entry := newEmptyDDLEntry(nil)
	err := entry.Unmarshal(payload)
	if err != nil {
		logutil.Warnf("%v", err)
	}
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
		return nil, n, err
	}
	return entry, n, err
}

func TestStore(t *testing.T) {
	dir := testutils.InitTestEnv(moduleName, t)
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

// func TestAynscEntry(t *testing.T) {
// 	queue := make(chan *AsyncBaseEntry, 1000)
// 	doneq := make(chan *AsyncBaseEntry, 1000)
// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	ctx, cancel := context.WithCancel(context.Background())
// 	ctx2, cancel2 := context.WithCancel(context.Background())
// 	var wg1 sync.WaitGroup
// 	var wg2 sync.WaitGroup
// 	go func() {
// 		defer wg.Done()
// 		for {
// 			select {
// 			case <-ctx.Done():
// 				return
// 			case entry := <-queue:
// 				// id := entry.GetPayload().(int)
// 				// t.Logf("Processing request %d", id)
// 				entry.DoneWithErr(nil)
// 				wg1.Done()
// 			}
// 		}
// 	}()
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for {
// 			select {
// 			case <-ctx2.Done():
// 				return
// 			case entry := <-doneq:
// 				// id := entry.GetPayload().(int)
// 				// t.Logf("Done request %d", id)
// 				entry.WaitDone()
// 				entry.Free()
// 				wg2.Done()
// 			}
// 		}
// 	}()

// 	for i := 0; i < 1000; i++ {
// 		entry := NewAsyncBaseEntry(i)
// 		wg1.Add(1)
// 		queue <- entry

// 		wg2.Add(1)
// 		doneq <- entry
// 	}

// 	wg1.Wait()
// 	wg2.Wait()

// 	cancel()
// 	cancel2()
// 	wg.Wait()
// }

func TestBatchStore(t *testing.T) {
	dir := testutils.InitTestEnv(moduleName, t)
	cfg := &RotationCfg{
		RotateChecker: &MaxSizeRotationChecker{MaxSize: 100 * int(common.M)},
	}
	s, err := NewBatchStore(dir, "basestore", cfg)
	assert.Nil(t, err)
	s.Start()
	defer s.Close()
	var wg sync.WaitGroup
	pool, _ := ants.NewPool(100)

	f := func(i int) func() {
		return func() {
			defer wg.Done()
			entry := NewAsyncBaseEntry()
			entry.GetMeta().SetType(ETFlush)
			err := s.AppendEntry(entry)
			assert.Nil(t, err)
			err = entry.WaitDone()
			assert.Nil(t, err)
			entry.Free()
		}
	}

	for i := 0; i < 200; i++ {
		wg.Add(1)
		err := pool.Submit(f(i))
		assert.Nil(t, err)
	}
	wg.Wait()
}
