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

package store

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func appendUncommitEntry(t *testing.T, s *baseStore, buf []byte, tid uint64) entry.Entry {
	e := entry.GetBase()
	uncommitInfo := &entry.Info{
		Group: entry.GTUncommit,
		Uncommits: []entry.Tid{{
			Group: 11,
			Tid:   tid,
		}},
	}
	e.SetType(entry.ETUncommitted)
	e.SetInfo(uncommitInfo)
	n := common.GPool.Alloc(common.K)
	copy(n.GetBuf(), buf)
	err := e.UnmarshalFromNode(n, true)
	assert.Nil(t, err)
	_, err = s.AppendEntry(entry.GTUncommit, e)
	assert.Nil(t, err)
	return e
}

func appendCommitEntry(t *testing.T, s *baseStore, buf []byte, tid uint64) entry.Entry {
	txnInfo := &entry.Info{
		Group: 11,
		TxnId: tid,
	}
	e := entry.GetBase()
	e.SetType(entry.ETTxn)
	e.SetInfo(txnInfo)
	n := common.GPool.Alloc(common.K)
	copy(n.GetBuf(), buf)
	err := e.UnmarshalFromNode(n, true)
	assert.Nil(t, err)
	_, err = s.AppendEntry(11, e)
	assert.Nil(t, err)
	return e
}

func appendAnotherEntry(t *testing.T, s *baseStore, buf []byte) {
	txnInfo := &entry.Info{
		Group: 12,
	}
	e := entry.GetBase()
	e.SetType(entry.ETTxn)
	e.SetInfo(txnInfo)
	n := common.GPool.Alloc(common.K)
	copy(n.GetBuf(), buf)
	err := e.UnmarshalFromNode(n, true)
	assert.Nil(t, err)
	_, err = s.AppendEntry(12, e)
	assert.Nil(t, err)
	assert.Nil(t, e.WaitDone())
	e.Free()
}

func appendCkpEntry(t *testing.T, s *baseStore, lsn uint64) entry.Entry {
	info := &entry.Info{
		Group: entry.GTCKp,
		Checkpoints: []entry.CkpRanges{{
			Group:  11,
			Ranges: common.NewClosedIntervalsByInterval(&common.ClosedInterval{Start: 0, End: lsn}),
		}},
	}
	e := entry.GetBase()
	e.SetType(entry.ETCheckpoint)
	e.SetInfo(info)
	_, err := s.AppendEntry(entry.GTCKp, e)
	assert.Nil(t, err)
	return e
}

func appendPartialCkpEntry(t *testing.T, s *baseStore, lsn uint64, csns []uint32, size uint32) entry.Entry {
	cmd := entry.CommandInfo{
		Size:       size,
		CommandIds: csns,
	}
	cmds := make(map[uint64]entry.CommandInfo)
	cmds[lsn] = cmd
	info := &entry.Info{
		Group: entry.GTCKp,
		Checkpoints: []entry.CkpRanges{{
			Group:   11,
			Command: cmds,
		}},
	}
	e := entry.GetBase()
	e.SetType(entry.ETCheckpoint)
	e.SetInfo(info)
	_, err := s.AppendEntry(entry.GTCKp, e)
	assert.Nil(t, err)
	return e
}

// append UC, C, CKP
func appendEntries(t *testing.T, s *baseStore, buf []byte, tid uint64) {
	e := entry.GetBase()
	uncommitInfo := &entry.Info{
		Group: entry.GTUncommit,
		Uncommits: []entry.Tid{{
			Group: 11,
			Tid:   tid,
		}},
	}
	e.SetType(entry.ETUncommitted)
	e.SetInfo(uncommitInfo)
	n := common.GPool.Alloc(common.K)
	copy(n.GetBuf(), buf)
	err := e.UnmarshalFromNode(n, true)
	assert.Nil(t, err)
	_, err = s.AppendEntry(entry.GTUncommit, e)
	assert.Nil(t, err)
	err = e.WaitDone()
	assert.Nil(t, err)
	e.Free()

	txnInfo := &entry.Info{
		Group: 11,
		TxnId: tid,
	}
	e = entry.GetBase()
	e.SetType(entry.ETTxn)
	e.SetInfo(txnInfo)
	n = common.GPool.Alloc(common.K)
	copy(n.GetBuf(), buf)
	err = e.UnmarshalFromNode(n, true)
	assert.Nil(t, err)
	cmtLsn, err := s.AppendEntry(11, e)
	assert.Nil(t, err)
	assert.Nil(t, e.WaitDone())
	e.Free()

	cmd := entry.CommandInfo{
		Size:       2,
		CommandIds: []uint32{0, 1},
	}
	cmds := make(map[uint64]entry.CommandInfo)
	cmds[cmtLsn] = cmd
	info := &entry.Info{
		Group: entry.GTCKp,
		Checkpoints: []entry.CkpRanges{{
			Group:   11,
			Command: cmds,
		}},
	}
	e = entry.GetBase()
	e.SetType(entry.ETCheckpoint)
	e.SetInfo(info)
	_, err = s.AppendEntry(entry.GTCKp, e)
	assert.Nil(t, err)
	assert.Nil(t, e.WaitDone())
	e.Free()
}

func initEnv(t *testing.T) (*baseStore, []byte) {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 3),
	}
	var bs bytes.Buffer
	for i := 0; i < 3000; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()

	s, err := NewBaseStore(dir, name, cfg)
	assert.Nil(t, err)
	return s, buf
}

func restartStore(t *testing.T, s *baseStore) *baseStore {
	t.Log(s.file.GetHistory().String())
	err := s.Close()
	assert.Nil(t, err)

	t.Log("******************Replay*********************")

	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 3),
	}
	s, err = NewBaseStore(s.dir, s.name, cfg)
	assert.Nil(t, err)
	pre := uint64(0)
	a := func(group uint32, commitId uint64, payload []byte, typ uint16, info any) {
		if group == 11 {
			assert.Less(t, pre, commitId)
			pre = commitId
		}
		// fmt.Printf("%s", payload)
	}
	err = s.Replay(a)
	assert.Nil(t, err)
	t.Log(s.file.GetHistory().String())
	return s
}

func logSyncbase(t *testing.T, s *baseStore) {
	t.Log(s.groupLSN)
	t.Log(s.checkpointing)
	t.Log(s.syncing)
	t.Log(s.checkpointed.ids)
	t.Log(s.synced.ids)
	t.Log(s.ckpCnt.ids)
	t.Log(s.tidLsnMaps)
	t.Log(s.addrs)
}

// append C and lots of CKP
// restart
// check CKPed
// append AnotherEntry
// check Compact
func TestReplay3(t *testing.T) {
	s, buf := initEnv(t)
	ckpSize := 105

	e1 := appendCommitEntry(t, s, buf, 1)
	entries := make([]entry.Entry, ckpSize)
	for i := 0; i < ckpSize; i++ {
		entries[i] = appendPartialCkpEntry(t, s, 1, []uint32{uint32(i)}, uint32(ckpSize))
	}
	assert.Nil(t, entries[len(entries)-1].WaitDone())
	e1.Free()
	for i := 0; i < ckpSize; i++ {
		entries[i].Free()
	}

	s = restartStore(t, s)
	t.Log(s.file.(*rotateFile).uncommitted[0].String())
	assert.Equal(t, uint64(1), s.GetCheckpointed(11))

	appendAnotherEntry(t, s, buf)
	appendAnotherEntry(t, s, buf)
	testutils.WaitExpect(400, func() bool {
		version := atomic.LoadUint64(&s.syncedVersion)
		ids := s.file.GetHistory().EntryIds()
		committedversion := uint64(ids[len(ids)-1])
		return version == committedversion
	})
	version := atomic.LoadUint64(&s.syncedVersion)
	ids := s.file.GetHistory().EntryIds()
	committedversion := uint64(ids[len(ids)-1])
	assert.Equal(t, version, committedversion)

	t.Log(s.file.GetHistory().String())
	assert.Nil(t, s.TryCompact())
	t.Log(s.file.GetHistory().String())
	t.Log(s.file.(*rotateFile).uncommitted[0].String())

	assert.Equal(t, 0, len(s.file.GetHistory().EntryIds()))
	assert.Nil(t, s.Close())

}
func TestMergePartialCKP(t *testing.T) {
	s, buf := initEnv(t)

	e1 := appendCommitEntry(t, s, buf, 1)
	e2 := appendUncommitEntry(t, s, buf, 1)
	e3 := appendPartialCkpEntry(t, s, 1, []uint32{0}, 2)
	assert.Nil(t, e3.WaitDone())
	e1.Free()
	e2.Free()
	e3.Free()
	assert.Equal(t, 0, len(s.file.GetHistory().EntryIds()))

	appendAnotherEntry(t, s, buf)
	e2 = appendPartialCkpEntry(t, s, 1, []uint32{1}, 2)
	assert.Nil(t, e2.WaitDone())
	e2.Free()
	testutils.WaitExpect(400, func() bool {
		return s.GetCheckpointed(11) == 1
	})
	assert.Equal(t, uint64(1), s.GetCheckpointed(11))

	testutils.WaitExpect(4000, func() bool {
		preversion := atomic.LoadUint64(&s.syncedVersion)
		return preversion >= 1
	})
	preversion := atomic.LoadUint64(&s.syncedVersion)
	assert.Less(t, uint64(0), preversion)

	assert.Nil(t, s.TryCompact())
	assert.Equal(t, 0, len(s.file.GetHistory().EntryIds()))

	s = restartStore(t, s)

	assert.Equal(t, uint64(1), s.GetCheckpointed(11))

	assert.Nil(t, s.Close())
}

// uncommit, commit, ckp  vf1
// ckp all                vf2
// truncate
// check vinfo not exist
// uncommit, commit       vf2
// replay
// ckp all                vf3
// truncate
// check vinfo not exist
func TestTruncate1(t *testing.T) {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 3),
	}
	var bs bytes.Buffer
	for i := 0; i < 3000; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()

	s, err := NewBaseStore(dir, name, cfg)
	assert.Nil(t, err)

	appendEntries(t, s, buf, 1)
	appendEntries(t, s, buf, 2)
	appendEntries(t, s, buf, 3)

	assert.Equal(t, 2, len(s.file.GetHistory().EntryIds()))
	t.Log(s.file.GetHistory().String())

	testutils.WaitExpect(400, func() bool {
		return s.GetCheckpointed(11) == 3
	})
	assert.Equal(t, uint64(3), s.GetCheckpointed(11))

	testutils.WaitExpect(400, func() bool {
		return s.GetSynced(entry.GTInternal) >= 2
	})
	assert.Less(t, uint64(1), s.GetSynced(4))

	assert.Nil(t, s.TryCompact())

	assert.Equal(t, 0, len(s.file.GetHistory().EntryIds()))
	t.Log(s.file.GetHistory().String())
	err = s.Close()
	assert.Nil(t, err)

	t.Log("******************Replay*********************")

	s2, err := NewBaseStore(dir, name, cfg)
	assert.Nil(t, err)
	a := func(group uint32, commitId uint64, payload []byte, typ uint16, info any) {
		// fmt.Printf("%s", payload)
	}
	err = s2.Replay(a)
	assert.Nil(t, err)

	appendEntries(t, s2, buf, 4)

	assert.Equal(t, 1, len(s2.file.GetHistory().EntryIds()))
	t.Log(s2.file.GetHistory().String())

	testutils.WaitExpect(400, func() bool {
		return s2.GetCheckpointed(11) == 4
	})
	assert.Equal(t, uint64(4), s2.GetCheckpointed(11))

	testutils.WaitExpect(400, func() bool {
		return s2.GetSynced(entry.GTInternal) >= 3
	})
	assert.Less(t, uint64(2), s2.GetSynced(4))

	assert.Nil(t, s2.TryCompact())

	assert.Equal(t, 0, len(s2.file.GetHistory().EntryIds()))
	t.Log(s2.file.GetHistory().String())

	err = s2.Close()
	assert.Nil(t, err)
}

func TestTruncate2(t *testing.T) {
	s, buf := initEnv(t)
	tidAlloc := &common.IdAllocator{}
	cmtEntryCnt := 5
	wg := sync.WaitGroup{}
	worker, _ := ants.NewPool(cmtEntryCnt)
	f := func() {
		tid := tidAlloc.Alloc()
		e1 := appendUncommitEntry(t, s, buf, tid)
		e2 := appendCommitEntry(t, s, buf, tid)
		err := e1.WaitDone()
		assert.Nil(t, err)
		err = e2.WaitDone()
		assert.Nil(t, err)
		e1.Free()
		e2.Free()
		wg.Done()
	}
	wg.Add(cmtEntryCnt)
	for i := 0; i < cmtEntryCnt; i++ {
		err := worker.Submit(f)
		assert.Nil(t, err)
	}
	wg.Wait()

	s = restartStore(t, s)

	assert.Equal(t, uint64(cmtEntryCnt), s.GetCurrSeqNum(11))
	e := appendCkpEntry(t, s, uint64(cmtEntryCnt))
	err := e.WaitDone()
	assert.Nil(t, err)
	e.Free()
	testutils.WaitExpect(400, func() bool {
		return s.GetCheckpointed(11) == uint64(cmtEntryCnt)
	})
	assert.Equal(t, uint64(cmtEntryCnt), s.GetCheckpointed(11))
	err = s.TryCompact()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(s.file.GetHistory().EntryIds()))
	t.Log(s.file.GetHistory().String())
}

//1. vf1 - vfn: C & lots of CKP
//2. vf n+1: anotherEntry
//3. compact
//4. replay
//5. check ckped
func TestTruncate3(t *testing.T) {
	s, buf := initEnv(t)
	ckpSize := 100

	e1 := appendCommitEntry(t, s, buf, 1)
	entries := make([]entry.Entry, ckpSize)
	for i := 0; i < ckpSize; i++ {
		entries[i] = appendPartialCkpEntry(t, s, 1, []uint32{uint32(i)}, uint32(ckpSize))
	}

	appendAnotherEntry(t, s, buf)
	appendAnotherEntry(t, s, buf)

	e1.Free()
	for i := 0; i < ckpSize; i++ {
		entries[i].Free()
	}

	_ = s.TryCompact()
	testutils.WaitExpect(400, func() bool {
		return uint64(1) == s.GetCheckpointed(11)
	})
	assert.Equal(t, uint64(1), s.GetCheckpointed(11))
	t.Log(s.GetCurrSeqNum(4))

	s = restartStore(t, s)
	t.Log(s.addrs)

	testutils.WaitExpect(400, func() bool {
		return uint64(1) == s.GetCheckpointed(11)
	})
	assert.Equal(t, uint64(1), s.GetCheckpointed(11))

	s.Close()
}

// append {C,UC,CKP}s
// wait until Internal Group Flushed
// restart
// append {C,UC}
// check synced
func TestReplay2(t *testing.T) {
	s, buf := initEnv(t)
	tidAlloc := &common.IdAllocator{}
	cmtEntryCnt := 5
	wg := sync.WaitGroup{}
	worker, _ := ants.NewPool(cmtEntryCnt)
	f := func() {
		tid := tidAlloc.Alloc()
		e1 := appendUncommitEntry(t, s, buf, tid)
		e2 := appendCommitEntry(t, s, buf, tid)
		e3 := appendPartialCkpEntry(t, s, e2.GetInfo().(*entry.Info).GroupLSN, []uint32{0}, 2)
		err := e1.WaitDone()
		assert.Nil(t, err)
		err = e2.WaitDone()
		assert.Nil(t, err)
		err = e3.WaitDone()
		assert.Nil(t, err)
		e1.Free()
		e2.Free()
		e3.Free()
		wg.Done()
	}
	wg.Add(cmtEntryCnt)
	for i := 0; i < cmtEntryCnt; i++ {
		err := worker.Submit(f)
		assert.Nil(t, err)
	}
	wg.Wait()

	//check syncbase
	testutils.WaitExpect(400, func() bool {
		return s.GetSynced(entry.GTCKp) == s.GetCurrSeqNum(entry.GTCKp)
	})
	testutils.WaitExpect(400, func() bool {
		version := atomic.LoadUint64(&s.syncedVersion)
		ids := s.file.GetHistory().EntryIds()
		committedversion := uint64(ids[len(ids)-1])
		return version == committedversion
	})
	assert.Equal(t, s.GetSynced(entry.GTCKp), s.GetCurrSeqNum(entry.GTCKp))

	// logSyncbase(t, s)
	s = restartStore(t, s)
	// logSyncbase(t, s)

	tid := tidAlloc.Alloc()
	e1 := appendUncommitEntry(t, s, buf, tid)
	e2 := appendCommitEntry(t, s, buf, tid)
	err := e1.WaitDone()
	assert.Nil(t, err)
	err = e2.WaitDone()
	assert.Nil(t, err)
	e1.Free()
	e2.Free()
	testutils.WaitExpect(400, func() bool {
		return s.GetSynced(11) == s.GetCurrSeqNum(11)
	})
	assert.Equal(t, s.GetSynced(11), s.GetCurrSeqNum(11))
	testutils.WaitExpect(400, func() bool {
		version := atomic.LoadUint64(&s.syncedVersion)
		ids := s.file.GetHistory().EntryIds()
		committedversion := uint64(ids[len(ids)-1])
		return version == committedversion
	})
	testutils.WaitExpect(400, func() bool {
		return s.GetSynced(4) == s.GetCurrSeqNum(4)
	})
	assert.Equal(t, s.GetSynced(4), s.GetCurrSeqNum(4))
	assert.Equal(t, s.GetSynced(entry.GTCKp), s.GetCurrSeqNum(entry.GTCKp))
	logSyncbase(t, s)
	s.Close()
}

func TestAddrVersion(t *testing.T) {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 3),
	}
	s, err := NewBaseStore(dir, name, cfg)
	assert.Nil(t, err)
	defer s.Close()

	var bs bytes.Buffer
	for i := 0; i < 3000; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()

	for i := 0; i < 10; i++ {
		e := entry.GetBase()
		uncommitInfo := &entry.Info{
			Group: entry.GTUncommit,
			Uncommits: []entry.Tid{{
				Group: 11,
				Tid:   1,
			}},
		}
		e.SetType(entry.ETUncommitted)
		e.SetInfo(uncommitInfo)
		n := common.GPool.Alloc(common.K)
		copy(n.GetBuf(), buf)
		err = e.UnmarshalFromNode(n, true)
		assert.Nil(t, err)
		_, err = s.AppendEntry(entry.GTUncommit, e)
		assert.Nil(t, err)
		err := e.WaitDone()
		assert.Nil(t, err)
		e.Free()
	}

	testutils.WaitExpect(4000, func() bool {
		t.Log(s.GetSynced(entry.GTUncommit))
		return s.GetSynced(entry.GTUncommit) == 10
	})
	s.addrmu.RLock()
	defer s.addrmu.RUnlock()
	assert.Equal(t, 5, len(s.addrs[entry.GTUncommit]))
	t.Log(s.addrs[entry.GTUncommit])
	for version, lsns := range s.addrs[entry.GTUncommit] {
		assert.True(t, lsns.Contains(*common.NewClosedIntervalsByInt(uint64(version)*2 - 1)))
		assert.True(t, lsns.Contains(*common.NewClosedIntervalsByInt(uint64(version) * 2)))
	}
}

func TestStore(t *testing.T) {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 2000),
	}
	s, err := NewBaseStore(dir, name, cfg)
	assert.Nil(t, err)
	defer s.Close()

	var wg sync.WaitGroup
	var fwg sync.WaitGroup
	ch := make(chan entry.Entry, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-ch:
				err := e.WaitDone()
				assert.Nil(t, err)
				v := e.GetInfo()
				e.Free()
				if v != nil {
					info := v.(*entry.Info)
					t.Logf("group-%d", info.Group)
					t.Logf("synced %d", s.GetSynced(info.Group))
					t.Logf("checkpointed %d", s.GetCheckpointed(info.Group))
					t.Logf("penddings %d", s.GetPenddingCnt(info.Group))
				}
				fwg.Done()
			}
		}
	}()

	var bs bytes.Buffer
	for i := 0; i < 3000; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()

	entryPerGroup := 1000
	groupCnt := 1
	worker, _ := ants.NewPool(groupCnt)
	fwg.Add(entryPerGroup * groupCnt)
	f := func(groupNo uint32) func() {
		return func() {
			tidAlloc := &common.IdAllocator{}
			pre := uint64(0)
			ckp := uint64(0)
			for i := 0; i < entryPerGroup; i++ {
				e := entry.GetBase()
				switch i % 100 {
				case 1, 2, 3, 4, 5:
					uncommitInfo := &entry.Info{
						Group: entry.GTUncommit,
						Uncommits: []entry.Tid{{
							Group: groupNo,
							Tid:   tidAlloc.Get() + 1 + uint64(rand.Intn(3)),
						}},
					}
					e.SetType(entry.ETUncommitted)
					e.SetInfo(uncommitInfo)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					err := e.UnmarshalFromNode(n, true)
					assert.Nil(t, err)
					_, err = s.AppendEntry(entry.GTUncommit, e)
					assert.Nil(t, err)
				case 99:
					end := ckp - 1
					checkpointInfo := &entry.Info{
						Group: entry.GTCKp,
						Checkpoints: []entry.CkpRanges{{
							Group: groupNo,
							Ranges: common.NewClosedIntervalsByInterval(
								&common.ClosedInterval{
									Start: pre,
									End:   end,
								}),
							Command: map[uint64]entry.CommandInfo{(end + 1): {
								CommandIds: []uint32{0},
								Size:       1,
							}},
						}},
					}
					pre = end
					e.SetType(entry.ETCheckpoint)
					e.SetInfo(checkpointInfo)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					err := e.UnmarshalFromNode(n, true)
					assert.Nil(t, err)
					_, err = s.AppendEntry(entry.GTCKp, e)
					assert.Nil(t, err)
				case 50, 51, 52, 53:
					txnInfo := &entry.Info{
						Group: groupNo,
						TxnId: tidAlloc.Alloc(),
					}
					e.SetType(entry.ETTxn)
					e.SetInfo(txnInfo)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					err := e.UnmarshalFromNode(n, true)
					assert.Nil(t, err)
					lsn, err := s.AppendEntry(groupNo, e)
					assert.Nil(t, err)
					ckp = lsn
				default:
					commitInterval := &entry.Info{
						Group: groupNo,
					}
					e.SetType(entry.ETCustomizedStart)
					e.SetInfo(commitInterval)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					err := e.UnmarshalFromNode(n, true)
					assert.Nil(t, err)
					lsn, err := s.AppendEntry(groupNo, e)
					assert.Nil(t, err)
					ckp = lsn
				}
				ch <- e
			}
		}
	}

	for j := 0; j < groupCnt; j++ {
		err := worker.Submit(f(uint32(j) + entry.GTCustomizedStart))
		assert.Nil(t, err)
	}

	fwg.Wait()
	cancel()
	wg.Wait()

	err = s.TryCompact()
	assert.Nil(t, err)
}

func TestPartialCkp(t *testing.T) {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 2),
	}
	s, err := NewBaseStore(dir, name, cfg)
	assert.Nil(t, err)

	var bs bytes.Buffer
	for i := 0; i < 300; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()

	uncommit := entry.GetBase()
	uncommitInfo := &entry.Info{
		Group: entry.GTUncommit,
		Uncommits: []entry.Tid{{
			Group: entry.GTCustomizedStart,
			Tid:   1,
		}},
	}
	uncommit.SetInfo(uncommitInfo)
	buf2 := make([]byte, common.K)
	copy(buf2, buf)
	err = uncommit.Unmarshal(buf2)
	assert.Nil(t, err)
	lsn, err := s.AppendEntry(entry.GTUncommit, uncommit)
	assert.Nil(t, err)
	err = uncommit.WaitDone()
	assert.Nil(t, err)
	uncommit.Free()
	testutils.WaitExpect(400, func() bool {
		_, err = s.Load(entry.GTUncommit, lsn)
		return err == nil
	})
	_, err = s.Load(entry.GTUncommit, lsn)
	assert.Nil(t, err)

	commit := entry.GetBase()
	commitInfo := &entry.Info{
		Group: entry.GTCustomizedStart,
		TxnId: 1,
	}
	commit.SetInfo(commitInfo)
	buf2 = make([]byte, common.K)
	copy(buf2, buf)
	err = commit.Unmarshal(buf2)
	assert.Nil(t, err)
	commitLsn, err := s.AppendEntry(entry.GTCustomizedStart, commit)
	assert.Nil(t, err)

	ckp1 := entry.GetBase()
	checkpointInfo := &entry.Info{
		Group: entry.GTCKp,
		Checkpoints: []entry.CkpRanges{{
			Group: entry.GTCustomizedStart,
			Command: map[uint64]entry.CommandInfo{commitLsn: {
				CommandIds: []uint32{0},
				Size:       2,
			}},
		}},
	}
	ckp1.SetInfo(checkpointInfo)
	buf2 = make([]byte, common.K)
	copy(buf2, buf)
	err = ckp1.Unmarshal(buf2)
	assert.Nil(t, err)
	_, err = s.AppendEntry(entry.GTCKp, ckp1)
	assert.Nil(t, err)

	ckp2 := entry.GetBase()
	checkpointInfo2 := &entry.Info{
		Group: entry.GTCKp,
		Checkpoints: []entry.CkpRanges{{
			Group: entry.GTCustomizedStart,
			Command: map[uint64]entry.CommandInfo{commitLsn: {
				CommandIds: []uint32{1},
				Size:       2,
			}},
		}},
	}
	ckp2.SetInfo(checkpointInfo2)
	buf2 = make([]byte, common.K)
	copy(buf2, buf)
	err = ckp2.Unmarshal(buf2)
	assert.Nil(t, err)
	_, err = s.AppendEntry(entry.GTCKp, ckp2)
	assert.Nil(t, err)

	anotherEntry := entry.GetBase()
	commitInfo = &entry.Info{
		Group: entry.GTCustomizedStart + 1,
	}
	anotherEntry.SetInfo(commitInfo)
	buf2 = make([]byte, common.K)
	copy(buf2, buf)
	err = anotherEntry.Unmarshal(buf2)
	assert.Nil(t, err)
	_, err = s.AppendEntry(entry.GTCustomizedStart, anotherEntry)
	assert.Nil(t, err)
	err = anotherEntry.WaitDone()
	assert.Nil(t, err)

	commit.Free()
	ckp1.Free()
	ckp2.Free()
	anotherEntry.Free()

	testutils.WaitExpect(400, func() bool {
		return s.GetCheckpointed(entry.GTCustomizedStart) == 1
	})
	err = s.TryCompact()
	assert.Nil(t, err)
	_, err = s.Load(entry.GTUncommit, lsn)
	assert.NotNil(t, err)

	s.Close()
}

func TestReplay1(t *testing.T) {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 2000),
	}
	s, err := NewBaseStore(dir, name, cfg)
	assert.Nil(t, err)

	var wg sync.WaitGroup
	var fwg sync.WaitGroup
	ch := make(chan entry.Entry, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-ch:
				info := e.GetInfo()
				err := e.WaitDone()
				e.Free()
				assert.Nil(t, err)
				if info != nil {
					groupNo := info.(*entry.Info).Group
					t.Logf("group %d", groupNo)
					t.Logf("synced %d", s.GetSynced(groupNo))
					t.Logf("checkpointed %d", s.GetCheckpointed(groupNo))
					t.Logf("penddings %d", s.GetPenddingCnt(groupNo))
				}
				fwg.Done()
			}
		}
	}()

	entryPerGroup := 50
	groupCnt := 2
	worker, _ := ants.NewPool(groupCnt)
	fwg.Add(entryPerGroup * groupCnt)
	f := func(groupNo uint32) func() {
		return func() {
			tidAlloc := &common.IdAllocator{}
			pre := uint64(0)
			ckp := uint64(0)
			for i := 0; i < entryPerGroup; i++ {
				e := entry.GetBase()
				switch i % 50 {
				case 1, 2, 3, 4, 5: //uncommit entry
					e.SetType(entry.ETUncommitted)
					uncommitInfo := &entry.Info{
						Group: entry.GTUncommit,
						Uncommits: []entry.Tid{{
							Group: groupNo,
							Tid:   tidAlloc.Get() + 1 + uint64(rand.Intn(3)),
						}},
					}
					e.SetInfo(uncommitInfo)
					str := uncommitInfo.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					err := e.UnmarshalFromNode(n, true)
					assert.Nil(t, err)
					_, err = s.AppendEntry(entry.GTUncommit, e)
					assert.Nil(t, err)
				case 49: //ckp entry
					e.SetType(entry.ETCheckpoint)
					end := ckp
					checkpointInfo := &entry.Info{
						Group: entry.GTCKp,
						Checkpoints: []entry.CkpRanges{{
							Group: groupNo,
							Ranges: common.NewClosedIntervalsByInterval(
								&common.ClosedInterval{
									Start: pre + 1,
									End:   end,
								}),
						}},
					}
					pre = end
					e.SetInfo(checkpointInfo)
					_, err := s.AppendEntry(entry.GTCKp, e)
					assert.Nil(t, err)
				case 20, 21, 22, 23: //txn entry
					e.SetType(entry.ETTxn)
					txnInfo := &entry.Info{
						Group: groupNo,
						TxnId: tidAlloc.Alloc(),
					}
					e.SetInfo(txnInfo)
					str := txnInfo.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					err := e.UnmarshalFromNode(n, true)
					assert.Nil(t, err)
					lsn, err := s.AppendEntry(groupNo, e)
					assert.Nil(t, err)
					ckp = lsn
				case 26, 28: //flush entry
					e.SetType(entry.ETFlush)
					payload := make([]byte, 0)
					err := e.Unmarshal(payload)
					assert.Nil(t, err)
					_, err = s.AppendEntry(entry.GTNoop, e)
					assert.Nil(t, err)
				default: //commit entry
					e.SetType(entry.ETCustomizedStart)
					commitInterval := &entry.Info{
						Group: groupNo,
					}
					e.SetInfo(commitInterval)
					str := commitInterval.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					err := e.UnmarshalFromNode(n, true)
					assert.Nil(t, err)
					lsn, err := s.AppendEntry(groupNo, e)
					assert.Nil(t, err)
					ckp = lsn
				}
				ch <- e
			}
		}
	}

	for j := 0; j < groupCnt; j++ {
		err := worker.Submit(f(entry.GTCustomizedStart + uint32(j)))
		assert.Nil(t, err)
		// worker.Submit(f(uint32(j)))
	}

	fwg.Wait()
	cancel()
	wg.Wait()

	err = s.TryCompact()
	assert.Nil(t, err)

	s.Close()

	s, _ = NewBaseStore(dir, name, cfg)
	a := func(group uint32, commitId uint64, payload []byte, typ uint16, info any) {
		t.Logf("%s", payload)
	}
	r := newReplayer(a)
	o := &noopObserver{}
	err = s.file.Replay(r, o)
	if err != nil {
		fmt.Printf("err is %v", err)
	}
	r.Apply()
	s.Close()
}

type entryWithLSN struct {
	entry entry.Entry
	info  *entry.Info
}

func TestLoad(t *testing.T) {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 2000),
	}
	s, err := NewBaseStore(dir, name, cfg)
	assert.Nil(t, err)

	var wg sync.WaitGroup
	var fwg sync.WaitGroup
	ch := make(chan *entryWithLSN, 1000)
	ch2 := make([]*entryWithLSN, 0)
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-ch:
				err := e.entry.WaitDone()
				assert.Nil(t, err)
				infoin := e.entry.GetInfo()
				t.Logf("entry is %s", e.entry.GetPayload())
				e.entry.Free()
				if infoin != nil {
					info := infoin.(*entry.Info)
					e.info = info
					_, err = s.Load(info.Group, e.info.GroupLSN)
					assert.Nil(t, err)
					t.Logf("synced %d", s.GetSynced(info.Group))
					t.Logf("checkpointed %d", s.GetCheckpointed(info.Group))
					t.Logf("penddings %d", s.GetPenddingCnt(info.Group))
				}
				fwg.Done()
				ch2 = append(ch2, e)
			}
		}
	}()

	entryPerGroup := 50
	groupCnt := 2
	worker, _ := ants.NewPool(groupCnt)
	fwg.Add(entryPerGroup * groupCnt)
	f := func(groupNo uint32) func() {
		return func() {
			tidAlloc := &common.IdAllocator{}
			pre := uint64(0)
			ckp := uint64(0)
			var entrywithlsn *entryWithLSN
			for i := 0; i < entryPerGroup; i++ {
				e := entry.GetBase()
				var lsn uint64
				switch i % 50 {
				case 1, 2, 3, 4, 5: //uncommit entry
					e.SetType(entry.ETUncommitted)
					uncommitInfo := &entry.Info{
						Uncommits: []entry.Tid{{
							Group: groupNo,
							Tid:   tidAlloc.Get() + 1 + uint64(rand.Intn(3)),
						}},
					}
					e.SetInfo(uncommitInfo)
					str := uncommitInfo.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					err := e.UnmarshalFromNode(n, true)
					assert.Nil(t, err)
					lsn, err = s.AppendEntry(entry.GTUncommit, e)
					assert.Nil(t, err)
					entrywithlsn = &entryWithLSN{
						entry: e,
					}
					t.Logf("alloc %d-%d", entry.GTUncommit, lsn)
				case 49: //ckp entry
					e.SetType(entry.ETCheckpoint)
					checkpointInfo := &entry.Info{
						Checkpoints: []entry.CkpRanges{{
							Group: groupNo,
							Ranges: common.NewClosedIntervalsByInterval(
								&common.ClosedInterval{
									Start: pre + 1,
									End:   ckp,
								}),
						}},
					}
					pre = ckp
					e.SetInfo(checkpointInfo)
					lsn, err := s.AppendEntry(entry.GTCKp, e)
					assert.Nil(t, err)
					entrywithlsn = &entryWithLSN{
						entry: e,
					}
					t.Logf("alloc %d-%d", entry.GTCKp, lsn)
				case 20, 21, 22, 23: //txn entry
					e.SetType(entry.ETTxn)
					txnInfo := &entry.Info{
						TxnId: tidAlloc.Alloc(),
					}
					e.SetInfo(txnInfo)
					str := txnInfo.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					err := e.UnmarshalFromNode(n, true)
					assert.Nil(t, err)
					lsn, err = s.AppendEntry(groupNo, e)
					assert.Nil(t, err)
					entrywithlsn = &entryWithLSN{
						entry: e,
					}
					ckp = lsn
					t.Logf("alloc %d-%d", groupNo, lsn)
				case 26, 28: //flush entry
					e.SetType(entry.ETFlush)
					payload := make([]byte, 0)
					err := e.Unmarshal(payload)
					assert.Nil(t, err)
					lsn, err = s.AppendEntry(entry.GTNoop, e)
					assert.Nil(t, err)
					entrywithlsn = &entryWithLSN{
						entry: e,
					}
					t.Logf("alloc %d-%d", entry.GTNoop, lsn)
				default: //commit entry
					e.SetType(entry.ETCustomizedStart)
					commitInterval := &entry.Info{}
					e.SetInfo(commitInterval)
					str := commitInterval.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					err := e.UnmarshalFromNode(n, true)
					assert.Nil(t, err)
					lsn, err = s.AppendEntry(groupNo, e)
					assert.Nil(t, err)
					entrywithlsn = &entryWithLSN{
						entry: e,
					}
					ckp = lsn
					t.Logf("alloc %d-%d", groupNo, lsn)
				}
				ch <- entrywithlsn
			}
		}
	}

	for j := entry.GTCustomizedStart; j < entry.GTCustomizedStart+uint32(groupCnt); j++ {
		err := worker.Submit(f(uint32(j)))
		assert.Nil(t, err)
	}

	fwg.Wait()
	cancel()
	wg.Wait()

	err = s.TryCompact()
	assert.Nil(t, err)

	s.Close()

	fmt.Printf("\n***********replay***********\n\n")
	s, _ = NewBaseStore(dir, name, cfg)
	a := func(group uint32, commitId uint64, payload []byte, typ uint16, info any) {
		t.Logf("%s", payload)
	}
	err = s.Replay(a)
	assert.Nil(t, err)
	t.Log(s.checkpointing)
	t.Log(s.syncing)
	t.Log(s.addrs)

	for _, e := range ch2 {
		t.Logf("entry is %s", e.entry.GetPayload())
		if e.info != nil {
			if e.info.Group != entry.GTNoop {
				t.Logf("load %d-%d", e.info.Group, e.info.GroupLSN)
				_, err = s.Load(e.info.Group, e.info.GroupLSN)
				assert.Nil(t, err)
				t.Logf("synced %d", s.GetSynced(e.info.Group))
				t.Logf("checkpointed %d", s.GetCheckpointed(e.info.Group))
				t.Logf("penddings %d", s.GetPenddingCnt(e.info.Group))
			}
		}
	}

	s.Close()
}
