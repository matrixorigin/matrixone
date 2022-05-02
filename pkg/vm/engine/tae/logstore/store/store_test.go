package store

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	// "time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

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
				if v != nil {
					info := v.(*entry.Info)
					t.Logf("group-%d", info.Group)
					t.Logf("synced %d", s.GetSynced(info.Group))
					t.Logf("checkpointed %d", s.GetCheckpointed(info.Group))
					t.Logf("penddings %d", s.GetPenddings(info.Group))
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

	entryPerGroup := 5000
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
					e.UnmarshalFromNode(n, true)
					s.AppendEntry(entry.GTUncommit, e)
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
							Command: map[uint64]entry.CommandInfo{(end + 1):{
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
					e.UnmarshalFromNode(n, true)
					s.AppendEntry(entry.GTCKp, e)
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
					e.UnmarshalFromNode(n, true)
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
					e.UnmarshalFromNode(n, true)
					lsn, err := s.AppendEntry(groupNo, e)
					assert.Nil(t, err)
					ckp = lsn
				}
				ch <- e
			}
		}
	}

	for j := 0; j < groupCnt; j++ {
		worker.Submit(f(uint32(j) + entry.GTCustomizedStart))
	}

	fwg.Wait()
	cancel()
	wg.Wait()

	h := s.file.GetHistory()
	t.Log(h.String())
	err = h.TryTruncate()
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
	uncommit.Unmarshal(buf2)
	lsn, err := s.AppendEntry(entry.GTUncommit, uncommit)
	assert.Nil(t, err)
	uncommit.WaitDone()
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
	commit.Unmarshal(buf2)
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
	ckp1.Unmarshal(buf2)
	s.AppendEntry(entry.GTCKp, ckp1)

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
	ckp2.Unmarshal(buf2)
	s.AppendEntry(entry.GTCKp, ckp2)

	anotherEntry := entry.GetBase()
	commitInfo = &entry.Info{
		Group: entry.GTCustomizedStart + 1,
	}
	anotherEntry.SetInfo(commitInfo)
	buf2 = make([]byte, common.K)
	copy(buf2, buf)
	anotherEntry.Unmarshal(buf2)
	s.AppendEntry(entry.GTCustomizedStart, anotherEntry)
	anotherEntry.WaitDone()

	s.TryCompact()
	_, err = s.Load(entry.GTUncommit, lsn)
	assert.NotNil(t, err)

	s.Close()
}

func TestReplay(t *testing.T) {
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
				assert.Nil(t, err)
				if info != nil {
					groupNo := info.(*entry.Info).Group
					t.Logf("group %d", groupNo)
					t.Logf("synced %d", s.GetSynced(groupNo))
					t.Logf("checkpointed %d", s.GetCheckpointed(groupNo))
					t.Logf("penddings %d", s.GetPenddings(groupNo))
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
					e.UnmarshalFromNode(n, true)
					s.AppendEntry(entry.GTUncommit, e)
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
					str := checkpointInfo.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					e.UnmarshalFromNode(n, true)
					s.AppendEntry(entry.GTCKp, e)
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
					e.UnmarshalFromNode(n, true)
					lsn, err := s.AppendEntry(groupNo, e)
					assert.Nil(t, err)
					ckp = lsn
				case 26, 28: //flush entry
					e.SetType(entry.ETFlush)
					payload := make([]byte, 0)
					e.Unmarshal(payload)
					s.AppendEntry(entry.GTNoop, e)
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
					e.UnmarshalFromNode(n, true)
					lsn, err := s.AppendEntry(groupNo, e)
					assert.Nil(t, err)
					ckp = lsn
				}
				ch <- e
			}
		}
	}

	for j := 0; j < groupCnt; j++ {
		worker.Submit(f(entry.GTCustomizedStart + uint32(j)))
		// worker.Submit(f(uint32(j)))
	}

	fwg.Wait()
	cancel()
	wg.Wait()

	h := s.file.GetHistory()
	t.Log(h.String())
	err = h.TryTruncate()
	assert.Nil(t, err)

	s.Close()

	s, _ = NewBaseStore(dir, name, cfg)
	a := func(group uint32, commitId uint64, payload []byte, typ uint16, info interface{}) (err error) {
		t.Logf("%s", payload)
		return nil
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
	lsn   uint64
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
				fmt.Printf("entry is %s", e.entry.GetPayload())
				if infoin != nil {
					info := infoin.(*entry.Info)
					var loadedEntry entry.Entry
					for i := 0; i < 5; i++ {
						loadedEntry, err = s.Load(info.Group, e.lsn)
						if err == nil {
							fmt.Printf("loaded entry is %s", loadedEntry.GetPayload())
							break
						}
						fmt.Printf("%d-%d:%v\n", info.Group, info.GroupLSN, err)
						time.Sleep(time.Millisecond * 500)
					}
					assert.Nil(t, err)
					t.Logf("synced %d", s.GetSynced(info.Group))
					t.Logf("checkpointed %d", s.GetCheckpointed(info.Group))
					t.Logf("penddings %d", s.GetPenddings(info.Group))
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
			var err error
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
					e.UnmarshalFromNode(n, true)
					lsn, err = s.AppendEntry(entry.GTUncommit, e)
					assert.Nil(t, err)
					entrywithlsn = &entryWithLSN{
						entry: e,
						lsn:   lsn,
					}
					fmt.Printf("alloc %d-%d\n", entry.GTUncommit, lsn)
				case 49: //ckp entry
					e.SetType(entry.ETCheckpoint)
					checkpointInfo := &entry.Info{
						Checkpoints: []entry.CkpRanges{{
							Group: 1,
							Ranges: common.NewClosedIntervalsByInterval(
								&common.ClosedInterval{
									Start: pre + 1,
									End:   ckp,
								}),
						}},
					}
					pre = ckp
					e.SetInfo(checkpointInfo)
					str := checkpointInfo.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					e.UnmarshalFromNode(n, true)
					lsn, err = s.AppendEntry(entry.GTCKp, e)
					assert.Nil(t, err)
					entrywithlsn = &entryWithLSN{
						entry: e,
						lsn:   lsn,
					}
					fmt.Printf("alloc %d-%d\n", entry.GTCKp, lsn)
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
					e.UnmarshalFromNode(n, true)
					lsn, err = s.AppendEntry(groupNo, e)
					assert.Nil(t, err)
					entrywithlsn = &entryWithLSN{
						entry: e,
						lsn:   lsn,
					}
					ckp = lsn
					fmt.Printf("alloc %d-%d\n", groupNo, lsn)
				case 26, 28: //flush entry
					e.SetType(entry.ETFlush)
					payload := make([]byte, 0)
					e.Unmarshal(payload)
					lsn, err = s.AppendEntry(entry.GTNoop, e)
					assert.Nil(t, err)
					entrywithlsn = &entryWithLSN{
						entry: e,
						lsn:   lsn,
					}
					fmt.Printf("alloc %d-%d\n", entry.GTNoop, lsn)
				default: //commit entry
					e.SetType(entry.ETCustomizedStart)
					commitInterval := &entry.Info{}
					e.SetInfo(commitInterval)
					str := commitInterval.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					e.UnmarshalFromNode(n, true)
					lsn, err = s.AppendEntry(groupNo, e)
					assert.Nil(t, err)
					entrywithlsn = &entryWithLSN{
						entry: e,
						lsn:   lsn,
					}
					ckp = lsn
					fmt.Printf("alloc %d-%d\n", groupNo, lsn)
				}
				ch <- entrywithlsn
			}
		}
	}

	for j := entry.GTCustomizedStart; j < entry.GTCustomizedStart+uint32(groupCnt); j++ {
		worker.Submit(f(uint32(j)))
	}

	fwg.Wait()
	cancel()
	wg.Wait()

	h := s.file.GetHistory()
	// t.Log(h.String())
	err = h.TryTruncate()
	assert.Nil(t, err)

	s.Close()

	fmt.Printf("\n***********replay***********\n\n")
	s, _ = NewBaseStore(dir, name, cfg)
	a := func(group uint32, commitId uint64, payload []byte, typ uint16, info interface{}) (err error) {
		fmt.Printf("%s", payload)
		return nil
	}
	s.Replay(a)
	// r := newReplayer(a)
	// o := &noopObserver{}
	// err = s.file.Replay(r.replayHandler, o)
	// if err != nil {
	// 	fmt.Printf("err is %v", err)
	// }
	// r.Apply()

	for _, e := range ch2 {
		err := e.entry.WaitDone()
		assert.Nil(t, err)
		infoin := e.entry.GetInfo()
		fmt.Printf("entry is %s", e.entry.GetPayload())
		if infoin != nil {
			info := infoin.(*entry.Info)
			var loadedEntry entry.Entry
			for i := 0; i < 5; i++ {
				loadedEntry, err = s.Load(info.Group, e.lsn)
				if err == nil {
					fmt.Printf("loaded entry is %s", loadedEntry.GetPayload())
					break
				}
				fmt.Printf("%d-%d:%v\n", info.Group, info.GroupLSN, err)
				time.Sleep(time.Millisecond * 500)
			}
			assert.Nil(t, err)
			t.Logf("synced %d", s.GetSynced(info.Group))
			t.Logf("checkpointed %d", s.GetCheckpointed(info.Group))
			t.Logf("penddings %d", s.GetPenddings(info.Group))
		}
	}

	s.Close()
}
