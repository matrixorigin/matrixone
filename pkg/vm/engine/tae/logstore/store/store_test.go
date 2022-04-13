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
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 20),
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
				t.Logf("synced %d", s.GetSynced(1))
				// t.Logf("checkpointed %d", s.GetCheckpointed())
				fwg.Done()
			}
		}
	}()

	var bs bytes.Buffer
	for i := 0; i < 1000; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()
	cnt := 5
	for i := 0; i < cnt; i++ {
		e := entry.GetBase()
		if i%2 == 0 && i > 0 {
			checkpointInfo := &entry.Info{
				Group: entry.GTCKp,
				Checkpoints: []entry.CkpRanges{{
					Group: 1,
					Ranges: common.NewClosedIntervalsByInterval(
						&common.ClosedInterval{
							Start: 0,
							End:   common.GetGlobalSeqNum(),
						}),
				}},
			}
			e.SetInfo(checkpointInfo)
			e.SetType(entry.ETCheckpoint)
			n := common.GPool.Alloc(uint64(len(buf)))
			n.Buf = n.Buf[:len(buf)]
			copy(n.GetBuf(), buf)
			e.UnmarshalFromNode(n, true)
			s.AppendEntry(entry.GTCKp, e)
		} else {
			commitInterval := &entry.Info{
				Group:    entry.GTCustomizedStart,
				CommitId: common.NextGlobalSeqNum(),
			}
			e.SetInfo(commitInterval)
			e.SetType(entry.ETCustomizedStart)
			n := common.GPool.Alloc(uint64(len(buf)))
			n.Buf = n.Buf[:len(buf)]
			copy(n.GetBuf(), buf)
			e.UnmarshalFromNode(n, true)
			s.AppendEntry(entry.GTCustomizedStart, e)
		}
		assert.Nil(t, err)
		fwg.Add(1)
		ch <- e
	}

	fwg.Wait()
	cancel()
	wg.Wait()

	h := s.file.GetHistory()
	t.Log(h.String())
	err = h.TryTruncate()
	assert.Nil(t, err)
}

func TestMultiGroup(t *testing.T) {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 200),
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
				t.Logf("synced %d", s.GetSynced(1))
				t.Logf("checkpointed %d", s.GetCheckpointed(1))
				t.Logf("penddings %d", s.GetPenddings(1))
				fwg.Done()
			}
		}
	}()

	var bs bytes.Buffer
	for i := 0; i < 10; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()

	entryPerGroup := 5000
	groupCnt := 5
	worker, _ := ants.NewPool(groupCnt)
	fwg.Add(entryPerGroup * groupCnt)
	f := func(groupNo uint32) func() {
		return func() {
			alloc := &common.IdAllocator{}
			ckp := uint64(0)
			for i := 0; i < entryPerGroup; i++ {
				e := entry.GetBase()
				if i%1000 == 0 && i > 0 {
					end := alloc.Get()
					checkpointInfo := &entry.Info{
						Group: entry.GTCKp,
						Checkpoints: []entry.CkpRanges{{
							Group: 1,
							Ranges: common.NewClosedIntervalsByInterval(
								&common.ClosedInterval{
									Start: ckp,
									End:   end,
								}),
						}},
					}
					ckp = end
					e.SetInfo(checkpointInfo)
					e.SetType(entry.ETCheckpoint)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					e.UnmarshalFromNode(n, true)
					s.AppendEntry(entry.GTCKp, e)
				} else {
					commitInterval := &entry.Info{
						Group:    groupNo,
						CommitId: alloc.Alloc(),
					}
					e.SetInfo(commitInterval)
					e.SetType(entry.ETCustomizedStart)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					e.UnmarshalFromNode(n, true)
					s.AppendEntry(groupNo, e)
				}
				ch <- e
			}
		}
	}

	for j := 0; j < groupCnt; j++ {
		no := uint32(j) + entry.GTCustomizedStart
		worker.Submit(f(no))
	}

	fwg.Wait()
	cancel()
	wg.Wait()
	s.file.GetHistory().TryTruncate()

	h := s.file.GetHistory()
	t.Log(h.String())
	err = h.TryTruncate()
	assert.Nil(t, err)
}

func TestUncommitEntry(t *testing.T) {
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

	entryPerGroup := 550
	groupCnt := 1
	worker, _ := ants.NewPool(groupCnt)
	fwg.Add(entryPerGroup * groupCnt)
	f := func(groupNo uint32) func() {
		return func() {
			cidAlloc := &common.IdAllocator{}
			tidAlloc := &common.IdAllocator{}
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
					end := cidAlloc.Get()
					checkpointInfo := &entry.Info{
						Group: entry.GTCKp,
						Checkpoints: []entry.CkpRanges{{
							Group: 1,
							Ranges: common.NewClosedIntervalsByInterval(
								&common.ClosedInterval{
									Start: ckp,
									End:   end,
								}),
						}},
					}
					ckp = end
					e.SetType(entry.ETCheckpoint)
					e.SetInfo(checkpointInfo)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					e.UnmarshalFromNode(n, true)
					s.AppendEntry(entry.GTCKp, e)
				case 50, 51, 52, 53:
					txnInfo := &entry.Info{
						Group:    groupNo,
						TxnId:    tidAlloc.Alloc(),
						CommitId: cidAlloc.Alloc(),
					}
					e.SetType(entry.ETTxn)
					e.SetInfo(txnInfo)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					e.UnmarshalFromNode(n, true)
					s.AppendEntry(groupNo, e)
				default:
					commitInterval := &entry.Info{
						Group:    groupNo,
						CommitId: cidAlloc.Alloc(),
					}
					e.SetType(entry.ETCustomizedStart)
					e.SetInfo(commitInterval)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					e.UnmarshalFromNode(n, true)
					s.AppendEntry(groupNo, e)
				}
				ch <- e
			}
		}
	}

	for j := 0; j < groupCnt; j++ {
		worker.Submit(f(uint32(j)))
	}

	fwg.Wait()
	cancel()
	wg.Wait()

	h := s.file.GetHistory()
	t.Log(h.String())
	err = h.TryTruncate()
	assert.Nil(t, err)
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
				err := e.WaitDone()
				assert.Nil(t, err)
				// t.Logf("synced %d", s.GetSynced("group1"))
				// t.Logf("checkpointed %d", s.GetCheckpointed("group1"))
				// t.Logf("penddings %d", s.GetPenddings("group1"))
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
			cidAlloc := &common.IdAllocator{}
			tidAlloc := &common.IdAllocator{}
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
					end := cidAlloc.Get()
					checkpointInfo := &entry.Info{
						Group: entry.GTCKp,
						Checkpoints: []entry.CkpRanges{{
							Group: 1,
							Ranges: common.NewClosedIntervalsByInterval(
								&common.ClosedInterval{
									Start: ckp,
									End:   end,
								}),
						}},
					}
					ckp = end
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
						Group:    groupNo,
						TxnId:    tidAlloc.Alloc(),
						CommitId: cidAlloc.Alloc(),
					}
					e.SetInfo(txnInfo)
					str := txnInfo.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					e.UnmarshalFromNode(n, true)
					s.AppendEntry(groupNo, e)
				case 26, 28: //flush entry
					e.SetType(entry.ETFlush)
					payload := make([]byte, 0)
					e.Unmarshal(payload)
					s.AppendEntry(entry.GTNoop, e)
				default: //commit entry
					e.SetType(entry.ETCustomizedStart)
					commitInterval := &entry.Info{
						Group:    groupNo,
						CommitId: cidAlloc.Alloc(),
					}
					e.SetInfo(commitInterval)
					str := commitInterval.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					e.UnmarshalFromNode(n, true)
					s.AppendEntry(groupNo, e)
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
	// t.Log(h.String())
	err = h.TryTruncate()
	assert.Nil(t, err)

	s.Close()

	s, _ = NewBaseStore(dir, name, cfg)
	a := func(group uint32, commitId uint64, payload []byte, typ uint16, info interface{}) (err error) {
		fmt.Printf("%s", payload)
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
			cidAlloc := &common.IdAllocator{}
			tidAlloc := &common.IdAllocator{}
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
					end := cidAlloc.Get()
					checkpointInfo := &entry.Info{
						Checkpoints: []entry.CkpRanges{{
							Group: 1,
							Ranges: common.NewClosedIntervalsByInterval(
								&common.ClosedInterval{
									Start: ckp,
									End:   end,
								}),
						}},
					}
					ckp = end
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
						TxnId:    tidAlloc.Alloc(),
						CommitId: cidAlloc.Alloc(),
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
					commitInterval := &entry.Info{
						CommitId: cidAlloc.Alloc(),
					}
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
