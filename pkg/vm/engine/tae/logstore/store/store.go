package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

var (
	DefaultMaxBatchSize  = 500
	DefaultMaxSyncSize   = 10
	DefaultMaxCommitSize = 10
	DefaultBatchPerSync  = 100
	DefaultSyncDuration  = time.Millisecond * 2
	FlushEntry           entry.Entry
)

func init() {
	FlushEntry = entry.GetBase()
	FlushEntry.SetType(entry.ETFlush)
	payload := make([]byte, 0)
	FlushEntry.Unmarshal(payload)
}

type storeInfo struct {
	syncTimes          int
	bySize             int
	byDuration         int
	syncDuration       time.Duration
	writeDuration      time.Duration
	syncQueueDuration  time.Duration
	syncLoopDuration   time.Duration
	flushQueueDuration time.Duration
	flushLoop1Duration time.Duration
	flushLoop2Duration time.Duration
	onEntriesDuration  time.Duration
	tickerTimes        int
	enqueueEntries     int32
	// appendInterval       = time.Second * 0
	append10µs  int
	append1ms   int
	append2ms   int
	append3ms   int
	appendgt3ms int
	// globalTime  = time.Now().Unix()
}

type baseStore struct {
	storeInfo
	syncBase
	common.ClosedState
	dir, name   string
	flushWg     sync.WaitGroup
	flushCtx    context.Context
	flushCancel context.CancelFunc
	flushQueue  chan entry.Entry
	syncQueue   chan []*batch
	commitQueue chan []*batch
	wg          sync.WaitGroup
	file        File
	mu          *sync.RWMutex
}

func NewBaseStore(dir, name string, cfg *StoreCfg) (*baseStore, error) {
	var err error
	bs := &baseStore{
		syncBase:    *newSyncBase(),
		dir:         dir,
		name:        name,
		flushQueue:  make(chan entry.Entry, DefaultMaxBatchSize*100),
		syncQueue:   make(chan []*batch, DefaultMaxSyncSize*100),
		commitQueue: make(chan []*batch, DefaultMaxCommitSize*100),
		mu:          &sync.RWMutex{},
	}
	if cfg == nil {
		cfg = &StoreCfg{}
	}
	bs.file, err = OpenRotateFile(dir, name, nil, cfg.RotateChecker, cfg.HistoryFactory, &bs.storeInfo)
	if err != nil {
		return nil, err
	}
	bs.flushCtx, bs.flushCancel = context.WithCancel(context.Background())
	bs.start()
	return bs, nil
}

func (bs *baseStore) start() {
	bs.wg.Add(3)
	go bs.flushLoop()
	go bs.syncLoop()
	go bs.commitLoop()
}

func (bs *baseStore) flushLoop() {
	t0 := time.Now()
	defer bs.wg.Done()
	entries := make([]entry.Entry, 0, DefaultMaxBatchSize)
	bats := make([]*batch, 0, DefaultBatchPerSync)
	ticker := time.NewTicker(DefaultSyncDuration)
	for {
		t1 := time.Now()
		select {
		case <-bs.flushCtx.Done():
			return
		case e := <-bs.flushQueue:
			entries = append(entries, e)
			bs.flushQueueDuration += time.Since(t1)
			a := rand.Intn(100)
			if a == 33 {
				fmt.Printf("receive takes %v(%d entries appended)\n", time.Since(t1), atomic.LoadInt32(&bs.enqueueEntries))
			}
			atomic.StoreInt32(&bs.enqueueEntries, 0)
			t1 = time.Now()
		Left:
			for i := 0; i < DefaultMaxBatchSize-1; i++ {
				select {
				case e = <-bs.flushQueue:
					entries = append(entries, e)
					atomic.StoreInt32(&bs.enqueueEntries, 0)
				default:
					// case <-ticker.C:
					break Left
				}
			}
			bs.flushLoop1Duration += time.Since(t1)
			t1 = time.Now()
			bat := bs.onEntries(entries)
			bs.onEntriesDuration += time.Since(t1)
			t1 = time.Now()
			bats = append(bats, bat)
			if len(bats) >= DefaultBatchPerSync || time.Since(t0) > DefaultSyncDuration {
				if len(bats) >= DefaultBatchPerSync {
					bs.bySize++
				} else {
					bs.byDuration++
				}
				t0 = time.Now()
				syncBatch := make([]*batch, len(bats))
				copy(syncBatch, bats)
				for _, b := range syncBatch {
					for _, e := range b.entrys {
						e.StartTime()
					}
				}
				bs.syncQueue <- syncBatch
				bats = bats[:0]
			}
			entries = entries[:0]
			bs.flushLoop2Duration += time.Since(t1)
			t1 = time.Now()
		case <-ticker.C:
			bs.tickerTimes++
			bs.flushQueueDuration += time.Since(t1)
			t1 = time.Now()
			if len(bats) != 0 {
				bs.byDuration++
				t0 = time.Now()
				syncBatch := make([]*batch, len(bats))
				copy(syncBatch, bats)
				bs.syncQueue <- syncBatch
				bats = bats[:0]
			}
			bs.flushLoop2Duration += time.Since(t1)
			t1 = time.Now()
		}
	}
}

func (bs *baseStore) syncLoop() {
	defer bs.wg.Done()
	batches := make([]*batch, 0, DefaultMaxSyncSize)
	for {
		t0 := time.Now()
		select {
		case <-bs.flushCtx.Done():
			return
		case e := <-bs.syncQueue:
			bs.syncQueueDuration += time.Since(t0)
			t0 = time.Now()
			batches = append(batches, e...)
			a := rand.Intn(100)
			if a == 33 {
				if len(batches) != 0 && len(batches[0].entrys) != 0 {
					fmt.Printf("sync queue takes %v\n", batches[0].entrys[0].Duration())
				}
			}
		Left:
			for i := 0; i < DefaultMaxBatchSize-1; i++ {
				select {
				case e = <-bs.syncQueue:
					batches = append(batches, e...)
				default:
					break Left
				}
			}
			if a == 33 {
				if len(batches) != 0 && len(batches[0].entrys) != 0 {
					fmt.Printf("sync queue takes %v(including wait)\n", batches[0].entrys[0].Duration())
				}
			}
			bs.syncLoopDuration += time.Since(t0)
			bs.onSyncs(batches)
			batches = batches[:0]
		}
	}
}

func (bs *baseStore) commitLoop() {
	defer bs.wg.Done()
	batches := make([]*batch, 0, DefaultMaxCommitSize)
	for {
		select {
		case <-bs.flushCtx.Done():
			return
		case e := <-bs.commitQueue:
			batches = append(batches, e...)
		Left:
			for i := 0; i < DefaultMaxCommitSize-1; i++ {
				select {
				case e = <-bs.commitQueue:
					batches = append(batches, e...)
				default:
					break Left
				}
			}
			bs.onCommits(batches)
			batches = batches[:0]
		}
	}
}

func (bs *baseStore) onCommits(batches []*batch) {
	t0 := time.Now()
	a := rand.Intn(100)
	if a == 33 {
		fmt.Printf("%d bats per commit\n", len(batches))
	}
	for _, bat := range batches {
		bs.syncBase.OnCommit(bat.preparecommit)
		for _, e := range bat.entrys {
			e.DoneWithErr(nil)
		}
		cnt := len(bat.entrys)
		// fmt.Printf("137count is %d\n",cnt)
		bs.flushWg.Add(-1 * cnt)
	}
	if a == 33 {
		fmt.Printf("onCommits takes %v\n", time.Since(t0))
	}
}

func (bs *baseStore) onSyncs(batches []*batch) {
	var err error
	t0 := time.Now()
	a := rand.Intn(100)
	if a == 33 {
		fmt.Printf("%d batch per sync\n", len(batches))
	}
	if err = bs.file.Sync(); err != nil { //TODO not allow to sync by write
		panic(err)
	}
	if a == 33 {
		fmt.Printf("sync takes %v\n", time.Since(t0))
	}
	bats := make([]*batch, len(batches))
	copy(bats, batches)
	bs.commitQueue <- bats
	// for _, bat := range batches {
	// 	bs.syncBase.OnCommit(bat.preparecommit)
	// 	for _, e := range bat.entrys {
	// 		e.DoneWithErr(nil)
	// 	}
	// 	cnt := len(bat.entrys)
	// 	// fmt.Printf("137count is %d\n",cnt)
	// 	bs.flushWg.Add(-1 * cnt)
	// }
}

//set infosize, mashal info
func (bs *baseStore) PrepareEntry(e entry.Entry) (entry.Entry, error) {
	v1 := e.GetInfo()
	if v1 == nil {
		return e, nil
	}
	v := v1.(*entry.Info)
	v.Info = &VFileAddress{}
	switch v.Group {
	// case entry.GTUncommit:
	// 	addrs := make([]*VFileAddress, 0)
	// 	for _, tids := range v.Uncommits {
	// 		addr := bs.GetLastAddr(tids.Group, tids.Tid)
	// 		if addr == nil {
	// 			addr = &VFileAddress{}
	// 		}
	// 		addr.Group = tids.Group
	// 		addr.LSN = tids.Tid
	// 		addrs = append(addrs, addr)
	// 	}
	// 	buf, err := json.Marshal(addrs)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	e.SetInfoSize(len(buf))
	// 	e.SetInfoBuf(buf)
	// 	return e, nil
	default:
		buf := v.Marshal()
		size := len(buf)
		e.SetInfoSize(size)
		e.SetInfoBuf(buf)
		return e, nil
	}
}

func (bs *baseStore) onEntries(entries []entry.Entry) *batch {
	// var err error
	t0 := time.Now()
	a := rand.Intn(100)
	if a == 33 {
		fmt.Printf("%d entries per batch\n", len(entries))
	}
	for _, e := range entries {
		appender := bs.file.GetAppender()
		e, err := bs.PrepareEntry(e)
		if err != nil {
			panic(err)
		}
		if err = appender.Prepare(e.TotalSize(), e.GetInfo()); err != nil {
			panic(err)
		}
		if _, err = e.WriteTo(appender); err != nil {
			panic(err)
		}
		if err = appender.Commit(); err != nil {
			panic(err)
		}
		bs.OnEntryReceived(e)
	}
	bat := &batch{
		preparecommit: bs.PrepareCommit(),
		entrys:        make([]entry.Entry, len(entries)),
	}
	// fmt.Printf("208count is %d\n",len(entries))
	copy(bat.entrys, entries)
	if a == 33 {
		fmt.Printf("on entries takes %v\n", time.Since(t0))
	}
	return bat
	// bs.syncQueue <- bat
	// t0 := time.Now()
	// if err = bs.file.Sync(); err != nil { //TODO not allow to sync by write
	// 	panic(err)
	// }
	// if a == 33 {
	// 	fmt.Printf("sync takes %v\n", time.Since(t0))
	// }
	// bs.OnCommit() //TODO prepare commit, commit commit

	// for _, e := range entries {
	// 	e.DoneWithErr(nil)
	// }
}

type batch struct {
	entrys []entry.Entry
	//on commit
	preparecommit *prepareCommit
}

type prepareCommit struct {
	checkpointing, syncing map[uint32]uint64
}

// func (bs *baseStore) OnCommit() {
// 	//buffer
// 	//offset
// 	//file
// 	bs.syncBase.OnCommit()
// }

func (bs *baseStore) Close() error {
	if !bs.TryClose() {
		return nil
	}
	bs.flushWg.Wait()
	bs.flushCancel()
	bs.wg.Wait()
	fmt.Printf("***********************\n")
	fmt.Printf("%d|10µs|%d|1ms|%d|5ms|%d|10ms|%d\n",
		bs.append10µs, bs.append1ms, bs.append2ms, bs.append3ms, bs.appendgt3ms)
	fmt.Printf("***********************\n")
	fmt.Printf("flush queue duration %v\nflush loop duration 1 %v\nonEntry duration %v\nflush loop duration 2 %v\nticker %d times\n",
		bs.flushQueueDuration, bs.flushLoop1Duration, bs.onEntriesDuration, bs.flushLoop2Duration, bs.tickerTimes)
	fmt.Printf("***********************\n")
	fmt.Printf("sync %d times(S%dD%d)\n", bs.syncTimes, bs.bySize, bs.byDuration)
	if bs.syncTimes != 0 {
		fmt.Printf("sync duration %v(avg%v)\nwrite durtion %v\nsync queue duration %v\nsync loop duration %v\n",
			bs.syncDuration, time.Duration(int(bs.syncDuration)/bs.syncTimes), bs.writeDuration, bs.syncQueueDuration, bs.syncLoopDuration)
	}
	return bs.file.Close()
}

func (bs *baseStore) Checkpoint(e entry.Entry) (err error) {
	if e.IsCheckpoint() {
		return errors.New("wrong entry type")
	}
	_, err = bs.AppendEntry(entry.GTCKp, e)
	return
}

func (bs *baseStore) TryCompact() error {
	return bs.file.GetHistory().TryTruncate()
}

func (bs *baseStore) TryTruncate(size int64) error {
	return bs.file.TryTruncate(size)
}

func (bs *baseStore) AppendEntry(groupId uint32, e entry.Entry) (id uint64, err error) {
	// duration := time.Since(globalTime)
	// if duration < time.Microsecond*10 {
	// 	append10µs++
	// } else if duration < time.Millisecond {
	// 	append1ms++
	// } else if duration < time.Millisecond*5 {
	// 	append2ms++
	// } else if duration < time.Millisecond*10 {
	// 	append3ms++
	// } else {
	// 	appendgt3ms++
	// }
	e.StartTime()
	if bs.IsClosed() {
		return 0, common.ClosedErr
	}
	bs.flushWg.Add(1)
	if bs.IsClosed() {
		bs.flushWg.Done()
		return 0, common.ClosedErr
	}
	bs.mu.Lock()
	lsn := bs.AllocateLsn(groupId)
	v1 := e.GetInfo()
	var info *entry.Info
	if v1 != nil {
		info = v1.(*entry.Info)
		info.Group = groupId
		info.GroupLSN = lsn
	} else {
		info = &entry.Info{
			Group:    groupId,
			GroupLSN: lsn,
		}
	}
	e.SetInfo(info)
	bs.flushQueue <- e
	bs.mu.Unlock()
	// globalTime = time.Now()
	atomic.AddInt32(&bs.enqueueEntries, 1)
	return lsn, nil
}

func (s *baseStore) Sync() error {
	if _, err := s.AppendEntry(entry.GTNoop, FlushEntry); err != nil {
		return err
	}
	// if err := s.writer.Flush(); err != nil {
	// 	return err
	// }
	err := s.file.Sync()
	return err
}

func (s *baseStore) Replay(h ApplyHandle) error {
	r := newReplayer(h)
	o := &noopObserver{}
	err := s.file.Replay(r, o)
	if err != nil {
		return err
	}
	for group, checkpointed := range r.checkpointrange {
		s.checkpointed.ids[group] = checkpointed.Intervals[0].End
	}
	for _, ent := range r.entrys {
		s.synced.ids[ent.group] = ent.commitId
	}
	//TODO uncommit entry info
	r.Apply()
	s.OnReplay(r)
	return nil
}

func (s *baseStore) Load(groupId uint32, lsn uint64) (entry.Entry, error) {
	ver, err := s.GetVersionByGLSN(groupId, lsn)
	if err != nil {
		return nil, err
	}
	return s.file.Load(ver, groupId, lsn)
}
