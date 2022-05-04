package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"

	"github.com/RoaringBitmap/roaring/roaring64"
)

type vInfo struct {
	vf *vFile

	groups map[uint32]VGroup
	groupmu sync.RWMutex
	// Commits     map[uint32]*common.ClosedInterval
	// Checkpoints map[uint32]*common.ClosedIntervals
	// UncommitTxn map[uint32][]uint64 // 2% uncommit txn
	// // TxnCommit   map[uint32]*roaring64.Bitmap
	// TidCidMap map[uint32]map[uint64]uint64 // 5% uncommit txn

	Addrs  map[uint32]map[uint64]int //group-groupLSN-offset 5%
	addrmu sync.RWMutex

	unloaded    bool
	loadmu      sync.Mutex
	logQueue    chan *entry.Info
	flushWg     sync.WaitGroup
	flushCtx    context.Context
	flushCancel context.CancelFunc
}

type VFileUncommitInfo struct {
	Index *roaring64.Bitmap
	Addr  *VFileAddress
}

type VFileAddress struct {
	Group   uint32
	LSN     uint64
	Version int
	Offset  int
}

func newVInfo(vf *vFile) *vInfo {
	info := &vInfo{
		// Commits:     make(map[uint32]*common.ClosedInterval),
		// Checkpoints: make(map[uint32]*common.ClosedIntervals),
		// UncommitTxn: make(map[uint32][]uint64),
		// TxnCommit:   make(map[string]*roaring64.Bitmap),
		// TidCidMap: make(map[uint32]map[uint64]uint64),
		groups: make(map[uint32]VGroup),
		groupmu: sync.RWMutex{},
		Addrs:  make(map[uint32]map[uint64]int),
		addrmu: sync.RWMutex{},
		vf:     vf,

		logQueue: make(chan *entry.Info, DefaultMaxCommitSize*100),
	}
	info.flushCtx, info.flushCancel = context.WithCancel(context.Background())
	go info.logLoop()
	return info
}

func (info *vInfo) OnReplay(r *replayer) {
	info.addrmu.Lock()
	info.Addrs = r.vinfoAddrs
	info.addrmu.Unlock()
}

func (info *vInfo) LoadMeta() error {
	info.loadmu.Lock()
	defer info.loadmu.Unlock()
	if !info.unloaded {
		return nil
	}
	err := info.vf.readMeta()
	if err != nil {
		return err
	}
	info.unloaded = false
	return nil
}

func (info *vInfo) FreeMeta() {
	info.loadmu.Lock()
	defer info.loadmu.Unlock()
	info.groups = nil
	info.Addrs = nil
	info.unloaded = true
}

// func (info *vInfo) ReadMeta(vf *vFile) error {
// 	buf := make([]byte, Metasize)
// 	vf.ReadAt(buf, int64(vf.size)-int64(Metasize))
// 	size := binary.BigEndian.Uint16(buf)
// 	buf = make([]byte, int(size))
// 	vf.ReadAt(buf, int64(vf.size)-int64(Metasize)-int64(size))
// 	json.Unmarshal(buf, info)
// 	if info == nil {
// 		return errors.New("read vfile meta failed")
// 	}
// 	return nil
// }

//history new cp -> cp merge+ is covered -> info merge -> group merge
func (info *vInfo) getGroupById(groupId uint32) VGroup {
	g := info.groups[groupId]
	return g
}
func (info *vInfo) MetatoBuf() []byte {
	buf, _ := json.Marshal(info)
	return buf
}
func (info *vInfo) PrepareCompactor(c *compactor) {
	for _, g := range info.groups {
		g.PrepareMerge(c)
	}
}
func (info *vInfo) IsToDelete(c *compactor) (toDelete bool) {
	toDelete = true
	for _, g := range info.groups {
		if g.IsCheckpointGroup() {
			// fmt.Printf("not covered\ntcmap:%v\nckp%v\ng:%v\n",c.tidCidMap,c.gIntervals,g)
			toDelete = false
		}
		if g.IsCommitGroup() {
			if !g.IsCovered(c) {
				// fmt.Printf("not covered\ntcmap:%v\nckp%v\ng:%v\n",c.tidCidMap,c.gIntervals,g)
				toDelete = false
			}
		}
		g.MergeCheckpointInfo(c)
	}
	for _, g := range info.groups {
		if g.IsUncommitGroup() {
			if !g.IsCovered(c) {
				toDelete = false
			}
		}
	}
	return
}

func (info *vInfo) String() string {
	s := ""
	info.groupmu.RLock()
	for gid, g := range info.groups {
		s = fmt.Sprintf("%s%d-%s\n", s, gid, g.String())
	}
	info.groupmu.RUnlock()
	return s
}

func (info *vInfo) logLoop() {
	infos := make([]*entry.Info, 0, DefaultMaxCommitSize)
	for {
		select {
		case <-info.flushCtx.Done():
			return
		case entryInfo := <-info.logQueue:
			infos = append(infos, entryInfo)
		Left:
			for i := 0; i < DefaultMaxCommitSize-1; i++ {
				select {
				case entryInfo = <-info.logQueue:
					infos = append(infos, entryInfo)
				default:
					break Left
				}
			}
			info.onLog(infos)
			infos = infos[:0]
		}
	}
}

func (info *vInfo) onLog(infos []*entry.Info) {
	for _, vi := range infos {
		var err error
		switch vi.Group {
		case entry.GTCKp:
			err = info.LogCheckpoint(vi)
		case entry.GTUncommit:
			err = info.LogUncommitInfo(vi)
		default:
			err = info.LogCommit(vi)
		}
		if err != nil {
			panic(err)
		}
		info.addrmu.Lock()
		addr := vi.Info.(*VFileAddress)
		addrsMap, ok := info.Addrs[addr.Group]
		if !ok {
			addrsMap = make(map[uint64]int)
		}
		addrsMap[addr.LSN] = addr.Offset
		info.Addrs[addr.Group] = addrsMap
		info.addrmu.Unlock()
	}
	info.flushWg.Add(-1 * len(infos))
}

func (info *vInfo) close() {
	info.flushWg.Wait()
	info.flushCancel()
}

func (info *vInfo) Log(v interface{}) error {
	if v == nil {
		return nil
	}
	info.flushWg.Add(1)
	info.logQueue <- v.(*entry.Info)
	// fmt.Printf("%p|addrs are %v\n", info, info.Addrs)
	return nil
}

func (info *vInfo) LogUncommitInfo(entryInfo *entry.Info) error {
	g, ok := info.groups[entryInfo.Group]
	if !ok {
		g = newuncommitGroup(info, entryInfo.Group)
	}
	g.Log(entryInfo)
	info.groupmu.Lock()
	info.groups[entryInfo.Group] = g
	info.groupmu.Unlock()
	return nil
}

func (info *vInfo) LogCommit(entryInfo *entry.Info) error {
	g, ok := info.groups[entryInfo.Group]
	if !ok {
		g = newcommitGroup(info, entryInfo.Group)
	}
	err := g.Log(entryInfo)
	if err != nil {
		return err
	}
	info.groupmu.Lock()
	info.groups[entryInfo.Group] = g
	info.groupmu.Unlock()
	return nil
}

func (info *vInfo) LogCheckpoint(entryInfo *entry.Info) error {
	g, ok := info.groups[entryInfo.Group]
	if !ok {
		g = newcheckpointGroup(info, entryInfo.Group)
	}
	g.Log(entryInfo)
	info.groupmu.Lock()
	info.groups[entryInfo.Group] = g
	info.groupmu.Unlock()
	return nil
}

func (info *vInfo) GetOffsetByLSN(groupId uint32, lsn uint64) (int, error) {
	info.addrmu.RLock()
	defer info.addrmu.RUnlock()
	lsnMap, ok := info.Addrs[groupId]
	if !ok {
		// fmt.Printf("%p|addrs are %v\n", info, info.Addrs)
		return 0, errors.New("vinfo group not existed")
	}
	offset, ok := lsnMap[lsn]
	if !ok {
		return 0, errors.New("vinfo lsn not existed")
	}
	return offset, nil
}
