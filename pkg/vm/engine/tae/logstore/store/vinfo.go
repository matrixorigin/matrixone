package store

import (
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
	// Commits     map[uint32]*common.ClosedInterval
	// Checkpoints map[uint32]*common.ClosedIntervals
	// UncommitTxn map[uint32][]uint64 // 2% uncommit txn
	// // TxnCommit   map[uint32]*roaring64.Bitmap
	// TidCidMap map[uint32]map[uint64]uint64 // 5% uncommit txn

	Addrs  map[uint32]map[uint64]int //group-groupLSN-offset 5%
	addrmu sync.RWMutex

	unloaded bool
	loadmu   sync.Mutex
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

//result contains addr, addr size
// func MarshalAddrs(addrs []*VFileAddress) ([]byte, error) {
// 	addrsBuf, err := json.Marshal(addrs)
// 	if err != nil {
// 		return nil, err
// 	}
// 	size := uint32(len(addrsBuf))
// 	sizebuf := make([]byte, 4)
// 	binary.BigEndian.PutUint32(sizebuf, size)
// 	addrsBuf = append(addrsBuf, sizebuf...)
// 	return addrsBuf, nil
// }

//marshal addresses, return remained bytes
// func UnmarshalAddrs(buf []byte) ([]byte, []*VFileAddress, error) {
// 	addrs := make([]*VFileAddress, 0)
// 	size := int(binary.BigEndian.Uint32(buf[len(buf)-4:]))
// 	err := json.Unmarshal(buf[len(buf)-4-size:len(buf)-4], addrs)
// 	if err != nil {
// 		return nil, nil, err
// 	}
// 	return buf[:len(buf)-4-size], addrs, nil
// }

func newVInfo(vf *vFile) *vInfo {
	return &vInfo{
		// Commits:     make(map[uint32]*common.ClosedInterval),
		// Checkpoints: make(map[uint32]*common.ClosedIntervals),
		// UncommitTxn: make(map[uint32][]uint64),
		// TxnCommit:   make(map[string]*roaring64.Bitmap),
		// TidCidMap: make(map[uint32]map[uint64]uint64),
		groups: make(map[uint32]VGroup),
		Addrs:  make(map[uint32]map[uint64]int),
		addrmu: sync.RWMutex{},
		vf:     vf,
	}
}

func (info *vInfo) OnReplay(r *replayer) {
	info.Addrs = r.vinfoAddrs
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

// func (info *vInfo) GetCommits(groupId uint32) (commits common.ClosedInterval) {
// 	commits = *info.Commits[groupId]
// 	return commits
// }

// func (info *vInfo) GetCheckpoints(groupId uint32) (checkpoint *common.ClosedInterval) {
// 	checkpoint = make([]common.ClosedInterval, 0)
// 	for _, interval := range info.Checkpoints[groupId] {
// 		checkpoint = append(checkpoint, *interval)
// 	}
// 	return checkpoint
// }

func (info *vInfo) String() string {
	s := ""
	for gid, g := range info.groups {
		s = fmt.Sprintf("%s%d-%s\n", s, gid, g)
	}
	return s
	// s := "("
	// groups := make(map[uint32]struct{})
	// for group := range info.Commits {
	// 	groups[group] = struct{}{}
	// }
	// for group := range info.Checkpoints {
	// 	groups[group] = struct{}{}
	// }
	// for group := range info.UncommitTxn {
	// 	groups[group] = struct{}{}
	// }
	// for group := range info.TidCidMap {
	// 	groups[group] = struct{}{}
	// }
	// for group := range groups {
	// 	s = fmt.Sprintf("%s<%d>-[", s, group)

	// 	commit, ok := info.Commits[group]
	// 	if ok {
	// 		s = fmt.Sprintf("%s%s|", s, commit.String())
	// 	} else {
	// 		s = fmt.Sprintf("%sNone|", s)
	// 	}

	// 	ckps, ok := info.Checkpoints[group]
	// 	if ok {
	// 		for _, ckp := range ckps.Intervals {
	// 			s = fmt.Sprintf("%s%s", s, ckp.String())
	// 		}
	// 		s = fmt.Sprintf("%s\n", s)
	// 	} else {
	// 		s = fmt.Sprintf("%sNone\n", s)
	// 	}

	// 	uncommits, ok := info.UncommitTxn[group]
	// 	if ok {
	// 		s = fmt.Sprintf("%s %v\n", s, uncommits)
	// 	} else {
	// 		s = fmt.Sprintf("%sNone\n", s)
	// 	}

	// 	tidcid, ok := info.TidCidMap[group]
	// 	if ok {
	// 		for tid, cid := range tidcid {
	// 			s = fmt.Sprintf("%s %v-%v,", s, tid, cid)
	// 		}
	// 		s = fmt.Sprintf("%s]\n", s)
	// 	} else {
	// 		s = fmt.Sprintf("%sNone]\n", s)
	// 	}
	// }
	// s = fmt.Sprintf("%s)", s)
	// return s
}

func (info *vInfo) Log(v interface{}) error {
	if v == nil {
		return nil
	}
	vi := v.(*entry.Info)
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
		return err
	}
	info.addrmu.Lock()
	defer info.addrmu.Unlock()
	addr := vi.Info.(*VFileAddress)
	addrsMap, ok := info.Addrs[addr.Group]
	if !ok {
		addrsMap = make(map[uint64]int)
	}
	addrsMap[addr.LSN] = addr.Offset
	info.Addrs[addr.Group] = addrsMap
	// fmt.Printf("%p|addrs are %v\n", info, info.Addrs)
	return nil
}

// func (info *vInfo) LogTxnInfo(entryInfo *entry.Info) error {
// 	g,ok:=info.groups[entryInfo.Group]
// 	if !ok{
// 		g=newcommitGroup(info)
// 	}
// 	g.Log(entryInfo)
// 	info.groups[entryInfo.Group]=g
// 	return nil
// }

func (info *vInfo) LogUncommitInfo(entryInfo *entry.Info) error {
	g, ok := info.groups[entryInfo.Group]
	if !ok {
		g = newuncommitGroup(info, entryInfo.Group)
	}
	g.Log(entryInfo)
	info.groups[entryInfo.Group] = g
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
	info.groups[entryInfo.Group] = g
	return nil
}

func (info *vInfo) LogCheckpoint(entryInfo *entry.Info) error {
	g, ok := info.groups[entryInfo.Group]
	if !ok {
		g = newcheckpointGroup(info, entryInfo.Group)
	}
	g.Log(entryInfo)
	info.groups[entryInfo.Group] = g
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
