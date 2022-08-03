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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	// "github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"

	"github.com/RoaringBitmap/roaring/roaring64"
)

var (
	ErrVFileGroupNotExist = errors.New("vfile: group not existed")
	ErrVFileLsnNotExist   = errors.New("vfile: lsn not existed")
	ErrVFileOffsetTimeOut = errors.New("get vfile offset timeout")
	ErrReadMetaFailed     = errors.New("read meta failed")
)

type vInfo struct {
	vf *vFile

	groups         map[uint32]VGroup
	groupmu        sync.RWMutex
	ckpInfoVersion int //the max version covered by the post commit entry
	// Commits     map[uint32]*common.ClosedInterval
	// Checkpoints map[uint32]*common.ClosedIntervals
	// UncommitTxn map[uint32][]uint64 // 2% uncommit txn
	// // TxnCommit   map[uint32]*roaring64.Bitmap
	// TidCidMap map[uint32]map[uint64]uint64 // 5% uncommit txn

	Addrs    map[uint32]map[uint64]int //group-groupLSN-offset 5%
	addrmu   *sync.RWMutex
	addrCond sync.Cond

	unloaded    bool
	inited      bool
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
		groups:   make(map[uint32]VGroup),
		groupmu:  sync.RWMutex{},
		Addrs:    make(map[uint32]map[uint64]int),
		addrmu:   &sync.RWMutex{},
		addrCond: *sync.NewCond(new(sync.Mutex)),
		vf:       vf,

		logQueue: make(chan *entry.Info, DefaultMaxCommitSize*100),
	}
	info.flushCtx, info.flushCancel = context.WithCancel(context.Background())
	go info.logLoop()
	return info
}

func (info *vInfo) MetaWriteTo(w io.Writer) (n int64, err error) {
	sn, err := info.AddrsWriteTo(w)
	n += sn
	if err != nil {
		return
	}
	sn, err = info.UncommitWriteTo(w)
	n += sn
	if err != nil {
		return
	}
	return
}

func (info *vInfo) MetaReadFrom(r io.Reader) (n int64, err error) {
	sn, err := info.AddrsReadFrom(r)
	n += sn
	if err != nil {
		return
	}
	sn, err = info.UncommitReadFrom(r)
	n += sn
	if err != nil {
		return
	}
	return
}
func (info *vInfo) GetUncommitGidTid(lsn uint64) *entry.Tid {
	g := info.groups[entry.GTUncommit]
	if g == nil {
		return nil
	}
	uncommit := g.(*uncommitGroup)
	return uncommit.UncommitTxn[lsn]
}
func (info *vInfo) UncommitWriteTo(w io.Writer) (n int64, err error) {
	info.groupmu.RLock()
	defer info.groupmu.RUnlock()
	g := info.groups[entry.GTUncommit]
	if g == nil {
		length := uint64(0)
		if err = binary.Write(w, binary.BigEndian, length); err != nil {
			return
		}
		n += 8
		return
	}
	uncommit := g.(*uncommitGroup)
	length := uint64(len(uncommit.UncommitTxn))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 8
	for lsn, gidtid := range uncommit.UncommitTxn {
		if err = binary.Write(w, binary.BigEndian, lsn); err != nil {
			return
		}
		n += 8
		if err = binary.Write(w, binary.BigEndian, gidtid.Group); err != nil {
			return
		}
		n += 4
		if err = binary.Write(w, binary.BigEndian, gidtid.Tid); err != nil {
			return
		}
		n += 8
	}
	return
}
func (info *vInfo) UncommitReadFrom(r io.Reader) (n int64, err error) {
	length := uint64(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	n += 8
	if length == 0 {
		return
	}
	g := newuncommitGroup(info, entry.GTUncommit)
	for i := 0; i < int(length); i++ {
		lsn := uint64(0)
		if err = binary.Read(r, binary.BigEndian, &lsn); err != nil {
			return
		}
		n += 4
		gid := uint32(0)
		if err = binary.Read(r, binary.BigEndian, &gid); err != nil {
			return
		}
		n += 4
		tid := uint64(0)
		if err = binary.Read(r, binary.BigEndian, &tid); err != nil {
			return
		}
		n += 4
		g.UncommitTxn[lsn] = &entry.Tid{Group: gid, Tid: tid}
	}
	info.groups[entry.GTUncommit] = g
	return
}
func (info *vInfo) AddrsWriteTo(w io.Writer) (n int64, err error) {
	info.addrmu.RLock()
	defer info.addrmu.RUnlock()
	length := uint64(len(info.Addrs))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 8
	for gid, addrs := range info.Addrs {
		if err = binary.Write(w, binary.BigEndian, gid); err != nil {
			return
		}
		n += 4
		length := uint64(len(addrs))
		if err = binary.Write(w, binary.BigEndian, length); err != nil {
			return
		}
		n += 8
		for lsn, offset := range addrs {
			if err = binary.Write(w, binary.BigEndian, lsn); err != nil {
				return
			}
			n += 8
			if err = binary.Write(w, binary.BigEndian, uint64(offset)); err != nil {
				return
			}
			n += 8
		}
	}
	return
}

func (info *vInfo) AddrsReadFrom(r io.Reader) (n int64, err error) {
	length := uint64(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	n += 8
	info.Addrs = make(map[uint32]map[uint64]int)
	for i := 0; i < int(length); i++ {
		gid := uint32(0)
		if err = binary.Read(r, binary.BigEndian, &gid); err != nil {
			return
		}
		n += 4
		length2 := uint64(0)
		if err = binary.Read(r, binary.BigEndian, &length2); err != nil {
			return
		}
		n += 8
		info.Addrs[gid] = make(map[uint64]int)
		for j := 0; j < int(length2); j++ {
			lsn := uint64(0)
			if err = binary.Read(r, binary.BigEndian, &lsn); err != nil {
				return
			}
			n += 8
			offset := uint64(0)
			if err = binary.Read(r, binary.BigEndian, &offset); err != nil {
				return
			}
			n += 8
			info.Addrs[gid][lsn] = int(offset)
		}
	}
	return
}

// not safe
func (info *vInfo) GetAddrs() (map[uint32]map[uint64]int, *sync.RWMutex) {
	return info.Addrs, info.addrmu
}

func (info *vInfo) MarshalMeta() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = info.MetaWriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (info *vInfo) UnmarshalMeta(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	e := entry.GetBase()
	defer e.Free()

	metaBuf := e.GetMetaBuf()
	_, err := bbuf.Read(metaBuf)
	if err != nil {
		return err
	}
	if e.GetType() != entry.ETMeta {
		return ErrReadMetaFailed
	}
	if e.TotalSize() != len(buf) {
		return ErrReadMetaFailed
	}
	if e.GetInfoSize() != 0 {
		return ErrReadMetaFailed
	}
	_, err = e.ReadFrom(bbuf)
	if err != nil {
		return err
	}
	bbuf2 := bytes.NewBuffer(e.GetPayload())
	_, err = info.MetaReadFrom(bbuf2)
	return err
}

func (info *vInfo) OnReplay(r *replayer) {
	if info.unloaded && info.vf.committed == int32(1) {
		info.vf.WriteMeta()
		err := info.vf.Sync()
		if err != nil {
			panic(err)
		}
	}
	info.inited = true
	info.flushWg.Wait()
}

func (info *vInfo) LoadMeta() error {
	info.loadmu.Lock()
	defer info.loadmu.Unlock()
	if info.inited && !info.unloaded {
		return nil
	}
	err := info.vf.readMeta()
	if err != nil {
		return err
	}
	info.inited = true
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

// history new cp -> cp merge+ is covered -> info merge -> group merge
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

// TODO: for ckp with payload, merge ckp after IsCovered()
func (info *vInfo) IsToDelete(c *compactor) (toDelete bool) {
	toDelete = true
	// for _, g := range info.groups {
	// 	g.MergeCheckpointInfo(c)
	// }
	for _, g := range info.groups {
		if !g.IsCovered(c) {
			// logutil.Infof("%p not covered %d\ntcmap:%v\nckp%v\nckpver %d\ng:%v\n",info, info.vf.Id(), c.tidCidMap, c.checkpointed, c.ckpInfoVersion, g)
			toDelete = false
		}
	}
	// if c.ckpInfoVersion < info.ckpInfoVersion {
	// 	c.ckpInfoVersion = info.ckpInfoVersion
	// }
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
			if err != nil {
				panic(err)
			}
		case entry.GTUncommit:
			err = info.LogUncommitInfo(vi)
			if err != nil {
				panic(err)
			}
		case entry.GTInternal:
			err = info.LogInternalInfo(vi)
			if err != nil {
				panic(err)
			}
		default:
			err = info.LogCommit(vi)
			if err != nil {
				panic(err)
			}
		}
		if err != nil {
			panic(err)
		}
		if vi.Info == nil {
			continue
		}
		info.addrmu.Lock()
		addr := vi.Info.(*VFileAddress)
		addrsMap, ok := info.Addrs[addr.Group]
		if !ok {
			addrsMap = make(map[uint64]int)
		}
		// logutil.Infof("log addr %d-%d at %d-%d", vi.Group, vi.GroupLSN, info.vf.Id(), addr.Offset)
		addrsMap[addr.LSN] = addr.Offset
		info.Addrs[addr.Group] = addrsMap
		info.addrmu.Unlock()
	}
	info.addrCond.L.Lock()
	info.addrCond.Broadcast()
	info.addrCond.L.Unlock()
	info.flushWg.Add(-1 * len(infos))
}

func (info *vInfo) close() {
	info.flushWg.Wait()
	info.flushCancel()
}

func (info *vInfo) Log(v any) error {
	if v == nil {
		return nil
	}
	info.flushWg.Add(1)
	info.logQueue <- v.(*entry.Info)
	// fmt.Printf("%p|addrs are %v\n", info, info.Addrs)
	return nil
}
func (info *vInfo) LogInternalInfo(entryInfo *entry.Info) error {
	id := entryInfo.PostCommitVersion
	if info.ckpInfoVersion < id {
		info.ckpInfoVersion = id
	}
	return nil
}
func (info *vInfo) LogUncommitInfo(entryInfo *entry.Info) error {
	g, ok := info.groups[entryInfo.Group]
	if !ok {
		g = newuncommitGroup(info, entryInfo.Group)
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
	err := g.Log(entryInfo)
	if err != nil {
		return err
	}
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
		logutil.Infof("group %d", groupId)
		logutil.Infof("%p|addrs are %v", info, info.Addrs)
		return 0, ErrVFileGroupNotExist
	}
	offset, ok := lsnMap[lsn]
	if !ok {
		return 0, ErrVFileLsnNotExist
	}
	return offset, nil
}
