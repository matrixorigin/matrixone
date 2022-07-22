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

package batchstoredriver

import (
	"context"
	"errors"
	"sync"

	// "github.com/matrixorigin/matrixone/pkg/logutil"
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

	ckpInfoVersion int

	Addrs    map[uint64]int //lsn-offset
	addrmu   *sync.RWMutex
	addrCond sync.Cond

	unloaded    bool
	inited      bool
	loadmu      sync.Mutex
	logQueue    chan any
	flushWg     sync.WaitGroup
	flushCtx    context.Context
	flushCancel context.CancelFunc
}

type VFileUncommitInfo struct {
	Index *roaring64.Bitmap
	Addr  *VFileAddress
}

type VFileAddress struct {
	LSN     uint64
	Version int
	Offset  int
}

func newVInfo(vf *vFile) *vInfo {
	info := &vInfo{
		Addrs:    make(map[uint64]int),
		addrmu:   &sync.RWMutex{},
		addrCond: *sync.NewCond(new(sync.Mutex)),
		vf:       vf,

		logQueue: make(chan any, DefaultMaxCommitSize*100),
	}
	info.flushCtx, info.flushCancel = context.WithCancel(context.Background())
	go info.logLoop()
	return info
}

// func (info *vInfo) OnReplay(r *replayer) {
// 	if info.unloaded && info.vf.committed == int32(1) {
// 		info.vf.WriteMeta()
// 		err := info.vf.Sync()
// 		if err != nil {
// 			panic(err)
// 		}
// 	}
// 	info.inited = true
// 	info.flushWg.Wait()
// }

// func (info *vInfo) LoadMeta() error {
// 	info.loadmu.Lock()
// 	defer info.loadmu.Unlock()
// 	if info.inited && !info.unloaded {
// 		return nil
// 	}
// 	err := info.vf.readMeta()
// 	if err != nil {
// 		return err
// 	}
// 	info.inited = true
// 	info.unloaded = false
// 	return nil
// }

// func (info *vInfo) FreeMeta() {
// 	info.loadmu.Lock()
// 	defer info.loadmu.Unlock()
// 	info.groups = nil
// 	info.Addrs = nil
// 	info.unloaded = true
// }

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


// TODO: for ckp with payload, merge ckp after IsCovered()
// func (info *vInfo) IsToDelete(c *compactor) (toDelete bool) {
// 	toDelete = true
// 	// for _, g := range info.groups {
// 	// 	g.MergeCheckpointInfo(c)
// 	// }
// 	for _, g := range info.groups {
// 		if !g.IsCovered(c) {
// 			// logutil.Infof("%p not covered %d\ntcmap:%v\nckp%v\nckpver %d\ng:%v\n",info, info.vf.Id(), c.tidCidMap, c.checkpointed, c.ckpInfoVersion, g)
// 			toDelete = false
// 		}
// 	}
// 	// if c.ckpInfoVersion < info.ckpInfoVersion {
// 	// 	c.ckpInfoVersion = info.ckpInfoVersion
// 	// }
// 	return
// }

func (info *vInfo) String() string {
	s := ""
	// info.groupmu.RLock()
	// for gid, g := range info.groups {
	// 	s = fmt.Sprintf("%s%d-%s\n", s, gid, g.String())
	// }
	// info.groupmu.RUnlock()
	return s
}

func (info *vInfo) logLoop() {
	infos := make([]any, 0, DefaultMaxCommitSize)
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

func (info *vInfo) onLog(infos []any) {
	for _, vi := range infos {
		// var err error
		// switch vi.Group {
		// default:
		// 	err = info.LogCommit(vi)
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// }
		// if err != nil {
		// 	panic(err)
		// }
		// if vi.Info == nil {
		// 	continue
		// }
		info.addrmu.Lock()
		addr := vi.(*VFileAddress)
		info.Addrs[addr.LSN] = addr.Offset
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
	info.logQueue <- v.(*VFileAddress)
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

// func (info *vInfo) LogCommit(entryInfo *entry.Info) error {
// 	g, ok := info.groups[entryInfo.Group]
// 	if !ok {
// 		g = newcommitGroup(info, entryInfo.Group)
// 	}
// 	err := g.Log(entryInfo)
// 	if err != nil {
// 		return err
// 	}
// 	info.groupmu.Lock()
// 	info.groups[entryInfo.Group] = g
// 	info.groupmu.Unlock()
// 	return nil
// }

func (info *vInfo) GetOffsetByLSN(lsn uint64) (int, error) {
	info.addrmu.RLock()
	defer info.addrmu.RUnlock()
	offset, ok := info.Addrs[lsn]
	if !ok {
		return 0, ErrVFileLsnNotExist
	}
	return offset, nil
}
