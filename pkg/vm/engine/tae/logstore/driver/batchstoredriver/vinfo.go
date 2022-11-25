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
	"sync"

	// "github.com/matrixorigin/matrixone/pkg/logutil"
	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	ErrVFileGroupNotExist = moerr.NewInternalErrorNoCtx("vfile: group not existed")
	ErrVFileLsnNotExist   = moerr.NewInternalErrorNoCtx("vfile: lsn not existed")
	ErrVFileOffsetTimeOut = moerr.NewInternalErrorNoCtx("get vfile offset timeout")
	ErrReadMetaFailed     = moerr.NewInternalErrorNoCtx("read meta failed")
)

type vInfo struct {
	vf *vFile

	Addrs    map[uint64]int //lsn-offset //r
	addrmu   *sync.RWMutex
	addrCond sync.Cond

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

func (info *vInfo) String() string {
	s := ""
	// info.groupmu.RLock()
	// for gid, g := range info.groups {
	// 	s = fmt.Sprintf("%s%d-%s\n", s, gid, g.String())
	// }
	// info.groupmu.RUnlock()
	return s
}
func (info *vInfo) onReplay(offset int, lsn uint64) {
	info.Addrs[lsn] = offset
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
	return nil
}

func (info *vInfo) GetOffsetByLSN(lsn uint64) (int, error) {
	info.addrmu.RLock()
	defer info.addrmu.RUnlock()
	offset, ok := info.Addrs[lsn]
	if !ok {
		return 0, ErrVFileLsnNotExist
	}
	return offset, nil
}
