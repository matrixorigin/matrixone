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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

var Metasize = 2

type vFileState struct {
	bufPos int
	pos    int
	file   *vFile
}

type vFile struct {
	*sync.RWMutex
	*os.File
	*vInfo
	version    int
	committed  int32
	size       int //update when write
	wg         sync.WaitGroup
	commitCond sync.Cond
	history    History
	buf        *bytes.Buffer
	bufpos     int //update when write
	syncpos    int //update when sync

	bsInfo *storeInfo
}

func newVFile(mu *sync.RWMutex, name string, version int, history History, bsInfo *storeInfo) (*vFile, error) {
	if mu == nil {
		mu = new(sync.RWMutex)
	}
	file, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	vf := &vFile{
		RWMutex:    mu,
		File:       file,
		version:    version,
		commitCond: *sync.NewCond(new(sync.Mutex)),
		history:    history,
		buf:        &bytes.Buffer{},
	}
	vf.vInfo = newVInfo(vf)
	return vf, nil
}

// func (vf *vFile) InCommits(intervals map[uint32]*common.ClosedIntervals) bool {
// 	for group, commits := range vf.Commits {
// 		interval, ok := intervals[group]
// 		if !ok {
// 			return false
// 		}
// 		if !interval.ContainsInterval(*commits) {
// 			return false
// 		}
// 	}
// 	return true
// }

func (vf *vFile) String() string {
	var w bytes.Buffer
	w.WriteString(fmt.Sprintf("[%s]\n%s", vf.Name(), vf.vInfo.String()))
	return w.String()
}

func (vf *vFile) Archive() error {
	if vf.history == nil {
		if err := vf.Destroy(); err != nil {
			return err
		}
	}
	vf.history.Append(vf)
	return nil
}

func (vf *vFile) Id() int {
	return vf.version
}

func (vf *vFile) GetState() *vFileState {
	vf.RLock()
	defer vf.RUnlock()
	return &vFileState{
		bufPos: vf.bufpos,
		pos:    vf.size,
		file:   vf,
	}
}

func (vf *vFile) HasCommitted() bool {
	return atomic.LoadInt32(&vf.committed) == int32(1)
}

func (vf *vFile) PrepareWrite(size int) {
	vf.wg.Add(1)
}

func (vf *vFile) FinishWrite() {
	vf.wg.Done()
}

func (vf *vFile) Close() error {
	err := vf.File.Close()
	if err != nil {
		return err
	}
	vf.buf = nil
	return nil
}

func (vf *vFile) Commit() {
	logutil.Infof("Committing %s\n", vf.Name())
	vf.wg.Wait()
	vf.flushWg.Wait()
	vf.WriteMeta()
	err := vf.Sync()
	if err != nil {
		panic(err)
	}
	vf.Lock()
	vf.buf = nil
	vf.Unlock()
	vf.commitCond.L.Lock()
	atomic.StoreInt32(&vf.committed, int32(1))
	vf.commitCond.Broadcast()
	vf.commitCond.L.Unlock()
	vf.vInfo.close()
	// fmt.Printf("sync-%s\n", vf.String())
	// vf.FreeMeta()
}

func (vf *vFile) Sync() error {
	vf.Lock()
	defer vf.Unlock()
	if vf.bsInfo != nil {
		vf.bsInfo.syncTimes++
	}
	if vf.buf == nil {
		err := vf.File.Sync()
		return err
	}
	targetSize := vf.size
	targetpos := vf.bufpos
	t0 := time.Now()
	buf := vf.buf.Bytes()
	n, err := vf.File.WriteAt(buf[:targetpos], int64(vf.syncpos))
	if n != targetpos {
		panic("logic err")
	}
	if vf.bsInfo != nil {
		vf.bsInfo.writeDuration += time.Since(t0)
	}
	if err != nil {
		return err
	}
	// fmt.Printf("%p|sync [%v,%v](total%v|n=%d)\n", vf, vf.syncpos, vf.syncpos+vf.bufpos, vf.bufpos, n)
	// buf := make([]byte, 10)
	// _, err = vf.ReadAt(buf, int64(vf.syncpos))
	// fmt.Printf("%p|read at %v, buf is %v, n=%d, err is %v\n", vf, vf.syncpos, buf, n, err)
	vf.syncpos += targetpos
	// fmt.Printf("syncpos is %v\n", vf.syncpos)
	if vf.syncpos != targetSize {
		panic(fmt.Sprintf("%p|logic error, sync %v, size %v", vf, vf.syncpos, targetSize))
	}
	vf.bufpos = 0
	vf.buf.Reset()
	// fmt.Printf("199bufpos is %v\n",vf.bufpos)
	t0 = time.Now()
	err = vf.File.Sync()
	if err != nil {
		return err
	}

	if vf.bsInfo != nil {
		vf.bsInfo.syncDuration += time.Since(t0)
	}
	return nil
}

func (vf *vFile) WriteMeta() {
	e := entry.GetBase()
	defer e.Free()
	buf, err := vf.MarshalMeta()
	if err != nil {
		panic(err)
	}
	e.SetType(entry.ETMeta)
	err = e.Unmarshal(buf)
	if err != nil {
		panic(err)
	}
	n1, err := vf.WriteAt(e.GetMetaBuf(), int64(vf.size))
	if err != nil {
		panic(err)
	}
	n2, err := vf.WriteAt(e.GetPayload(), int64(vf.size))
	if err != nil {
		panic(err)
	}
	if n1+n2 != e.TotalSize() {
		panic("logic err")
	}

	buf = make([]byte, Metasize)
	binary.BigEndian.PutUint16(buf, uint16(e.TotalSize()))
	n, err := vf.WriteAt(buf, int64(vf.size))
	if err != nil {
		panic(err)
	}
	if n != 2 {
		panic("logic err")
	}
}

func (vf *vFile) WaitCommitted() {
	if atomic.LoadInt32(&vf.committed) == int32(1) {
		return
	}
	vf.commitCond.L.Lock()
	if atomic.LoadInt32(&vf.committed) != int32(1) {
		vf.commitCond.Wait()
	}
	vf.commitCond.L.Unlock()
}

func (vf *vFile) WriteAt(b []byte, off int64) (n int, err error) {
	dataLength := len(b)
	vf.Lock()
	if vf.buf == nil {
		vf.bufpos = 0
		vf.buf = &bytes.Buffer{}
	}
	if dataLength+int(off)-vf.syncpos > vf.buf.Cap() {
		vf.buf.Grow(dataLength + int(off) - vf.syncpos)
	}
	n, err = vf.buf.Write(b)
	if err != nil {
		panic(err)
	}
	// logutil.Infof("%p|write in buf[%v,%v]", vf, int(off)-vf.syncpos, int(off)-vf.syncpos+n)
	// logutil.Infof("%p|write vf in buf [%v,%v]", vf, int(off), int(off)+n)
	vf.bufpos = int(off) + n - vf.syncpos
	vf.size += n
	// logutil.Infof("%p|size is %v",vf,vf.size)
	vf.Unlock()
	// logutil.Infof("%p|bufpos is %v",vf,vf.bufpos)
	if err != nil {
		return
	}
	return
}

func (vf *vFile) Write(b []byte) (n int, err error) {
	n, err = vf.File.Write(b)
	if err != nil {
		return
	}
	return
}

func (vf *vFile) SizeLocked() int {
	vf.RLock()
	defer vf.RUnlock()
	return vf.size
}

func (vf *vFile) Destroy() error {
	if err := vf.Close(); err != nil {
		return err
	}
	name := vf.Name()
	logutil.Infof("Removing version file: %s", name)
	err := os.Remove(name)
	return err
}

func (vf *vFile) Replay(r *replayer, observer ReplayObserver) error {
	observer.OnNewEntry(vf.Id())
	for {
		if err := r.replayHandler(vf, vf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}
	vf.OnReplay(r)
	return nil
}

func (vf *vFile) ReplayCWithCkp(r *replayer, observer ReplayObserver) error {
	observer.OnNewEntry(vf.Id())
	if err := r.replayHandlerWithCkpForCommitGroups(vf, vf); err != nil {
		return err
	}
	vf.OnReplay(r)
	return nil
}

func (vf *vFile) ReplayUCWithCkp(r *replayer, observer ReplayObserver) error {
	observer.OnNewEntry(vf.Id())
	if err := r.replayHandlerWithCkpForUCGroups(vf, vf); err != nil {
		return err
	}
	vf.OnReplay(r)
	return nil
}
func (vf *vFile) OnNewEntry(int) {}
func (vf *vFile) OnLogInfo(info *entry.Info) {
	err := vf.Log(info)
	if err != nil {
		panic(err)
	}
}
func (vf *vFile) OnNewCheckpoint(info *entry.Info) {
	err := vf.Log(info)
	if err != nil {
		panic(err)
	}
}
func (vf *vFile) OnNewTxn(info *entry.Info) {
	err := vf.Log(info)
	if err != nil {
		panic(err)
	}
}
func (vf *vFile) OnNewUncommit(addrs []*VFileAddress) {}

func (vf *vFile) Load(groupID uint32, lsn uint64) (entry.Entry, error) {
	// if vf.HasCommitted() {
	// err := vf.LoadMeta()
	// defer vf.FreeMeta()
	// if err != nil {
	// 	return nil, err
	// }
	// }
	offset, err := vf.GetOffsetByLSN(groupID, lsn)
	if err == ErrVFileGroupNotExist || err == ErrVFileLsnNotExist {
		for i := 0; i < 10; i++ {
			logutil.Infof("load retry %d-%d", groupID, lsn)
			vf.addrCond.L.Lock()
			offset, err = vf.GetOffsetByLSN(groupID, lsn)
			if err == nil {
				vf.addrCond.L.Unlock()
				break
			}
			vf.addrCond.Wait()
			vf.addrCond.L.Unlock()
			offset, err = vf.GetOffsetByLSN(groupID, lsn)
			if err == nil {
				break
			}
		}
		if err != nil {
			return nil, ErrVFileVersionTimeOut
		}
	}
	if err != nil {
		return nil, err
	}
	return vf.readEntryAt(offset)
}
func (vf *vFile) LoadByOffset(offset int) (entry.Entry, error) {
	return vf.readEntryAt(offset)
}
func (vf *vFile) readEntryAt(offset int) (entry.Entry, error) {
	entry := entry.GetBase()
	metaBuf := entry.GetMetaBuf()
	_, err := vf.ReadAt(metaBuf, int64(offset))
	// fmt.Printf("%p|read meta [%v,%v]\n", vf, offset, offset+n)
	if err != nil {
		return nil, err
	}
	_, err = entry.ReadAt(vf.File, offset)
	return entry, err
}
func (vf *vFile) OnReplayCommitted() {
	vf.committed = 1
}
func (vf *vFile) readMeta() error {
	buf := make([]byte, Metasize)
	_, err := vf.ReadAt(buf, int64(vf.size)-int64(Metasize))
	if err != nil {
		return ErrReadMetaFailed
	}
	size := binary.BigEndian.Uint16(buf)
	buf = make([]byte, int(size))
	_, err = vf.ReadAt(buf, int64(vf.size)-int64(Metasize)-int64(size))
	if err != nil {
		return ErrReadMetaFailed
	}
	err = vf.UnmarshalMeta(buf)
	if err != nil {
		return ErrReadMetaFailed
	}
	return nil
}
