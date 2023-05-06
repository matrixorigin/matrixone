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
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
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
	committed  atomic.Int32
	size       int //update when write
	wg         sync.WaitGroup
	commitCond sync.Cond
	history    History
	buf        *bytes.Buffer
	bufpos     int //update when write
	syncpos    int //update when sync

}

func newVFile(mu *sync.RWMutex, name string, version int, history History) (*vFile, error) {
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

func (vf *vFile) String() string {
	var w bytes.Buffer
	w.WriteString(fmt.Sprintf("[%s]\n%s", vf.Name(), vf.vInfo.String()))
	return w.String()
}

func (vf *vFile) Archive() error {
	// if vf.history == nil {
	// 	if err := vf.Destroy(); err != nil {
	// 		return err
	// 	}
	// }
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
	return vf.committed.Load() == int32(1)
}

func (vf *vFile) PrepareWrite(size int) {
	vf.wg.Add(1)
}

func (vf *vFile) FinishWrite() {
	vf.wg.Done()
}

func (vf *vFile) Close() error {
	// logutil.Infof("v%d addr is %v",vf.Id(),vf.Addrs)
	err := vf.File.Close()
	if err != nil {
		return err
	}
	if vf.vInfo != nil {
		vf.vInfo.close()
	}
	vf.buf = nil
	return nil
}

func (vf *vFile) Commit() {
	logutil.Debugf("Committing %s\n", vf.Name())
	vf.wg.Wait()
	vf.flushWg.Wait()
	err := vf.Sync()
	if err != nil {
		panic(err)
	}
	vf.Lock()
	vf.buf = nil
	vf.Unlock()
	vf.commitCond.L.Lock()
	vf.committed.Store(1)
	vf.commitCond.Broadcast()
	vf.commitCond.L.Unlock()
	vf.vInfo.close()
	// logutil.Infof("sync-%s\n", vf.String())
	// vf.FreeMeta()
}

func (vf *vFile) Sync() error {
	vf.Lock()
	defer vf.Unlock()
	if vf.buf == nil {
		err := vf.File.Sync()
		return err
	}
	targetSize := vf.size
	targetpos := vf.bufpos
	buf := vf.buf.Bytes()
	n, err := vf.File.WriteAt(buf[:targetpos], int64(vf.syncpos))
	if n != targetpos {
		panic(fmt.Sprintf("logic err, expect %d, write %d err is %v", targetpos, n, err))
	}
	if err != nil {
		return err
	}
	// logutil.Infof("%p|sync [%v,%v](total%v|n=%d)\n", vf, vf.syncpos, vf.syncpos+vf.bufpos, vf.bufpos, n)
	// buf := make([]byte, 10)
	// _, err = vf.ReadAt(buf, int64(vf.syncpos))
	// logutil.Infof("%p|read at %v, buf is %v, n=%d, err is %v\n", vf, vf.syncpos, buf, n, err)
	vf.syncpos += targetpos
	// logutil.Infof("syncpos is %v\n", vf.syncpos)
	if vf.syncpos != targetSize {
		panic(fmt.Sprintf("%p|logic error, sync %v, size %v", vf, vf.syncpos, targetSize))
	}
	vf.bufpos = 0
	vf.buf.Reset()
	// logutil.Infof("199bufpos is %v\n",vf.bufpos)
	err = vf.File.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (vf *vFile) WaitCommitted() {
	if vf.committed.Load() == int32(1) {
		return
	}
	vf.commitCond.L.Lock()
	if vf.committed.Load() != int32(1) {
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
	logutil.Debugf("Removing version file: %s", name)
	err := os.Remove(name)
	return err
}

func (vf *vFile) Replay(r *replayer) error {
	for {
		if err := r.replayHandler(vf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}
	// logutil.Infof("v%d addr is %v",vf.Id(),vf.Addrs)
	return nil
}

func (vf *vFile) OnLogInfo(info any) {
	err := vf.Log(info)
	if err != nil {
		panic(err)
	}
}
func (vf *vFile) Load(lsn uint64) (*entry.Entry, error) {
	offset, err := vf.GetOffsetByLSN(lsn)
	if err == ErrVFileGroupNotExist || err == ErrVFileLsnNotExist {
		for i := 0; i < 10; i++ {
			logutil.Debugf("load retry %d", lsn)
			vf.addrCond.L.Lock()
			offset, err = vf.GetOffsetByLSN(lsn)
			if err == nil {
				vf.addrCond.L.Unlock()
				break
			}
			vf.addrCond.Wait()
			vf.addrCond.L.Unlock()
			offset, err = vf.GetOffsetByLSN(lsn)
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
func (vf *vFile) LoadByOffset(offset int) (*entry.Entry, error) {
	return vf.readEntryAt(offset)
}
func (vf *vFile) readEntryAt(offset int) (*entry.Entry, error) {
	e := entry.NewEmptyEntry()
	_, err := e.ReadAt(vf.File, offset)
	return e, err
}
func (vf *vFile) OnReplayCommitted() {
	vf.committed.Store(1)
}
