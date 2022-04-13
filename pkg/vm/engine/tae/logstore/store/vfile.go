package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	log "github.com/sirupsen/logrus"
)

var Metasize = 2
var DefaultBufSize = common.M * 20

type vFileState struct {
	bufPos  int
	bufSize int
	pos     int
	file    *vFile
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
	buf        []byte
	bufpos     int //update when write
	syncpos    int //update when sync
	bufSize    int

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
		buf:        make([]byte, DefaultBufSize),
		bufSize:    int(DefaultBufSize),
	}
	vf.vInfo = newVInfo(vf)
	return vf, nil
}

func (vf *vFile) InCommits(intervals map[uint32]*common.ClosedIntervals) bool {
	for group, commits := range vf.Commits {
		interval, ok := intervals[group]
		if !ok {
			return false
		}
		if !interval.ContainsInterval(*commits) {
			return false
		}
	}
	return true
}

func (vf *vFile) InCheckpoint(intervals map[uint32]*common.ClosedIntervals) bool {
	for group, ckps := range vf.Checkpoints {
		interval, ok := intervals[group]
		if !ok {
			return false
		}
		for _, ckp := range ckps.Intervals {
			if !interval.ContainsInterval(*ckp) {
				return false
			}
		}
	}
	return true
}

// TODO: process multi checkpoints.
func (vf *vFile) MergeCheckpoint(interval map[uint32]*common.ClosedIntervals) {
	if len(vf.Checkpoints) == 0 {
		return
	}
	if interval == nil {
		ret := make(map[uint32]*common.ClosedIntervals)
		for group, ckps := range vf.Checkpoints {
			ret[group] = common.NewClosedIntervalsByIntervals(ckps)
		}
		interval = ret
		return
	}
	for group, ckps := range vf.Checkpoints {
		if len(ckps.Intervals) == 0 {
			continue
		}
		_, ok := interval[group]
		if !ok {
			interval[group] = &common.ClosedIntervals{}
		}
		interval[group].TryMerge(*ckps)
	}
}

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
		bufPos:  vf.bufpos,
		bufSize: vf.bufSize,
		pos:     vf.size,
		file:    vf,
	}
}

func (vf *vFile) HasCommitted() bool {
	return atomic.LoadInt32(&vf.committed) == int32(1)
}

func (vf *vFile) PrepareWrite(size int) {
	// fmt.Printf("PrepareWrite %s\n", vf.Name())
	vf.wg.Add(1)
	// vf.size += size
	// fmt.Printf("\n%p|prepare write %d->%d\n", vf, vf.size-size, vf.size)
}

func (vf *vFile) FinishWrite() {
	// fmt.Printf("FinishWrite %s\n", vf.Name())
	vf.wg.Done()
}

func (vf *vFile) Commit() {
	// fmt.Printf("Committing %s\n", vf.Name())
	vf.wg.Wait()
	vf.WriteMeta()
	vf.Sync()
	fmt.Printf("sync-%s\n", vf.String())
	vf.buf = nil
	vf.commitCond.L.Lock()
	atomic.StoreInt32(&vf.committed, int32(1))
	vf.commitCond.Broadcast()
	vf.commitCond.L.Unlock()
	vf.FreeMeta()
}

//TODO reuse wait sync
func (vf *vFile) Sync() error {
	if vf.bsInfo != nil {
		vf.bsInfo.syncTimes++
	}
	if vf.buf == nil {
		vf.File.Sync()
		return nil
	}
	vf.Lock()
	targetSize := vf.size //TODO race size, bufpos
	targetpos := vf.bufpos
	t0 := time.Now()
	_, err := vf.File.WriteAt(vf.buf[:targetpos], int64(vf.syncpos))

	if vf.bsInfo != nil {
		vf.bsInfo.writeDuration += time.Since(t0)
	}
	if err != nil {
		return err
	}
	//TODO safe (call by write at, call by store)
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
	// fmt.Printf("199bufpos is %v\n",vf.bufpos)
	t0 = time.Now()
	vf.File.Sync()

	if vf.bsInfo != nil {
		vf.bsInfo.syncDuration += time.Since(t0)
	}
	vf.Unlock()
	return nil
}

func (vf *vFile) WriteMeta() {
	buf := vf.MetatoBuf()
	n, _ := vf.WriteAt(buf, int64(vf.size))
	// vf.size += n
	buf = make([]byte, Metasize)
	binary.BigEndian.PutUint16(buf, uint16(n))
	n, _ = vf.WriteAt(buf, int64(vf.size))
	// vf.size += n
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

func (vf *vFile) PrepareSync() {
	// write at buf -> wait sync buf
	// wait sync size
	// wait sync bufsize
}

//TODO alloc&free buf
func (vf *vFile) WriteAt(b []byte, off int64) (n int, err error) {
	// n, err = vf.File.WriteAt(b, off)
	dataLength := len(b)
	// if vf.buf == nil || dataLength > vf.bufSize {
	// 	vf.Sync()
	// 	n, err := vf.File.WriteAt(b, int64(vf.syncpos))
	// 	// fmt.Printf("%p|write vf in buf [%v,%v]\n", vf, vf.syncpos, vf.syncpos+n)
	// 	vf.syncpos += n
	// 	vf.size += n
	// 	return n, err
	// }
	// if dataLength+int(off)-vf.syncpos > vf.bufSize {
	// 	vf.Sync()
	// }
	vf.Lock()
	if vf.buf == nil {
		vf.bufpos = 0
		vf.buf = make([]byte, dataLength)
	}
	if dataLength+int(off)-vf.syncpos > vf.bufSize {
		vf.buf = append(vf.buf, make([]byte, dataLength)...)
	}
	n = copy(vf.buf[int(off)-vf.syncpos:], b)
	// fmt.Printf("%p|write in buf[%v,%v]\n", vf, int(off)-vf.syncpos, int(off)-vf.syncpos+n)
	// fmt.Printf("%p|write vf in buf [%v,%v]\n", vf, int(off), int(off)+n)
	vf.bufpos = int(off) + n - vf.syncpos
	vf.size += n
	// fmt.Printf("%p|size is %v\n",vf,vf.size)
	vf.Unlock()
	// fmt.Printf("243bufpos is %v\n",vf.bufpos)
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
	log.Infof("Removing version file: %s", name)
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

func (vf *vFile) OnNewEntry(int) {}
func (vf *vFile) OnNewCommit(info *entry.Info) {
	vf.Log(info)
}
func (vf *vFile) OnNewCheckpoint(info *entry.Info) {
	vf.Log(info)
}
func (vf *vFile) OnNewTxn(info *entry.Info) {
	vf.Log(info)
}
func (vf *vFile) OnNewUncommit(addrs []*VFileAddress) {
	for _, addr := range addrs {
		exist := false
		tids, ok := vf.UncommitTxn[addr.Group]
		if !ok {
			tids = make([]uint64, 0)
		}
		for _, tid := range tids {
			if tid == addr.LSN {
				exist = true
			}
		}
		if !exist {
			tids = append(tids, addr.LSN)
			vf.UncommitTxn[addr.Group] = tids
		}
	}
}

func (vf *vFile) Load(groupId uint32, lsn uint64) (entry.Entry, error) {
	if vf.HasCommitted() {
		err := vf.LoadMeta()
		defer vf.FreeMeta()
		if err != nil {
			return nil, err
		}
	}
	offset, err := vf.GetOffsetByLSN(groupId, lsn)
	if err != nil {
		return nil, err
	}
	entry := entry.GetBase()
	metaBuf := entry.GetMetaBuf()
	_, err = vf.ReadAt(metaBuf, int64(offset))
	// fmt.Printf("%p|read meta [%v,%v]\n", vf, offset, offset+n)
	if err != nil {
		return nil, err
	}
	_, err = entry.ReadAt(vf.File, offset)
	return entry, err
}

func (vf *vFile) readMeta() error {
	buf := make([]byte, Metasize)
	vf.ReadAt(buf, int64(vf.size)-int64(Metasize))
	size := binary.BigEndian.Uint16(buf)
	buf = make([]byte, int(size))
	vf.ReadAt(buf, int64(vf.size)-int64(Metasize)-int64(size))
	json.Unmarshal(buf, vf.vInfo)
	if vf.vInfo == nil {
		return errors.New("read vfile meta failed")
	}
	return nil
}
