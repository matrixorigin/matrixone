// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package batchstoredriver

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	logstoreEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

var suffix = ".rot"

func MakeVersionFile(dir, name string, version uint64) string {
	return fmt.Sprintf("%s-%d%s", filepath.Join(dir, name), version, suffix)
}

func ParseVersion(name, prefix, suffix string) (n int, ok bool) {
	woPrefix := strings.TrimPrefix(name, prefix+"-")
	if len(woPrefix) == len(name) {
		return 0, false
	}
	strVersion := strings.TrimSuffix(woPrefix, suffix)
	if len(strVersion) == len(woPrefix) {
		return 0, false
	}
	v, err := strconv.Atoi(strVersion)
	if err != nil {
		return 0, false
	}
	return v, true
}

type rotateFile struct {
	*sync.RWMutex
	dir, name   string
	checker     RotateChecker
	uncommitted []*vFile
	history     History

	commitWg     sync.WaitGroup
	commitCtx    context.Context
	commitCancel context.CancelFunc
	commitQueue  chan *vFile

	nextVer uint64

	wg sync.WaitGroup
}

func OpenRotateFile(dir, name string, mu *sync.RWMutex, rotateChecker RotateChecker,
	historyFactory HistoryFactory, observer ReplayObserver) (*rotateFile, error) {
	var err error
	if mu == nil {
		mu = new(sync.RWMutex)
	}
	newDir := false
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
		newDir = true
	}

	if rotateChecker == nil {
		rotateChecker = NewMaxSizeRotateChecker(DefaultRotateCheckerMaxSize)
	}
	if historyFactory == nil {
		historyFactory = DefaultHistoryFactory
	}

	rf := &rotateFile{
		RWMutex:     mu,
		dir:         dir,
		name:        name,
		uncommitted: make([]*vFile, 0),
		checker:     rotateChecker,
		commitQueue: make(chan *vFile, 10000),
		history:     historyFactory(),
	}
	if !newDir {
		files, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		vfiles := make([]VFile, 0)
		for _, f := range files {
			version, ok := ParseVersion(f.Name(), rf.name, suffix)
			if !ok {
				continue
			}
			file, err := os.OpenFile(
				path.Join(dir, f.Name()), os.O_RDWR, os.ModePerm)
			if err != nil {
				return nil, err
			}
			info, err := f.Info()
			if err != nil {
				return nil, err
			}
			vf := &vFile{
				RWMutex:    &sync.RWMutex{},
				File:       file,
				version:    version,
				commitCond: *sync.NewCond(new(sync.Mutex)),
				history:    rf.history,
				size:       int(info.Size()),
				syncpos:    int(info.Size()),
			}
			vf.vInfo = newVInfo(vf)
			// vf.ReadMeta()
			vfiles = append(vfiles, vf)
		}
		if len(vfiles) == 0 {
			err = rf.scheduleNew()
			if err != nil {
				return nil, err
			}
		} else {
			sort.Slice(vfiles, func(i, j int) bool {
				return vfiles[i].(*vFile).version < vfiles[j].(*vFile).version
			})
			observer.onTruncatedFile(vfiles[0].Id() - 1)
			rf.history.Extend(vfiles[:len(vfiles)-1]...)
			for _, vf := range vfiles[:len(vfiles)-1] {
				vf.OnReplayCommitted()
			}
			rf.uncommitted = append(
				rf.uncommitted, vfiles[len(vfiles)-1].(*vFile))
			rf.nextVer = uint64(vfiles[len(vfiles)-1].Id())
		}
	} else {
		err = rf.scheduleNew()
	}
	rf.commitCtx, rf.commitCancel = context.WithCancel(context.Background())
	rf.wg.Add(1)
	go rf.commitLoop()
	return rf, err
}

func (rf *rotateFile) getEntryFromUncommitted(id int) (e *vFile) {
	for _, vf := range rf.uncommitted {
		if vf.version == id {
			return vf
		}
	}
	return nil
}
func (rf *rotateFile) Replay(r *replayer, allocator logstoreEntry.Allocator) error {
	entryIDs := rf.history.EntryIds()
	for _, vf := range rf.uncommitted {
		entryIDs = append(entryIDs, vf.Id())
	}
	for _, id := range entryIDs {
		entry := rf.history.GetEntry(id)
		if entry == nil {
			vf := rf.getEntryFromUncommitted(id)
			if vf == nil {
				panic("wrong id")
			}
			entry = vf
		}

		err := entry.Replay(r, allocator)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func (rf *rotateFile) commitLoop() {
	defer rf.wg.Done()
	for {
		select {
		case <-rf.commitCtx.Done():
			return
		case file := <-rf.commitQueue:
			file.Commit()
			rf.commitFile()
			rf.commitWg.Done()
		}
	}
}

func (rf *rotateFile) scheduleCommit(file *vFile) {
	rf.commitWg.Add(1)
	rf.commitQueue <- file
}

func (rf *rotateFile) GetHistory() History {
	return rf.history
}

func (rf *rotateFile) Close() error {
	rf.commitWg.Wait()
	rf.commitCancel()
	rf.wg.Wait()
	rf.history.Close()
	for _, vf := range rf.uncommitted {
		vf.Close()
		return nil
	}
	return nil
}

func (rf *rotateFile) scheduleNew() error {
	rf.nextVer++
	fname := MakeVersionFile(rf.dir, rf.name, rf.nextVer)
	vf, err := newVFile(nil, fname, int(rf.nextVer), rf.history)
	if err != nil {
		return err
	}
	rf.uncommitted = append(rf.uncommitted, vf)
	return nil
}

func (rf *rotateFile) getFileState() *vFileState {
	l := len(rf.uncommitted)
	if l == 0 {
		return nil
	}
	return rf.uncommitted[l-1].GetState()
}

func (rf *rotateFile) makeSpace(size int) (rotated *vFile, curr *vFileState, err error) {
	var (
		rotNeeded bool
	)
	l := len(rf.uncommitted)
	if l == 0 {
		rotNeeded, err = rf.checker.PrepareAppend(nil, size)
	} else {
		rotNeeded, err = rf.checker.PrepareAppend(rf.uncommitted[l-1], size)
	}
	if err != nil {
		return nil, nil, err
	}
	if l == 0 || rotNeeded {
		if rotNeeded {
			rotated = rf.uncommitted[l-1]
			rf.scheduleCommit(rotated)
		}
		if err = rf.scheduleNew(); err != nil {
			return nil, nil, err
		}
	}
	curr = rf.getFileState()
	// if size > curr.bufSize {
	// 	return nil, nil, moerr.NewInternalErrorNoCtx("buff size is %v, but entry size is %v", rf.getFileState().file.bufSize, size) //TODO write without buf
	// }
	// if size+curr.bufPos > curr.bufSize {
	// 	curr.file.Sync()
	// 	logutil.Info("rf.250\n")
	// 	curr.bufPos = 0
	// }
	curr.file.PrepareWrite(size)
	return rotated, curr, nil
}

func (rf *rotateFile) GetAppender() FileAppender {
	return newFileAppender(rf)
}

func (rf *rotateFile) commitFile() {
	rf.Lock()
	f := rf.uncommitted[0]
	if !f.HasCommitted() {
		panic("logic error")
	}
	rf.uncommitted = rf.uncommitted[1:]
	err := f.Archive()
	if err != nil {
		panic(err)
	}
	rf.Unlock()
	logutil.Debugf("Committed %s", f.Name())
}

func (rf *rotateFile) Sync() error {
	rf.RLock()
	if len(rf.uncommitted) == 0 {
		rf.RUnlock()
		return nil
	}
	if len(rf.uncommitted) == 1 {
		f := rf.uncommitted[0]
		rf.RUnlock()
		return f.Sync()
	}
	lastFile := rf.uncommitted[len(rf.uncommitted)-1]
	waitFile := rf.uncommitted[len(rf.uncommitted)-2]
	rf.RUnlock()
	waitFile.WaitCommitted()
	return lastFile.Sync()
}

func (rf *rotateFile) Load(ver int, groupId uint32, lsn uint64, allocator logstoreEntry.Allocator) (*entry.Entry, error) {
	vf, err := rf.GetEntryByVersion(ver)
	if err != nil {
		return nil, err
	}
	return vf.Load(lsn, allocator)
}

func (rf *rotateFile) GetEntryByVersion(version int) (VFile, error) {
	var vf VFile
	rf.RLock()
	defer rf.RUnlock()
	for _, vf := range rf.uncommitted {
		if vf.version == version {
			return vf, nil
		}
	}
	vf = rf.GetHistory().GetEntry(version)
	if vf != nil {
		return vf, nil
	}
	return nil, moerr.NewInternalErrorNoCtx("version not existed")
}
