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
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

var suffix = ".rot"

func MakeVersionFile(dir, name string, version uint64) string {
	return fmt.Sprintf("%s-%d%s", filepath.Join(dir, name), version, suffix)
}

func ParseVersion(name, prefix, suffix string) (int, error) {
	woPrefix := strings.TrimPrefix(name, prefix+"-")
	if len(woPrefix) == len(name) {
		return 0, errors.New("parse version error")
	}
	strVersion := strings.TrimSuffix(woPrefix, suffix)
	if len(strVersion) == len(woPrefix) {
		return 0, errors.New("parse version error")
	}
	v, err := strconv.Atoi(strVersion)
	if err != nil {
		return 0, errors.New("parse version error")
	}
	return v, nil
}

type rotateFile struct {
	*sync.RWMutex
	dir, name   string
	checker     RotateChecker
	uncommitted []*vFile
	history     History

	commitWg        sync.WaitGroup
	commitCtx       context.Context
	commitCancel    context.CancelFunc
	commitQueue     chan *vFile
	postCommitQueue chan *vFile

	nextVer uint64

	wg sync.WaitGroup

	bsInfo         *storeInfo
	postCommitFunc func(VFile)
}

func OpenRotateFile(dir, name string, mu *sync.RWMutex, rotateChecker RotateChecker,
	historyFactory HistoryFactory, bsInfo *storeInfo, postCommitFunc func(VFile)) (*rotateFile, error) {
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
		RWMutex:         mu,
		dir:             dir,
		name:            name,
		uncommitted:     make([]*vFile, 0),
		checker:         rotateChecker,
		commitQueue:     make(chan *vFile, 10000),
		postCommitQueue: make(chan *vFile, 10000),
		history:         historyFactory(),
		bsInfo:          bsInfo,
		postCommitFunc:  postCommitFunc,
	}
	if !newDir {
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		vfiles := make([]VFile, 0)
		for _, f := range files {
			version, err := ParseVersion(f.Name(), rf.name, suffix)
			if err != nil {
				continue
			}
			file, err := os.OpenFile(
				path.Join(dir, f.Name()), os.O_RDWR, os.ModePerm)
			if err != nil {
				return nil, err
			}
			vf := &vFile{
				RWMutex:    &sync.RWMutex{},
				File:       file,
				version:    version,
				commitCond: *sync.NewCond(new(sync.Mutex)),
				history:    rf.history,
				size:       int(f.Size()),
				syncpos:    int(f.Size()),
			}
			vf.vInfo = newVInfo(vf)
			// vf.ReadMeta()
			vfiles = append(vfiles, vf)
		}
		sort.Slice(vfiles, func(i, j int) bool {
			return vfiles[i].(*vFile).version < vfiles[j].(*vFile).version
		})
		if len(vfiles) == 0 {
			err = rf.scheduleNew()
			if err != nil {
				return nil, err
			}
		} else {
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
	go rf.postCommitLoop()
	return rf, err
}

func (rf *rotateFile) Replay(r *replayer, o ReplayObserver) error {
	entryIDs := rf.history.EntryIds()
	for _, vf := range rf.uncommitted {
		entryIDs = append(entryIDs, vf.Id())
	}
	entries := make([]VFile, 0)
	failedIDs := make([]int, 0)
	for _, id := range entryIDs {
		entry := rf.history.GetEntry(id)
		if entry == nil {
			vf := rf.getEntryFromUncommitted(id)
			if vf == nil {
				panic("wrong id")
			}
			entry = vf
		}

		err := entry.LoadMeta()
		if err != nil {
			err := entry.Replay(r, o)
			if err != nil {
				panic(err)
			}
			failedIDs = append(failedIDs, id)
			continue
		}
		entries = append(entries, entry)
	}
	r.MergeCkps(entries)
	for _, vf := range entries {
		for _, id := range failedIDs {
			if vf.Id() == id {
				continue
			}
		}
		err := vf.ReplayCWithCkp(r, o)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

// for replay
func (rf *rotateFile) getEntryFromUncommitted(id int) (e *vFile) {
	for _, vf := range rf.uncommitted {
		if vf.version == id {
			return vf
		}
	}
	return nil
}
func (rf *rotateFile) TryTruncate(size int64) error {
	l := len(rf.uncommitted)
	if l == 0 {
		return errors.New("all files committed")
	}
	err := rf.uncommitted[l-1].File.Truncate(size)
	if err != nil {
		return err
	}
	rf.uncommitted[l-1].size = int(size)
	return err
}

func (rf *rotateFile) commitLoop() {
	defer rf.wg.Done()
	for {
		select {
		case <-rf.commitCtx.Done():
			return
		case file := <-rf.commitQueue:
			// fmt.Printf("Receive request: %s\n", file.Name())
			file.Commit()
			rf.commitFile()
			rf.commitWg.Done()
			rf.postCommitQueue <- file
		}
	}
}

func (rf *rotateFile) postCommitLoop() {
	for {
		select {
		case <-rf.commitCtx.Done():
			return
		case file := <-rf.postCommitQueue:
			if rf.postCommitFunc != nil {
				rf.postCommitFunc(file)
			}
		}
	}
}

func (rf *rotateFile) scheduleCommit(file *vFile) {
	rf.commitWg.Add(1)
	// fmt.Printf("Schedule request: %s\n", file.Name())
	rf.commitQueue <- file
}

func (rf *rotateFile) GetHistory() History {
	return rf.history
}

func (rf *rotateFile) Close() error {
	rf.commitWg.Wait()
	rf.commitCancel()
	rf.wg.Wait()
	for _, vf := range rf.uncommitted {
		vf.Close()
		return nil
	}
	return nil
}

func (rf *rotateFile) scheduleNew() error {
	rf.nextVer++
	fname := MakeVersionFile(rf.dir, rf.name, rf.nextVer)
	vf, err := newVFile(nil, fname, int(rf.nextVer), rf.history, rf.bsInfo)
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
	// 	return nil, nil, fmt.Errorf("buff size is %v, but entry size is %v", rf.getFileState().file.bufSize, size) //TODO write without buf
	// }
	// if size+curr.bufPos > curr.bufSize {
	// 	curr.file.Sync()
	// 	fmt.Printf("rf.250\n")
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
	fmt.Printf("Committed %s\n", f.Name())
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

func (rf *rotateFile) Load(ver int, groupID uint32, lsn uint64) (entry.Entry, error) {
	vf, err := rf.GetEntryByVersion(ver)
	if err != nil {
		return nil, err
	}
	return vf.Load(groupID, lsn)
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
	return nil, errors.New("version not existed")
}
