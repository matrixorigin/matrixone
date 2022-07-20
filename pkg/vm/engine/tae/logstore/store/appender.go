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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

type fileAppender struct {
	rfile         *rotateFile
	capacity      int
	size          int
	tempPos       int
	rollbackState *vFileState
	syncWaited    *vFile
	info          any
}

func newFileAppender(rfile *rotateFile) *fileAppender {
	appender := &fileAppender{
		rfile: rfile,
	}
	return appender
}

func (appender *fileAppender) Prepare(size int, info any) error {
	var err error
	appender.capacity = size
	appender.rfile.Lock()
	defer appender.rfile.Unlock()
	if appender.syncWaited, appender.rollbackState, err = appender.rfile.makeSpace(size); err != nil {
		return err
	}
	appender.tempPos = appender.rollbackState.bufPos
	if info == nil {
		return nil
	}
	v := info.(*entry.Info)
	switch v.Group {
	// case entry.GTUncommit:
	default:
		v.Info = &VFileAddress{
			Group:   v.Group,
			LSN:     v.GroupLSN,
			Version: appender.rollbackState.file.version,
			Offset:  appender.rollbackState.pos,
		}
	}
	// logutil.Infof("log %d-%d at %d-%d",v.Group,v.GroupLSN,appender.rollbackState.file.version,appender.rollbackState.pos)
	appender.info = info
	// appender.activeId = appender.rfile.idAlloc.Alloc()
	return err
}

func (appender *fileAppender) Write(data []byte) (int, error) {
	appender.size += len(data)
	if appender.size > appender.capacity {
		panic("write logic error")
	}
	// n := copy(appender.rollbackState.file.buf[appender.tempPos:], data)
	// fmt.Printf("%p|write in buf[%v,%v]\n", appender, appender.tempPos, appender.tempPos+n)
	// vf := appender.rollbackState.file
	// fmt.Printf("%p|write vf in buf [%v,%v]\n", vf, vf.syncpos+appender.tempPos, vf.syncpos+appender.tempPos+n)
	n, err := appender.rollbackState.file.WriteAt(data,
		int64(appender.size-len(data)+appender.rollbackState.pos))
	appender.tempPos += n
	return n, err
}

func (appender *fileAppender) Commit() error {
	err := appender.rollbackState.file.Log(appender.info)
	// appender.rollbackState.file.bufpos = appender.tempPos
	if err != nil {
		return err
	}
	if appender.info == nil {
		return nil
	}
	appender.rollbackState.file.FinishWrite()
	return nil
}

func (appender *fileAppender) Rollback() {
	appender.rollbackState.file.FinishWrite()
	appender.Revert()
}

func (appender *fileAppender) Sync() error {
	if appender.size != appender.capacity {
		panic("write logic error")
	}
	if appender.syncWaited != nil {
		// fmt.Printf("Sync Waiting %s\n", appender.syncWaited.Name())
		appender.syncWaited.WaitCommitted()
	}
	return appender.rollbackState.file.Sync()
}

func (appender *fileAppender) Revert() {

	// if err := appender.rollbackState.file.Truncate(int64(appender.rollbackState.pos)); err != nil {
	// 	panic(err)
	// }
}
