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

type fileAppender struct {
	rfile         *rotateFile
	capacity      int
	size          int
	rollbackState *vFileState
	info          any //for address
}

func newFileAppender(rfile *rotateFile) *fileAppender {
	appender := &fileAppender{
		rfile: rfile,
	}
	return appender
}

func (appender *fileAppender) Prepare(size int, lsn uint64) (any, error) {
	var err error
	appender.capacity = size
	appender.rfile.Lock()
	defer appender.rfile.Unlock()
	if appender.rollbackState, err = appender.rfile.makeSpace(size); err != nil {
		return nil, err
	}
	addr := &VFileAddress{
		LSN:     lsn,
		Version: appender.rollbackState.file.version,
		Offset:  appender.rollbackState.pos,
	}
	appender.info = addr
	return addr, err
}

func (appender *fileAppender) Write(data []byte) (int, error) {
	appender.size += len(data)
	if appender.size > appender.capacity {
		panic("write logic error")
	}
	n, err := appender.rollbackState.file.WriteAt(data,
		int64(appender.size-len(data)+appender.rollbackState.pos))
	return n, err
}

func (appender *fileAppender) Commit() error {
	err := appender.rollbackState.file.Log(appender.info)
	if err != nil {
		return err
	}
	if appender.info == nil {
		return nil
	}
	appender.rollbackState.file.FinishWrite()
	return nil
}
