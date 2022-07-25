// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mockio

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
)

var mockFS *fileSystem

func init() {
	mockFS = new(fileSystem)
	mockFS.files = make(map[uint64]*segmentFile)
}

func ResetFS() {
	mockFS.Reset()
}

func ListFiles() []uint64 {
	return mockFS.ListFiles()
}

type fileSystem struct {
	sync.RWMutex
	files map[uint64]*segmentFile
}

func (fs *fileSystem) Reset() {
	fs.files = make(map[uint64]*segmentFile)
}

func (fs *fileSystem) ListFiles() (ids []uint64) {
	fs.RLock()
	defer fs.RUnlock()
	for id := range fs.files {
		ids = append(ids, id)
	}
	return
}

func (fs *fileSystem) OpenFile(name string, id uint64) file.Segment {
	fs.RLock()
	f := fs.files[id]
	fs.RUnlock()
	if f != nil {
		return f
	}
	fs.Lock()
	defer fs.Unlock()
	f = fs.files[id]
	if f != nil {
		return f
	}
	f = newSegmentFile(name, id)
	fs.files[id] = f
	return f
}

func (fs *fileSystem) RemoveFile(id uint64) (err error) {
	fs.Lock()
	defer fs.Unlock()
	delete(fs.files, id)
	return nil
}
