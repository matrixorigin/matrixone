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
