package dataio

import (
	"errors"
	"fmt"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	"sync"
)

var (
	DupSegError = errors.New("duplicate seg")
)

type FileType uint8

const (
	UnsortedSegFile FileType = iota
	SortedSegFile
)

var DefaultFsMgr IManager
var MockFsMgr IManager

func init() {
	DefaultFsMgr = NewManager(dio.WRITER_FACTORY.Dirname)
	MockFsMgr = NewMockManager()
}

type IManager interface {
	RegisterSortedFiles(common.ID) (ISegmentFile, error)
	RegisterUnsortedFiles(common.ID) (ISegmentFile, error)
	UpgradeFile(common.ID) ISegmentFile
	GetSortedFile(common.ID) ISegmentFile
	GetUnsortedFile(common.ID) ISegmentFile
	String() string
}

type Manager struct {
	sync.RWMutex
	UnsortedFiles map[common.ID]ISegmentFile
	SortedFiles   map[common.ID]ISegmentFile
	Dir           string
}

type MockManager struct {
}

func NewManager(dir string) *Manager {
	return &Manager{
		UnsortedFiles: make(map[common.ID]ISegmentFile),
		SortedFiles:   make(map[common.ID]ISegmentFile),
		Dir:           dir,
	}
}

func (mgr *Manager) RegisterUnsortedFiles(id common.ID) (ISegmentFile, error) {
	usf := NewUnsortedSegmentFile(mgr.Dir, id)
	mgr.Lock()
	defer mgr.Unlock()
	_, ok := mgr.UnsortedFiles[id]
	if ok {
		usf.Close()
		return nil, DupSegError
	}
	mgr.UnsortedFiles[id] = usf
	return usf, nil
}

func (mgr *Manager) RegisterSortedFiles(id common.ID) (ISegmentFile, error) {
	sf := NewSortedSegmentFile(mgr.Dir, id)
	mgr.Lock()
	defer mgr.Unlock()
	_, ok := mgr.UnsortedFiles[id]
	if ok {
		sf.Close()
		return nil, DupSegError
	}
	_, ok = mgr.SortedFiles[id]
	if ok {
		sf.Close()
		return nil, DupSegError
	}
	mgr.SortedFiles[id] = sf
	return sf, nil
}

func (mgr *Manager) UpgradeFile(id common.ID) ISegmentFile {
	sf := NewSortedSegmentFile(mgr.Dir, id)
	mgr.Lock()
	staleFile, ok := mgr.UnsortedFiles[id]
	if !ok {
		panic(fmt.Sprintf("upgrade file %s not found", id.SegmentString()))
	}
	defer staleFile.Close()
	delete(mgr.UnsortedFiles, id)
	_, ok = mgr.SortedFiles[id]
	if ok {
		panic(fmt.Sprintf("duplicate file %s", id.SegmentString()))
	}
	mgr.SortedFiles[id] = sf
	mgr.Unlock()
	return sf
}

func (mgr *Manager) GetUnsortedFile(id common.ID) ISegmentFile {
	mgr.RLock()
	defer mgr.RUnlock()
	f, ok := mgr.UnsortedFiles[id]
	if !ok {
		return nil
	}
	return f
}

func (mgr *Manager) GetSortedFile(id common.ID) ISegmentFile {
	mgr.RLock()
	defer mgr.RUnlock()
	f, ok := mgr.SortedFiles[id]
	if !ok {
		return nil
	}
	return f
}

func (mgr *Manager) String() string {
	mgr.RLock()
	defer mgr.RUnlock()
	s := fmt.Sprintf("<Manager>: Unsorted[%d], Sorted[%d]\n", len(mgr.UnsortedFiles), len(mgr.SortedFiles))
	if len(mgr.UnsortedFiles) > 0 {
		for k, _ := range mgr.UnsortedFiles {
			s = fmt.Sprintf("%s %s", s, k.SegmentString())
		}
		s = fmt.Sprintf("%s\n", s)
	}
	if len(mgr.SortedFiles) > 0 {
		for k, _ := range mgr.SortedFiles {
			s = fmt.Sprintf("%s %s", s, k.SegmentString())
		}
	}
	return s
}

func (mgr *Manager) Close() error {
	var err error
	for _, f := range mgr.UnsortedFiles {
		err = f.Close()
		if err != nil {
			panic(err)
		}
	}
	for _, f := range mgr.SortedFiles {
		err = f.Close()
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func NewMockManager() IManager {
	return &MockManager{}
}

func (mmgr *MockManager) RegisterSortedFiles(id common.ID) (ISegmentFile, error) {
	sf := &MockSegmentFile{}
	return sf, nil
}

func (mmgr *MockManager) RegisterUnsortedFiles(id common.ID) (ISegmentFile, error) {
	sf := &MockSegmentFile{}
	return sf, nil
}

func (mmgr *MockManager) UpgradeFile(id common.ID) ISegmentFile {
	sf := &MockSegmentFile{}
	return sf
}

func (mmgr *MockManager) GetSortedFile(id common.ID) ISegmentFile {
	sf := &MockSegmentFile{}
	return sf
}

func (mmgr *MockManager) GetUnsortedFile(id common.ID) ISegmentFile {
	sf := &MockSegmentFile{}
	return sf
}

func (mmgr *MockManager) String() string {
	return "<MockManager>"
}
