package dataio

import (
	"errors"
	"fmt"
	"matrixone/pkg/vm/engine/aoe/storage/common"
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

type Manager struct {
	sync.RWMutex
	UnsortedFiles map[common.ID]ISegmentFile
	SortedFiles   map[common.ID]ISegmentFile
	Dir           string
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
	if !ok {
		usf.Close()
		return nil, DupSegError
	}
	mgr.UnsortedFiles[id] = usf
	return usf, nil
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
