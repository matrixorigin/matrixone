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

package dataio

import (
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
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
	UnsortedFiles map[common.ID]base.ISegmentFile
	SortedFiles   map[common.ID]base.ISegmentFile
	Dir           string
	Mock          bool
}

func NewManager(dir string, mock bool) *Manager {
	return &Manager{
		UnsortedFiles: make(map[common.ID]base.ISegmentFile),
		SortedFiles:   make(map[common.ID]base.ISegmentFile),
		Dir:           dir,
		Mock:          mock,
	}
}

func (mgr *Manager) UnregisterUnsortedFile(id common.ID) {
	mgr.Lock()
	defer mgr.Unlock()
	delete(mgr.UnsortedFiles, id)
}

func (mgr *Manager) UnregisterSortedFile(id common.ID) {
	mgr.Lock()
	defer mgr.Unlock()
	delete(mgr.SortedFiles, id)
}

func (mgr *Manager) RegisterUnsortedFile(id common.ID) (base.ISegmentFile, error) {
	var usf base.ISegmentFile
	if mgr.Mock {
		usf = NewMockSegmentFile(mgr.Dir, UnsortedSegFile, id)
	} else {
		usf = NewUnsortedSegmentFile(mgr.Dir, id)
	}
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

func (mgr *Manager) RegisterSortedFile(meta *metadata.Segment) (base.ISegmentFile, error) {
	var sf base.ISegmentFile
	if mgr.Mock {
		sf = NewMockSegmentFile(mgr.Dir, SortedSegFile, meta.AsCommonID().AsSegmentID())
	} else {
		sf = NewSortedSegmentFile(mgr.Dir, meta)
	}
	id := meta.AsCommonID().AsSegmentID()
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

func (mgr *Manager) UpgradeFile(meta *metadata.Segment) base.ISegmentFile {
	var sf base.ISegmentFile
	if mgr.Mock {
		sf = NewMockSegmentFile(mgr.Dir, SortedSegFile, meta.AsCommonID().AsSegmentID())
	} else {
		sf = NewSortedSegmentFile(mgr.Dir, meta)
	}
	id := meta.AsCommonID().AsSegmentID()
	mgr.Lock()
	_, ok := mgr.UnsortedFiles[id]
	if !ok {
		logutil.Debug(mgr.stringNoLock())
		panic(fmt.Sprintf("upgrade file %s not found", id.SegmentString()))
	}
	// defer staleFile.Unref()
	delete(mgr.UnsortedFiles, id)
	_, ok = mgr.SortedFiles[id]
	if ok {
		panic(fmt.Sprintf("duplicate file %s", id.SegmentString()))
	}
	mgr.SortedFiles[id] = sf
	mgr.Unlock()
	return sf
}

func (mgr *Manager) GetUnsortedFile(id common.ID) base.ISegmentFile {
	mgr.RLock()
	defer mgr.RUnlock()
	f, ok := mgr.UnsortedFiles[id]
	if !ok {
		return nil
	}
	return f
}

func (mgr *Manager) GetSortedFile(id common.ID) base.ISegmentFile {
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
	return mgr.stringNoLock()
}

func (mgr *Manager) stringNoLock() string {
	s := fmt.Sprintf("<Manager:%s>[%v]: Unsorted[%d], Sorted[%d]\n", mgr.Dir, mgr.Mock, len(mgr.UnsortedFiles), len(mgr.SortedFiles))
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
