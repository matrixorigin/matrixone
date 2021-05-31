package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout"
	"sync"
	// "matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
)

type Manager struct {
	sync.RWMutex
	Files map[layout.ID]ISegmentFile
}

// func MakeSegmentFile(id layout.ID, segType col.SegmentType) ISegmentFile {
// 	return nil
// }

func (mgr *Manager) RegisterSegment(id layout.ID, sf ISegmentFile) {
	mgr.Files[id] = sf
}

func (mgr *Manager) DropSegment(id layout.ID) {
	delete(mgr.Files, id)
}

func (mgr *Manager) GetFile(id layout.ID) ISegmentFile {
	f, ok := mgr.Files[id]
	if !ok {
		panic("logic error")
	}
	return f
}
