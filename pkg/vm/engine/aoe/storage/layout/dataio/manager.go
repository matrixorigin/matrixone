package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"sync"
	// "matrixone/pkg/vm/engine/aoe/storage/common/table/col"
)

type Manager struct {
	sync.RWMutex
	Files map[common.ID]ISegmentFile
}

// func MakeSegmentFile(id common.ID, segType col.SegmentType) ISegmentFile {
// 	return nil
// }

func (mgr *Manager) RegisterSegment(id common.ID, sf ISegmentFile) {
	mgr.Files[id] = sf
}

func (mgr *Manager) DropSegment(id common.ID) {
	delete(mgr.Files, id)
}

func (mgr *Manager) GetFile(id common.ID) ISegmentFile {
	f, ok := mgr.Files[id]
	if !ok {
		panic("logic error")
	}
	return f
}
