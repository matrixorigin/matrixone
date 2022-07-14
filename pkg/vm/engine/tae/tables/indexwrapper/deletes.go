package indexwrapper

import (
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type DeletesMap struct {
	// key -> rows index
	idx index.MutipleRowsIndex
	// all op ts
	tss    []uint64
	tssIdx map[uint64]bool
	// max ts
	maxTs uint64
}

func NewDeletesMap(typ types.Type) *DeletesMap {
	return &DeletesMap{
		idx:    index.NewMultiplRowsART(typ),
		tss:    make([]uint64, 0),
		tssIdx: make(map[uint64]bool),
	}
}

func (m *DeletesMap) LogDeletedKey(key any, row uint32, ts uint64) (err error) {
	err = m.idx.Insert(key, row)
	if err != nil {
		err = TranslateError(err)
		return
	}
	if _, ok := m.tssIdx[ts]; !ok {
		m.tssIdx[ts] = true
		m.tss = append(m.tss, ts)
		sort.Slice(m.tss, func(i, j int) bool {
			return m.tss[i] < m.tss[j]
		})
		if m.maxTs < ts {
			m.maxTs = ts
		}
	}
	return
}

func (m *DeletesMap) RemoveOne(key any, row uint32) {
	m.idx.DeleteOne(key, row)
}

func (m *DeletesMap) RemoveTs(ts uint64) {
	if _, existed := m.tssIdx[ts]; !existed {
		panic(fmt.Errorf("RemoveTs cannot found ts %d", ts))
	}
	pos := compute.BinarySearch[uint64](m.tss, ts)
	if pos == -1 {
		panic(fmt.Errorf("RemoveTs cannot found ts %d", ts))
	}
	m.tss = append(m.tss[:pos], m.tss[pos+1:]...)
	delete(m.tssIdx, ts)
	if len(m.tss) == 0 {
		m.maxTs = 0
	} else {
		m.maxTs = m.tss[len(m.tss)-1]
	}
}

func (m *DeletesMap) HasDeleteFrom(key any, fromTs uint64) (existed bool) {
	existed = m.idx.Contains(key)
	if !existed {
		return
	}
	for i := len(m.tss) - 1; i >= 0; i-- {
		ts := m.tss[i]
		if ts > fromTs {
			existed = true
			break
		}
	}
	return
}

func (m *DeletesMap) IsKeyDeleted(key any, ts uint64) (deleted bool, existed bool) {
	existed = m.idx.Contains(key)
	if !existed {
		return
	}
	for i := len(m.tss) - 1; i >= 0; i-- {
		rowTs := m.tss[i]
		if rowTs <= ts {
			deleted = true
		}
	}
	return
}

func (m *DeletesMap) GetMaxTS() uint64 { return m.maxTs }

func (m *DeletesMap) Size() int            { return m.idx.Size() }
func (m *DeletesMap) RowCount(key any) int { return m.idx.RowCount(key) }
