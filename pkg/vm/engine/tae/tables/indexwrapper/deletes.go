package indexwrapper

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type DeletesMap struct {
	impl  index.MutipleRowsIndex
	tss   []uint64
	maxTs uint64
}

func NewDeletesMap(typ types.Type) *DeletesMap {
	return &DeletesMap{
		impl: index.NewMultiplRowsART(typ),
		tss:  make([]uint64, 0),
	}
}

func (m *DeletesMap) LogDeletedKey(key any, row uint32, ts uint64) (err error) {
	err = m.impl.Insert(key, row)
	if err != nil {
		err = TranslateError(err)
		return
	}
	m.tss = append(m.tss, ts)
	if m.maxTs < ts {
		m.maxTs = ts
	}
	return
}

func (m *DeletesMap) CleanOneDelete(key any, row uint32) (err error) {
	pos, err := m.impl.DeleteOne(key, row)
	if err != nil {
		err = TranslateError(err)
		return
	}
	m.tss = append(m.tss[:pos], m.tss[pos+1:]...)
	return
}

func (m *DeletesMap) HasDeleteFrom(key any, fromTs uint64) (existed bool) {
	existed = m.impl.Contains(key)
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
	existed = m.impl.Contains(key)
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

func (m *DeletesMap) Size() int            { return m.impl.Size() }
func (m *DeletesMap) RowCount(key any) int { return m.impl.RowCount(key) }
