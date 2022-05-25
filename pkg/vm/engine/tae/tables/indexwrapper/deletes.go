package indexwrapper

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type DeletesMap struct {
	impl index.MutipleRowsIndex
	tss  []uint64
}

func NewDeletesMap(typ types.Type) *DeletesMap {
	return &DeletesMap{
		impl: index.NewMultiplRowsART(typ),
		tss:  make([]uint64, 0),
	}
}

func (m *DeletesMap) Upsert(key any, row uint32, ts uint64) (err error) {
	err = m.impl.Insert(key, row)
	if err != nil {
		err = TranslateError(err)
		return
	}
	m.tss = append(m.tss, ts)
	return
}

func (m *DeletesMap) DeleteOne(key any, row uint32) (err error) {
	pos, err := m.impl.DeleteOne(key, row)
	if err != nil {
		err = TranslateError(err)
		return
	}
	m.tss = append(m.tss[:pos], m.tss[pos+1:]...)
	return
}

func (m *DeletesMap) GetDeletedRows(key any) (rows []uint32, tss []uint64, exist bool) {
	n, exist := m.impl.GetRowsNode(key)
	if !exist {
		return
	}
	rows = n.Ids
	tss = m.tss
	return
}

func (m *DeletesMap) Size() int            { return m.impl.Size() }
func (m *DeletesMap) RowCount(key any) int { return m.impl.RowCount(key) }
