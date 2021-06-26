package table

import (
	"matrixone/pkg/container/types"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table2/iface"
)

type BlockHandle struct {
	Host    iface.IBlock
	Columns map[int]iface.IColBlockHandle
}

func (h *BlockHandle) GetPageNode(colIdx, pos int) bmgrif.MangaedNode {
	column, ok := h.Columns[colIdx]
	if !ok {
		return bmgrif.MangaedNode{}
	}
	return column.GetPageNode(pos)
}

func (h *BlockHandle) GetHost() iface.IBlock {
	return h.Host
}

func (h *BlockHandle) ColType(idx int) types.Type {
	return h.Host.GetMeta().Segment.Schema.ColDefs[idx].Type
}

func (h *BlockHandle) Cols() int {
	return len(h.Columns)
}

func (h *BlockHandle) Close() error {
	for _, column := range h.Columns {
		column.Close()
	}
	h.Host.Unref()
	return nil
}
