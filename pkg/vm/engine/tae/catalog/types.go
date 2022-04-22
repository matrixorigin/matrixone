package catalog

import "github.com/matrixorigin/matrixone/pkg/container/types"

type EntryState int8

const (
	ES_Appendable EntryState = iota
	ES_NotAppendable
	ES_Frozen
)

func (es EntryState) Repr() string {
	switch es {
	case ES_Appendable:
		return "Appendable"
	case ES_NotAppendable:
		return "NonAppendable"
	case ES_Frozen:
		return "Frozen"
	}
	panic("not supported")
}

func EstimateColumnBlockSize(colIdx int, rows uint32, meta *BlockEntry) uint32 {
	switch meta.GetSegment().GetTable().GetSchema().ColDefs[colIdx].Type.Oid {
	case types.T_json, types.T_char, types.T_varchar:
		return rows * 2 * 4
	default:
		return rows * uint32(meta.GetSegment().GetTable().GetSchema().ColDefs[colIdx].Type.Size)
	}
}

func EstimateBlockSize(meta *BlockEntry, rows uint32) uint32 {
	size := uint32(0)
	for colIdx := range meta.GetSegment().GetTable().GetSchema().ColDefs {
		size += EstimateColumnBlockSize(colIdx, rows, meta)
	}
	return size
}
