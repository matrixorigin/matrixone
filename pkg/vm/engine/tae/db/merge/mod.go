package merge

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"

const (
	constMergeMinBlks       = 5
	const1GBytes            = 1 << 30
	const1MBytes            = 1 << 20
	constMergeExpansionRate = 6
)

type Policy interface {
	OnObject(obj *catalog.SegmentEntry)
	Revise(cpu, mem float64) []*catalog.SegmentEntry
	ResetForTable(id uint64, schema *catalog.Schema)
}
