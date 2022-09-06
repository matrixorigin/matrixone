package objectio

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"

type BlockMetadata struct {
	header  *BlockHeader
	columns []*ColumnMeta
}

type ColumnMeta struct {
	typ         uint8
	idx         uint16
	alg         uint8
	location    *Extent
	zoneMap     *index.ZoneMap
	bloomFilter *Extent
	checksum    uint32
}

type BlockHeader struct {
	tableId     uint64
	segmentId   uint64
	blockId     uint64
	columnCount uint16
	checksum    uint32
}

type Header struct {
	magic   uint64
	version uint16
}

type Footer struct {
	alg   uint8
	metas []Extent
	magic uint64
}
