package config

import (
	engine "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type CacheSizeType uint8

const (
	CST_None CacheSizeType = iota
	CST_Customize
)

type BlockSizeType uint8

const (
	BST_None BlockSizeType = iota
	BST_S
	BST_M
	BST_L
)

var blockSizes map[BlockSizeType]uint64 = map[BlockSizeType]uint64{
	BST_None: engine.DefaultBlockMaxRows,
	BST_S:    uint64(16),
	BST_M:    uint64(1600),
	BST_L:    uint64(160000),
}

type SegmentSizeType uint8

const (
	SST_None SegmentSizeType = iota
	SST_S
	SST_M
	SST_L
)

var segmentSizes map[SegmentSizeType]uint64 = map[SegmentSizeType]uint64{
	SST_None: engine.DefaultBlocksPerSegment,
	SST_S:    uint64(4),
	SST_M:    uint64(40),
	SST_L:    uint64(400),
}

func NewOptions(dir string, cst CacheSizeType, bst BlockSizeType, sst SegmentSizeType) *engine.Options {
	blockSize := blockSizes[bst]
	segmentSize := segmentSizes[sst]
	opts := new(engine.Options)
	metaCfg := new(metadata.Configuration)
	metaCfg.Dir = dir
	metaCfg.BlockMaxRows = blockSize
	metaCfg.SegmentMaxBlocks = segmentSize
	opts.Meta.Conf = metaCfg

	if cst == CST_Customize {
		cacheCfg := new(engine.CacheCfg)
		cacheCfg.IndexCapacity = blockSize * segmentSize * 80
		cacheCfg.InsertCapacity = blockSize * segmentSize * 800
		cacheCfg.DataCapacity = blockSize * segmentSize * 80
		opts.CacheCfg = cacheCfg
	}
	opts.FillDefaults(dir)
	return opts
}

func NewCustomizedMetaOptions(dir string, cst CacheSizeType, blockRows, blockCnt uint64) *engine.Options {
	opts := new(engine.Options)
	metaCfg := &metadata.Configuration{
		Dir:              dir,
		BlockMaxRows:     blockRows,
		SegmentMaxBlocks: blockCnt,
	}
	opts.Meta.Conf = metaCfg
	if cst == CST_Customize {
		cacheCfg := new(engine.CacheCfg)
		cacheCfg.IndexCapacity = blockRows * blockCnt * 80
		cacheCfg.InsertCapacity = blockRows * blockCnt * 800
		cacheCfg.DataCapacity = blockRows * blockCnt * 80
		opts.CacheCfg = cacheCfg
	}
	opts.FillDefaults(dir)
	return opts
}
