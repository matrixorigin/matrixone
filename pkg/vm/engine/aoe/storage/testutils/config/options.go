// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"matrixone/pkg/vm/engine/aoe/storage"
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
	BST_None: storage.DefaultBlockMaxRows,
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
	SST_None: storage.DefaultBlocksPerSegment,
	SST_S:    uint64(4),
	SST_M:    uint64(40),
	SST_L:    uint64(400),
}

func NewOptions(dir string, cst CacheSizeType, bst BlockSizeType, sst SegmentSizeType) *storage.Options {
	blockSize := blockSizes[bst]
	segmentSize := segmentSizes[sst]
	opts := new(storage.Options)
	metaCfg := new(storage.MetaCfg)
	metaCfg.BlockMaxRows = blockSize
	metaCfg.SegmentMaxBlocks = segmentSize
	opts.Meta.Conf = metaCfg

	if cst == CST_Customize {
		cacheCfg := new(storage.CacheCfg)
		cacheCfg.IndexCapacity = blockSize * segmentSize * 80
		cacheCfg.InsertCapacity = blockSize * segmentSize * 800
		cacheCfg.DataCapacity = blockSize * segmentSize * 80
		opts.CacheCfg = cacheCfg
	}
	opts.FillDefaults(dir)
	return opts
}

func NewCustomizedMetaOptions(dir string, cst CacheSizeType, blockRows, blockCnt uint64, opts *storage.Options) *storage.Options {
	if opts == nil {
		opts = new(storage.Options)
	}
	metaCfg := &storage.MetaCfg{
		BlockMaxRows:     blockRows,
		SegmentMaxBlocks: blockCnt,
	}
	opts.Meta.Conf = metaCfg
	if cst == CST_Customize {
		cacheCfg := new(storage.CacheCfg)
		cacheCfg.IndexCapacity = blockRows * blockCnt * 80
		cacheCfg.InsertCapacity = blockRows * blockCnt * 800
		cacheCfg.DataCapacity = blockRows * blockCnt * 80
		opts.CacheCfg = cacheCfg
	}
	opts.FillDefaults(dir)
	return opts
}
