package dataio

import "matrixone/pkg/vm/engine/aoe/storage/common"

type fileStat struct {
	size  int64
	osize int64
	name  string
	algo  uint8
}

func (info *fileStat) Size() int64 {
	return info.size
}

func (info *fileStat) OriginSize() int64 {
	return info.osize
}

func (info *fileStat) Name() string {
	return info.name
}

func (info *fileStat) CompressAlgo() int {
	return int(info.algo)
}

type colPartFileStat struct {
	fileStat
	id *common.ID
}

func (info *colPartFileStat) Name() string {
	return info.id.ToPartFilePath()
}
