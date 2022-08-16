package intersectall

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

type container struct {
	state int

	sels [][]int64

	inBuckets []uint8
	bat       *batch.Batch

	mp *hashmap.StrHashMap
}

type Argument struct {
	ctr     *container
	Ibucket uint64 // index in buckets
	Nbucket uint64 // buckets count
}
