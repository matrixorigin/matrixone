package intersectall

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
)

type container struct {
	// operator state: Build, Probe or End
	state int

	// helper data structure during probe
	counter []uint64

	// process mark
	inBuckets []uint8

	// built for the smaller of the two relations
	hashTable *hashmap.StrHashMap

	inserted      []uint8
	resetInserted []uint8
}

type Argument struct {
	// execution container
	ctr *container
	// index in buckets
	IBucket uint64
	// buckets count
	NBucket uint64
}
