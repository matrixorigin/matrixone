package logservice

import "github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"

type replica struct {
	uuid    util.StoreID
	shardID uint64
	epoch   uint64

	replicaID uint64
}

// stats collects all replicas that need to be processed.
type stats struct {
	// toStop collects replicas that are already removed in config but still
	// running on log stores.
	toStop []replica

	// toStart collects replicas that are already added in config but
	// not running on log stores.
	toStart []replica

	// toRemove collects replicas that needs to be removed in config.
	// The key is shardID and the value is the slice of replicas.
	toRemove map[uint64][]replica

	// toAdd collects replicas that needs to be added in config.
	// The key is shardID and the value is the number of replicas to be added.
	toAdd map[uint64]int
}

func newStats() *stats {
	return &stats{
		toRemove: make(map[uint64][]replica),
		toAdd:    make(map[uint64]int),
	}
}
