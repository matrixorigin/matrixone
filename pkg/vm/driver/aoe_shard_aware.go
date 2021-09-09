package driver

import (
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
)

func (h *driver) addShardCallback(shard bhmetapb.Shard) error {

	return nil
}

func (h *driver) Created(shard bhmetapb.Shard) {

}

func (h *driver) Splited(shard bhmetapb.Shard) {

}

func (h *driver) Destory(shard bhmetapb.Shard) {
}

func (h *driver) BecomeLeader(shard bhmetapb.Shard) {

}

func (h *driver) BecomeFollower(shard bhmetapb.Shard) {

}

func (h *driver) SnapshotApplied(shard bhmetapb.Shard) {

}
