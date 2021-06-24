package dist

import (
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
)

func (h *aoeStorage) addShardCallback(shard bhmetapb.Shard) error {

	return nil
}

func (h *aoeStorage) Created(shard bhmetapb.Shard) {

}

func (h *aoeStorage) Splited(shard bhmetapb.Shard) {

}

func (h *aoeStorage) Destory(shard bhmetapb.Shard) {
}

func (h *aoeStorage) BecomeLeader(shard bhmetapb.Shard) {

}

func (h *aoeStorage) BecomeFollower(shard bhmetapb.Shard) {

}

func (h *aoeStorage) SnapshotApplied(shard bhmetapb.Shard) {

}
