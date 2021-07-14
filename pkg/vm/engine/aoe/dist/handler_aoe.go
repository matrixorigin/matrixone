package dist

import (
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine/aoe/dist/aoe"
	rpcpb "matrixone/pkg/vm/engine/aoe/dist/pb"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
)

func (h *aoeStorage) append(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := &rpcpb.AppendRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	bat, _, err := protocol.DecodeBatch(customReq.Data)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	store := h.getStoreByGroup(shard.Group, shard.ID).(*aoe.Storage)
	err = store.Append(customReq.TabletName, bat, &md.LogIndex{

	})
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key) + len(customReq.Data))
	changedBytes := int64(writtenBytes)
	resp.Value = []byte("OK")
	return writtenBytes, changedBytes, resp
}