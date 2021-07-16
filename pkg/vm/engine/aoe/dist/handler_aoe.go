package dist

import (
	"encoding/json"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine/aoe/dist/aoe"
	rpcpb "matrixone/pkg/vm/engine/aoe/dist/pb"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
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
		ID: ctx.LogIndex(),
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

func (h *aoeStorage) getSnapshot(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64){
	resp := pb.AcquireResponse()
	customReq := &rpcpb.GetSnapshotRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	var c dbi.GetSnapshotCtx
	if err := json.Unmarshal(customReq.Ctx, &c); err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	rsp, err := h.getStoreByGroup(shard.Group, req.ToShard).(*aoe.Storage).GetSnapshot(&c)
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	resp.Value, _ = json.Marshal(rsp)
	return resp, 0
}

func (h *aoeStorage) relation(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	customReq := &rpcpb.RelationRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	rsp, err := h.getStoreByGroup(shard.Group, req.ToShard).(*aoe.Storage).Relation(customReq.Name)
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	resp.Value, _ = json.Marshal(rsp)
	return resp, 0
}