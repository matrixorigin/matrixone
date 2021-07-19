package dist

import (
	"encoding/json"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine/aoe"
	daoe"matrixone/pkg/vm/engine/aoe/dist/aoe"
	rpcpb "matrixone/pkg/vm/engine/aoe/dist/pb"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

func (h *aoeStorage) createTablet(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := &rpcpb.CreateTabletRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	var tabletInfo aoe.TabletInfo
	err := json.Unmarshal(customReq.TabletInfo, &tabletInfo)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	store := h.getStoreByGroup(shard.Group, shard.ID).(*daoe.Storage)
	id, err := store.CreateTable(&tabletInfo, &md.LogIndex{
		ID: ctx.LogIndex(),
	})
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key) + len(customReq.TabletInfo))
	changedBytes := int64(writtenBytes)
	resp.Value = format.Uint64ToBytes(id)
	return writtenBytes, changedBytes, resp
}

func (h *aoeStorage) append(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := &rpcpb.AppendRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	bat, _, err := protocol.DecodeBatch(customReq.Data)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	store := h.getStoreByGroup(shard.Group, shard.ID).(*daoe.Storage)
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
	rsp, err := h.getStoreByGroup(shard.Group, req.ToShard).(*daoe.Storage).GetSnapshot(&c)
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	resp.Value, _ = json.Marshal(rsp)
	return resp, 0
}

func (h *aoeStorage) tableIDs(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64){
	resp := pb.AcquireResponse()
	customReq := &rpcpb.TabletIDsRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	rsp, err := h.getStoreByGroup(shard.Group, req.ToShard).(*daoe.Storage).TableIDs()
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	resp.Value, _ = json.Marshal(rsp)
	return resp, 0
}

func (h *aoeStorage) tableNames(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64){
	resp := pb.AcquireResponse()
	customReq := &rpcpb.TabletIDsRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	rsp := h.getStoreByGroup(shard.Group, req.ToShard).(*daoe.Storage).TableNames()
	resp.Value, _ = json.Marshal(rsp)
	return resp, 0
}