package dist

import (
	"encoding/json"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"matrixone/pkg/logutil"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine/aoe/common/codec"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	daoe "matrixone/pkg/vm/engine/aoe/dist/aoe"
	rpcpb "matrixone/pkg/vm/engine/aoe/dist/pb"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"time"
)

func (h *driver) createTablet(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := &rpcpb.CreateTabletRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	t, err := helper.DecodeTable(customReq.TableInfo)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	store := h.store.DataStorageByGroup(shard.Group, shard.ID).(*daoe.Storage)
	id, err := store.CreateTable(&t, dbi.TableOpCtx{
		OpIndex:   ctx.LogIndex(),
		TableName: customReq.Name,
	})
	if err != nil {
		resp.Value = errorResp(err, "Call CreateTable Failed")
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key) + len(customReq.TableInfo))
	changedBytes := int64(writtenBytes)
	resp.Value = codec.Uint642Bytes(id)
	return writtenBytes, changedBytes, resp
}

func (h *driver) dropTablet(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := &rpcpb.DropTabletRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	store := h.store.DataStorageByGroup(shard.Group, shard.ID).(*daoe.Storage)
	id, err := store.DropTable(dbi.DropTableCtx{
		OpIndex:   ctx.LogIndex(),
		TableName: customReq.Name,
	})
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key) + len(customReq.Name))
	changedBytes := int64(writtenBytes)
	resp.Value = codec.Uint642Bytes(id)
	return writtenBytes, changedBytes, resp
}

func (h *driver) append(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("[logIndex:%d]append handler cost %d ms", ctx.LogIndex(), time.Since(t0).Milliseconds())
	}()
	resp := pb.AcquireResponse()
	customReq := &rpcpb.AppendRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	bat, _, err := protocol.DecodeBatch(customReq.Data)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	store := h.store.DataStorageByGroup(shard.Group, shard.ID).(*daoe.Storage)
	err = store.Append(customReq.TabletName, bat, &md.LogIndex{
		ID: ctx.LogIndex(),
	})
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key) + len(customReq.Data))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

func (h *driver) getSegmentedId(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	customReq := &rpcpb.GetSegmentedIdRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	rsp, err := h.store.DataStorageByGroup(shard.Group, req.ToShard).(*daoe.Storage).GetSegmentedId(codec.Uint642String(customReq.ShardId))
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	resp.Value = codec.Uint642Bytes(rsp)
	return resp, 0
}

func (h *driver) getSegmentIds(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	customReq := &rpcpb.GetSegmentIdsRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	rsp := h.store.DataStorageByGroup(shard.Group, req.ToShard).(*daoe.Storage).GetSegmentIds(dbi.GetSegmentsCtx{
		TableName: customReq.Name,
	})
	resp.Value, _ = json.Marshal(rsp)
	return resp, 0
}

func (h *driver) getSnapshot(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	customReq := &rpcpb.GetSnapshotRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	var c dbi.GetSnapshotCtx
	if err := json.Unmarshal(customReq.Ctx, &c); err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	rsp, err := h.store.DataStorageByGroup(shard.Group, req.ToShard).(*daoe.Storage).GetSnapshot(&c)
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	resp.Value, _ = json.Marshal(rsp)
	return resp, 0
}

func (h *driver) tableIDs(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	customReq := &rpcpb.TabletIDsRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	rsp, err := h.store.DataStorageByGroup(shard.Group, req.ToShard).(*daoe.Storage).TableIDs()
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	resp.Value, _ = json.Marshal(rsp)
	return resp, 0
}

func (h *driver) tableNames(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	customReq := &rpcpb.TabletIDsRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	rsp := h.store.DataStorageByGroup(shard.Group, req.ToShard).(*daoe.Storage).TableNames()
	resp.Value, _ = json.Marshal(rsp)
	return resp, 0
}
