// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"encoding/json"
	"matrixone/pkg/logutil"
	aoe2 "matrixone/pkg/vm/driver/aoe"
	pb3 "matrixone/pkg/vm/driver/pb"
	"matrixone/pkg/vm/engine/aoe/common/codec"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	//"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine/aoe/protocol"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
)

//createTablet responses the requests whose CustemType is CreateTablet.
//It creates a tablet for the table.
//If fail, it returns the err in resp.Value.
//If success, it returns the amount of the written bytes and returns the id of the table in resp.Value.
func (h *driver) createTablet(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := &pb3.CreateTabletRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	t, err := helper.DecodeTable(customReq.TableInfo)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	store := h.store.DataStorageByGroup(shard.Group, shard.ID).(*aoe2.Storage)
	id, err := store.CreateTable(&t, dbi.TableOpCtx{
		ShardId:   shard.ID,
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

//dropTablet responses the requests whose CustemType is DropTablet.
//It drops the table.
//If fail, it returns the err in resp.Value.
//If success, it returns the amount of the written bytes and returns the id of the table in resp.Value.
func (h *driver) dropTablet(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := &pb3.DropTabletRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	store := h.store.DataStorageByGroup(shard.Group, shard.ID).(*aoe2.Storage)
	id, err := store.DropTable(dbi.DropTableCtx{
		ShardId:   shard.ID,
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

//append responses the requests whose CustemType is Append.
//It appends data in the table.
//If fail, it returns the err in resp.Value.
//If success, it returns the amount of the written bytes.
func (h *driver) append(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("[logIndex:%d,%d]append handler cost %d ms", ctx.LogIndex(), ctx.Offset(), time.Since(t0).Milliseconds())
	}()
	resp := pb.AcquireResponse()
	customReq := &pb3.AppendRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	bat, _, err := protocol.DecodeBatch(customReq.Data)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	store := h.store.DataStorageByGroup(shard.Group, shard.ID).(*aoe2.Storage)
	err = store.Append(customReq.TabletName, bat, shard.ID, ctx.LogIndex(), ctx.Offset(), ctx.BatchSize())
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key) + len(customReq.Data))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

//getSegmentedId responses the requests whose CustemType is GetSegmentedId.
//It returns the id of one of the segments of the table.
//If fail, it returns the err in resp.Value and returns 500.
//If success, it returns the id in resp.Value and returns 0.
func (h *driver) getSegmentedId(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	customReq := &pb3.GetSegmentedIdRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	rsp, err := h.store.DataStorageByGroup(shard.Group, req.ToShard).(*aoe2.Storage).GetSegmentedId(codec.Uint642String(customReq.ShardId))
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	resp.Value = codec.Uint642Bytes(rsp)
	return resp, 0
}

//getSegmentIds responses the requests whose CustemType is GetSegmentIds.
//It gets the ids of all the segments of the table.
//It returns the ids in resp.Value and returns 0.
func (h *driver) getSegmentIds(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	customReq := &pb3.GetSegmentIdsRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	rsp := h.store.DataStorageByGroup(shard.Group, req.ToShard).(*aoe2.Storage).GetSegmentIds(dbi.GetSegmentsCtx{
		TableName: customReq.Name,
	})
	resp.Value, _ = json.Marshal(rsp)
	return resp, 0
}

//getSnapshot responses the requests.
//It gets a snapshot of the table.
//If fail, it returns the err in resp.Value and returns 500.
//If success, it returns the snapshot in resp.Value and returns 0.
func (h *driver) getSnapshot(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	customReq := &pb3.GetSnapshotRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	var c dbi.GetSnapshotCtx
	if err := json.Unmarshal(customReq.Ctx, &c); err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	rsp, err := h.store.DataStorageByGroup(shard.Group, req.ToShard).(*aoe2.Storage).GetSnapshot(&c)
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	resp.Value, _ = json.Marshal(rsp)
	return resp, 0
}

//tableIDs responses the requests.
//It gets the ids of all the tables in the store.
//If fail, it returns the err in resp.Value and returns 500.
//If success, it returns the ids in resp.Value and returns 0.
func (h *driver) tableIDs(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	customReq := &pb3.TabletIDsRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	rsp, err := h.store.DataStorageByGroup(shard.Group, req.ToShard).(*aoe2.Storage).TableIDs()
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	resp.Value, _ = json.Marshal(rsp)
	return resp, 0
}

//tableNames responses the requests.
//It gets the names of all the tables in the store.
//It returns the names in resp.Value and returns 0.
func (h *driver) tableNames(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	customReq := &pb3.TabletIDsRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	rsp := h.store.DataStorageByGroup(shard.Group, req.ToShard).(*aoe2.Storage).TableNames()
	resp.Value, _ = json.Marshal(rsp)
	return resp, 0
}
