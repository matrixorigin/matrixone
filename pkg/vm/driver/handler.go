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
	"github.com/fagongzi/goetty/codec"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
)

//init adds write and read functions to handle pb requests.
func (h *driver) init() {
	h.AddWriteFunc(uint64(pb.Set), h.set)
	h.AddWriteFunc(uint64(pb.SetIfNotExist), h.setIfNotExist)
	h.AddWriteFunc(uint64(pb.Del), h.del)
	h.AddWriteFunc(uint64(pb.Incr), h.incr)
	h.AddReadFunc(uint64(pb.Get), h.get)
	h.AddReadFunc(uint64(pb.PrefixScan), h.prefixScan)
	h.AddReadFunc(uint64(pb.Scan), h.scan)

	h.AddWriteFunc(uint64(pb.CreateTablet), h.createTablet)
	h.AddWriteFunc(uint64(pb.DropTablet), h.dropTablet)
	h.AddWriteFunc(uint64(pb.Append), h.append)
	h.AddReadFunc(uint64(pb.TabletNames), h.tableNames)
	h.AddReadFunc(uint64(pb.GetSegmentIds), h.getSegmentIds)
	h.AddReadFunc(uint64(pb.GetSegmentedId), h.getSegmentedId)
}

//BuildRequest build the request according to the command
func (h *driver) BuildRequest(req *raftcmdpb.Request, cmd interface{}) error {
	customReq := cmd.(pb.Request)
	switch customReq.Type {
	case pb.Set:
		msg := customReq.Set
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb.Set)
		req.Type = raftcmdpb.CMDType_Write
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.SetIfNotExist:
		msg := customReq.Set
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb.SetIfNotExist)
		req.Type = raftcmdpb.CMDType_Write
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.Del:
		msg := customReq.Delete
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb.Del)
		req.Type = raftcmdpb.CMDType_Write
	case pb.DelIfNotExist:
		msg := customReq.Delete
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb.DelIfNotExist)
		req.Type = raftcmdpb.CMDType_Write
	case pb.Get:
		msg := customReq.Get
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb.Get)
		req.Type = raftcmdpb.CMDType_Read
	case pb.PrefixScan:
		msg := customReq.PrefixScan
		req.Key = msg.StartKey
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb.PrefixScan)
		req.Type = raftcmdpb.CMDType_Read
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.Scan:
		msg := customReq.Scan
		req.Key = msg.Start
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb.Scan)
		req.Type = raftcmdpb.CMDType_Read
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.Incr:
		msg := customReq.AllocID
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb.Incr)
		req.Type = raftcmdpb.CMDType_Write
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.CreateTablet:
		msg := customReq.CreateTablet
		req.Group = uint64(customReq.Group)
		req.ToShard = customReq.Shard
		req.CustemType = uint64(pb.CreateTablet)
		req.Type = raftcmdpb.CMDType_Write
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.DropTablet:
		msg := customReq.DropTablet
		req.Group = uint64(customReq.Group)
		req.ToShard = customReq.Shard
		req.CustemType = uint64(pb.DropTablet)
		req.Type = raftcmdpb.CMDType_Write
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.Append:
		msg := customReq.Append
		req.Group = uint64(customReq.Group)
		req.ToShard = customReq.Shard
		req.CustemType = uint64(pb.Append)
		req.Type = raftcmdpb.CMDType_Write
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.TabletNames:
		msg := customReq.TabletIds
		req.Group = uint64(customReq.Group)
		req.ToShard = customReq.Shard
		req.CustemType = uint64(pb.TabletNames)
		req.Type = raftcmdpb.CMDType_Read
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.GetSegmentIds:
		msg := customReq.GetSegmentIds
		req.Group = uint64(customReq.Group)
		req.ToShard = customReq.Shard
		req.CustemType = uint64(pb.GetSegmentIds)
		req.Type = raftcmdpb.CMDType_Read
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.GetSegmentedId:
		msg := customReq.GetSegmentedId
		req.Group = uint64(customReq.Group)
		req.ToShard = customReq.Shard
		req.CustemType = uint64(pb.GetSegmentedId)
		req.Type = raftcmdpb.CMDType_Read
		req.Cmd = protoc.MustMarshal(&msg)
	}
	return nil
}

func (h *driver) Codec() (codec.Encoder, codec.Decoder) {
	return nil, nil
}

// AddReadFunc add read handler func
func (h *driver) AddReadFunc(cmdType uint64, cb command.ReadCommandFunc) {
	h.cmds[cmdType] = raftcmdpb.CMDType_Read
	h.store.RegisterReadFunc(cmdType, cb)
}

// AddWriteFunc add write handler func
func (h *driver) AddWriteFunc(cmdType uint64, cb command.WriteCommandFunc) {
	h.cmds[cmdType] = raftcmdpb.CMDType_Write
	h.store.RegisterWriteFunc(cmdType, cb)
}
