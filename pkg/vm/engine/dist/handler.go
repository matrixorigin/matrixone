package dist

import (
	"github.com/fagongzi/goetty/codec"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	pb2 "matrixone/pkg/vm/engine/dist/pb"
)

type cmdType uint64

func (h *driver) init() {
	h.AddWriteFunc(uint64(pb2.Set), h.set)
	h.AddWriteFunc(uint64(pb2.SetIfNotExist), h.setIfNotExist)
	h.AddWriteFunc(uint64(pb2.Del), h.del)
	h.AddWriteFunc(uint64(pb2.Incr), h.incr)
	h.AddReadFunc(uint64(pb2.Get), h.get)
	h.AddReadFunc(uint64(pb2.PrefixScan), h.prefixScan)
	h.AddReadFunc(uint64(pb2.Scan), h.scan)

	h.AddWriteFunc(uint64(pb2.CreateTablet), h.createTablet)
	h.AddWriteFunc(uint64(pb2.DropTablet), h.dropTablet)
	h.AddWriteFunc(uint64(pb2.Append), h.append)
	h.AddReadFunc(uint64(pb2.TabletNames), h.tableNames)
	h.AddReadFunc(uint64(pb2.GetSegmentIds), h.getSegmentIds)
	h.AddReadFunc(uint64(pb2.GetSegmentedId), h.getSegmentedId)
}

func (h *driver) BuildRequest(req *raftcmdpb.Request, cmd interface{}) error {
	customReq := cmd.(pb2.Request)
	switch customReq.Type {
	case pb2.Set:
		msg := customReq.Set
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb2.Set)
		req.Type = raftcmdpb.CMDType_Write
		req.Cmd = protoc.MustMarshal(&msg)
	case pb2.SetIfNotExist:
		msg := customReq.Set
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb2.SetIfNotExist)
		req.Type = raftcmdpb.CMDType_Write
		req.Cmd = protoc.MustMarshal(&msg)
	case pb2.Del:
		msg := customReq.Delete
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb2.Del)
		req.Type = raftcmdpb.CMDType_Write
	case pb2.DelIfNotExist:
		msg := customReq.Delete
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb2.DelIfNotExist)
		req.Type = raftcmdpb.CMDType_Write
	case pb2.Get:
		msg := customReq.Get
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb2.Get)
		req.Type = raftcmdpb.CMDType_Read
	case pb2.PrefixScan:
		msg := customReq.PrefixScan
		req.Key = msg.StartKey
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb2.PrefixScan)
		req.Type = raftcmdpb.CMDType_Read
		req.Cmd = protoc.MustMarshal(&msg)
	case pb2.Scan:
		msg := customReq.Scan
		req.Key = msg.Start
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb2.Scan)
		req.Type = raftcmdpb.CMDType_Read
		req.Cmd = protoc.MustMarshal(&msg)
	case pb2.Incr:
		msg := customReq.AllocID
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustemType = uint64(pb2.Incr)
		req.Type = raftcmdpb.CMDType_Write
		req.Cmd = protoc.MustMarshal(&msg)
	case pb2.CreateTablet:
		msg := customReq.CreateTablet
		req.Group = uint64(customReq.Group)
		req.ToShard = customReq.Shard
		req.CustemType = uint64(pb2.CreateTablet)
		req.Type = raftcmdpb.CMDType_Write
		req.Cmd = protoc.MustMarshal(&msg)
	case pb2.DropTablet:
		msg := customReq.DropTablet
		req.Group = uint64(customReq.Group)
		req.ToShard = customReq.Shard
		req.CustemType = uint64(pb2.DropTablet)
		req.Type = raftcmdpb.CMDType_Write
		req.Cmd = protoc.MustMarshal(&msg)
	case pb2.Append:
		msg := customReq.Append
		req.Group = uint64(customReq.Group)
		req.ToShard = customReq.Shard
		req.CustemType = uint64(pb2.Append)
		req.Type = raftcmdpb.CMDType_Write
		req.Cmd = protoc.MustMarshal(&msg)
	case pb2.TabletNames:
		msg := customReq.TabletIds
		req.Group = uint64(customReq.Group)
		req.ToShard = customReq.Shard
		req.CustemType = uint64(pb2.TabletNames)
		req.Type = raftcmdpb.CMDType_Read
		req.Cmd = protoc.MustMarshal(&msg)
	case pb2.GetSegmentIds:
		msg := customReq.GetSegmentIds
		req.Group = uint64(customReq.Group)
		req.ToShard = customReq.Shard
		req.CustemType = uint64(pb2.GetSegmentIds)
		req.Type = raftcmdpb.CMDType_Read
		req.Cmd = protoc.MustMarshal(&msg)
	case pb2.GetSegmentedId:
		msg := customReq.GetSegmentedId
		req.Group = uint64(customReq.Group)
		req.ToShard = customReq.Shard
		req.CustemType = uint64(pb2.GetSegmentedId)
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
