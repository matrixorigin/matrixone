package dist

import (
	"github.com/fagongzi/goetty/codec"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"matrixone/pkg/vm/engine/aoe/dist/pb"
)

type cmdType uint64

func (h *aoeStorage) init() {
	h.AddWriteFunc(uint64(pb.Set), h.set)
	h.AddWriteFunc(uint64(pb.Del), h.del)
	h.AddWriteFunc(uint64(pb.Incr), h.incr)
	h.AddReadFunc(uint64(pb.Get), h.get)
	h.AddReadFunc(uint64(pb.PrefixScan), h.prefixScan)
	h.AddReadFunc(uint64(pb.Scan), h.scan)
}

func (h *aoeStorage) BuildRequest(req *raftcmdpb.Request, cmd interface{}) error {
	customReq := cmd.(pb.Request)
	switch customReq.Type {
	case pb.Set:
		msg := customReq.Set
		req.Key = msg.Key
		req.CustemType = uint64(pb.Set)
		req.Type = raftcmdpb.CMDType_Write
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.Del:
		msg := customReq.Delete
		req.Key = msg.Key
		req.CustemType = uint64(pb.Del)
		req.Type = raftcmdpb.CMDType_Write
	case pb.Get:
		msg := customReq.Get
		req.Key = msg.Key
		req.CustemType = uint64(pb.Get)
		req.Type = raftcmdpb.CMDType_Read
	case pb.PrefixScan:
		msg := customReq.PrefixScan
		req.Key = msg.Prefix
		req.CustemType = uint64(pb.PrefixScan)
		req.Type = raftcmdpb.CMDType_Read
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.Incr:
		msg := customReq.AllocID
		req.Key = msg.Key
		req.CustemType = uint64(pb.Incr)
		req.Type = raftcmdpb.CMDType_Write
		req.Cmd = protoc.MustMarshal(&msg)/*
	case pb.Append:
		msg := customReq.Append
		req.ToShard = customReq.Shard*/


	}
	return nil
}

func (h *aoeStorage) Codec() (codec.Encoder, codec.Decoder) {
	return nil, nil
}

// AddReadFunc add read handler func
func (h *aoeStorage) AddReadFunc(cmdType uint64, cb command.ReadCommandFunc) {
	h.cmds[cmdType] = raftcmdpb.CMDType_Read
	h.store.RegisterReadFunc(cmdType, cb)
}

// AddWriteFunc add write handler func
func (h *aoeStorage) AddWriteFunc(cmdType uint64, cb command.WriteCommandFunc) {
	h.cmds[cmdType] = raftcmdpb.CMDType_Write
	h.store.RegisterWriteFunc(cmdType, cb)
}

