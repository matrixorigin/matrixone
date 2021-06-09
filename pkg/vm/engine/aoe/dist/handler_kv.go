package dist

import (
	"encoding/json"
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/raftstore"
)

func (h *aoeStorage) set(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	args := &Args{}
	err := json.Unmarshal(req.Cmd, &args)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	if len(args.Args) <= 1 {
		resp.Value = errorResp(ErrInvalidValue)
		return 0, 0, resp
	}
	err = h.getStoreByGroup(shard.Group, shard.ID).Set(req.Key, args.Args[1])
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key) + len(args.Args[1]))
	changedBytes := int64(writtenBytes)
	resp.Value = []byte("OK")
	return writtenBytes, changedBytes, resp
}

func (h *aoeStorage) batchSet(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	args := &Args{}
	err := json.Unmarshal(req.Cmd, &args)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	if len(args.Args)%2 != 0 {
		resp.Value = errorResp(ErrInvalidValue)
		return 0, 0, resp
	}

	writtenBytes := uint64(0)
	for i := 0; i < len(args.Args)/2; i++ {
		key := raftstore.EncodeDataKey(shard.Group, args.Args[2*i])
		err = ctx.WriteBatch().Set(key, args.Args[2*i+1])
		writtenBytes += uint64(len(key))
		writtenBytes += uint64(len(args.Args[2*i+1]))
	}
	changedBytes := int64(writtenBytes)
	resp.Value = []byte("OK")
	return writtenBytes, changedBytes, resp
}

func (h *aoeStorage) get(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()

	args := &Args{}
	err := json.Unmarshal(req.Cmd, &args)

	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	value, err := h.getStoreByGroup(shard.Group, req.ToShard).Get(req.Key)
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	resp.Value = value
	return resp, 0
}

func (h *aoeStorage) incr(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	args := &Args{}
	err := json.Unmarshal(req.Cmd, &args)

	if err != nil {
		resp.Value = []byte(err.Error())
		return 0, 0, resp
	}

	id := uint64(0)
	if v, ok := ctx.Attrs()[string(req.Key)]; ok {
		id = format.MustBytesToUint64(v.([]byte))
	} else {
		value, err := h.getStoreByGroup(shard.Group, req.ToShard).Get(req.Key)
		if err != nil {
			return 0, 0, resp
		}
		if len(value) > 0 {
			id = format.MustBytesToUint64(value)
		}
	}

	id++
	newV := format.Uint64ToBytes(id)
	ctx.Attrs()[string(req.Key)] = newV

	err = ctx.WriteBatch().Set(req.Key, newV)
	if err != nil {
		return 0, 0, resp
	}

	writtenBytes := uint64(len(req.Key))
	changedBytes := int64(writtenBytes)
	resp.Value = newV
	return writtenBytes, changedBytes, resp
}
