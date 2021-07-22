package dist

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	rpcpb "matrixone/pkg/vm/engine/aoe/dist/pb"
)

func (h *aoeStorage) set(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := &rpcpb.SetRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)


	err := h.getStoreByGroup(shard.Group, shard.ID).(*pebble.Storage).Set(req.Key, customReq.Value)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key) + len(customReq.Value))
	changedBytes := int64(writtenBytes)
	resp.Value = []byte("OK")
	return writtenBytes, changedBytes, resp
}

func (h *aoeStorage) setIfNotExist(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := &rpcpb.SetRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	value, err := h.getStoreByGroup(shard.Group, shard.ID).(*pebble.Storage).Get(req.Key)

	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	if value != nil {
		resp.Value = errorResp(errors.New("key is already existed"))
		return 0, 0, resp
	}

	err = h.getStoreByGroup(shard.Group, shard.ID).(*pebble.Storage).Set(req.Key, customReq.Value)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key) + len(customReq.Value))
	changedBytes := int64(writtenBytes)
	resp.Value = []byte("OK")
	return writtenBytes, changedBytes, resp
}

func (h *aoeStorage) del(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	err := h.getStoreByGroup(shard.Group, shard.ID).(*pebble.Storage).Delete(req.Key)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key))
	changedBytes := int64(writtenBytes)
	resp.Value = []byte("OK")
	return writtenBytes, changedBytes, resp
}


func (h *aoeStorage) get(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()

	value, err := h.getStoreByGroup(shard.Group, req.ToShard).(*pebble.Storage).Get(req.Key)
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	resp.Value = value
	return resp, 0
}

func (h *aoeStorage) prefixScan(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	customReq := &rpcpb.PrefixScanRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	prefix := raftstore.EncodeDataKey(shard.Group, customReq.Prefix)
	var data [][]byte
	err := h.getStoreByGroup(shard.Group, req.ToShard).(*pebble.Storage).PrefixScan(prefix, func(key, value []byte) (bool, error) {
		if (shard.Start != nil && bytes.Compare(shard.Start, raftstore.DecodeDataKey(key)) > 0) ||
			(shard.End != nil && bytes.Compare(shard.End, raftstore.DecodeDataKey(key)) <= 0) {
			return true, nil
		}
		data = append(data, raftstore.DecodeDataKey(key))
		data = append(data, value)
		return true, nil
	}, false)
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	if data != nil && shard.End != nil {
		data = append(data, shard.End)
	}
	if data != nil {
		resp.Value, err = json.Marshal(data)
	}
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	return resp, 0
}

func (h *aoeStorage) scan(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	var data [][]byte

	err := h.getStoreByGroup(shard.Group, req.ToShard).(*pebble.Storage).PrefixScan(req.Key, func(key, value []byte) (bool, error) {
		data = append(data, key)
		data = append(data, value)
		return true, nil
	}, false)
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}

	if data != nil && shard.End != nil {
		/*		lastKey := data[len(data)-2]
				if */
	}
	resp.Value, err = json.Marshal(data)
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	return resp, 0
}

func (h *aoeStorage) incr(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	id := uint64(0)
	if v, ok := ctx.Attrs()[string(req.Key)]; ok {
		id = format.MustBytesToUint64(v.([]byte))
	} else {
		value, err := h.getStoreByGroup(shard.Group, req.ToShard).(*pebble.Storage).Get(req.Key)
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

	err := ctx.WriteBatch().Set(req.Key, newV)
	if err != nil {
		return 0, 0, resp
	}

	writtenBytes := uint64(len(req.Key))
	changedBytes := int64(writtenBytes)
	resp.Value = newV
	return writtenBytes, changedBytes, resp
}
