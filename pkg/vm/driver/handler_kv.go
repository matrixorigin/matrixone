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
	"bytes"
	"encoding/json"
	"errors"
	pb3 "github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/storage/pebble"
)

//set responses the requests whose CustemType is Set.
//It sets key value.
//If fail, it returns the err in resp.Value.
//If success, it returns the amount of the written bytes.
func (h *driver) set(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := &pb3.SetRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	err := ctx.WriteBatch().Set(req.Key, customReq.Value)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key) + len(customReq.Value))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

//setIfNotExist responses the requests whose CustemType is SetIfNotExist.
//It sets key value if the key doesn't exist.
//If fail (including the condition that the key exists), it returns the err in resp.Value.
//If success, it returns the amount of the written bytes.
func (h *driver) setIfNotExist(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := &pb3.SetRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	if _, ok := ctx.Attrs()[codec.Bytes2String(req.Key)]; ok {
		resp.Value = errorResp(errors.New("key is already existed"))
		return 0, 0, resp
	} else {
		ctx.Attrs()[codec.Bytes2String(req.Key)] = "1"
	}

	value, err := h.store.DataStorageByGroup(shard.Group, shard.ID).(*pebble.Storage).Get(req.Key)

	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}

	if value != nil {
		resp.Value = errorResp(errors.New("key is already existed"))
		return 0, 0, resp
	}

	err = ctx.WriteBatch().Set(req.Key, customReq.Value)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key) + len(customReq.Value))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

//del responses the requests whose CustemType is Del.
//It removes the key
//If fail, it returns 0.
//If success, it returns the amount of the written bytes.
func (h *driver) del(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	err := ctx.WriteBatch().Delete(req.Key)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

//delIfExist responses the requests.
//It deletes the key if it exists.
//If fail (including the condition that the key doesn't exist), it returns the error in resp.Value.
//If success, it returns the amount of the written bytes.
func (h *driver) delIfExist(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	v, err := h.store.DataStorageByGroup(shard.Group, shard.ID).(*pebble.Storage).Get(req.Key)
	if len(v) == 0 || err != nil {
		resp.Value = errorResp(ErrKeyNotExisted)
		return 0, 0, resp
	}
	err = ctx.WriteBatch().Delete(req.Key)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key))
	changedBytes := int64(writtenBytes)
	return writtenBytes, changedBytes, resp
}

//get responses the requests whose CustemType is Get.
//It gets the value of the key
//If fail, it returns the error in resp.Value and returns 500.
//If success, it returns the value got in resp.Value and returns 0.
func (h *driver) get(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()

	value, err := h.store.DataStorageByGroup(shard.Group, req.ToShard).(*pebble.Storage).Get(req.Key)
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	resp.Value = value
	return resp, 0
}

//prefixScan responses the requests whose CustemType is PrefixScan.
//It scans the kv-pairs whose key starts with the prefix.
//If fail, it returns the error in resp.Value and returns 500.
//If success, it returns the kv-pairs in resp.Value and returns 0.
func (h *driver) prefixScan(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	customReq := &pb3.PrefixScanRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	prefix := raftstore.EncodeDataKey(shard.Group, customReq.Prefix)
	var data [][]byte
	err := h.store.DataStorageByGroup(shard.Group, req.ToShard).(*pebble.Storage).PrefixScan(prefix, func(key, value []byte) (bool, error) {
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

	if shard.End != nil && bytes.HasPrefix(shard.End, customReq.Prefix) {
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

//scan responses the requests whose CustemType is Scan.
//It scans all the kv-pairs in the store.
//If fail, it returns the error in resp.Value and returns 500.
//If success, it returns the kv-pairs in resp.Value and returns 0.
func (h *driver) scan(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {

	resp := pb.AcquireResponse()
	customReq := &pb3.ScanRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	startKey := raftstore.EncodeDataKey(shard.Group, customReq.Start)
	endKey := raftstore.EncodeDataKey(shard.Group, customReq.End)

	if customReq.Start == nil {
		startKey = nil
	}
	if customReq.End == nil {
		endKey = nil
	}

	var data [][]byte

	err := h.store.DataStorageByGroup(shard.Group, req.ToShard).(*pebble.Storage).Scan(startKey, endKey, func(key, value []byte) (bool, error) {
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
	if shard.End != nil && bytes.Compare(shard.End, customReq.End) <= 0 {
		data = append(data, shard.End)
	}
	if data != nil {
		if resp.Value, err = json.Marshal(data); err != nil {
			resp.Value = errorResp(err)
			return resp, 500
		}
	}
	return resp, 0
}

//incr responses the requests whose CustemType is Incr.
//It allocates the id that increases the length of batch from the former id.
//If fail, it returns 0,0 and returns empty resp.
//If success, it returns amount of the written bytes and returns the id in resp.Value.
func (h *driver) incr(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	customReq := &pb3.AllocIDRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	id := uint64(0)
	if v, ok := ctx.Attrs()[string(req.Key)]; ok {
		id, _ = codec.Bytes2Uint64(v.([]byte))
	} else {
		value, err := h.store.DataStorageByGroup(shard.Group, req.ToShard).(*pebble.Storage).Get(req.Key)
		if err != nil {
			return 0, 0, resp
		}
		if len(value) > 0 {
			id, _ = codec.Bytes2Uint64(value)
		}
	}

	if customReq.Batch <= 1 {
		id++
	} else {
		id += customReq.Batch
	}

	newV := codec.Uint642Bytes(id)
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
