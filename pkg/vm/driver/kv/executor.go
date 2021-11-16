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

package kv

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	errDriver "github.com/matrixorigin/matrixone/pkg/vm/driver/error"
	pb3 "github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/kv"

	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"

	"github.com/matrixorigin/matrixcube/util"
	// "github.com/matrixorigin/matrixcube/pb/rpc"
)

// Storage memory storage
type kvExecutor struct {
	kv    storage.KVStorage
	attrs map[string]interface{}
}

var _ storage.Executor = (*kvExecutor)(nil)

func NewkvExecutor(kv storage.KVStorage) storage.Executor {
	return &kvExecutor{
		attrs: make(map[string]interface{}),
		kv:    kv,
	}
}

func (ce *kvExecutor) set(wb util.WriteBatch, req storage.Request) (uint64, []byte) {
	customReq := &pb3.SetRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)
	wb.Set(req.Key, customReq.Value)
	writtenBytes := uint64(len(req.Key) + len(req.Cmd))
	return writtenBytes, req.Cmd
}

func (ce *kvExecutor) get(req storage.Request) ([]byte, error) {
	value, err := ce.kv.Get(req.Key)
	if err != nil {
		value := errDriver.ErrorResp(err)
		return value, err
	}
	return value, nil
}

func (ce *kvExecutor) scan(shard meta.Shard, req storage.Request) ([]byte, error) {

	customReq := &pb.ScanRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	startKey := req.Key
	endKey := kv.EncodeDataKey(customReq.End,nil)

	if customReq.Start == nil {
		startKey = nil
	}
	if customReq.End == nil {
		endKey = nil
	}

	var data [][]byte
	var rep []byte

	err := ce.kv.Scan(startKey, endKey, func(key, value []byte) (bool, error) {
		if (shard.Start != nil && bytes.Compare(shard.Start, key) > 0) ||
			(shard.End != nil && bytes.Compare(shard.End, key) <= 0) {
			return true, nil
		}
		data = append(data, key)
		data = append(data, value)
		return true, nil
	}, false)
	if err != nil {
		rep = errDriver.ErrorResp(err)
		return rep, nil
	}
	if shard.End != nil && bytes.Compare(shard.End, customReq.End) <= 0 {
		data = append(data, shard.End)
	}
	if data != nil {
		if rep, err = json.Marshal(data); err != nil {
			rep = errDriver.ErrorResp(err)
			return rep, err
		}
	}
	return rep, nil
}
func (ce *kvExecutor) prefixScan(shard meta.Shard, req storage.Request) ([]byte, error) {

	customReq := &pb.PrefixScanRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	prefix := req.Key

	var data [][]byte
	err := ce.kv.PrefixScan(prefix, func(key, value []byte) (bool, error) {
		if (shard.Start != nil && bytes.Compare(shard.Start, key) > 0) ||
			(shard.End != nil && bytes.Compare(shard.End, key) <= 0) {
			return true, nil
		}
		data = append(data, kv.DecodeDataKey(key))
		data = append(data, value)
		return true, nil
	}, true)
	if err != nil {
		return nil, err
	}

	if shard.End != nil && bytes.HasPrefix(shard.End, customReq.Prefix) {
		data = append(data, shard.End)
	}
	var byteData []byte
	if data != nil {
		byteData, err = json.Marshal(data)
	}
	if err != nil {
		return nil, err
	}
	return byteData, nil
}

func (ce *kvExecutor) incr(wb util.WriteBatch, req storage.Request) (uint64, []byte) {

	customReq := &pb.AllocIDRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	id := uint64(0)
	if v, ok := ce.attrs[string(req.Key)]; ok {
		id, _ = codec.Bytes2Uint64(v.([]byte))
	} else {
		value, err := ce.kv.Get(req.Key)
		if err != nil {
			rep := errDriver.ErrorResp(err)
			return 0, rep
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
	ce.attrs[string(req.Key)] = newV

	wb.Set(req.Key, newV)

	writtenBytes := uint64(len(req.Key))
	return writtenBytes, newV
}

func (ce *kvExecutor) del(wb util.WriteBatch, req storage.Request) (uint64, []byte) {
	wb.Delete(req.Key)
	writtenBytes := uint64(len(req.Key))
	return writtenBytes, req.Cmd
}

func (ce *kvExecutor) setIfNotExist(wb util.WriteBatch, req storage.Request) (uint64, []byte) {

	customReq := &pb.SetRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	var rep []byte

	if _, ok := ce.attrs[codec.Bytes2String(req.Key)]; ok {
		rep = errDriver.ErrorResp(errors.New("key is already existed"))
		return 0, rep
	} else {
		ce.attrs[codec.Bytes2String(req.Key)] = "1"
	}

	value, err := ce.kv.Get(req.Key)

	if err != nil {
		rep = errDriver.ErrorResp(err)
		return 0, rep
	}

	if value != nil {
		rep = errDriver.ErrorResp(errors.New("key is already existed"))
		return 0, rep
	}

	wb.Set(req.Key, customReq.Value)

	writtenBytes := uint64(len(req.Key) + len(customReq.Value))
	return writtenBytes, rep
}

func (ce *kvExecutor) UpdateWriteBatch(ctx storage.WriteContext) error {
	ce.attrs = map[string]interface{}{}
	writtenBytes := uint64(0)
	r := ctx.WriteBatch()
	wb := r.(util.WriteBatch)
	batch := ctx.Batch()
	requests := batch.Requests
	// var rep []byte
	for j := range requests {
		switch requests[j].CmdType {
		case uint64(pb.Set):
			writtenBytes, rep := ce.set(wb, requests[j])
			ctx.AppendResponse(rep)
			writtenBytes += writtenBytes
		case uint64(pb.Del):
			writtenBytes, rep := ce.del(wb, requests[j])
			ctx.AppendResponse(rep)
			writtenBytes += writtenBytes
		case uint64(pb.Incr):
			writtenByte, rep := ce.incr(wb, requests[j])
			ctx.AppendResponse(rep)
			writtenBytes += writtenByte
		case uint64(pb.SetIfNotExist):
			writtenByte, rep := ce.setIfNotExist(wb, requests[j])
			ctx.AppendResponse(rep)
			writtenBytes += writtenByte
		default:
			panic(fmt.Errorf("invalid write cmd %d", requests[j].CmdType))
		}
	}

	// ctx.AppendResponse(rep)
	writtenBytes += uint64(16)
	ctx.SetDiffBytes(int64(writtenBytes))
	ctx.SetWrittenBytes(writtenBytes)
	return nil
}

func (ce *kvExecutor) ApplyWriteBatch(r storage.Resetable) error {
	wb := r.(util.WriteBatch)
	return ce.kv.Write(wb, false)
}

func (ce *kvExecutor) Read(ctx storage.ReadContext) ([]byte, error) {
	request := ctx.Request()
	switch request.CmdType {
	case uint64(pb.Get):
		v, err := ce.get(request)
		if err != nil {
			return nil, err
		}
		ctx.SetReadBytes(uint64(len(v)))
		return v, nil
	case uint64(pb.Scan):
		v, err := ce.scan(ctx.Shard(), request)
		if err != nil {
			return nil, err
		}
		ctx.SetReadBytes(uint64(len(v)))
		return v, nil
	case uint64(pb.PrefixScan):
		v, err := ce.prefixScan(ctx.Shard(), request)
		if err != nil {
			return nil, err
		}
		ctx.SetReadBytes(uint64(len(v)))
		return v, nil
	default:
		panic(fmt.Errorf("invalid read cmd %d", request.CmdType))
	}
}
