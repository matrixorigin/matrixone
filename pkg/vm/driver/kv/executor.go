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

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/buf"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/driver"
	errDriver "github.com/matrixorigin/matrixone/pkg/vm/driver/error"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	pb3 "github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
)

var (
	errorPrefixLengthIsLongerThanStartKey = errors.New("the preifx length is longer than the startKey 3")
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

func (ce *kvExecutor) tpeSetBatch(ctx storage.WriteContext, wb util.WriteBatch, req storage.Request) (uint64, []byte) {
	userReq := &pb3.TpeSetBatchRequest{}
	protoc.MustUnmarshal(userReq, req.Cmd)
	writtenBytes := uint64(len(req.Cmd))
	keys := userReq.GetKeys()
	values := userReq.GetValues()
	for i, key := range keys {
		setKey := kv.EncodeDataKey(key, ctx.ByteBuf())
		wb.Set(setKey, values[i])
		writtenBytes += uint64(len(setKey))
		writtenBytes += uint64(len(values[i]))
	}
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

func (ce *kvExecutor) scan(shard metapb.Shard, req storage.Request) ([]byte, error) {

	customReq := &pb.ScanRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	startKey := req.Key
	endKey := kv.EncodeDataKey(customReq.End, nil)

	if customReq.Start == nil {
		startKey = nil
	}
	if customReq.End == nil {
		endKey = nil
	}

	var data [][]byte
	var rep []byte

	readCount := uint64(0)
	needCheckLimit := customReq.Limit != 0

	err := ce.kv.Scan(startKey, endKey, func(key, value []byte) (bool, error) {
		if (shard.Start != nil && bytes.Compare(shard.Start, key) > 0) ||
			(shard.End != nil && bytes.Compare(shard.End, key) <= 0) {
			return true, nil
		}

		if needCheckLimit {
			if readCount >= customReq.Limit {
				return false, nil
			}
			readCount++
		}
		data = append(data, key)
		data = append(data, value)
		return true, nil
	}, true)
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

func (ce *kvExecutor) clone(value []byte, buffer *buf.ByteBuf) ([]byte, error) {
	if buffer == nil {
		v := make([]byte, len(value))
		copy(v, value)
		return v, nil
	}
	buffer.MarkWrite()
	write, err := buffer.Write(value)
	if err != nil || write != len(value) {
		return nil, err
	}
	return buffer.WrittenDataAfterMark().Data(), nil
}

func (ce *kvExecutor) tpeScan(readCtx storage.ReadContext, shard metapb.Shard, req storage.Request) ([]byte, error) {
	userReq := &pb.TpeScanRequest{}
	protoc.MustUnmarshal(userReq, req.Cmd)

	scanner := executor.NewKVBasedDataStorageScanner(ce.kv)

	var keys [][]byte = nil
	var values [][]byte = nil
	var rep []byte = nil

	options := []executor.ScanOption{
		executor.WithScanStartKey(userReq.GetStart()),
		executor.WithScanEndKey(userReq.GetEnd()),
		executor.WithScanCountLimit(userReq.GetLimit()),
	}

	if len(userReq.GetPrefix()) != 0 {
		prefixFilter := func(key []byte) bool {
			return bytes.HasPrefix(key, userReq.GetPrefix())
		}
		options = append(options, executor.WithScanFilterFunc(prefixFilter))
	}

	needKey := userReq.GetNeedKey()
	var lastKey []byte = nil
	var copyValue []byte = nil
	var err error = nil

	callback := func(key []byte, value []byte) error {
		lastKey, err = ce.clone(key, readCtx.ByteBuf())
		if err != nil {
			return err
		}

		if needKey {
			keys = append(keys, lastKey)
		}

		copyValue, err = ce.clone(value, readCtx.ByteBuf())
		if err != nil {
			return err
		}

		values = append(values, copyValue)
		return nil
	}

	completed, policy, err := scanner.Scan(shard, callback, options...)
	if err != nil {
		rep = errDriver.ErrorResp(err)
		return rep, nil
	}

	var nextKey []byte = nil

	if !completed {
		switch policy {
		case executor.None:
			nextKey = nil
		case executor.GenWithResultLastKey:
			nextKey = kv.NextKey(lastKey, readCtx.ByteBuf())
		case executor.UseShardEnd:
			nextKey, err = ce.clone(shard.GetEnd(), readCtx.ByteBuf())
		}
	}

	if err != nil {
		rep = errDriver.ErrorResp(err)
		return rep, nil
	}

	tsr := pb.TpeScanResponse{
		Keys:                keys,
		Values:              values,
		CompleteInAllShards: completed,
		NextScanKey:         nextKey,
	}

	rep = protoc.MustMarshal(&tsr)

	return rep, nil
}

func (ce *kvExecutor) prefixScan(shard metapb.Shard, req storage.Request) ([]byte, error) {

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

func (ce *kvExecutor) tpePrefixScan(readCtx storage.ReadContext, shard metapb.Shard, req storage.Request) ([]byte, error) {
	userReq := &pb.TpePrefixScanRequest{}
	protoc.MustUnmarshal(userReq, req.Cmd)

	scanner := executor.NewKVBasedDataStorageScanner(ce.kv)

	if userReq.GetPrefixLength() > int64(len(userReq.GetPrefixOrStartKey())) {
		return nil, errorPrefixLengthIsLongerThanStartKey
	}

	prefix := userReq.GetPrefixOrStartKey()[:userReq.GetPrefixLength()]

	prefixFilter := func(key []byte) bool {
		return bytes.HasPrefix(key, prefix)
	}

	options := []executor.ScanOption{
		executor.WithScanStartKey(userReq.GetPrefixOrStartKey()),
		executor.WithScanEndKey(userReq.GetPrefixEnd()),
		executor.WithScanCountLimit(userReq.GetLimit()),
		executor.WithScanFilterFunc(prefixFilter),
	}

	var err error
	var lastKey []byte
	var rep []byte
	var keys [][]byte
	var values [][]byte
	var copyValue []byte

	callback := func(key []byte, value []byte) error {
		lastKey, err = ce.clone(key, readCtx.ByteBuf())
		if err != nil {
			return err
		}

		keys = append(keys, lastKey)

		copyValue, err = ce.clone(value, readCtx.ByteBuf())
		if err != nil {
			return err
		}

		values = append(values, copyValue)
		return nil
	}

	completed, policy, err := scanner.Scan(shard, callback, options...)
	if err != nil {
		return nil, err
	}

	var nextKey []byte = nil

	if !completed {
		switch policy {
		case executor.None:
			nextKey = nil
		case executor.GenWithResultLastKey:
			nextKey = kv.NextKey(lastKey, readCtx.ByteBuf())
		case executor.UseShardEnd:
			nextKey, err = ce.clone(shard.GetEnd(), readCtx.ByteBuf())
		}
	}

	if err != nil {
		rep = errDriver.ErrorResp(err)
		return rep, nil
	}

	tsr := pb.TpeScanResponse{
		Keys:                keys,
		Values:              values,
		CompleteInAllShards: completed,
		NextScanKey:         nextKey,
	}

	rep = protoc.MustMarshal(&tsr)

	return rep, nil
}

func (ce *kvExecutor) tpeCheckKeysExistInBatch(readCtx storage.ReadContext, shard metapb.Shard, req storage.Request) ([]byte, error) {
	userReq := &pb.TpeCheckKeysExistInBatchRequest{}
	protoc.MustUnmarshal(userReq, req.Cmd)

	view := ce.kv.GetView()
	defer func(view storage.View) {
		err := view.Close()
		if err != nil {
			logutil.Errorf("tpeCheckKeysExistInBatch close view of kv failed. error: %v", err)
		}
	}(view)

	//to be sure that, keys in the request are needed to be sorted.

	keyCnt := len(userReq.GetKeys())

	startKey := kv.EncodeShardStart(userReq.GetKeys()[0], readCtx.ByteBuf())
	endKey := kv.NextKey(userReq.GetKeys()[keyCnt-1], readCtx.ByteBuf())
	endKey = kv.EncodeShardEnd(endKey, readCtx.ByteBuf())

	copyKeyValue := false
	keyIndex := 0

	var existedKeyIndex int = -1

	callback := func(key []byte, value []byte) (bool, error) {
		decodedKey := kv.DecodeDataKey(key)
		for keyIndex < keyCnt {
			curKey := userReq.GetKeys()[keyIndex]
			cmp := bytes.Compare(curKey, decodedKey)
			if cmp < 0 {
				keyIndex++
			} else if cmp == 0 {
				existedKeyIndex = keyIndex
				return false, nil
			} else {
				//check next key
				return true, nil
			}
		}
		//if keyIndex >= keyCnt, there are no keys exist in the storage from this request
		return false, nil
	}

	err := ce.kv.ScanInView(view, startKey, endKey, callback, copyKeyValue)
	if err != nil {
		logutil.Errorf("tpeCheckKeysExistInBatch scan in view failed. error:%v", err)
		return nil, err
	}

	var rep []byte
	tcke := pb.TpeCheckKeysExistInBatchResponse{
		ExistedKeyIndex: int32(existedKeyIndex),
		ShardID:         userReq.GetShardID(),
	}

	rep = protoc.MustMarshal(&tcke)

	return rep, nil
}

func (ce *kvExecutor) incr(wb util.WriteBatch, req storage.Request) (uint64, []byte) {

	customReq := &pb.AllocIDRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	id := uint64(0)
	if v, ok := ce.attrs[string(req.Key)]; ok {
		id, _ = driver.Bytes2Uint64(v.([]byte))
	} else {
		value, err := ce.kv.Get(req.Key)
		if err != nil {
			rep := errDriver.ErrorResp(err)
			return 0, rep
		}
		if len(value) > 0 {
			id, _ = driver.Bytes2Uint64(value)
		}
	}

	if customReq.Batch <= 1 {
		id++
	} else {
		id += customReq.Batch
	}

	newV := driver.Uint642Bytes(id)
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

func (ce *kvExecutor) tpeDeleteBatch(ctx storage.WriteContext, wb util.WriteBatch, req storage.Request) (uint64, []byte) {
	userReq := &pb.TpeDeleteBatchRequest{}
	protoc.MustUnmarshal(userReq, req.Cmd)

	shard := ctx.Shard()

	adjustRange := func(start, end []byte, s metapb.Shard) ([]byte, []byte) {
		if len(s.Start) > 0 && bytes.Compare(start, s.Start) < 0 {
			start = s.Start
		}

		if len(s.End) > 0 && bytes.Compare(end, s.End) > 0 {
			end = s.End
		}
		return start, end
	}

	adjustKey := func(key []byte, s metapb.Shard) []byte {
		if len(s.Start) > 0 && bytes.Compare(key, s.Start) < 0 {
			key = s.Start
		}

		if len(s.End) > 0 && bytes.Compare(key, s.End) > 0 {
			key = s.End
		}
		return key
	}

	writtenBytes := uint64(0)
	if userReq.GetKeys() != nil {
		for _, key := range userReq.GetKeys() {
			adjKey := adjustKey(key, shard)
			wb.Delete(adjKey)
			writtenBytes += uint64(len(adjKey))
		}
	} else {
		startKey, endKey := adjustRange(userReq.GetStart(), userReq.GetEnd(), shard)

		encodedStartKey := kv.EncodeDataKey(startKey, ctx.ByteBuf())
		encodedEndKey := kv.EncodeDataKey(endKey, ctx.ByteBuf())

		wb.DeleteRange(encodedStartKey, encodedEndKey)
		writtenBytes += uint64(len(encodedStartKey)) + uint64(len(encodedEndKey))
	}
	return writtenBytes, req.Cmd
}

func (ce *kvExecutor) setIfNotExist(wb util.WriteBatch, req storage.Request) (uint64, []byte) {

	customReq := &pb.SetRequest{}
	protoc.MustUnmarshal(customReq, req.Cmd)

	var rep []byte

	if _, ok := ce.attrs[driver.Bytes2String(req.Key)]; ok {
		rep = errDriver.ErrorResp(errors.New("key is already existed"))
		return 0, rep
	} else {
		ce.attrs[driver.Bytes2String(req.Key)] = "1"
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
		case uint64(pb.TpeSetBatch):
			bytes, rep := ce.tpeSetBatch(ctx, wb, requests[j])
			ctx.AppendResponse(rep)
			writtenBytes += bytes
		case uint64(pb.Del):
			writtenBytes, rep := ce.del(wb, requests[j])
			ctx.AppendResponse(rep)
			writtenBytes += writtenBytes
		case uint64(pb.TpeDeleteBatch):
			bytes, rep := ce.tpeDeleteBatch(ctx, wb, requests[j])
			ctx.AppendResponse(rep)
			writtenBytes += bytes
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
	ret := ce.kv.Write(wb, false)
	return ret
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
	case uint64(pb.TpeScan):
		v, err := ce.tpeScan(ctx, ctx.Shard(), request)
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
	case uint64(pb.TpePrefixScan):
		v, err := ce.tpePrefixScan(ctx, ctx.Shard(), request)
		if err != nil {
			return nil, err
		}
		ctx.SetReadBytes(uint64(len(v)))
		return v, nil
	case uint64(pb.TpeCheckKeysExistInBatch):
		v, err := ce.tpeCheckKeysExistInBatch(ctx, ctx.Shard(), request)
		if err != nil {
			return nil, err
		}
		ctx.SetReadBytes(uint64(len(v)))
		return v, nil
	default:
		panic(fmt.Errorf("invalid read cmd %d", request.CmdType))
	}
}
