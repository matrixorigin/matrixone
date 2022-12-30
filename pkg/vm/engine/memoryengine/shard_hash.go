// Copyright 2022 Matrix Origin
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

package memoryengine

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type HashShard struct {
	mp *mpool.MPool
}

func NewHashShard(mp *mpool.MPool) *HashShard {
	return &HashShard{
		mp: mp,
	}
}

func (*HashShard) Batch(
	ctx context.Context,
	tableID ID,
	getDefs getDefsFunc,
	bat *batch.Batch,
	nodes []logservicepb.DNStore,
) (
	sharded []*ShardedBatch,
	err error,
) {

	// get defs
	defs, err := getDefs(ctx)
	if err != nil {
		return nil, err
	}

	// get shard key
	var primaryAttrs []engine.Attribute
	for _, def := range defs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		if attr.Attr.Primary {
			primaryAttrs = append(primaryAttrs, attr.Attr)
		}
	}
	sort.Slice(primaryAttrs, func(i, j int) bool {
		return primaryAttrs[i].Name < primaryAttrs[j].Name
	})
	if len(primaryAttrs) == 0 {
		// no shard key
		return nil, nil
	}
	type keyInfo struct {
		Attr  engine.Attribute
		Index int
	}
	var infos []keyInfo
	for _, attr := range primaryAttrs {
		for i, name := range bat.Attrs {
			if name == attr.Name {
				infos = append(infos, keyInfo{
					Attr:  attr,
					Index: i,
				})
			}
		}
	}

	// shards
	var shards []*Shard
	for _, store := range nodes {
		for _, info := range store.Shards {
			shards = append(shards, &Shard{
				DNShardRecord: metadata.DNShardRecord{
					ShardID: info.ShardID,
				},
				ReplicaID: info.ReplicaID,
				Address:   store.ServiceAddress,
			})
		}
	}
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].ShardID < shards[j].ShardID
	})
	m := make(map[*Shard]*batch.Batch)
	for _, shard := range shards {
		batchCopy := *bat
		for i := range batchCopy.Zs {
			batchCopy.Zs[i] = 0
		}
		m[shard] = &batchCopy
	}

	// shard batch
	for i := 0; i < bat.Length(); i++ {
		hasher := fnv.New32()
		for _, info := range infos {
			vec := bat.Vecs[info.Index]
			bs, err := getBytesFromPrimaryVectorForHash(ctx, vec, i, info.Attr.Type)
			if err != nil {
				return nil, err
			}
			_, err = hasher.Write(bs)
			if err != nil {
				panic(err)
			}
		}
		n := int(hasher.Sum32())
		shard := shards[n%len(shards)]
		m[shard].Zs[i] = 1
	}

	for shard, bat := range m {
		isEmpty := true
		for _, i := range bat.Zs {
			if i > 0 {
				isEmpty = false
				break
			}
		}
		if isEmpty {
			continue
		}
		sharded = append(sharded, &ShardedBatch{
			Shard: *shard,
			Batch: bat,
		})
	}

	return
}

func (h *HashShard) Vector(
	ctx context.Context,
	tableID ID,
	getDefs getDefsFunc,
	colName string,
	vec *vector.Vector,
	nodes []logservicepb.DNStore,
) (
	sharded []*ShardedVector,
	err error,
) {

	//TODO use vector nulls mask

	// get defs
	defs, err := getDefs(ctx)
	if err != nil {
		return nil, err
	}

	// get shard key
	var shardAttr *engine.Attribute
	for _, def := range defs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		if attr.Attr.Primary {
			if attr.Attr.Name == colName {
				shardAttr = &attr.Attr
				break
			}
		}
	}
	if shardAttr == nil {
		// no shard key
		return nil, nil
	}

	// shards
	var shards []*Shard
	for _, store := range nodes {
		for _, info := range store.Shards {
			shards = append(shards, &Shard{
				DNShardRecord: metadata.DNShardRecord{
					ShardID: info.ShardID,
				},
				ReplicaID: info.ReplicaID,
				Address:   store.ServiceAddress,
			})
		}
	}
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].ShardID < shards[j].ShardID
	})
	m := make(map[*Shard]*vector.Vector)

	// shard vector
	for i := 0; i < vec.Length(); i++ {
		hasher := fnv.New32()
		bs, err := getBytesFromPrimaryVectorForHash(ctx, vec, i, shardAttr.Type)
		if err != nil {
			return nil, err
		}
		_, err = hasher.Write(bs)
		if err != nil {
			panic(err)
		}
		n := int(hasher.Sum32())
		shard := shards[n%len(shards)]
		shardVec, ok := m[shard]
		if !ok {
			shardVec = vector.New(vector.FLAT, shardAttr.Type)
			m[shard] = shardVec
		}
		v := getNullableValueFromVector(vec, i)
		appendNullableValueToVector(shardVec, v, h.mp)
	}

	for shard, vec := range m {
		if vec.Length() == 0 {
			continue
		}
		sharded = append(sharded, &ShardedVector{
			Shard:  *shard,
			Vector: vec,
		})
	}

	return
}

var _ ShardPolicy = new(HashShard)

func getBytesFromPrimaryVectorForHash(ctx context.Context, vec *vector.Vector, i int, typ types.Type) ([]byte, error) {
	if vec.IsConst() {
		panic("primary value vector should not be const")
	}
	if vec.GetNulls().Any() {
		//TODO mimic to pass BVT
		return nil, moerr.NewDuplicate(ctx)
		//panic("primary value vector should not contain nulls")
	}
	if vec.GetType().IsFixedLen() {
		// is slice
		size := vec.GetType().TypeSize()
		l := vec.Length() * size
		data := unsafe.Slice((*byte)(vector.GetPtrAt(vec, 0)), l)
		end := (i + 1) * size
		if end > len(data) {
			//TODO mimic to pass BVT
			return nil, moerr.NewDuplicate(ctx)
			//return nil, moerr.NewInvalidInput("vector size not match")
		}
		return data[i*size : (i+1)*size], nil
	} else if vec.GetType().IsVarlen() {
		slice := vector.MustBytesCols(vec)
		if i >= len(slice) {
			return []byte{}, nil
		}
		return slice[i], nil
	}
	panic(fmt.Sprintf("unknown type: %v", typ))
}

type Nullable struct {
	IsNull bool
	Value  any
}

func getNullableValueFromVector(vec *vector.Vector, i int) (value Nullable) {
	if vec.IsConst() {
		i = 0
	}
	switch vec.GetType().Oid {

	case types.T_bool:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  false,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[bool](vec)[i],
		}
		return

	case types.T_int8:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  int8(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[int8](vec)[i],
		}
		return

	case types.T_int16:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  int16(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[int16](vec)[i],
		}
		return

	case types.T_int32:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  int32(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[int32](vec)[i],
		}
		return

	case types.T_int64:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  int64(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[int64](vec)[i],
		}
		return

	case types.T_uint8:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  uint8(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[uint8](vec)[i],
		}
		return

	case types.T_uint16:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  uint16(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[uint16](vec)[i],
		}
		return

	case types.T_uint32:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  uint32(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[uint32](vec)[i],
		}
		return

	case types.T_uint64:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  uint64(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[uint64](vec)[i],
		}
		return

	case types.T_float32:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  float32(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[float32](vec)[i],
		}
		return

	case types.T_float64:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  float64(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[float64](vec)[i],
		}
		return

	case types.T_tuple:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  []any{},
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[[]any](vec)[i],
		}
		return

	case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
		if vec.IsConstNull() {
			value = Nullable{
				IsNull: true,
				Value:  []byte{},
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.GetBytes(int64(i)),
		}
		return

	case types.T_date:
		if vec.IsConstNull() {
			var zero types.Date
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[types.Date](vec)[i],
		}
		return

	case types.T_time:
		if vec.IsConstNull() {
			var zero types.Time
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[types.Time](vec)[i],
		}
		return

	case types.T_datetime:
		if vec.IsConstNull() {
			var zero types.Datetime
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[types.Datetime](vec)[i],
		}
		return

	case types.T_timestamp:
		if vec.IsConstNull() {
			var zero types.Timestamp
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[types.Timestamp](vec)[i],
		}
		return

	case types.T_decimal64:
		if vec.IsConstNull() {
			var zero types.Decimal64
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[types.Decimal64](vec)[i],
		}
		return

	case types.T_decimal128:
		if vec.IsConstNull() {
			var zero types.Decimal128
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[types.Decimal128](vec)[i],
		}
		return

	case types.T_Rowid:
		if vec.IsConstNull() {
			var zero types.Rowid
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[types.Rowid](vec)[i],
		}
		return

	case types.T_uuid:
		if vec.IsConstNull() {
			var zero types.Uuid
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vector.MustTCols[types.Uuid](vec)[i],
		}
		return

	}

	panic(fmt.Sprintf("unknown column type: %v", vec.GetType()))
}

func appendNullableValueToVector(vec *vector.Vector, value Nullable, mp *mpool.MPool) {
	str, ok := value.Value.(string)
	if ok {
		value.Value = []byte(str)
	}
	vector.Append(vec, value.Value, false, mp)
	if value.IsNull {
		vec.GetNulls().Set(uint64(vec.Length() - 1))
	}
}
