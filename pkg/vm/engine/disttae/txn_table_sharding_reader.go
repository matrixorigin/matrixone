// Copyright 2021-2024 Matrix Origin
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

package disttae

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/util/protoc"
)

// TODO(fagongzi): remove this method to lower after used. Currently, it only used
// upper to avoid SCA check.
func (tbl *txnTableDelegate) NewShardingDataReader(
	param shard.ReadDataParam,
) ShardingDataReader {
	if tbl.isLocal() {
		panic("can not create stream reader for local table")
	}

	return newStreamReader(tbl, param)
}

type streamReader struct {
	tbl   *txnTableDelegate
	param shard.ReadDataParam
	// offset is used to keep track of where the data has been read.
	offset []byte
	end    bool
}

func newStreamReader(
	tbl *txnTableDelegate,
	param shard.ReadDataParam,
) ShardingDataReader {
	r := reuse.Alloc[streamReader](nil)
	r.tbl = tbl
	r.param = param
	r.offset = nil
	return r
}

func (r *streamReader) Read() (bool, []byte, error) {
	if r.end {
		panic("read after end")
	}

	tbl := r.tbl

	var result shard.ReadDataResult
	request, err := tbl.getReadRequest(
		shardservice.ReadData,
		func(b []byte) {
			protoc.MustUnmarshal(&result, b)
		},
	)
	if err != nil {
		return false, nil, err
	}

	shardID := uint64(0)
	switch tbl.shard.policy {
	case shard.Policy_Partition:
		// Partition sharding only send to the current shard. The partition table id
		// is the shard id
		shardID = tbl.origin.tableId
	default:
		// otherwise, we need send to all shards
	}

	request.Param.ReadDataParam = r.param
	request.Param.ReadDataParam.Offset = r.offset

	err = tbl.shard.service.Read(
		tbl.origin.proc.Load().Ctx,
		request,
		shardservice.ReadOptions{}.Shard(shardID),
	)
	if err != nil {
		return false, nil, err
	}

	r.end = result.Last
	r.offset = result.Offset
	return r.end, result.Data, nil
}

func (r *streamReader) Close() {
	reuse.Free(r, nil)
}

func (r streamReader) TypeName() string {
	return "sharding.streamReader"
}

func (r *streamReader) reset() {
	*r = streamReader{}
}
