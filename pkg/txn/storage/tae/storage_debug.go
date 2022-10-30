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

package taestorage

import (
	"context"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/debug"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

func (s *taeStorage) Debug(ctx context.Context,
	txnMeta txn.TxnMeta,
	opCode uint32,
	data []byte) ([]byte, error) {
	switch opCode {
	case uint32(debug.CmdMethod_Ping):
		return s.handlePing(data), nil
	default:
		return nil, moerr.NewNotSupported("TAEStorage not support debug method %d", opCode)
	}
}

func (s *taeStorage) handlePing(data []byte) []byte {
	req := debug.DNPingRequest{}
	protoc.MustUnmarshal(&req, data)

	return protoc.MustMarshal(&debug.DNPingResponse{
		ShardID:        s.shard.ShardID,
		ReplicaID:      s.shard.ReplicaID,
		LogShardID:     s.shard.LogShardID,
		ServiceAddress: s.shard.Address,
	})
}
