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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
)

func (s *taeStorage) Debug(ctx context.Context,
	txnMeta txn.TxnMeta,
	opCode uint32,
	data []byte) ([]byte, error) {
	switch opCode {
	case uint32(api.OpCode_OpPing):
		return s.handlePing(data), nil
	case uint32(api.OpCode_OpFlush):
		_, err := handleRead(ctx, txnMeta, data, s.taeHandler.HandleFlushTable)
		if err != nil {
			resp := protoc.MustMarshal(&api.TNStringResponse{
				ReturnStr: "Failed",
			})
			return resp, err
		}
		resp := protoc.MustMarshal(&api.TNStringResponse{
			ReturnStr: "OK",
		})
		return resp, err
	case uint32(api.OpCode_OpCheckpoint):
		_, err := handleRead(ctx, txnMeta, data, s.taeHandler.HandleForceCheckpoint)
		if err != nil {
			resp := protoc.MustMarshal(&api.TNStringResponse{
				ReturnStr: "Failed",
			})
			return resp, err
		}
		resp := protoc.MustMarshal(&api.TNStringResponse{
			ReturnStr: "OK",
		})
		return resp, err
	case uint32(api.OpCode_OpGlobalCheckpoint):
		_, err := handleRead(ctx, txnMeta, data, s.taeHandler.HandleForceGlobalCheckpoint)
		if err != nil {
			resp := protoc.MustMarshal(&api.TNStringResponse{
				ReturnStr: "Failed",
			})
			return resp, err
		}
		resp := protoc.MustMarshal(&api.TNStringResponse{
			ReturnStr: "OK",
		})
		return resp, err

	case uint32(api.OpCode_OpInspect):
		resp, err := handleRead(ctx, txnMeta, data, s.taeHandler.HandleInspectTN)
		if err != nil {
			return types.Encode(&db.InspectResp{
				Message: "Failed",
			})
		}
		return resp.Read()
	case uint32(api.OpCode_OpAddFaultPoint):
		_, err := handleRead(ctx, txnMeta, data, s.taeHandler.HandleAddFaultPoint)
		if err != nil {
			resp := protoc.MustMarshal(&api.TNStringResponse{
				ReturnStr: "Failed",
			})
			return resp, err
		}
		resp := protoc.MustMarshal(&api.TNStringResponse{
			ReturnStr: "OK",
		})
		return resp, err
	case uint32(api.OpCode_OpBackup):
		resp, err := handleRead(ctx, txnMeta, data, s.taeHandler.HandleBackup)
		if err != nil {
			return types.Encode(&api.SyncLogTailResp{
				CkpLocation: "Failed",
			})
		}
		return resp.Read()
	case uint32(api.OpCode_OpTraceSpan):
		req := db.TraceSpan{}
		if err := req.Unmarshal(data); err != nil {
			return nil, err
		}
		ret := ctl.UpdateCurrentCNTraceSpan(req.Cmd, req.Spans, req.Threshold)
		return []byte(ret), nil

	case uint32(api.OpCode_OpStorageUsage):
		resp, _ := handleRead(ctx, txnMeta, data, s.taeHandler.HandleStorageUsage)
		return resp.Read()

	case uint32(api.OpCode_OpInterceptCommit):
		resp, err := handleRead(ctx, txnMeta, data, s.taeHandler.HandleInterceptCommit)
		if err != nil {
			return types.Encode(&api.SyncLogTailResp{
				CkpLocation: "Failed",
			})
		}
		return resp.Read()
	case uint32(api.OpCode_OpDiskDiskCleaner):
		_, err := handleRead(ctx, txnMeta, data, s.taeHandler.HandleDiskCleaner)
		if err != nil {
			resp := protoc.MustMarshal(&api.TNStringResponse{
				ReturnStr: "Failed!" + err.Error(),
			})
			return resp, err
		}
		resp := protoc.MustMarshal(&api.TNStringResponse{
			ReturnStr: "OK",
		})
		return resp, nil
	case uint32(api.OpCode_OpGetLatestCheckpoint):
		resp, err := handleRead(ctx, txnMeta, data, s.taeHandler.HandleGetLatestCheckpoint)
		if err != nil {
			return nil, err
		}
		return resp.Read()
	default:
		return nil, moerr.NewNotSupportedNoCtxf("TAEStorage not support ctl method %d", opCode)
	}
}

func (s *taeStorage) handlePing(data []byte) []byte {
	req := api.TNPingRequest{}
	protoc.MustUnmarshal(&req, data)

	return protoc.MustMarshal(&api.TNPingResponse{
		ShardID:        s.shard.ShardID,
		ReplicaID:      s.shard.ReplicaID,
		LogShardID:     s.shard.LogShardID,
		ServiceAddress: s.shard.Address,
	})
}
