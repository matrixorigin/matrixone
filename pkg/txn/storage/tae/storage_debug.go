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
	"github.com/matrixorigin/matrixone/pkg/pb/api"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	moctl "github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
)

func (s *taeStorage) Debug(ctx context.Context,
	txnMeta txn.TxnMeta,
	opCode uint32,
	data []byte) ([]byte, error) {
	switch opCode {
	case uint32(ctl.CmdMethod_Ping):
		return s.handlePing(data), nil
	case uint32(ctl.CmdMethod_Flush):
		_, err := handleRead(
			ctx, s,
			txnMeta, data,
			s.taeHandler.HandleFlushTable,
		)
		if err != nil {
			resp := protoc.MustMarshal(&ctl.TNStringResponse{
				ReturnStr: "Failed",
			})
			return resp, err
		}
		resp := protoc.MustMarshal(&ctl.TNStringResponse{
			ReturnStr: "OK",
		})
		return resp, err
	case uint32(ctl.CmdMethod_Checkpoint):
		_, err := handleRead(
			ctx, s, txnMeta, data, s.taeHandler.HandleForceCheckpoint,
		)
		if err != nil {
			resp := protoc.MustMarshal(&ctl.TNStringResponse{
				ReturnStr: "Failed",
			})
			return resp, err
		}
		resp := protoc.MustMarshal(&ctl.TNStringResponse{
			ReturnStr: "OK",
		})
		return resp, err

	case uint32(ctl.CmdMethod_Inspect):
		resp, err := handleRead(
			ctx, s, txnMeta, data, s.taeHandler.HandleInspectTN,
		)
		if err != nil {
			return types.Encode(&db.InspectResp{
				Message: "Failed",
			})
		}
		return resp.Read()
	case uint32(ctl.CmdMethod_AddFaultPoint):
		_, err := handleRead(
			ctx, s, txnMeta, data, s.taeHandler.HandleAddFaultPoint,
		)
		if err != nil {
			resp := protoc.MustMarshal(&ctl.TNStringResponse{
				ReturnStr: "Failed",
			})
			return resp, err
		}
		resp := protoc.MustMarshal(&ctl.TNStringResponse{
			ReturnStr: "OK",
		})
		return resp, err
	case uint32(ctl.CmdMethod_Backup):
		resp, err := handleRead(
			ctx, s, txnMeta, data, s.taeHandler.HandleBackup,
		)
		if err != nil {
			return types.Encode(&api.SyncLogTailResp{
				CkpLocation: "Failed",
			})
		}
		return resp.Read()
	case uint32(ctl.CmdMethod_TraceSpan):
		handleRead(
			ctx, s, txnMeta, data, s.taeHandler.HandleTraceSpan,
		)
		req := db.TraceSpan{}
		if err := req.Unmarshal(data); err != nil {
			return nil, err
		}
		ret := moctl.SelfProcess(req.Cmd, req.Spans)
		return []byte(ret), nil

	default:
		return nil, moerr.NewNotSupportedNoCtx("TAEStorage not support ctl method %d", opCode)
	}
}

func (s *taeStorage) handlePing(data []byte) []byte {
	req := ctl.TNPingRequest{}
	protoc.MustUnmarshal(&req, data)

	return protoc.MustMarshal(&ctl.TNPingResponse{
		ShardID:        s.shard.ShardID,
		ReplicaID:      s.shard.ReplicaID,
		LogShardID:     s.shard.LogShardID,
		ServiceAddress: s.shard.Address,
	})
}
