// Copyright 2026 Matrix Origin
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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/rpchandle"
	"github.com/stretchr/testify/require"
)

var errStorageUsage = errors.New("storage usage failed")

type storageUsageErrorHandler struct {
	rpchandle.Handler
}

type backupErrorHandler struct {
	rpchandle.Handler
}

func (h *storageUsageErrorHandler) HandleStorageUsage(
	context.Context,
	txn.TxnMeta,
	*cmd_util.StorageUsageReq,
	*cmd_util.StorageUsageResp_V3,
) (func(), error) {
	return nil, errStorageUsage
}

func (h *backupErrorHandler) HandleBackup(
	context.Context,
	txn.TxnMeta,
	*cmd_util.Checkpoint,
	*api.SyncLogTailResp,
) (func(), error) {
	return nil, context.DeadlineExceeded
}

func TestDebugBackupReturnsCheckpointError(t *testing.T) {
	s := &taeStorage{taeHandler: &backupErrorHandler{}}
	resp, err := s.Debug(
		context.Background(),
		txn.TxnMeta{},
		uint32(api.OpCode_OpBackup),
		nil,
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, resp)
}

func TestDebugStorageUsageReturnsHandlerError(t *testing.T) {
	payload, err := (&cmd_util.StorageUsageReq{}).MarshalBinary()
	require.NoError(t, err)

	s := &taeStorage{taeHandler: &storageUsageErrorHandler{}}
	resp, err := s.Debug(
		context.Background(),
		txn.TxnMeta{},
		uint32(api.OpCode_OpStorageUsage),
		payload,
	)
	require.ErrorIs(t, err, errStorageUsage)
	require.Nil(t, resp)
}

func TestDebugStorageUsageReturnsDecodeError(t *testing.T) {
	s := &taeStorage{taeHandler: &storageUsageErrorHandler{}}
	resp, err := s.Debug(
		context.Background(),
		txn.TxnMeta{},
		uint32(api.OpCode_OpStorageUsage),
		[]byte{0xff},
	)
	require.Error(t, err)
	require.Nil(t, resp)
}

func TestDebugPingReturnsDecodeError(t *testing.T) {
	s := &taeStorage{}
	resp, err := s.Debug(
		context.Background(),
		txn.TxnMeta{},
		uint32(api.OpCode_OpPing),
		[]byte{0xff},
	)
	require.Error(t, err)
	require.Nil(t, resp)
}

func TestDebugPing(t *testing.T) {
	shard := metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{
			ShardID:    1,
			LogShardID: 2,
		},
		ReplicaID: 3,
		Address:   "tn-address",
	}
	s := &taeStorage{shard: shard}
	payload, err := (&api.TNPingRequest{}).Marshal()
	require.NoError(t, err)

	data, err := s.Debug(
		context.Background(),
		txn.TxnMeta{},
		uint32(api.OpCode_OpPing),
		payload,
	)
	require.NoError(t, err)
	resp := &api.TNPingResponse{}
	require.NoError(t, resp.Unmarshal(data))
	require.Equal(t, shard.ShardID, resp.ShardID)
	require.Equal(t, shard.LogShardID, resp.LogShardID)
	require.Equal(t, shard.ReplicaID, resp.ReplicaID)
	require.Equal(t, shard.Address, resp.ServiceAddress)
}
