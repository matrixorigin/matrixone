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

package process

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	rt "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type fakeCodecTxnOperator struct {
	client.TxnOperator
	snapshot txnpb.CNTxnSnapshot
}

func (f fakeCodecTxnOperator) Snapshot() (txnpb.CNTxnSnapshot, error) {
	return f.snapshot, nil
}

type fakeCodecTxnClient struct {
	client.TxnClient
	op client.TxnOperator
}

func (f fakeCodecTxnClient) NewWithSnapshot(snapshot txnpb.CNTxnSnapshot) (client.TxnOperator, error) {
	return f.op, nil
}

func newCodecTestProcess(t *testing.T) (*Process, client.TxnOperator) {
	t.Helper()

	txnOp := fakeCodecTxnOperator{snapshot: txnpb.CNTxnSnapshot{
		Txn: txnpb.TxnMeta{ID: []byte("txn1")},
	}}

	ctx := defines.AttachAccountId(context.Background(), 42)
	proc := NewTopProcess(
		ctx,
		mpool.MustNewZero(),
		nil,
		txnOp,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	proc.SetQueryId("query-1")
	proc.Base.UnixTime = 12345
	proc.Base.SessionInfo = SessionInfo{
		Account:         "acc",
		User:            "user",
		Host:            "host",
		Role:            "role",
		ConnectionID:    99,
		Database:        "db1",
		Version:         "v1",
		TimeZone:        time.FixedZone("UTC+8", 8*3600),
		LockWaitTimeout: 7,
		QueryId:         []string{"stmt-qid"},
		LogLevel:        zap.WarnLevel,
		SessionId:       uuid.MustParse("11111111-2222-3333-4444-555555555555"),
	}
	sp := NewStmtProfile(uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"))
	sp.SetTxnId([]byte("txn-profile-123456"))
	sp.SetStmtId(uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc"))
	proc.SetStmtProfile(sp)

	vec := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(vec, []byte("a"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(vec, []byte("b"), true, proc.Mp()))
	proc.SetPrepareParams(vec)
	return proc, txnOp
}

func TestProcessCodecHelpers(t *testing.T) {
	t.Run("limitation conversion", func(t *testing.T) {
		lim := Limitation{Size: 1, BatchRows: 2, BatchSize: 3, PartitionRows: 4, ReaderSize: 5}
		pb := convertToPipelineLimitation(lim)
		require.Equal(t, lim.Size, pb.Size)
		require.Equal(t, lim.BatchRows, pb.BatchRows)
		require.Equal(t, lim.BatchSize, pb.BatchSize)
		require.Equal(t, lim.PartitionRows, pb.PartitionRows)
		require.Equal(t, lim.ReaderSize, pb.ReaderSize)
		require.Equal(t, lim, ConvertToProcessLimitation(pb))
	})

	t.Run("log level mapping", func(t *testing.T) {
		require.Equal(t, pipeline.SessionLoggerInfo_Debug, zapLogLevel2EnumLogLevel(zap.DebugLevel))
		require.Equal(t, pipeline.SessionLoggerInfo_Info, zapLogLevel2EnumLogLevel(zapcore.Level(99)))
		require.Equal(t, zap.WarnLevel, EnumLogLevel2ZapLogLevel(pipeline.SessionLoggerInfo_Warn))
		require.Equal(t, zap.InfoLevel, EnumLogLevel2ZapLogLevel(pipeline.SessionLoggerInfo_LogLevel(99)))
	})

	t.Run("session info conversion", func(t *testing.T) {
		timeBytes, err := time.Now().In(time.UTC).MarshalBinary()
		require.NoError(t, err)
		info, err := ConvertToProcessSessionInfo(pipeline.SessionInfo{
			User:            "u",
			Host:            "h",
			Role:            "r",
			ConnectionId:    1,
			Database:        "d",
			Version:         "v",
			Account:         "a",
			QueryId:         []string{"q1"},
			TimeZone:        timeBytes,
			LockWaitTimeout: 9,
		})
		require.NoError(t, err)
		require.Equal(t, "u", info.User)
		require.Equal(t, int64(9), info.LockWaitTimeout)
		require.Equal(t, "UTC", info.TimeZone.String())

		info, err = ConvertToProcessSessionInfo(pipeline.SessionInfo{TimeZone: []byte("bad")})
		require.NoError(t, err)
		require.Nil(t, info.TimeZone)
	})

	t.Run("lock wait timeout resolution", func(t *testing.T) {
		require.Equal(t, int64(0), procSessionLockWaitTimeout(nil))
		require.Equal(t, int64(0), resolveLockWaitTimeoutSeconds(nil))
		require.Equal(t, int64(3), lockWaitTimeoutSeconds(int64(3)))
		require.Equal(t, int64(4), lockWaitTimeoutSeconds(int(4)))
		require.Equal(t, int64(5), lockWaitTimeoutSeconds(uint64(5)))
		require.Equal(t, int64(0), lockWaitTimeoutSeconds(uint64(^uint64(0))))
		require.Equal(t, int64(0), lockWaitTimeoutSeconds("bad"))

		proc := &Process{Base: &BaseProcess{SessionInfo: SessionInfo{LockWaitTimeout: 7}}}
		require.Equal(t, int64(7), resolveLockWaitTimeoutSeconds(proc))

		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
			return int64(11), nil
		})
		require.Equal(t, int64(11), resolveLockWaitTimeoutSeconds(proc))

		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
			return int64(0), nil
		})
		require.Equal(t, int64(7), resolveLockWaitTimeoutSeconds(proc))
	})
}

func TestBuildProcessInfoAndMockProcessInfoWithPro(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	info, err := proc.BuildProcessInfo("select 1")
	require.NoError(t, err)
	require.Equal(t, "query-1", info.Id)
	require.Equal(t, "select 1", info.Sql)
	require.Equal(t, uint32(42), info.AccountId)
	require.Equal(t, int64(2), info.PrepareParams.Length)
	require.Equal(t, []bool{false, true}, info.PrepareParams.Nulls)
	require.Equal(t, uint64(99), info.SessionInfo.ConnectionId)
	require.Equal(t, int64(7), info.SessionInfo.LockWaitTimeout)
	require.Equal(t, pipeline.SessionLoggerInfo_Warn, info.SessionLogger.LogLevel)

	mockInfo, err := MockProcessInfoWithPro("select 2", proc)
	require.NoError(t, err)
	require.Equal(t, "select 2", mockInfo.Sql)
	require.Equal(t, "UTC", proc.Base.SessionInfo.TimeZone.String())
}

func TestCodecServiceEncodeDecodeAndLookup(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	decodedTxn := fakeCodecTxnOperator{}
	txnClient := fakeCodecTxnClient{op: decodedTxn}

	codec := NewCodecService(txnClient, nil, nil, nil, nil, nil, nil, nil)
	svc := codec.(*codecService)
	require.NotNil(t, svc.mp)

	data, err := svc.Encode(proc, "select 3")
	require.NoError(t, err)
	require.NotEmpty(t, data)

	info, err := proc.BuildProcessInfo("select 3")
	require.NoError(t, err)

	decodedProc, err := svc.Decode(context.Background(), info)
	require.NoError(t, err)
	require.Equal(t, info.Id, decodedProc.QueryId())
	require.Equal(t, info.UnixTime, decodedProc.Base.UnixTime)
	require.Equal(t, info.SessionInfo.User, decodedProc.Base.SessionInfo.User)
	require.Equal(t, info.SessionInfo.LockWaitTimeout, decodedProc.Base.SessionInfo.LockWaitTimeout)
	require.NotNil(t, decodedProc.GetPrepareParams())
	require.Equal(t, 2, decodedProc.GetPrepareParams().Length())
	require.True(t, decodedProc.GetPrepareParams().GetNulls().Contains(1))

	rtSvc := "codec-test-svc"
	runtime := rt.DefaultRuntime()
	rt.SetupServiceBasedRuntime(rtSvc, runtime)
	runtime.SetGlobalVariables(rt.ProcessCodecService, svc)
	require.Same(t, svc, GetCodecService(rtSvc))
}

func TestGetCodecServicePanicsWhenMissing(t *testing.T) {
	rtSvc := "codec-missing-svc"
	runtime := rt.NewRuntime(metadata.ServiceType_CN, rtSvc, nil)
	rt.SetupServiceBasedRuntime(rtSvc, runtime)
	require.Panics(t, func() {
		_ = GetCodecService(rtSvc)
	})
}
