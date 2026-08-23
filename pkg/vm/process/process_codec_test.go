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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	rt "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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

func (f fakeCodecTxnClient) NewWithSnapshot(context.Context, txnpb.CNTxnSnapshot) (client.TxnOperator, error) {
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
		Account:                             "acc",
		User:                                "user",
		Host:                                "host",
		Role:                                "role",
		ConnectionID:                        99,
		Database:                            "db1",
		Version:                             "v1",
		TimeZone:                            time.FixedZone("UTC+8", 8*3600),
		LockWaitTimeout:                     7,
		LockWaitTimeoutSet:                  true,
		QueryId:                             []string{"stmt-qid"},
		MatrixOneNativeMode:                 true,
		LogLevel:                            zap.WarnLevel,
		SessionId:                           uuid.MustParse("11111111-2222-3333-4444-555555555555"),
		ExplicitZeroTemporalCastReturnsNull: true,
		SqlMode:                             "STRICT_TRANS_TABLES",
	}
	sp := NewStmtProfile(uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"))
	sp.SetTxnId([]byte("txn-profile-123456"))
	sp.SetStmtId(uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc"))
	proc.SetStmtProfile(sp)
	sp.SetStatementRuntimeProfile("Insert", "DML", true)

	vec := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(vec, []byte("a"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(vec, []byte("b"), true, proc.Mp()))
	proc.SetPrepareParamsWithMetadata(vec, []bool{true, false}, []bool{false, true})
	proc.SetAffectedRows(42)
	proc.SetPlanSnapshotTS(timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4})
	return proc, txnOp
}

func TestProcessCodecHelpers(t *testing.T) {
	t.Run("limitation conversion", func(t *testing.T) {
		lim := Limitation{Size: 1, BatchRows: 2, BatchSize: 3, PartitionRows: 4, ReaderSize: 5, SpillSize: 6}
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
			User:                                "u",
			Host:                                "h",
			Role:                                "r",
			ConnectionId:                        1,
			Database:                            "d",
			Version:                             "v",
			Account:                             "a",
			QueryId:                             []string{"q1"},
			TimeZone:                            timeBytes,
			LockWaitTimeout:                     9,
			LockWaitTimeoutSet:                  true,
			MatrixoneNativeMode:                 true,
			ExplicitZeroTemporalCastReturnsNull: true,
			SqlMode:                             "STRICT_ALL_TABLES",
		})
		require.NoError(t, err)
		require.Equal(t, "u", info.User)
		require.Equal(t, int64(9), info.LockWaitTimeout)
		require.True(t, info.MatrixOneNativeMode)
		require.True(t, info.LockWaitTimeoutSet)
		require.True(t, info.ExplicitZeroTemporalCastReturnsNull)
		require.Equal(t, "STRICT_ALL_TABLES", info.SqlMode)
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

		proc.Base.SessionInfo.LockWaitTimeout = 5
		proc.Base.SessionInfo.LockWaitTimeoutSet = true
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
			return int64(11), nil
		})
		require.Equal(t, int64(5), resolveLockWaitTimeoutSeconds(proc),
			"an explicit positive execution timeout must survive remote encoding")

		proc.Base.SessionInfo.LockWaitTimeout = 0
		require.Equal(t, int64(11), resolveLockWaitTimeoutSeconds(proc),
			"an explicit zero clears the txn override and falls back to the resolver")
		proc.SetResolveVariableFunc(nil)
		require.Equal(t, defines.DefaultLockWaitTimeoutSeconds, resolveLockWaitTimeoutSeconds(proc),
			"the legacy wire field must remain positive when an explicit clear is sent to an old peer")
	})

	t.Run("sql mode resolution", func(t *testing.T) {
		require.Equal(t, "", resolveSqlMode(nil))

		// Resolver present: its value wins.
		proc := &Process{Base: &BaseProcess{SessionInfo: SessionInfo{SqlMode: "STRICT_ALL_TABLES"}}}
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
			return "STRICT_TRANS_TABLES", nil
		})
		require.Equal(t, "STRICT_TRANS_TABLES", resolveSqlMode(proc))

		// A frontend resolver returning an explicit empty string means the
		// session is intentionally non-strict.
		proc.Base.IsFrontend = true
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
			return "", nil
		})
		require.Equal(t, EmptySqlModeSentinel, resolveSqlMode(proc))

		// A background resolver may return its empty compiled default, but the
		// captured strict snapshot must survive the first serialization.
		proc.Base.IsFrontend = false
		require.Equal(t, "STRICT_ALL_TABLES", resolveSqlMode(proc))
		proc.Base.SessionInfo.SqlMode = EmptySqlModeSentinel
		require.Equal(t, EmptySqlModeSentinel, resolveSqlMode(proc),
			"an already-captured explicit empty mode must remain non-strict")
		proc.Base.SessionInfo.SqlMode = "STRICT_ALL_TABLES"

		// Resolver error / non-string -> fall back to captured SessionInfo.SqlMode.
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
			return nil, moerr.NewInternalErrorNoCtx("boom")
		})
		require.Equal(t, "STRICT_ALL_TABLES", resolveSqlMode(proc))

		// Resolver is nil (remote CN): fall back to SessionInfo.SqlMode so a second
		// forward preserves the upstream mode instead of defaulting to strict.
		strictProc := &Process{Base: &BaseProcess{SessionInfo: SessionInfo{SqlMode: "STRICT_TRANS_TABLES"}}}
		require.Equal(t, "STRICT_TRANS_TABLES", resolveSqlMode(strictProc))

		sentinelProc := &Process{Base: &BaseProcess{SessionInfo: SessionInfo{SqlMode: EmptySqlModeSentinel}}}
		require.Equal(t, EmptySqlModeSentinel, resolveSqlMode(sentinelProc))

		emptyProc := &Process{Base: &BaseProcess{SessionInfo: SessionInfo{}}}
		require.Equal(t, "", resolveSqlMode(emptyProc))
	})
}

func TestBuildProcessInfoPreservesBackgroundSqlModeAcrossForwards(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	proc.Base.IsFrontend = false
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return "", nil
	})

	first, err := proc.BuildProcessInfo("select 1")
	require.NoError(t, err)
	require.Equal(t, "STRICT_TRANS_TABLES", first.SessionInfo.SqlMode)

	svc := NewCodecService(fakeCodecTxnClient{op: fakeCodecTxnOperator{}}, nil, nil, nil, nil, nil, nil, nil)
	decoded, err := svc.Decode(defines.AttachAccountId(context.Background(), 42), first)
	require.NoError(t, err)
	defer decoded.Free()

	// The decoded remote process has no resolver. A second forward must retain
	// the snapshot produced by the first forward rather than defaulting to
	// non-strict mode.
	second, err := decoded.BuildProcessInfo("select 1")
	require.NoError(t, err)
	require.Equal(t, "STRICT_TRANS_TABLES", second.SessionInfo.SqlMode)
}

func TestPrepareParamMetadataForRemoteCompatibility(t *testing.T) {
	runtime := rt.ServiceRuntime("")
	original, hadOriginal := runtime.GetGlobalVariables(rt.MOProtocolVersion)
	defer func() {
		if hadOriginal {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, original)
		} else {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}()

	metadata := make([]bool, 8) // N=2: legacy flags + three one-bit sections.
	metadata[2] = true          // parameter 0 has integer provenance.
	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion11)
	_, err := PrepareParamMetadataForRemote("", 2, metadata)
	require.Error(t, err)

	metadata[2] = false
	metadata[0] = true // binary-only extended metadata is safe to down-pack.
	legacy, err := PrepareParamMetadataForRemote("", 2, metadata)
	require.NoError(t, err)
	require.Equal(t, []bool{true, false}, legacy)

	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion12)
	metadata[2] = true
	extended, err := PrepareParamMetadataForRemote("", 2, metadata)
	require.NoError(t, err)
	require.Equal(t, metadata, extended)

	_, err = PrepareParamMetadataForRemote("", 2, []bool{false, false, true})
	require.Error(t, err, "partial extended metadata must not be silently interpreted")

	invalidKind := []bool{false, true, false, true} // integer + boolean = 5
	_, err = PrepareParamMetadataForRemote("", 1, invalidKind)
	require.Error(t, err, "invalid packed kind bits must be rejected")
}

func TestBinaryStringPrepareParamMetadataForRemoteCompatibility(t *testing.T) {
	runtime := rt.ServiceRuntime("")
	original, hadOriginal := runtime.GetGlobalVariables(rt.MOProtocolVersion)
	defer func() {
		if hadOriginal {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, original)
		} else {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}()

	metadata := []bool{true, false}
	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion17)
	_, err := BinaryStringPrepareParamMetadataForRemote("", 2, metadata)
	require.Error(t, err)

	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion18)
	decoded, err := BinaryStringPrepareParamMetadataForRemote("", 2, metadata)
	require.NoError(t, err)
	require.Equal(t, metadata, decoded)
	decoded[0] = false
	require.True(t, metadata[0], "the receiver must own an independent metadata generation")

	_, err = BinaryStringPrepareParamMetadataForRemote("", 2, []bool{true})
	require.Error(t, err)
}

func TestStringSourcePrepareParamMetadataForRemoteCompatibility(t *testing.T) {
	runtime := rt.ServiceRuntime("")
	original, hadOriginal := runtime.GetGlobalVariables(rt.MOProtocolVersion)
	defer func() {
		if hadOriginal {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, original)
		} else {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}()

	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion24)
	metadata, err := StringSourcePrepareParamMetadataForRemote("", 2, []uint32{0, 4})
	require.NoError(t, err)
	require.Nil(t, metadata, "old peers must receive a source-free compatible payload")

	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion27)
	metadata, err = StringSourcePrepareParamMetadataForRemote("", 2, []uint32{0, 4})
	require.NoError(t, err)
	require.Equal(t, []uint32{0, 4}, metadata)
	_, err = StringSourcePrepareParamMetadataForRemote("", 2, []uint32{0, 255})
	require.ErrorContains(t, err, "invalid string source")
	_, err = StringSourcePrepareParamMetadataForRemote("", 2, []uint32{4})
	require.ErrorContains(t, err, "metadata length")

	metadata, err = StringSourcePrepareParamMetadataForRemote("", 2, []uint32{0, 0})
	require.NoError(t, err)
	require.Nil(t, metadata, "source-free metadata must not change the payload")
}

func TestCodecServiceRejectsPreparedProvenanceForOldProtocol(t *testing.T) {
	runtime := rt.ServiceRuntime("")
	original, hadOriginal := runtime.GetGlobalVariables(rt.MOProtocolVersion)
	defer func() {
		if hadOriginal {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, original)
		} else {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}()

	proc, _ := newCodecTestProcess(t)
	defer proc.Free()
	params := proc.GetPrepareParams()
	proc.SetPrepareParamsWithMeta(params, []bool{false, false}, []vector.PrepareParamKind{
		vector.PrepareParamFloat,
		vector.PrepareParamNone,
	})
	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion12)
	info, err := proc.BuildProcessInfo("select ?")
	require.NoError(t, err)

	svc := NewCodecService(
		fakeCodecTxnClient{op: fakeCodecTxnOperator{}},
		nil, nil, nil, nil, nil, nil, nil,
	).(*codecService)
	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion11)
	_, err = svc.Decode(context.Background(), info)
	require.Error(t, err)

	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion12)
	decoded, err := svc.Decode(context.Background(), info)
	require.NoError(t, err)
	defer decoded.Free()
	require.Equal(t, vector.PrepareParamFloat, decoded.GetPrepareParamKind(0))
}

func TestCodecServiceRejectsBinaryStringMetadataForOldProtocol(t *testing.T) {
	runtime := rt.ServiceRuntime("")
	original, hadOriginal := runtime.GetGlobalVariables(rt.MOProtocolVersion)
	defer func() {
		if hadOriginal {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, original)
		} else {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}()

	proc, _ := newCodecTestProcess(t)
	defer proc.Free()
	params := proc.GetPrepareParams()
	proc.SetPrepareParamsWithMetadata(params, []bool{false, false}, []bool{true, false})
	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion18)
	info, err := proc.BuildProcessInfo("select ?")
	require.NoError(t, err)

	svc := NewCodecService(
		fakeCodecTxnClient{op: fakeCodecTxnOperator{}},
		nil, nil, nil, nil, nil, nil, nil,
	).(*codecService)
	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion17)
	_, err = svc.Decode(context.Background(), info)
	require.Error(t, err)

	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion18)
	decoded, err := svc.Decode(context.Background(), info)
	require.NoError(t, err)
	defer decoded.Free()
	require.True(t, decoded.GetPrepareParamIsBinaryString(0))
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
	require.Equal(t, []bool{true, false}, info.PrepareParams.IsBin)
	require.Equal(t, []bool{false, true}, info.PrepareParams.IsBinaryString)
	require.Equal(t, int64(42), info.AffectedRows)
	require.True(t, info.StatementRuntimeIgnore)
	require.Equal(t, &timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4}, info.PlanSnapshotTs)
	require.Equal(t, uint64(99), info.SessionInfo.ConnectionId)
	require.Equal(t, int64(7), info.SessionInfo.LockWaitTimeout)
	require.True(t, info.SessionInfo.MatrixoneNativeMode)
	require.True(t, info.SessionInfo.ExplicitZeroTemporalCastReturnsNull)
	require.Equal(t, "STRICT_TRANS_TABLES", info.SessionInfo.SqlMode)
	require.True(t, info.SessionInfo.LockWaitTimeoutSet)
	require.Equal(t, pipeline.SessionLoggerInfo_Warn, info.SessionLogger.LogLevel)

	// A rolling-upgrade receiver compiled before LockWaitTimeoutSet ignores the
	// presence bit. It must still see the product fallback in the legacy value
	// field rather than zero, which would revive the reused txn's stale budget.
	proc.SetResolveVariableFunc(nil)
	proc.Base.SessionInfo.LockWaitTimeout = 0
	proc.Base.SessionInfo.LockWaitTimeoutSet = true
	legacyInfo, err := proc.BuildProcessInfo("select legacy")
	require.NoError(t, err)
	require.Equal(t, defines.DefaultLockWaitTimeoutSeconds, legacyInfo.SessionInfo.LockWaitTimeout)
	legacyInfo.SessionInfo.LockWaitTimeoutSet = false // simulate an old decoder
	legacySession, err := ConvertToProcessSessionInfo(legacyInfo.SessionInfo)
	require.NoError(t, err)
	require.False(t, legacySession.LockWaitTimeoutSet)
	require.Equal(t, defines.DefaultLockWaitTimeoutSeconds, legacySession.LockWaitTimeout)

	mockInfo, err := MockProcessInfoWithPro("select 2", proc)
	require.NoError(t, err)
	require.Equal(t, "select 2", mockInfo.Sql)
	require.Equal(t, "UTC", proc.Base.SessionInfo.TimeZone.String())
}

func TestBuildProcessInfoGatesBinaryStringMetadataByProtocolVersion(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	serviceRuntime := rt.ServiceRuntime(proc.GetService())
	original, hadOriginal := serviceRuntime.GetGlobalVariables(rt.MOProtocolVersion)
	defer func() {
		if hadOriginal {
			serviceRuntime.SetGlobalVariables(rt.MOProtocolVersion, original)
		} else {
			serviceRuntime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		proc.Free()
	}()

	serviceRuntime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion10)
	_, err := proc.BuildProcessInfo("select ?")
	require.Error(t, err)

	proc.SetPrepareParamsWithMetadata(proc.GetPrepareParams(), []bool{false, false}, []bool{false, false})
	info, err := proc.BuildProcessInfo("select ?")
	require.NoError(t, err)
	require.Empty(t, info.PrepareParams.IsBinaryString)

	serviceRuntime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion11)
	proc.SetPrepareParamsWithMetadata(proc.GetPrepareParams(), []bool{false, false}, []bool{false, true})
	_, err = proc.BuildProcessInfo("select ?")
	require.Error(t, err)

	serviceRuntime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion12)
	_, err = proc.BuildProcessInfo("select ?")
	require.Error(t, err)

	serviceRuntime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion13)
	_, err = proc.BuildProcessInfo("select ?")
	require.Error(t, err)

	serviceRuntime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion14)
	_, err = proc.BuildProcessInfo("select ?")
	require.Error(t, err)

	serviceRuntime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion15)
	_, err = proc.BuildProcessInfo("select ?")
	require.Error(t, err)

	serviceRuntime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion16)
	_, err = proc.BuildProcessInfo("select ?")
	require.Error(t, err)

	serviceRuntime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion17)
	_, err = proc.BuildProcessInfo("select ?")
	require.Error(t, err)

	serviceRuntime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion18)
	proc.SetPrepareParamsWithMetadata(proc.GetPrepareParams(), []bool{false, false}, []bool{false, true})
	info, err = proc.BuildProcessInfo("select ?")
	require.NoError(t, err)
	require.Equal(t, []bool{false, true}, info.PrepareParams.IsBinaryString)
}

func TestBuildProcessInfoRejectsInvalidBinaryStringMetadataLength(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	defer proc.Free()
	serviceRuntime := rt.ServiceRuntime(proc.GetService())
	original, hadOriginal := serviceRuntime.GetGlobalVariables(rt.MOProtocolVersion)
	defer func() {
		if hadOriginal {
			serviceRuntime.SetGlobalVariables(rt.MOProtocolVersion, original)
		} else {
			serviceRuntime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}()
	serviceRuntime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion18)

	params := proc.GetPrepareParams()
	for _, tc := range []struct {
		name     string
		params   *vector.Vector
		metadata []bool
	}{
		{name: "too-short", params: params, metadata: []bool{true}},
		{name: "too-long", params: params, metadata: []bool{true, false, false}},
		{name: "zero-params-with-metadata", params: vector.NewVec(types.T_varchar.ToType()), metadata: []bool{true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.params != params {
				defer tc.params.Free(proc.Mp())
			}
			proc.SetPrepareParamsWithMetadata(tc.params, nil, tc.metadata)
			_, err := proc.BuildProcessInfo("select ?")
			require.ErrorContains(t, err, "invalid binary-string prepare parameter metadata length")
		})
	}

	proc.SetPrepareParamsWithMetadata(params, nil, []bool{false, false})
	info, err := proc.BuildProcessInfo("select ?")
	require.NoError(t, err)
	require.Empty(t, info.PrepareParams.IsBinaryString)
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
	require.Equal(t, info.SessionInfo.MatrixoneNativeMode, decodedProc.Base.SessionInfo.MatrixOneNativeMode)
	require.True(t, decodedProc.Base.SessionInfo.ExplicitZeroTemporalCastReturnsNull)
	require.Equal(t, info.SessionInfo.SqlMode, decodedProc.Base.SessionInfo.SqlMode)
	require.Equal(t, info.SessionInfo.LockWaitTimeoutSet, decodedProc.Base.SessionInfo.LockWaitTimeoutSet)
	require.NotNil(t, decodedProc.GetPrepareParams())
	require.Equal(t, 2, decodedProc.GetPrepareParams().Length())
	require.True(t, decodedProc.GetPrepareParams().GetNulls().Contains(1))
	require.True(t, decodedProc.GetPrepareParamIsBin(0))
	require.False(t, decodedProc.GetPrepareParamIsBin(1))
	require.Equal(t, vector.PrepareParamNone, decodedProc.GetPrepareParamKind(0))
	require.Equal(t, vector.PrepareParamNone, decodedProc.GetPrepareParamKind(1))
	require.Equal(t, int64(42), decodedProc.GetAffectedRows())
	require.True(t, decodedProc.GetStmtProfile().GetStatementIgnore())
	decodedPlanSnapshot, ok := decodedProc.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4}, decodedPlanSnapshot)
	decodedParams := decodedProc.GetPrepareParams()
	require.NotPanics(t, decodedProc.Free)
	require.Nil(t, decodedParams.GetData())
	require.Nil(t, decodedParams.GetArea())

	info.PlanSnapshotTs = nil // simulate a sender from before the field existed
	legacyProc, err := svc.Decode(context.Background(), info)
	require.NoError(t, err)
	_, ok = legacyProc.GetPlanSnapshotTS()
	require.False(t, ok)
	require.NotPanics(t, legacyProc.Free)

	rtSvc := "codec-test-svc"
	runtime := rt.DefaultRuntime()
	rt.SetupServiceBasedRuntime(rtSvc, runtime)
	runtime.SetGlobalVariables(rt.ProcessCodecService, svc)
	require.Same(t, svc, GetCodecService(rtSvc))
}

func TestPlanSnapshotIsCopiedPerPipelineProcess(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	firstSnapshot := timestamp.Timestamp{PhysicalTime: 10}
	secondSnapshot := timestamp.Timestamp{PhysicalTime: 20}
	proc.SetPlanSnapshotTS(firstSnapshot)

	child := proc.NewNoContextChildProc(0)
	got, ok := child.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, firstSnapshot, got)
	channelChild := proc.NewNoContextChildProcWithChannel(1, []int32{1}, []int32{0})
	got, ok = channelChild.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, firstSnapshot, got)

	// A later generation on the top process cannot mutate an already-created
	// pipeline generation because setting a generation installs a new immutable
	// binding rather than mutating the prior one.
	proc.SetPlanSnapshotTS(secondSnapshot)
	got, ok = child.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, firstSnapshot, got)

	proc.ClearPlanSnapshotTS()
	legacyChild := proc.NewNoContextChildProc(0)
	_, ok = legacyChild.GetPlanSnapshotTS()
	require.False(t, ok)
	proc.Free()
}

func TestCodecServiceRoundTripsPreparedRowsFrameParams(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	frameParams := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(frameParams, []byte("1"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(frameParams, []byte("0"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(frameParams, []byte("true"), false, proc.Mp()))
	require.NoError(t, frameParams.SetStringSourcesWithMP([]types.StringSource{
		types.StringSourceCOMStmt,
		types.StringSourceSQLPrepare,
		types.StringSourceUserVariable,
	}, proc.Mp()))
	proc.SetPrepareParamsWithMeta(frameParams, []bool{true, false, false}, []vector.PrepareParamKind{
		vector.PrepareParamNone,
		vector.PrepareParamDecimal,
		vector.PrepareParamBoolean,
	})

	svc := NewCodecService(fakeCodecTxnClient{op: fakeCodecTxnOperator{}}, nil, nil, nil, nil, nil, nil, nil)
	payload, err := svc.Encode(proc, "select sum(n) over (order by id rows between ? preceding and ? following)")
	require.NoError(t, err)

	info := pipeline.ProcessInfo{}
	require.NoError(t, info.Unmarshal(payload))
	require.Equal(t, []bool{
		true, false, false,
		false, true, false,
		false, true, false,
		false, false, true,
	}, info.PrepareParams.IsBin)
	require.Equal(t, []uint32{4, 3, 2}, info.PrepareParams.StringSources)
	decodedProc, err := svc.Decode(context.Background(), info)
	require.NoError(t, err)
	defer decodedProc.Free()

	decodedParams := decodedProc.GetPrepareParams()
	require.NotNil(t, decodedParams)
	require.Equal(t, 3, decodedParams.Length())
	require.False(t, decodedParams.GetNulls().Contains(0))
	require.False(t, decodedParams.GetNulls().Contains(1))
	require.False(t, decodedParams.GetNulls().Contains(2))
	require.True(t, decodedProc.GetPrepareParamIsBin(0))
	require.False(t, decodedProc.GetPrepareParamIsBin(1))
	require.Equal(t, vector.PrepareParamNone, decodedProc.GetPrepareParamKind(0))
	require.Equal(t, vector.PrepareParamDecimal, decodedProc.GetPrepareParamKind(1))
	require.Equal(t, vector.PrepareParamBoolean, decodedProc.GetPrepareParamKind(2))
	require.Equal(t, "1", decodedParams.GetStringAt(0))
	require.Equal(t, "0", decodedParams.GetStringAt(1))
	require.Equal(t, "true", decodedParams.GetStringAt(2))
	require.Equal(t, types.StringSourceCOMStmt, decodedParams.GetStringSourceAt(0))
	require.Equal(t, types.StringSourceSQLPrepare, decodedParams.GetStringSourceAt(1))
	require.Equal(t, types.StringSourceUserVariable, decodedParams.GetStringSourceAt(2))
}

func TestCodecServiceDecodesLegacyPrepareParamsWithoutBinaryFlags(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	info, err := proc.BuildProcessInfo("select ?")
	require.NoError(t, err)
	info.PrepareParams.IsBin = nil
	info.PrepareParams.IsBinaryString = nil
	// An old coordinator does not send the new field. Protobuf decodes that
	// absence as false, preserving the prior strict-mode behavior remotely.
	info.StatementRuntimeIgnore = false

	payload, err := info.Marshal()
	require.NoError(t, err)
	legacyInfo := pipeline.ProcessInfo{}
	require.NoError(t, legacyInfo.Unmarshal(payload))
	require.Empty(t, legacyInfo.PrepareParams.IsBin)
	require.Empty(t, legacyInfo.PrepareParams.IsBinaryString)

	svc := NewCodecService(fakeCodecTxnClient{op: fakeCodecTxnOperator{}}, nil, nil, nil, nil, nil, nil, nil)
	decodedProc, err := svc.Decode(context.Background(), legacyInfo)
	require.NoError(t, err)
	require.NotNil(t, decodedProc.GetPrepareParams())
	require.Equal(t, 2, decodedProc.GetPrepareParams().Length())
	require.False(t, decodedProc.GetPrepareParamIsBin(0))
	require.False(t, decodedProc.GetPrepareParamIsBin(1))
	require.Equal(t, vector.PrepareParamNone, decodedProc.GetPrepareParamKind(0))
	require.Equal(t, vector.PrepareParamNone, decodedProc.GetPrepareParamKind(1))
	require.False(t, decodedProc.GetStmtProfile().GetStatementIgnore())
	decodedProc.Free()
}

func TestGetCodecServicePanicsWhenMissing(t *testing.T) {
	rtSvc := "codec-missing-svc"
	runtime := rt.NewRuntime(metadata.ServiceType_CN, rtSvc, nil)
	rt.SetupServiceBasedRuntime(rtSvc, runtime)
	require.Panics(t, func() {
		_ = GetCodecService(rtSvc)
	})
}
