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
	"fmt"
	"strings"
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
		ExplicitZeroTemporalCastReturnsNull: false,
		SqlMode:                             "STRICT_TRANS_TABLES",
		AutoIncrementIncrement:              7,
		AutoIncrementOffset:                 4,
		LastInsertID:                        500,
		MaxErrorCount:                       128,
		MaxErrorCountSet:                    true,
		LCTimeNames:                         "fr_FR",
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
	proc.SetPlanGenerationReused(true)
	proc.SetStringShuffleHashAlgorithm(StringShuffleHashComplete)
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
			AutoIncrementIncrement:              7,
			AutoIncrementOffset:                 4,
			LastInsertId:                        500,
			MaxErrorCount:                       128,
			MaxErrorCountSet:                    true,
		})
		require.NoError(t, err)
		require.Equal(t, "u", info.User)
		require.Equal(t, int64(9), info.LockWaitTimeout)
		require.True(t, info.MatrixOneNativeMode)
		require.True(t, info.LockWaitTimeoutSet)
		require.True(t, info.ExplicitZeroTemporalCastReturnsNull)
		require.Equal(t, "STRICT_ALL_TABLES", info.SqlMode)
		require.Equal(t, uint64(7), info.AutoIncrementIncrement)
		require.Equal(t, uint64(4), info.AutoIncrementOffset)
		require.Equal(t, uint64(500), info.LastInsertID)
		require.Equal(t, 128, info.MaxErrorCount)
		require.True(t, info.MaxErrorCountSet)
		require.Equal(t, "UTC", info.TimeZone.String())

		info, err = ConvertToProcessSessionInfo(pipeline.SessionInfo{TimeZone: []byte("bad")})
		require.NoError(t, err)
		require.Nil(t, info.TimeZone)
		_, err = ConvertToProcessSessionInfo(pipeline.SessionInfo{
			MaxErrorCount:    uint32(^uint16(0)) + 1,
			MaxErrorCountSet: true,
		})
		require.Error(t, err)
		zero, err := ConvertToProcessSessionInfo(pipeline.SessionInfo{
			MaxErrorCountSet: true,
		})
		require.NoError(t, err)
		require.Zero(t, zero.MaxErrorCount)
		require.True(t, zero.MaxErrorCountSet)
		max, err := ConvertToProcessSessionInfo(pipeline.SessionInfo{
			MaxErrorCount:    uint32(^uint16(0)),
			MaxErrorCountSet: true,
		})
		require.NoError(t, err)
		require.Equal(t, int(^uint16(0)), max.MaxErrorCount)
		require.True(t, max.MaxErrorCountSet)
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

func TestProcessCodecFixedTimeZoneAcrossForwards(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	defer proc.Free()
	for _, offset := range []int{-14 * 3600, -120, -119, -61, -60, -59, 0, 61, 14 * 3600} {
		proc.Base.SessionInfo.TimeZone = time.FixedZone("fixed", offset)
		first, err := proc.BuildProcessInfo("select 1")
		require.NoError(t, err)
		require.True(t, first.SessionInfo.FixedOffsetPresent)
		require.Equal(t, int64(offset), first.SessionInfo.FixedOffsetSeconds)
		require.Empty(t, first.SessionInfo.TimeZoneName)
		require.Empty(t, first.SessionInfo.TimeZone)
		wire, err := first.Marshal()
		require.NoError(t, err)
		var received pipeline.ProcessInfo
		require.NoError(t, received.Unmarshal(wire))
		decoded, err := ConvertToProcessSessionInfo(received.SessionInfo)
		require.NoError(t, err)
		for _, date := range []time.Time{{}, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)} {
			_, actual := date.In(decoded.TimeZone).Zone()
			require.Equal(t, offset, actual)
		}
		proc.Base.SessionInfo.TimeZone = decoded.TimeZone
		forwarded, err := proc.BuildProcessInfo("select 1")
		require.NoError(t, err)
		require.True(t, forwarded.SessionInfo.FixedOffsetPresent)
		require.Equal(t, int64(offset), forwarded.SessionInfo.FixedOffsetSeconds)
		require.Empty(t, forwarded.SessionInfo.TimeZone)
	}
	for _, offset := range []int{1 << 32, -(1 << 32)} {
		proc.Base.SessionInfo.TimeZone = time.FixedZone("", offset)
		_, err := proc.BuildProcessInfo("select 1")
		require.Error(t, err, "out-of-range offsets must fail instead of publishing UTC")
	}
	_, err := ConvertToProcessSessionInfo(pipeline.SessionInfo{
		FixedOffsetPresent: true, FixedOffsetSeconds: 1, TimeZoneName: "UTC",
	})
	require.Error(t, err)
	_, err = ConvertToProcessSessionInfo(pipeline.SessionInfo{
		FixedOffsetPresent: true, FixedOffsetSeconds: 1 << 32,
	})
	require.Error(t, err)
	_, err = ConvertToProcessSessionInfo(pipeline.SessionInfo{FixedOffsetSeconds: 1})
	require.Error(t, err)
}

func TestBuildProcessInfoPreservesBackgroundSqlModeAcrossForwards(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	proc.Base.IsFrontend = false
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "default_week_format" {
			return int64(0), nil
		}
		return "", nil
	})

	first, err := proc.BuildProcessInfo("select 1")
	require.NoError(t, err)
	require.Equal(t, "STRICT_TRANS_TABLES", first.SessionInfo.SqlMode)
	require.Equal(t, uint32(128), first.SessionInfo.MaxErrorCount)
	require.True(t, first.SessionInfo.MaxErrorCountSet)

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
	require.Equal(t, uint32(128), second.SessionInfo.MaxErrorCount)
	require.True(t, second.SessionInfo.MaxErrorCountSet)
}

func TestBuildProcessInfoPreservesWeekModePerExecution(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	mode := int64(3)
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "default_week_format" {
			return mode, nil
		}
		return nil, nil
	})
	first, err := proc.BuildProcessInfo("select week(d)")
	require.NoError(t, err)
	require.Equal(t, uint32(3), first.SessionInfo.DefaultWeekFormat)
	require.True(t, first.SessionInfo.DefaultWeekFormatSet)
	wire, err := first.Marshal()
	require.NoError(t, err)
	var received pipeline.ProcessInfo
	require.NoError(t, received.Unmarshal(wire))
	svc := NewCodecService(fakeCodecTxnClient{op: fakeCodecTxnOperator{}}, nil, nil, nil, nil, nil, nil, nil)
	remote, err := svc.Decode(defines.AttachAccountId(context.Background(), 42), received)
	require.NoError(t, err)
	defer remote.Free()
	got, present, err := ResolveDefaultWeekFormatMode(remote)
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, 3, got)
	forwarded, err := remote.BuildProcessInfo("select week(d)")
	require.NoError(t, err)
	require.Equal(t, first.SessionInfo.DefaultWeekFormat, forwarded.SessionInfo.DefaultWeekFormat)
	require.True(t, forwarded.SessionInfo.DefaultWeekFormatSet)

	mode = 0 // A new prepared execution reads the new session value.
	second, err := proc.BuildProcessInfo("select week(d)")
	require.NoError(t, err)
	require.Equal(t, uint32(0), second.SessionInfo.DefaultWeekFormat)
	require.True(t, second.SessionInfo.DefaultWeekFormatSet)
	missing := &Process{Base: &BaseProcess{SessionInfo: SessionInfo{}}}
	_, present, err = ResolveDefaultWeekFormatMode(missing)
	require.NoError(t, err)
	require.False(t, present)
}

func TestBuildProcessInfoUsesEffectiveWarningSinkAcrossForwards(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	proc.Base.SessionInfo.MaxErrorCount = WarningDiagnosticDefaultRetentionLimit
	proc.Base.SessionInfo.MaxErrorCountSet = true
	proc.WarningSink = &retentionWarningSession{limit: 2048}

	first, err := proc.BuildProcessInfo("select 1")
	require.NoError(t, err)
	require.Equal(t, uint32(2048), first.SessionInfo.MaxErrorCount)
	require.True(t, first.SessionInfo.MaxErrorCountSet)

	// An explicit zero from the active attempt must survive serialization even
	// when the reused Process still carries the normal SessionInfo default.
	proc.WarningSink = &retentionWarningSession{limit: 0}
	zero, err := proc.BuildProcessInfo("select 1")
	require.NoError(t, err)
	require.Zero(t, zero.SessionInfo.MaxErrorCount)
	require.True(t, zero.SessionInfo.MaxErrorCountSet)

	// Decode the effective snapshot and forward it again from a second CN. The
	// second hop must use its current attempt sink, rather than resurrecting the
	// NewTopProcess default of 1024.
	proc.WarningSink = &retentionWarningSession{limit: 2048}
	svc := NewCodecService(fakeCodecTxnClient{op: fakeCodecTxnOperator{}}, nil, nil, nil, nil, nil, nil, nil)
	decoded, err := svc.Decode(defines.AttachAccountId(context.Background(), 42), first)
	require.NoError(t, err)
	defer decoded.Free()
	decoded.WarningSink = &retentionWarningSession{limit: 2048}
	second, err := decoded.BuildProcessInfo("select 1")
	require.NoError(t, err)
	require.Equal(t, uint32(2048), second.SessionInfo.MaxErrorCount)
	require.True(t, second.SessionInfo.MaxErrorCountSet)
}

func TestPrepareParamMetadataForRemoteCompatibility(t *testing.T) {
	// Current peers send exactly N binary flags.
	flags, err := PrepareParamMetadataForRemote("", 2, []bool{true, false})
	require.NoError(t, err)
	require.Equal(t, []bool{true, false}, flags)

	flags, err = PrepareParamMetadataForRemote("", 2, nil)
	require.NoError(t, err)
	require.Nil(t, flags)

	// Older peers appended three conversion-kind sections (4N) and optionally
	// eight concrete-type sections (12N). Both are accepted and truncated to
	// the leading N binary flags.
	legacy4N := make([]bool, 8)
	legacy4N[0] = true
	legacy4N[2] = true // retired integer-kind bit for parameter 0
	legacy4N[7] = true
	flags, err = PrepareParamMetadataForRemote("", 2, legacy4N)
	require.NoError(t, err)
	require.Equal(t, []bool{true, false}, flags)

	legacy12N := make([]bool, 24)
	legacy12N[1] = true
	for i := 2; i < len(legacy12N); i += 3 {
		legacy12N[i] = true
	}
	flags, err = PrepareParamMetadataForRemote("", 2, legacy12N)
	require.NoError(t, err)
	require.Equal(t, []bool{false, true}, flags)
	require.Len(t, flags, 2)

	_, err = PrepareParamMetadataForRemote("", 2, []bool{false, false, true})
	require.Error(t, err, "partial extended metadata must not be silently interpreted")
	_, err = PrepareParamMetadataForRemote("", 1, make([]bool, 6))
	require.Error(t, err)
	_, err = PrepareParamMetadataForRemote("", 0, []bool{true})
	require.Error(t, err)
	_, err = PrepareParamMetadataForRemote("", -1, nil)
	require.Error(t, err)
}

func TestCodecServiceAcceptsLegacyKindMetadataFromOldPeer(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	defer proc.Free()
	info, err := proc.BuildProcessInfo("select ?, ?")
	require.NoError(t, err)
	require.Equal(t, []bool{true, false}, info.PrepareParams.IsBin)

	svc := NewCodecService(
		fakeCodecTxnClient{op: fakeCodecTxnOperator{}},
		nil, nil, nil, nil, nil, nil, nil,
	).(*codecService)
	for _, legacyLen := range []int{8, 24} {
		legacy := make([]bool, legacyLen)
		legacy[0] = true
		for i := 2; i < legacyLen; i++ {
			legacy[i] = true // retired kind/type sections
		}
		info.PrepareParams.IsBin = legacy
		decoded, err := svc.Decode(context.Background(), info)
		require.NoError(t, err)
		require.True(t, decoded.GetPrepareParamIsBin(0))
		require.False(t, decoded.GetPrepareParamIsBin(1))
		require.Len(t, decoded.Base.prepareParamsIsBin, 2)
		decoded.Free()
	}
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

	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion36)
	metadata, err := StringSourcePrepareParamMetadataForRemote("", 2, []uint32{0, 4})
	require.NoError(t, err)
	require.Nil(t, metadata, "old peers must receive a source-free compatible payload")

	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion37)
	metadata, err = StringSourcePrepareParamMetadataForRemote("", 2, []uint32{0, 4})
	require.NoError(t, err)
	require.Equal(t, []uint32{0, 4}, metadata)
	for _, rawSource := range []uint32{255, 256, 257, ^uint32(0)} {
		_, err = StringSourcePrepareParamMetadataForRemote("", 2, []uint32{0, rawSource})
		require.ErrorContains(t, err, "invalid string source")
	}
	_, err = StringSourcePrepareParamMetadataForRemote("", 2, []uint32{4})
	require.ErrorContains(t, err, "metadata length")

	metadata, err = StringSourcePrepareParamMetadataForRemote("", 2, []uint32{0, 0})
	require.NoError(t, err)
	require.Nil(t, metadata, "source-free metadata must not change the payload")
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

func TestCodecServiceRejectsMalformedStringSourceMetadata(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	defer proc.Free()
	info, err := proc.BuildProcessInfo("select ?")
	require.NoError(t, err)

	svc := NewCodecService(
		fakeCodecTxnClient{op: fakeCodecTxnOperator{}},
		nil, nil, nil, nil, nil, nil, nil,
	).(*codecService)
	for _, test := range []struct {
		name    string
		length  int64
		sources []uint32
		wantErr string
	}{
		{
			name:    "zero count with metadata",
			sources: []uint32{uint32(types.StringSourceCOMStmt)},
			wantErr: "invalid string source prepare parameter metadata length",
		},
		{
			name:    "count mismatch",
			length:  2,
			sources: []uint32{uint32(types.StringSourceCOMStmt)},
			wantErr: "invalid string source prepare parameter metadata length",
		},
		{
			name:    "invalid enum",
			length:  2,
			sources: []uint32{uint32(types.StringSourceExpression), 999},
			wantErr: "invalid string source",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			malformed := info
			malformed.PrepareParams = pipeline.PrepareParamInfo{
				Length:        test.length,
				StringSources: test.sources,
			}
			_, err := svc.Decode(context.Background(), malformed)
			require.ErrorContains(t, err, test.wantErr)
		})
	}
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
	require.True(t, info.PlanGenerationReused)
	require.Equal(t, uint64(99), info.SessionInfo.ConnectionId)
	require.Equal(t, int64(7), info.SessionInfo.LockWaitTimeout)
	require.True(t, info.SessionInfo.MatrixoneNativeMode)
	require.False(t, info.SessionInfo.ExplicitZeroTemporalCastReturnsNull)
	require.Equal(t, "STRICT_TRANS_TABLES", info.SessionInfo.SqlMode)
	require.True(t, info.SessionInfo.LockWaitTimeoutSet)
	require.Equal(t, uint64(7), info.SessionInfo.AutoIncrementIncrement)
	require.Equal(t, uint64(4), info.SessionInfo.AutoIncrementOffset)
	require.Equal(t, uint32(128), info.SessionInfo.MaxErrorCount)
	require.True(t, info.SessionInfo.MaxErrorCountSet)
	require.Equal(t, "fr_FR", info.SessionInfo.LcTimeNames)
	decodedSession, err := ConvertToProcessSessionInfo(info.SessionInfo)
	require.NoError(t, err)
	require.Equal(t, "fr_FR", decodedSession.LCTimeNames)
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

	var info pipeline.ProcessInfo
	require.NoError(t, info.Unmarshal(data))
	require.Equal(t, proc.Base.SessionInfo.Account, info.SessionInfo.Account)

	decodedProc, err := svc.Decode(context.Background(), info)
	require.NoError(t, err)
	require.Equal(t, info.Id, decodedProc.QueryId())
	require.Equal(t, info.UnixTime, decodedProc.Base.UnixTime)
	require.Equal(t, info.SessionInfo.User, decodedProc.Base.SessionInfo.User)
	require.Equal(t, info.SessionInfo.Account, decodedProc.Base.SessionInfo.Account)
	require.Equal(t, info.SessionInfo.LockWaitTimeout, decodedProc.Base.SessionInfo.LockWaitTimeout)
	require.Equal(t, info.SessionInfo.MatrixoneNativeMode, decodedProc.Base.SessionInfo.MatrixOneNativeMode)
	require.False(t, decodedProc.Base.SessionInfo.ExplicitZeroTemporalCastReturnsNull)
	require.Equal(t, info.SessionInfo.SqlMode, decodedProc.Base.SessionInfo.SqlMode)
	require.Equal(t, info.SessionInfo.LcTimeNames, decodedProc.Base.SessionInfo.LCTimeNames)
	require.Equal(t, info.SessionInfo.LockWaitTimeoutSet, decodedProc.Base.SessionInfo.LockWaitTimeoutSet)
	require.Equal(t, info.SessionInfo.AutoIncrementIncrement, decodedProc.Base.SessionInfo.AutoIncrementIncrement)
	require.Equal(t, info.SessionInfo.AutoIncrementOffset, decodedProc.Base.SessionInfo.AutoIncrementOffset)
	require.Equal(t, info.SessionInfo.MaxErrorCount, uint32(decodedProc.Base.SessionInfo.MaxErrorCount))
	require.Equal(t, info.SessionInfo.MaxErrorCountSet, decodedProc.Base.SessionInfo.MaxErrorCountSet)
	require.NotNil(t, decodedProc.GetPrepareParams())
	require.Equal(t, 2, decodedProc.GetPrepareParams().Length())
	require.True(t, decodedProc.GetPrepareParams().GetNulls().Contains(1))
	require.True(t, decodedProc.GetPrepareParamIsBin(0))
	require.False(t, decodedProc.GetPrepareParamIsBin(1))
	require.Equal(t, int64(42), decodedProc.GetAffectedRows())
	require.True(t, decodedProc.GetStmtProfile().GetStatementIgnore())
	decodedPlanSnapshot, ok := decodedProc.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4}, decodedPlanSnapshot)
	require.True(t, decodedProc.PlanGenerationReused())
	require.Equal(t, StringShuffleHashComplete, decodedProc.StringShuffleHashAlgorithm())
	decodedParams := decodedProc.GetPrepareParams()
	require.NotPanics(t, decodedProc.Free)
	require.Nil(t, decodedParams.GetData())
	require.Nil(t, decodedParams.GetArea())

	info.PlanSnapshotTs = nil // simulate a sender from before the fields existed
	info.PlanGenerationReused = false
	info.StringShuffleHashAlgorithm = 0
	legacyProc, err := svc.Decode(context.Background(), info)
	require.NoError(t, err)
	_, ok = legacyProc.GetPlanSnapshotTS()
	require.False(t, ok)
	require.False(t, legacyProc.PlanGenerationReused())
	require.Equal(t, StringShuffleHashLegacy, legacyProc.StringShuffleHashAlgorithm())
	require.NotPanics(t, legacyProc.Free)

	for _, invalid := range []uint32{2, 99, 257} {
		info.StringShuffleHashAlgorithm = invalid
		_, err = svc.Decode(context.Background(), info)
		require.ErrorContains(t, err,
			fmt.Sprintf("string shuffle hash algorithm %d is not supported", invalid))
	}
	proc.SetStringShuffleHashAlgorithm(StringShuffleHashAlgorithm(99))
	_, err = proc.BuildProcessInfo("select invalid hash algorithm")
	require.ErrorContains(t, err, "string shuffle hash algorithm 99 is not supported")

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
	proc.SetPlanGenerationReused(true)

	child := proc.NewNoContextChildProc(0)
	got, ok := child.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, firstSnapshot, got)
	require.True(t, child.PlanGenerationReused())
	channelChild := proc.NewNoContextChildProcWithChannel(1, []int32{1}, []int32{0})
	got, ok = channelChild.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, firstSnapshot, got)

	// A later generation on the top process cannot mutate an already-created
	// pipeline generation because setting a generation installs a new immutable
	// binding rather than mutating the prior one.
	proc.SetPlanSnapshotTS(secondSnapshot)
	require.False(t, proc.PlanGenerationReused())
	got, ok = child.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, firstSnapshot, got)
	require.True(t, child.PlanGenerationReused())

	proc.ClearPlanSnapshotTS()
	legacyChild := proc.NewNoContextChildProc(0)
	_, ok = legacyChild.GetPlanSnapshotTS()
	require.False(t, ok)
	require.False(t, legacyChild.PlanGenerationReused())
	proc.Free()
}

func TestStringShuffleHashAlgorithmIsCopiedPerPipelineProcess(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	defer proc.Free()
	proc.SetStringShuffleHashAlgorithm(StringShuffleHashComplete)

	child := proc.NewNoContextChildProc(0)
	channelChild := proc.NewNoContextChildProcWithChannel(1, []int32{1}, []int32{0})
	require.Equal(t, StringShuffleHashComplete, child.StringShuffleHashAlgorithm())
	require.Equal(t, StringShuffleHashComplete, channelChild.StringShuffleHashAlgorithm())

	// Selecting the next execution after rollback cannot mutate processes that
	// already belong to the running execution.
	proc.SetStringShuffleHashAlgorithm(StringShuffleHashLegacy)
	require.Equal(t, StringShuffleHashComplete, child.StringShuffleHashAlgorithm())
	require.Equal(t, StringShuffleHashComplete, channelChild.StringShuffleHashAlgorithm())
	require.Equal(t, StringShuffleHashLegacy,
		proc.NewNoContextChildProc(0).StringShuffleHashAlgorithm())
}

func TestRuntimeStringDomainPrepareParamMetadataForRemoteValidation(t *testing.T) {
	runtime := rt.ServiceRuntime("")
	original, hadOriginal := runtime.GetGlobalVariables(rt.MOProtocolVersion)
	defer func() {
		if hadOriginal {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, original)
		} else {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}()

	metadata := []uint32{uint32(types.RuntimeStringText)}
	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion57)
	_, err := RuntimeStringDomainPrepareParamMetadataForRemote("", 1, metadata)
	require.ErrorContains(t, err, "protocol version 58")

	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion58)
	decoded, err := RuntimeStringDomainPrepareParamMetadataForRemote("", 1, metadata)
	require.NoError(t, err)
	require.Equal(t, metadata, decoded)
	decoded[0] = uint32(types.RuntimeStringBinary)
	require.Equal(t, uint32(types.RuntimeStringText), metadata[0],
		"the receiver must own an independent runtime-domain generation")

	_, err = RuntimeStringDomainPrepareParamMetadataForRemote("", 1, []uint32{3})
	require.ErrorContains(t, err, "invalid runtime string domain")
	_, err = RuntimeStringDomainPrepareParamMetadataForRemote("", 2, metadata)
	require.ErrorContains(t, err, "metadata length")
}

func TestBuildProcessInfoSerializesUniformRuntimeBinaryDomain(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(params, []byte("a"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(params, []byte("b"), false, proc.Mp()))
	require.NoError(t, params.SetRuntimeStringDomainWithMP(types.RuntimeStringBinary, proc.Mp()))
	proc.SetPrepareParams(params)

	info, err := proc.BuildProcessInfo("select ?")
	require.NoError(t, err)
	require.Equal(t, []uint32{2, 2}, info.PrepareParams.RuntimeStringDomains)
}

func TestCodecServiceRoundTripsPreparedRowsFrameParams(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	frameParams := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(frameParams, []byte("1"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(frameParams, []byte("0"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(frameParams, []byte("true"), false, proc.Mp()))
	proc.SetPrepareParamsWithIsBin(frameParams, []bool{true, false, false})
	proc.SetPrepareParamsStringSource(types.StringSourceCOMStmt)
	require.NoError(t, frameParams.SetRuntimeStringDomainsWithMP([]types.RuntimeStringDomain{
		types.RuntimeStringInherit, types.RuntimeStringText, types.RuntimeStringBinary,
	}, proc.Mp()))

	svc := NewCodecService(fakeCodecTxnClient{op: fakeCodecTxnOperator{}}, nil, nil, nil, nil, nil, nil, nil)
	payload, err := svc.Encode(proc, "select sum(n) over (order by id rows between ? preceding and ? following)")
	require.NoError(t, err)

	info := pipeline.ProcessInfo{}
	require.NoError(t, info.Unmarshal(payload))
	require.Equal(t, []bool{true, false, false}, info.PrepareParams.IsBin)
	require.Equal(t, []uint32{4, 4, 4}, info.PrepareParams.StringSources)
	require.Equal(t, []uint32{0, 1, 2}, info.PrepareParams.RuntimeStringDomains)
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
	require.False(t, decodedProc.GetPrepareParamIsBin(2))
	require.Equal(t, "1", decodedParams.GetStringAt(0))
	require.Equal(t, "0", decodedParams.GetStringAt(1))
	require.Equal(t, "true", decodedParams.GetStringAt(2))
	require.Equal(t, types.StringSourceCOMStmt, decodedProc.GetPrepareParamsStringSource())
	require.Equal(t, types.RuntimeStringInherit, decodedParams.GetRuntimeStringDomainAt(0))
	require.Equal(t, types.RuntimeStringText, decodedParams.GetRuntimeStringDomainAt(1))
	require.Equal(t, types.RuntimeStringBinary, decodedParams.GetRuntimeStringDomainAt(2))
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

func TestBuildProcessInfoBaselinePresenceRequiresActualOwner(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	proc.SetResolveVariableFunc(nil)
	proc.Base.SessionInfo.LastInsertIDSet = false
	proc.SetLastInsertID(0)
	info, err := proc.BuildProcessInfo("select last_insert_id()")
	require.NoError(t, err)
	require.True(t, info.SessionInfo.LastInsertIdSet)
	require.Zero(t, info.SessionInfo.LastInsertId)
	proc.Base.LastInsertID = nil
	info, err = proc.BuildProcessInfo("select 1")
	require.NoError(t, err)
	require.False(t, info.SessionInfo.LastInsertIdSet, "a missing owner cannot be advertised as authoritative0")
}

func TestSelectStatementProfileCodecRoundTrip(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	defer proc.Free()
	proc.GetStmtProfile().SetStatementRuntimeProfile("Select", "DQL", false)
	proc.GetSessionInfo().SqlMode = "ERROR_FOR_DIVISION_BY_ZERO"
	info, err := proc.BuildProcessInfo("select 1/0")
	require.NoError(t, err)
	require.True(t, info.StatementRuntimeSelect)
	data, err := info.Marshal()
	require.NoError(t, err)
	var decodedInfo pipeline.ProcessInfo
	require.NoError(t, decodedInfo.Unmarshal(data))
	svc := NewCodecService(fakeCodecTxnClient{op: fakeCodecTxnOperator{}}, nil, nil, nil, nil, nil, nil, nil).(*codecService)
	for _, selectStatement := range []bool{true, false} {
		func() {
			decodedInfo.StatementRuntimeSelect = selectStatement
			decoded, err := svc.Decode(defines.AttachAccountId(context.Background(), 42), decodedInfo)
			require.NoError(t, err)
			defer decoded.Free()
			require.Equal(t, selectStatement, decoded.IsSelectStatement())
			require.Equal(t, selectStatement, decoded.DivisionByZeroWarningsEnabled())
			forwarded, err := decoded.BuildProcessInfo("select 1/0")
			require.NoError(t, err)
			require.Equal(t, selectStatement, forwarded.StatementRuntimeSelect)
		}()
	}
}

func TestDMLStatementProfileCodecRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		statement, query string
		ignore           bool
	}{
		{"Select", "DQL", false}, {"Insert", "DML", false}, {"Update", "DML", false},
		{"Load", "DML", true}, {"Insert", "DML", true}, {"", "", false}, {"Create Table", "DDL", false},
	} {
		t.Run(fmt.Sprintf("%s/%t", tc.statement, tc.ignore), func(t *testing.T) {
			proc, _ := newCodecTestProcess(t)
			t.Cleanup(proc.Free)
			proc.GetStmtProfile().SetStatementRuntimeProfile(tc.statement, tc.query, tc.ignore)
			info, err := proc.BuildProcessInfo("context roundtrip")
			require.NoError(t, err)
			data, err := info.Marshal()
			require.NoError(t, err)
			var wire pipeline.ProcessInfo
			require.NoError(t, wire.Unmarshal(data))
			svc := NewCodecService(fakeCodecTxnClient{op: fakeCodecTxnOperator{}}, nil, nil, nil, nil, nil, nil, nil).(*codecService)
			decoded, err := svc.Decode(defines.AttachAccountId(context.Background(), wire.AccountId), wire)
			require.NoError(t, err)
			t.Cleanup(decoded.Free)
			require.Equal(t, proc.IsDMLStatement(), decoded.IsDMLStatement())
			require.Equal(t, proc.IsSelectStatement(), decoded.IsSelectStatement())
			require.Equal(t, tc.ignore, decoded.GetStmtProfile().GetStatementIgnore())
			forwarded, err := decoded.BuildProcessInfo("forward context")
			require.NoError(t, err)
			require.Equal(t, info.StatementRuntimeDml, forwarded.StatementRuntimeDml)
			// An old sender's missing DML bit remains unknown, never !SELECT.
			wire.StatementRuntimeDml = false
			legacy, err := svc.Decode(defines.AttachAccountId(context.Background(), wire.AccountId), wire)
			require.NoError(t, err)
			t.Cleanup(legacy.Free)
			require.False(t, legacy.IsDMLStatement())
			wire.StatementRuntimeDml = true
			wire.StatementRuntimeSelect = true
			_, err = svc.Decode(defines.AttachAccountId(context.Background(), wire.AccountId), wire)
			require.ErrorContains(t, err, "conflicting SELECT and DML")
		})
	}
}

func TestCodecServiceRoundTripsPrepareParamsStringSource(t *testing.T) {
	svc := NewCodecService(
		fakeCodecTxnClient{op: fakeCodecTxnOperator{}},
		nil, nil, nil, nil, nil, nil, nil,
	).(*codecService)
	for _, source := range []types.StringSource{
		types.StringSourceExpression,
		types.StringSourceLiteral,
		types.StringSourceUserVariable,
		types.StringSourceSQLPrepare,
		types.StringSourceCOMStmt,
	} {
		t.Run(fmt.Sprintf("source_%d", source), func(t *testing.T) {
			proc, _ := newCodecTestProcess(t)
			defer proc.Free()
			require.Equal(t, int64(2), int64(proc.GetPrepareParams().Length()))
			proc.SetPrepareParamsStringSource(source)
			require.Equal(t, source, proc.GetPrepareParamsStringSource())

			payload, err := svc.Encode(proc, "select ?, ?")
			require.NoError(t, err)
			info := pipeline.ProcessInfo{}
			require.NoError(t, info.Unmarshal(payload))
			if source == types.StringSourceExpression {
				require.Nil(t, info.PrepareParams.StringSources,
					"the default source must not change the payload")
			} else {
				require.Equal(t, []uint32{uint32(source), uint32(source)}, info.PrepareParams.StringSources,
					"the wire keeps one equal entry per parameter")
			}

			decoded, err := svc.Decode(context.Background(), info)
			require.NoError(t, err)
			defer decoded.Free()
			require.Equal(t, source, decoded.GetPrepareParamsStringSource())
			require.Equal(t, 2, decoded.GetPrepareParams().Length())
		})
	}
}

func TestBuildProcessInfoOmitsStringSourceForOldProtocol(t *testing.T) {
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
	proc.SetPrepareParamsStringSource(types.StringSourceSQLPrepare)
	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion36)
	info, err := proc.BuildProcessInfo("select ?")
	require.NoError(t, err)
	require.Nil(t, info.PrepareParams.StringSources)
}

func TestCodecServiceMergesPerParamStringSourcesFromOldPeer(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	defer proc.Free()
	info, err := proc.BuildProcessInfo("select ?, ?")
	require.NoError(t, err)
	require.Nil(t, info.PrepareParams.StringSources)
	require.Equal(t, int64(2), info.PrepareParams.Length)

	svc := NewCodecService(
		fakeCodecTxnClient{op: fakeCodecTxnOperator{}},
		nil, nil, nil, nil, nil, nil, nil,
	).(*codecService)
	for _, test := range []struct {
		name    string
		sources []uint32
		want    types.StringSource
	}{
		{
			name:    "equal entries",
			sources: []uint32{uint32(types.StringSourceCOMStmt), uint32(types.StringSourceCOMStmt)},
			want:    types.StringSourceCOMStmt,
		},
		{
			name:    "differing entries merge to expression",
			sources: []uint32{uint32(types.StringSourceCOMStmt), uint32(types.StringSourceSQLPrepare)},
			want:    types.StringSourceExpression,
		},
		{
			name:    "expression mixed with a source",
			sources: []uint32{uint32(types.StringSourceExpression), uint32(types.StringSourceLiteral)},
			want:    types.StringSourceExpression,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			old := info
			old.PrepareParams.StringSources = test.sources
			payload, err := old.Marshal()
			require.NoError(t, err)
			var wire pipeline.ProcessInfo
			require.NoError(t, wire.Unmarshal(payload))

			decoded, err := svc.Decode(context.Background(), wire)
			require.NoError(t, err)
			defer decoded.Free()
			require.Equal(t, test.want, decoded.GetPrepareParamsStringSource())
			want := test.want
			merged := types.StringSource(test.sources[0])
			for _, raw := range test.sources[1:] {
				merged, err = types.MergeStringSources(merged, types.StringSource(raw))
				require.NoError(t, err)
			}
			require.Equal(t, want, merged, "decode must agree with types.MergeStringSources")
			// Other parameter metadata is restored independently of the source.
			params := decoded.GetPrepareParams()
			require.Equal(t, 2, params.Length())
			require.Equal(t, proc.GetPrepareParams().GetNulls().Contains(1), params.GetNulls().Contains(1))
			require.Equal(t, proc.GetPrepareParamIsBin(0), decoded.GetPrepareParamIsBin(0))
			require.Equal(t, proc.GetPrepareParamIsBinaryString(1), decoded.GetPrepareParamIsBinaryString(1))
		})
	}
}

func TestPrepareParamsStringSourceLifecycle(t *testing.T) {
	proc, _ := newCodecTestProcess(t)
	defer proc.Free()
	mp := proc.Mp()

	newParams := func() *vector.Vector {
		params := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(params, []byte("x"), false, mp))
		return params
	}

	// Every TEXT transport install resets the source to Expression.
	resets := []struct {
		name    string
		install func(*vector.Vector)
	}{
		{"SetPrepareParams", func(v *vector.Vector) { proc.SetPrepareParams(v) }},
		{"SetPrepareParamsWithIsBin", func(v *vector.Vector) { proc.SetPrepareParamsWithIsBin(v, []bool{false}) }},
		{"SetPrepareParamsWithMetadata", func(v *vector.Vector) {
			proc.SetPrepareParamsWithMetadata(v, []bool{false}, []bool{false})
		}},
		{"SetOwnedPrepareParamsWithIsBin", func(v *vector.Vector) {
			proc.SetOwnedPrepareParamsWithIsBin(v, []bool{false})
		}},
		{"SetOwnedPrepareParamsWithMetadata", func(v *vector.Vector) {
			proc.SetOwnedPrepareParamsWithMetadata(v, []bool{false}, []bool{false})
		}},
	}
	var borrowed []*vector.Vector
	for _, reset := range resets {
		proc.SetPrepareParamsStringSource(types.StringSourceCOMStmt)
		params := newParams()
		reset.install(params)
		require.Equal(t, types.StringSourceExpression, proc.GetPrepareParamsStringSource(), reset.name)
		if !strings.HasPrefix(reset.name, "SetOwned") {
			borrowed = append(borrowed, params)
		}
	}
	defer func() {
		for _, v := range borrowed {
			v.Free(mp)
		}
	}()

	// Detach / Borrow / Restore carry the source.
	proc.SetPrepareParamsStringSource(types.StringSourceSQLPrepare)
	state := proc.DetachPrepareParams()
	require.Equal(t, types.StringSourceSQLPrepare, state.source)

	child, _ := newCodecTestProcess(t)
	defer child.Free()
	child.BorrowPrepareParams(state)
	require.Equal(t, types.StringSourceSQLPrepare, child.GetPrepareParamsStringSource())
	child.SetPrepareParams(nil)
	require.Equal(t, types.StringSourceExpression, child.GetPrepareParamsStringSource())

	proc.RestorePrepareParams(state)
	require.Equal(t, types.StringSourceSQLPrepare, proc.GetPrepareParamsStringSource())
}
