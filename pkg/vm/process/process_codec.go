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
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/version"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const maxStatementHashResolverErrorBytes = 4 << 10

func MockProcessInfoWithPro(
	sql string,
	pro any,
) (pipeline.ProcessInfo, error) {
	process := pro.(*Process)
	process.Base.SessionInfo.TimeZone = time.UTC
	return process.BuildProcessInfo(sql)
}

func (proc *Process) BuildProcessInfo(
	sql string,
) (pipeline.ProcessInfo, error) {
	return proc.buildProcessInfo(sql, false)
}

// BuildProcessInfoWithStatementHash captures the SQL-mode snapshot, or a
// serialized resolver error, for a remote scope that contains
// MO_STATEMENT_HASH. Resolver errors are deferred until an active row evaluates
// the function on the receiving CN.
func (proc *Process) BuildProcessInfoWithStatementHash(
	sql string,
) (pipeline.ProcessInfo, error) {
	return proc.buildProcessInfo(sql, true)
}

// StatementHashBuildCommitIDForRemote returns the immutable coordinator build
// identity to carry and probe when dispatching a remote MO_STATEMENT_HASH
// scope. A received process may forward only when its own full build matches
// the identity it received; it must never substitute its local build ID.
func (proc *Process) StatementHashBuildCommitIDForRemote() (string, error) {
	if proc == nil || proc.Base == nil {
		return "", moerr.NewNotSupportedNoCtx(
			"MO_STATEMENT_HASH remote execution requires a process build identity",
		)
	}
	if proc.Base.SessionInfo.statementHashProcessInfoReceived {
		if err := proc.ValidateStatementHashBuildCommitID(); err != nil {
			return "", err
		}
		return proc.Base.SessionInfo.StatementHashExpectedBuildCommitID, nil
	}
	if !version.IsFullBuildCommitID(version.BuildCommitID) {
		return "", moerr.NewNotSupportedNoCtx(
			"MO_STATEMENT_HASH remote execution requires a full 40-character local build commit ID from a clean source tree",
		)
	}
	return version.BuildCommitID, nil
}

// ValidateStatementHashBuildCommitID checks the build fence when a hash
// expression is actually evaluated. Local-only execution remains usable in a
// build without an identity; received remote work must carry a valid identity.
func (proc *Process) ValidateStatementHashBuildCommitID() error {
	if proc == nil || proc.Base == nil {
		return nil
	}
	sessionInfo := proc.Base.SessionInfo
	expected := sessionInfo.StatementHashExpectedBuildCommitID
	if expected == "" {
		if sessionInfo.statementHashProcessInfoReceived {
			return moerr.NewNotSupportedNoCtx(
				"MO_STATEMENT_HASH received process info is missing a valid full 40-character coordinator build commit ID",
			)
		}
		return nil
	}
	if !version.IsFullBuildCommitID(expected) {
		return moerr.NewNotSupportedNoCtx(
			"MO_STATEMENT_HASH received process info has an invalid coordinator build commit ID",
		)
	}
	if !version.IsFullBuildCommitID(version.BuildCommitID) || expected != version.BuildCommitID {
		return moerr.NewNotSupportedNoCtx(
			"MO_STATEMENT_HASH worker build does not match the build selected by its coordinator",
		)
	}
	return nil
}

func (proc *Process) buildProcessInfo(
	sql string,
	captureStatementHash bool,
) (pipeline.ProcessInfo, error) {
	procInfo := pipeline.ProcessInfo{}
	{
		procInfo.Id = proc.QueryId()
		procInfo.Sql = sql
		procInfo.Lim = convertToPipelineLimitation(proc.GetLim())
		procInfo.UnixTime = proc.Base.UnixTime
		accountId, err := defines.GetAccountId(proc.Ctx)
		if err != nil {
			return procInfo, err
		}
		procInfo.AccountId = accountId
		// Carry ROW_COUNT() state so it is correct when an expression that reads
		// it (e.g. row_count() in a projection) is pushed down to a remote CN.
		procInfo.AffectedRows = proc.GetAffectedRows()
		// Assignment casts can run in a remote scan scope. Carry INSERT IGNORE
		// semantics with the process so those casts take the same adjustment
		// path as they do on the coordinating CN.
		procInfo.StatementRuntimeIgnore = proc.GetStmtProfile().GetStatementIgnore()
		if planSnapshotTS, ok := proc.GetPlanSnapshotTS(); ok {
			procInfo.PlanSnapshotTs = &planSnapshotTS
		}
		procInfo.PlanGenerationReused = proc.PlanGenerationReused()
		stringShuffleHashAlgorithm, err := DecodeStringShuffleHashAlgorithm(
			uint32(proc.StringShuffleHashAlgorithm()),
		)
		if err != nil {
			return procInfo, err
		}
		procInfo.StringShuffleHashAlgorithm = uint32(stringShuffleHashAlgorithm)
		snapshot, err := proc.GetTxnOperator().Snapshot()
		if err != nil {
			return procInfo, err
		}
		procInfo.Snapshot = snapshot

		vec := proc.GetPrepareParams()
		if vec != nil {
			var runtimeStringDomains []uint32
			if vec.HasBinaryStringMetadata() {
				runtimeStringDomains = make([]uint32, vec.Length())
				for i := range runtimeStringDomains {
					runtimeStringDomains[i] = uint32(vec.GetRuntimeStringDomainAt(i))
				}
			}
			runtimeStringDomains, err = RuntimeStringDomainPrepareParamMetadataForRemote(
				proc.GetService(), vec.Length(), runtimeStringDomains)
			if err != nil {
				return procInfo, err
			}
			var stringSources []uint32
			if vec.HasStringSourceMetadata() {
				stringSources = make([]uint32, vec.Length())
				for i := range stringSources {
					stringSources[i] = uint32(vec.GetStringSourceAt(i))
				}
			}
			stringSources, err = StringSourcePrepareParamMetadataForRemote(
				proc.GetService(), vec.Length(), stringSources)
			if err != nil {
				return procInfo, err
			}
			binaryStringMetadata, err := BinaryStringPrepareParamMetadataForRemote(
				proc.GetService(), vec.Length(), proc.Base.prepareParamsBinaryString)
			if err != nil {
				return procInfo, err
			}
			procInfo.PrepareParams.Length = int64(vec.Length())
			procInfo.PrepareParams.Data = make([]byte, 0, len(vec.GetData()))
			procInfo.PrepareParams.Data = append(procInfo.PrepareParams.Data, vec.GetData()...)
			procInfo.PrepareParams.Area = make([]byte, 0, len(vec.GetArea()))
			procInfo.PrepareParams.Area = append(procInfo.PrepareParams.Area, vec.GetArea()...)
			procInfo.PrepareParams.Nulls = make([]bool, procInfo.PrepareParams.Length)
			for i := range procInfo.PrepareParams.Nulls {
				procInfo.PrepareParams.Nulls[i] = vec.GetNulls().Contains(uint64(i))
			}
			metadata, err := PrepareParamMetadataForRemote(
				proc.GetService(),
				vec.Length(),
				proc.Base.prepareParamsIsBin,
			)
			if err != nil {
				return procInfo, err
			}
			procInfo.PrepareParams.IsBin = metadata
			if binaryStringMetadata != nil {
				procInfo.PrepareParams.IsBinaryString = binaryStringMetadata
			}
			procInfo.PrepareParams.StringSources = stringSources
			procInfo.PrepareParams.RuntimeStringDomains = runtimeStringDomains
		}
	}
	{ // session info
		loc := proc.Base.SessionInfo.TimeZone
		if loc == nil {
			loc = time.Local
		}
		timeBytes, err := time.Time{}.In(loc).MarshalBinary()
		if err != nil {
			return procInfo, err
		}
		var (
			sqlMode                            string
			statementHashResolverErr           []byte
			statementHashResolverErrDetail     string
			statementHashExpectedBuildCommitID string
		)
		if captureStatementHash {
			statementHashExpectedBuildCommitID, err = proc.StatementHashBuildCommitIDForRemote()
			if err != nil {
				return procInfo, err
			}
			// Preserve both the originating SQL mode and any resolver failure.
			// The error is raised only if the remote function evaluates an active
			// row; process-info serialization itself must not make a masked or
			// empty remote branch fail.
			sqlMode, err = ResolveSQLMode(proc)
			if err != nil {
				statementHashResolverErr, statementHashResolverErrDetail, err =
					encodeStatementHashSQLModeError(err)
				if err != nil {
					return procInfo, err
				}
			}
		} else {
			// Preserve received provenance through ordinary intermediate hops
			// without applying the hash-only build gate to unrelated scopes.
			if proc.Base.SessionInfo.statementHashProcessInfoReceived {
				statementHashExpectedBuildCommitID = proc.Base.SessionInfo.StatementHashExpectedBuildCommitID
				// An ordinary scope can be an intermediate hop for a later
				// MO_STATEMENT_HASH scope. Preserve the deferred resolver failure
				// just like the build identity; otherwise the next hash hop could
				// silently resolve sql_mode locally and produce a placement-
				// dependent hash.
				statementHashResolverErr = append([]byte(nil), proc.Base.SessionInfo.StatementHashSQLModeError...)
				statementHashResolverErrDetail = proc.Base.SessionInfo.StatementHashSQLModeErrorDetail
				// A received snapshot is authoritative. Re-running the receiving
				// CN's resolver here would replace the coordinator's SQL mode and
				// make a later hash depend on the intermediate placement.
				sqlMode = proc.Base.SessionInfo.SqlMode
			} else {
				sqlMode = resolveSqlMode(proc)
			}
		}

		procInfo.SessionInfo = pipeline.SessionInfo{
			User:                               proc.Base.SessionInfo.GetUser(),
			Host:                               proc.Base.SessionInfo.GetHost(),
			Role:                               proc.Base.SessionInfo.GetRole(),
			ConnectionId:                       proc.Base.SessionInfo.GetConnectionID(),
			Database:                           proc.Base.SessionInfo.GetDatabase(),
			Version:                            proc.Base.SessionInfo.GetVersion(),
			TimeZone:                           timeBytes,
			TimeZoneName:                       TimeZoneLocationName(loc),
			QueryId:                            proc.Base.SessionInfo.QueryId,
			LockWaitTimeout:                    resolveLockWaitTimeoutSeconds(proc),
			LockWaitTimeoutSet:                 proc.Base.SessionInfo.LockWaitTimeoutSet,
			MatrixoneNativeMode:                proc.Base.SessionInfo.MatrixOneNativeMode,
			SqlMode:                            sqlMode,
			StatementHashSqlModeError:          statementHashResolverErr,
			StatementHashSqlModeErrorDetail:    statementHashResolverErrDetail,
			StatementHashExpectedBuildCommitId: statementHashExpectedBuildCommitID,
			AutoIncrementIncrement:             proc.Base.SessionInfo.AutoIncrementIncrement,
			AutoIncrementOffset:                proc.Base.SessionInfo.AutoIncrementOffset,
		}
		var nullifyZeroTemporal bool
		if captureStatementHash {
			// The hash path already resolved (or restored) the SQL-mode
			// snapshot above. Derive the temporal-cast flag from that same
			// snapshot instead of consulting a receiving-CN resolver a second
			// time. A false snapshot is meaningful too: it must not be
			// replaced by the receiving CN's current mode.
			nullifyZeroTemporal = proc.Base.SessionInfo.ExplicitZeroTemporalCastReturnsNull ||
				IsStrictNoZeroDateMode(sqlMode)
		} else if proc.Base.SessionInfo.statementHashProcessInfoReceived {
			// Keep the flag derived from the received snapshot. The receiving CN
			// must not recompute it from its own SQL-mode resolver.
			nullifyZeroTemporal = proc.Base.SessionInfo.ExplicitZeroTemporalCastReturnsNull
		} else {
			nullifyZeroTemporal, err = ResolveExplicitZeroTemporalCastReturnsNull(proc)
			if err != nil {
				return procInfo, err
			}
		}
		procInfo.SessionInfo.ExplicitZeroTemporalCastReturnsNull = nullifyZeroTemporal
	}
	{ // log info
		stmtId := proc.GetStmtProfile().GetStmtId()
		txnId := proc.GetStmtProfile().GetTxnId()
		procInfo.SessionLogger = pipeline.SessionLoggerInfo{
			SessId:   proc.Base.SessionInfo.SessionId[:],
			StmtId:   stmtId[:],
			TxnId:    txnId[:],
			LogLevel: zapLogLevel2EnumLogLevel(proc.Base.SessionInfo.LogLevel),
		}
	}
	return procInfo, nil
}

type ProcessCodecService interface {
	Encode(
		proc *Process,
		sql string,
	) ([]byte, error)

	Decode(
		ctx context.Context,
		data pipeline.ProcessInfo,
	) (*Process, error)
}

func NewCodecService(
	txnClient client.TxnClient,
	fileService fileservice.FileService,
	lockService lockservice.LockService,
	partitionService partitionservice.PartitionService,
	queryClient qclient.QueryClient,
	hakeeper logservice.CNHAKeeperClient,
	udfService udf.Service,
	engine engine.Engine,
) ProcessCodecService {
	mp, err := mpool.NewMPool("codec", 1<<40, mpool.NoFixed)
	if err != nil {
		panic(err)
	}
	return &codecService{
		txnClient:        txnClient,
		fileService:      fileService,
		lockService:      lockService,
		partitionService: partitionService,
		queryClient:      queryClient,
		hakeeper:         hakeeper,
		udfService:       udfService,
		engine:           engine,
		mp:               mp,
	}
}

type codecService struct {
	txnClient        client.TxnClient
	fileService      fileservice.FileService
	lockService      lockservice.LockService
	partitionService partitionservice.PartitionService
	queryClient      qclient.QueryClient
	hakeeper         logservice.CNHAKeeperClient
	udfService       udf.Service
	mp               *mpool.MPool
	engine           engine.Engine
}

func GetCodecService(service string) ProcessCodecService {
	v, ok := runtime.ServiceRuntime(service).GetGlobalVariables(runtime.ProcessCodecService)
	if !ok {
		panic("codec service not found")
	}
	return v.(ProcessCodecService)
}

func (c *codecService) Encode(
	proc *Process,
	sql string,
) ([]byte, error) {
	procInfo, err := proc.BuildProcessInfo(sql)
	if err != nil {
		return nil, err
	}
	return procInfo.Marshal()
}

func (c *codecService) Decode(
	ctx context.Context,
	value pipeline.ProcessInfo,
) (*Process, error) {
	stringShuffleHashAlgorithm, err := DecodeStringShuffleHashAlgorithm(
		value.StringShuffleHashAlgorithm,
	)
	if err != nil {
		return nil, err
	}
	service := ""
	if c.lockService != nil {
		service = c.lockService.GetConfig().ServiceID
	}
	prepareParamMetadata, err := PrepareParamMetadataForRemote(
		service,
		int(value.PrepareParams.Length),
		value.PrepareParams.IsBin,
	)
	if err != nil {
		return nil, err
	}
	binaryStringMetadata, err := BinaryStringPrepareParamMetadataForRemote(
		service,
		int(value.PrepareParams.Length),
		value.PrepareParams.IsBinaryString,
	)
	if err != nil {
		return nil, err
	}
	stringSources, err := StringSourcePrepareParamMetadataForRemote(
		service,
		int(value.PrepareParams.Length),
		value.PrepareParams.StringSources,
	)
	if err != nil {
		return nil, err
	}
	runtimeStringDomains, err := RuntimeStringDomainPrepareParamMetadataForRemote(
		service,
		int(value.PrepareParams.Length),
		value.PrepareParams.RuntimeStringDomains,
	)
	if err != nil {
		return nil, err
	}
	txnOp, err := c.txnClient.NewWithSnapshot(ctx, value.Snapshot)
	if err != nil {
		return nil, err
	}

	sessionInfo, err := ConvertToProcessSessionInfo(value.SessionInfo)
	if err != nil {
		return nil, err
	}

	proc := NewTopProcess(
		ctx,
		c.mp,
		c.txnClient,
		txnOp,
		c.fileService,
		c.lockService,
		c.queryClient,
		c.hakeeper,
		c.udfService,
		nil,
		nil,
	)
	proc.Base.LockService = c.lockService
	proc.Base.PartitionService = c.partitionService
	proc.Base.UnixTime = value.UnixTime
	proc.Base.Id = value.Id
	proc.Base.Lim = ConvertToProcessLimitation(value.Lim)
	proc.Base.SessionInfo = sessionInfo
	proc.Base.SessionInfo.StorageEngine = c.engine
	proc.SetStringShuffleHashAlgorithm(stringShuffleHashAlgorithm)
	if value.PlanSnapshotTs != nil {
		proc.SetPlanSnapshotTS(*value.PlanSnapshotTs)
		proc.SetPlanGenerationReused(value.PlanGenerationReused)
	}
	proc.SetAffectedRows(value.AffectedRows)
	stmtProfile := NewStmtProfile(uuid.Nil, uuid.Nil)
	stmtProfile.SetStatementRuntimeProfile("", "", value.StatementRuntimeIgnore)
	proc.Base.StmtProfile = stmtProfile
	if value.PrepareParams.Length > 0 {
		prepareParams, err := vector.NewVecWithDataCopy(
			types.T_text.ToType(),
			int(value.PrepareParams.Length),
			value.PrepareParams.Data,
			value.PrepareParams.Area,
			proc.Mp(),
		)
		if err != nil {
			proc.Free()
			return nil, err
		}
		for i := range value.PrepareParams.Nulls {
			if value.PrepareParams.Nulls[i] {
				prepareParams.GetNulls().Add(uint64(i))
			}
		}
		if len(stringSources) > 0 {
			sources := make([]types.StringSource, len(stringSources))
			for i, source := range stringSources {
				sources[i] = types.StringSource(source)
			}
			if err = prepareParams.SetStringSourcesWithMP(sources, proc.Mp()); err != nil {
				prepareParams.Free(proc.Mp())
				proc.Free()
				return nil, err
			}
		}
		proc.SetOwnedPrepareParamsWithMetadata(
			prepareParams,
			prepareParamMetadata,
			binaryStringMetadata,
		)
		if len(runtimeStringDomains) > 0 {
			domains := make([]types.RuntimeStringDomain, len(runtimeStringDomains))
			for i, domain := range runtimeStringDomains {
				domains[i] = types.RuntimeStringDomain(domain)
			}
			if err = prepareParams.SetRuntimeStringDomainsWithMP(domains, proc.Mp()); err != nil {
				prepareParams.Free(proc.Mp())
				proc.Free()
				return nil, err
			}
		}
	}
	return proc, nil
}

// convert process.Limitation to pipeline.ProcessLimitation
func convertToPipelineLimitation(lim Limitation) pipeline.ProcessLimitation {
	return pipeline.ProcessLimitation{
		Size:          lim.Size,
		BatchRows:     lim.BatchRows,
		BatchSize:     lim.BatchSize,
		PartitionRows: lim.PartitionRows,
		ReaderSize:    lim.ReaderSize,
		SpillSize:     lim.SpillSize,
	}
}

var zapLogLevel2EnumLogLevelMap = map[zapcore.Level]pipeline.SessionLoggerInfo_LogLevel{
	zap.DebugLevel:  pipeline.SessionLoggerInfo_Debug,
	zap.InfoLevel:   pipeline.SessionLoggerInfo_Info,
	zap.WarnLevel:   pipeline.SessionLoggerInfo_Warn,
	zap.ErrorLevel:  pipeline.SessionLoggerInfo_Error,
	zap.DPanicLevel: pipeline.SessionLoggerInfo_Panic,
	zap.PanicLevel:  pipeline.SessionLoggerInfo_Panic,
	zap.FatalLevel:  pipeline.SessionLoggerInfo_Fatal,
}

func zapLogLevel2EnumLogLevel(level zapcore.Level) pipeline.SessionLoggerInfo_LogLevel {
	if lvl, exist := zapLogLevel2EnumLogLevelMap[level]; exist {
		return lvl
	}
	return pipeline.SessionLoggerInfo_Info
}

var enumLogLevel2ZapLogLevelMap = map[pipeline.SessionLoggerInfo_LogLevel]zapcore.Level{
	pipeline.SessionLoggerInfo_Debug: zap.DebugLevel,
	pipeline.SessionLoggerInfo_Info:  zap.InfoLevel,
	pipeline.SessionLoggerInfo_Warn:  zap.WarnLevel,
	pipeline.SessionLoggerInfo_Error: zap.ErrorLevel,
	pipeline.SessionLoggerInfo_Panic: zap.PanicLevel,
	pipeline.SessionLoggerInfo_Fatal: zap.FatalLevel,
}

func EnumLogLevel2ZapLogLevel(level pipeline.SessionLoggerInfo_LogLevel) zapcore.Level {
	if lvl, exist := enumLogLevel2ZapLogLevelMap[level]; exist {
		return lvl
	}
	return zap.InfoLevel
}

// convert pipeline.ProcessLimitation to process.Limitation
func ConvertToProcessLimitation(
	lim pipeline.ProcessLimitation,
) Limitation {
	return Limitation{
		Size:          lim.Size,
		BatchRows:     lim.BatchRows,
		BatchSize:     lim.BatchSize,
		PartitionRows: lim.PartitionRows,
		ReaderSize:    lim.ReaderSize,
		SpillSize:     lim.SpillSize,
	}
}

// convert pipeline.SessionInfo to process.SessionInfo
func ConvertToProcessSessionInfo(
	sei pipeline.SessionInfo,
) (SessionInfo, error) {
	sessionInfo := SessionInfo{
		User:                                sei.User,
		Host:                                sei.Host,
		Role:                                sei.Role,
		ConnectionID:                        sei.ConnectionId,
		Database:                            sei.Database,
		Version:                             sei.Version,
		Account:                             sei.Account,
		QueryId:                             sei.QueryId,
		LockWaitTimeout:                     sei.LockWaitTimeout,
		LockWaitTimeoutSet:                  sei.LockWaitTimeoutSet,
		MatrixOneNativeMode:                 sei.MatrixoneNativeMode,
		ExplicitZeroTemporalCastReturnsNull: sei.ExplicitZeroTemporalCastReturnsNull,
		SqlMode:                             sei.SqlMode,
		StatementHashSQLModeError:           append([]byte(nil), sei.StatementHashSqlModeError...),
		StatementHashSQLModeErrorDetail:     sei.StatementHashSqlModeErrorDetail,
		StatementHashExpectedBuildCommitID:  sei.StatementHashExpectedBuildCommitId,
		statementHashProcessInfoReceived:    true,
		AutoIncrementIncrement:              sei.AutoIncrementIncrement,
		AutoIncrementOffset:                 sei.AutoIncrementOffset,
	}
	if sei.TimeZoneName != "" {
		if sei.TimeZoneName == "Local" {
			return sessionInfo, moerr.NewInvalidInputNoCtx("remote time zone must not refer to worker Local")
		}
		location, err := time.LoadLocation(sei.TimeZoneName)
		if err != nil {
			return sessionInfo, moerr.NewInvalidInputNoCtxf("cannot load remote time zone %q: %v", sei.TimeZoneName, err)
		}
		sessionInfo.TimeZone = location
		return sessionInfo, nil
	}
	t := time.Time{}
	err := t.UnmarshalBinary(sei.TimeZone)
	if err != nil {
		return sessionInfo, nil
	}
	sessionInfo.TimeZone = t.Location()
	return sessionInfo, nil
}

// ResolveSQLMode returns the SQL-mode snapshot used by MO_STATEMENT_HASH.
// Captured remote resolver errors are reconstructed for function evaluation;
// BuildProcessInfoWithStatementHash serializes them so inactive rows remain
// lazy. Callers that preserve historical best-effort behavior may use
// resolveSqlMode below.
func ResolveSQLMode(proc *Process) (string, error) {
	if proc == nil || proc.Base == nil {
		return "", nil
	}
	if len(proc.Base.SessionInfo.StatementHashSQLModeError) > 0 {
		var captured moerr.Error
		if err := captured.UnmarshalBinary(proc.Base.SessionInfo.StatementHashSQLModeError); err != nil {
			return proc.Base.SessionInfo.SqlMode,
				moerr.NewInternalErrorNoCtxf("invalid captured sql_mode resolver error for MO_STATEMENT_HASH: %v", err)
		}
		captured.SetDetail(proc.Base.SessionInfo.StatementHashSQLModeErrorDetail)
		return proc.Base.SessionInfo.SqlMode, &captured
	}
	// A remote/background process carries the coordinator's session snapshot.
	// It must survive every encode/decode/forward hop unchanged; a resolver on
	// the receiving CN describes that CN's defaults, not the remote statement.
	if !proc.Base.IsFrontend && proc.Base.SessionInfo.SqlMode != "" {
		return proc.Base.SessionInfo.SqlMode, nil
	}
	if f := proc.GetResolveVariableFunc(); f != nil {
		v, err := f("sql_mode", true, false)
		if err != nil {
			return proc.Base.SessionInfo.SqlMode, err
		}
		if s, ok := v.(string); ok {
			if s == "" {
				return EmptySqlModeSentinel, nil // explicitly non-strict
			}
			return s, nil
		}
		if v != nil {
			// A non-string resolver result is a malformed session variable,
			// not an absent value.  Preserve the captured snapshot for callers
			// that need the last known mode while making the strict hash path
			// fail before it can be dispatched under a different mode.
			return proc.Base.SessionInfo.SqlMode,
				moerr.NewInternalErrorNoCtxf("unexpected sql_mode type %T", v)
		}
	}
	// Resolver is nil on a remote CN (no session). Fall back to the sql_mode
	// captured from the upstream CN so it survives a second forward
	// (encode -> decode -> encode); otherwise the next hop defaults to strict.
	return proc.Base.SessionInfo.SqlMode, nil
}

func encodeStatementHashSQLModeError(err error) ([]byte, string, error) {
	var captured *moerr.Error
	var detail string
	if value, ok := err.(*moerr.Error); ok {
		captured = value
		detail = value.Detail()
		messageOnly := *value
		messageOnly.SetDetail("")
		if len(messageOnly.Error())+len(detail) > maxStatementHashResolverErrorBytes {
			captured = statementHashResolverErrorTooLarge()
			detail = ""
		}
	} else {
		message := "MO_STATEMENT_HASH sql_mode resolver failed"
		if err != nil {
			detail = err.Error()
			if len(detail)+len(message) > maxStatementHashResolverErrorBytes {
				captured = statementHashResolverErrorTooLarge()
				detail = ""
			} else {
				captured = moerr.NewInternalErrorNoCtx(message)
			}
		} else {
			captured = moerr.NewInternalErrorNoCtx(message)
		}
	}
	encoded, marshalErr := captured.MarshalBinary()
	if marshalErr != nil {
		return nil, "", marshalErr
	}
	if len(encoded)+len(detail) > maxStatementHashResolverErrorBytes {
		captured = statementHashResolverErrorTooLarge()
		encoded, marshalErr = captured.MarshalBinary()
		if marshalErr != nil {
			return nil, "", marshalErr
		}
		detail = ""
	}
	return encoded, detail, nil
}

func statementHashResolverErrorTooLarge() *moerr.Error {
	return moerr.NewInternalErrorNoCtxf(
		"MO_STATEMENT_HASH sql_mode resolver error exceeds the %d-byte remote diagnostic limit",
		maxStatementHashResolverErrorBytes,
	)
}

func resolveSqlMode(proc *Process) string {
	// Preserve the legacy best-effort behavior for ordinary remote scopes.
	// MO_STATEMENT_HASH uses ResolveSQLMode directly because it must preserve a
	// captured SQL-mode snapshot and defer captured resolver failures to active
	// evaluation. Ordinary scopes keep this best-effort behavior.
	if proc == nil {
		return ""
	}
	if f := proc.GetResolveVariableFunc(); f != nil {
		if v, err := f("sql_mode", true, false); err == nil {
			if s, ok := v.(string); ok {
				if s == "" {
					if proc.Base != nil && !proc.Base.IsFrontend {
						if snapshot := proc.Base.SessionInfo.SqlMode; snapshot != "" {
							return snapshot
						}
					}
					return EmptySqlModeSentinel
				}
				return s
			}
		}
	}
	if proc.Base == nil {
		return ""
	}
	return proc.Base.SessionInfo.SqlMode
}

func resolveLockWaitTimeoutSeconds(proc *Process) int64 {
	// A positive per-execution timeout must survive remote pipeline encoding
	// without being replaced by the background resolver's compiled default.
	// For an explicit zero, continue to the resolver so clearing an old txn
	// override falls back to the normal default when one is available.
	if proc != nil && proc.GetSessionInfo() != nil &&
		proc.GetSessionInfo().LockWaitTimeoutSet &&
		proc.GetSessionInfo().LockWaitTimeout > 0 {
		return proc.GetSessionInfo().LockWaitTimeout
	}
	if proc == nil || proc.GetResolveVariableFunc() == nil {
		if proc != nil && proc.GetSessionInfo() != nil &&
			proc.GetSessionInfo().LockWaitTimeoutSet {
			// Older pipeline peers ignore LockWaitTimeoutSet. Encode an explicit
			// clear as the shared positive fallback in the legacy timeout field,
			// so they cannot resurrect a stale timeout from the reused txn.
			return defines.DefaultLockWaitTimeoutSeconds
		}
		return procSessionLockWaitTimeout(proc)
	}
	if v, err := proc.GetResolveVariableFunc()("lock_wait_timeout", true, false); err == nil {
		if seconds := lockWaitTimeoutSeconds(v); seconds > 0 {
			return seconds
		}
	}
	return procSessionLockWaitTimeout(proc)
}

func procSessionLockWaitTimeout(proc *Process) int64 {
	if proc == nil || proc.GetSessionInfo() == nil {
		return 0
	}
	return proc.GetSessionInfo().LockWaitTimeout
}

func lockWaitTimeoutSeconds(v any) int64 {
	switch n := v.(type) {
	case int64:
		if n > 0 {
			return n
		}
	case int:
		if n > 0 {
			return int64(n)
		}
	case uint64:
		if n > 0 && n <= math.MaxInt64 {
			return int64(n)
		}
	}
	return 0
}
