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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (proc *Process) BuildProcessInfo(
	sql string,
) (pipeline.ProcessInfo, error) {
	procInfo := pipeline.ProcessInfo{}
	if len(proc.AnalInfos) == 0 {
		proc.Error(proc.Ctx, "empty plan", zap.String("sql", sql))
	}
	{
		procInfo.Id = proc.Id
		procInfo.Sql = sql
		procInfo.Lim = convertToPipelineLimitation(proc.Lim)
		procInfo.UnixTime = proc.UnixTime
		accountId, err := defines.GetAccountId(proc.Ctx)
		if err != nil {
			return procInfo, err
		}
		procInfo.AccountId = accountId
		snapshot, err := proc.TxnOperator.Snapshot()
		if err != nil {
			return procInfo, err
		}
		procInfo.Snapshot = snapshot
		procInfo.AnalysisNodeList = make([]int32, len(proc.AnalInfos))
		for i := range procInfo.AnalysisNodeList {
			procInfo.AnalysisNodeList[i] = proc.AnalInfos[i].NodeId
		}
	}
	{ // session info
		timeBytes, err := time.Time{}.In(proc.SessionInfo.TimeZone).MarshalBinary()
		if err != nil {
			return procInfo, err
		}

		procInfo.SessionInfo = pipeline.SessionInfo{
			User:         proc.SessionInfo.GetUser(),
			Host:         proc.SessionInfo.GetHost(),
			Role:         proc.SessionInfo.GetRole(),
			ConnectionId: proc.SessionInfo.GetConnectionID(),
			Database:     proc.SessionInfo.GetDatabase(),
			Version:      proc.SessionInfo.GetVersion(),
			TimeZone:     timeBytes,
			QueryId:      proc.SessionInfo.QueryId,
		}
	}
	{ // log info
		stmtId := proc.StmtProfile.GetStmtId()
		txnId := proc.StmtProfile.GetTxnId()
		procInfo.SessionLogger = pipeline.SessionLoggerInfo{
			SessId:   proc.SessionInfo.SessionId[:],
			StmtId:   stmtId[:],
			TxnId:    txnId[:],
			LogLevel: zapLogLevel2EnumLogLevel(proc.SessionInfo.LogLevel),
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
	queryClient qclient.QueryClient,
	hakeeper logservice.CNHAKeeperClient,
	udfService udf.Service,
	engine engine.Engine,
) ProcessCodecService {
	mp, err := mpool.NewMPool("codec", 1024*1024*32, mpool.NoFixed)
	if err != nil {
		panic(err)
	}
	return &codecService{
		txnClient:   txnClient,
		fileService: fileService,
		lockService: lockService,
		queryClient: queryClient,
		hakeeper:    hakeeper,
		udfService:  udfService,
		engine:      engine,
		mp:          mp,
	}
}

type codecService struct {
	txnClient   client.TxnClient
	fileService fileservice.FileService
	lockService lockservice.LockService
	queryClient qclient.QueryClient
	hakeeper    logservice.CNHAKeeperClient
	udfService  udf.Service
	mp          *mpool.MPool
	engine      engine.Engine
}

func GetCodecService() ProcessCodecService {
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.ProcessCodecService)
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
	txnOp, err := c.txnClient.NewWithSnapshot(value.Snapshot)
	if err != nil {
		return nil, err
	}

	sessionInfo, err := ConvertToProcessSessionInfo(value.SessionInfo)
	if err != nil {
		return nil, err
	}

	proc := New(
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
	)
	proc.UnixTime = value.UnixTime
	proc.Id = value.Id
	proc.Lim = ConvertToProcessLimitation(value.Lim)
	proc.SessionInfo = sessionInfo
	proc.SessionInfo.StorageEngine = c.engine
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
	}
}

// convert pipeline.SessionInfo to process.SessionInfo
func ConvertToProcessSessionInfo(
	sei pipeline.SessionInfo,
) (SessionInfo, error) {
	sessionInfo := SessionInfo{
		User:         sei.User,
		Host:         sei.Host,
		Role:         sei.Role,
		ConnectionID: sei.ConnectionId,
		Database:     sei.Database,
		Version:      sei.Version,
		Account:      sei.Account,
		QueryId:      sei.QueryId,
	}
	t := time.Time{}
	err := t.UnmarshalBinary(sei.TimeZone)
	if err != nil {
		return sessionInfo, nil
	}
	sessionInfo.TimeZone = t.Location()
	return sessionInfo, nil
}
