package process

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
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
		procInfo.Snapshot = string(snapshot)
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

		procInfo.SessionInfo = &pipeline.SessionInfo{
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
		procInfo.SessionLogger = &pipeline.SessionLoggerInfo{
			SessId:   proc.SessionInfo.SessionId[:],
			StmtId:   stmtId[:],
			TxnId:    txnId[:],
			LogLevel: zapLogLevel2EnumLogLevel(proc.SessionInfo.LogLevel),
		}
	}
	return procInfo, nil
}

type ProcessCodec struct {
	txnClient   client.TxnClient
	fileService fileservice.FileService
	lockService lockservice.LockService
	queryClient qclient.QueryClient
	hakeeper    logservice.CNHAKeeperClient
	udfService  udf.Service
}

func (c *ProcessCodec) Encode(
	proc *Process,
	sql string,
) ([]byte, error) {
	procInfo, err := proc.BuildProcessInfo(sql)
	if err != nil {
		return nil, err
	}
	return procInfo.Marshal()
}

func NewProcessFromPipelineInfo(
	ctx context.Context,
	mp *mpool.MPool,
	data []byte,
) (*Process, error) {
	value := pipeline.ProcessInfo{}
	if err := value.Unmarshal(data); err != nil {
		return nil, err
	}

	proc := New(
		ctx,
		mp,
		pHelper.txnClient,
		pHelper.txnOperator,
		cnInfo.fileService,
		cnInfo.lockService,
		cnInfo.queryClient,
		cnInfo.hakeeper,
		cnInfo.udfService,
		cnInfo.aicm)
	proc.UnixTime = pHelper.unixTime
	proc.Id = pHelper.id
	proc.Lim = pHelper.lim
	proc.SessionInfo = pHelper.sessionInfo
	proc.SessionInfo.StorageEngine = cnInfo.storeEngine
}

// convert process.Limitation to pipeline.ProcessLimitation
func convertToPipelineLimitation(lim Limitation) *pipeline.ProcessLimitation {
	return &pipeline.ProcessLimitation{
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
