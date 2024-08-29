// Copyright 2021 Matrix Origin
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
	"encoding/hex"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/util/trace"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const DefaultBatchSize = 8192

func (wreg *WaitRegister) CleanChannel(m *mpool.MPool) {
	for len(wreg.Ch) > 0 {
		msg := <-wreg.Ch
		if msg != nil && msg.Batch != nil {
			msg.Batch.Clean(m)
		}
	}
}

func (proc *Process) QueryId() string {
	return proc.Base.Id
}

func (proc *Process) SetQueryId(id string) {
	proc.Base.Id = id
}

// XXX MPOOL
// Some times we call an expr eval function without a proc (test only?)
// in that case, all expr eval code get an nil mp which is wrong.
// so far the most cases come from
// plan.ConstantFold -> colexec.EvalExpr, busted.
// hack in a fall back mpool.  This is by design a Zero MP so that there
// will not be real leaks, except we leak counters in globalStats
var xxxProcMp = mpool.MustNewNoFixed("fallback_proc_mp")

func (proc *Process) GetMPool() *mpool.MPool {
	if proc == nil {
		return xxxProcMp
	}
	return proc.Base.mp
}

func (proc *Process) Mp() *mpool.MPool {
	return proc.GetMPool()
}

func (proc *Process) GetService() string {
	if proc == nil {
		return ""
	}
	if ls := proc.GetLockService(); ls != nil {
		return ls.GetConfig().ServiceID
	}
	return ""
}

func (proc *Process) GetLim() Limitation {
	return proc.Base.Lim
}

func (proc *Process) GetQueryClient() qclient.QueryClient {
	return proc.Base.QueryClient
}

func (proc *Process) GetFileService() fileservice.FileService {
	return proc.Base.FileService
}

func (proc *Process) GetUnixTime() int64 {
	return proc.Base.UnixTime
}

func (proc *Process) GetIncrService() incrservice.AutoIncrementService {
	return proc.Base.IncrService
}

func (proc *Process) GetLoadLocalReader() *io.PipeReader {
	return proc.Base.LoadLocalReader
}

func (proc *Process) GetLockService() lockservice.LockService {
	return proc.Base.LockService
}

func (proc *Process) GetWaitPolicy() lock.WaitPolicy {
	return proc.Base.WaitPolicy
}

func (proc *Process) GetHaKeeper() logservice.CNHAKeeperClient {
	return proc.Base.Hakeeper
}

func (proc *Process) GetPrepareParams() *vector.Vector {
	return proc.Base.prepareParams
}

func (proc *Process) SetPrepareParams(prepareParams *vector.Vector) {
	proc.Base.prepareParams = prepareParams
}

func (proc *Process) SetPrepareBatch(bat *batch.Batch) {
	proc.Base.prepareBatch = bat
}

func (proc *Process) GetPrepareBatch() *batch.Batch {
	return proc.Base.prepareBatch
}

func (proc *Process) SetPrepareExprList(exprList any) {
	proc.Base.prepareExprList = exprList
}

func (proc *Process) GetPrepareExprList() any {
	return proc.Base.prepareExprList
}

func (proc *Process) OperatorOutofMemory(size int64) bool {
	return proc.Mp().Cap() < size
}

func (proc *Process) GetAnalyze(idx, parallelIdx int, parallelMajor bool) Analyze {
	if idx >= len(proc.Base.AnalInfos) || idx < 0 {
		return &operatorAnalyzer{analInfo: nil, parallelIdx: parallelIdx, parallelMajor: parallelMajor}
	}
	return &operatorAnalyzer{analInfo: proc.Base.AnalInfos[idx], wait: 0, parallelIdx: parallelIdx, parallelMajor: parallelMajor}
}

func (proc *Process) AllocVectorOfRows(typ types.Type, nele int, nsp *nulls.Nulls) (*vector.Vector, error) {
	vec := proc.GetVector(typ)
	err := vec.PreExtend(nele, proc.Mp())
	if err != nil {
		return nil, err
	}
	vec.SetLength(nele)
	if nsp != nil {
		nulls.Set(vec.GetNulls(), nsp)
	}
	return vec, nil
}

func (proc *Process) CopyValueScanBatch(src *Process) {
	proc.Base.valueScanBatch = src.Base.valueScanBatch
}

func (proc *Process) SetVectorPoolSize(limit int) {
	proc.Base.vp.modifyCapacity(limit, proc.Mp())
}

func (proc *Process) CopyVectorPool(src *Process) {
	proc.Base.vp = src.Base.vp
}

func (proc *Process) NewBatchFromSrc(src *batch.Batch, preAllocSize int) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(src.Vecs))
	bat.SetAttributes(src.Attrs)
	bat.Recursive = src.Recursive
	for i := range bat.Vecs {
		v := proc.GetVector(*src.Vecs[i].GetType())
		if v.Capacity() < preAllocSize {
			err := v.PreExtend(preAllocSize, proc.Mp())
			if err != nil {
				return nil, err
			}
		}
		bat.Vecs[i] = v
	}
	return bat, nil
}

// log do logging.
// just for Info/Error/Warn/Debug/Fatal
func (proc *Process) log(ctx context.Context, level zapcore.Level, msg string, fields ...zap.Field) {
	if proc.Base.SessionInfo.LogLevel.Enabled(level) {
		fields = appendSessionField(fields, proc)
		fields = appendTraceField(fields, ctx)
		proc.Base.logger.Log(msg, log.DefaultLogOptions().WithLevel(level).AddCallerSkip(2), fields...)
	}
}

func (proc *Process) logf(ctx context.Context, level zapcore.Level, msg string, args ...any) {
	if proc.Base.SessionInfo.LogLevel.Enabled(level) {
		fields := make([]zap.Field, 0, 5)
		fields = appendSessionField(fields, proc)
		fields = appendTraceField(fields, ctx)
		proc.Base.logger.Log(fmt.Sprintf(msg, args...), log.DefaultLogOptions().WithLevel(level).AddCallerSkip(2), fields...)
	}
}

func (proc *Process) Info(ctx context.Context, msg string, fields ...zap.Field) {
	proc.log(ctx, zap.InfoLevel, msg, fields...)
}

func (proc *Process) Error(ctx context.Context, msg string, fields ...zap.Field) {
	proc.log(ctx, zap.ErrorLevel, msg, fields...)
}

func (proc *Process) Warn(ctx context.Context, msg string, fields ...zap.Field) {
	proc.log(ctx, zap.WarnLevel, msg, fields...)
}

func (proc *Process) Fatal(ctx context.Context, msg string, fields ...zap.Field) {
	proc.log(ctx, zap.FatalLevel, msg, fields...)
}

func (proc *Process) Debug(ctx context.Context, msg string, fields ...zap.Field) {
	proc.log(ctx, zap.DebugLevel, msg, fields...)
}

func (proc *Process) Infof(ctx context.Context, msg string, args ...any) {
	proc.logf(ctx, zap.InfoLevel, msg, args...)
}

func (proc *Process) Errorf(ctx context.Context, msg string, args ...any) {
	proc.logf(ctx, zap.ErrorLevel, msg, args...)
}

func (proc *Process) Warnf(ctx context.Context, msg string, args ...any) {
	proc.logf(ctx, zap.WarnLevel, msg, args...)
}

func (proc *Process) Fatalf(ctx context.Context, msg string, args ...any) {
	proc.logf(ctx, zap.FatalLevel, msg, args...)
}

func (proc *Process) Debugf(ctx context.Context, msg string, args ...any) {
	proc.logf(ctx, zap.DebugLevel, msg, args...)
}

// appendSessionField append session id, transaction id and statement id to the fields
func appendSessionField(fields []zap.Field, proc *Process) []zap.Field {
	if proc != nil {
		fields = append(fields, logutil.SessionIdField(proc.Base.SessionInfo.SessionId.String()))
		if p := proc.GetStmtProfile(); p != nil {
			fields = append(fields, logutil.StatementIdField(p.stmtId.String()))
			fields = append(fields, logutil.TxnIdField(hex.EncodeToString(p.txnId[:])))
		}
	}
	return fields
}

func appendTraceField(fields []zap.Field, ctx context.Context) []zap.Field {
	if sc := trace.SpanFromContext(ctx).SpanContext(); !sc.IsEmpty() {
		fields = append(fields, trace.ContextField(ctx))
	}
	return fields
}
