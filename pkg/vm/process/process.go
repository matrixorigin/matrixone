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

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/taskservice"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/util/trace"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

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
var xxxProcMp = mpool.MustNew("fallback_proc_mp")

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

func (proc *Process) GetTaskService() taskservice.TaskService {
	if proc == nil {
		return nil
	}
	if proc.Base.TaskService != nil {
		return proc.Base.TaskService
	}
	// best-effort fallback: try to fetch from service runtime if available
	sid := proc.GetService()
	if sid != "" {
		if v, ok := runtime.ServiceRuntime(sid).GetGlobalVariables("task-service"); ok {
			if ts, ok2 := v.(taskservice.TaskService); ok2 {
				proc.Base.TaskService = ts
				return ts
			}
		}
	}
	return nil
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

func (proc *Process) GetPartitionService() partitionservice.PartitionService {
	ps := proc.Base.PartitionService
	if ps == nil {
		return partitionservice.DisabledService
	}
	return ps
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

// SetPrepareParams borrows prepareParams. The caller remains responsible for releasing it.
func (proc *Process) SetPrepareParams(prepareParams *vector.Vector) {
	proc.setPrepareParams(prepareParams, nil, false)
}

// SetPrepareParamsWithIsBin borrows prepareParams. The caller remains responsible for releasing it.
func (proc *Process) SetPrepareParamsWithIsBin(prepareParams *vector.Vector, isBin []bool) {
	proc.setPrepareParams(prepareParams, isBin, false)
}

// SetOwnedPrepareParamsWithIsBin transfers prepareParams to proc. Replacing or freeing proc releases it.
func (proc *Process) SetOwnedPrepareParamsWithIsBin(prepareParams *vector.Vector, isBin []bool) {
	proc.setPrepareParams(prepareParams, isBin, true)
}

// PrepareParamMetadataForRemote validates the packed per-parameter binary
// flags at a process wire boundary. Older peers appended retired sections
// (three conversion-kind bit sections, optionally eight type sections) after
// the first N binary flags; they are accepted and dropped.
func PrepareParamMetadataForRemote(
	service string,
	paramCount int,
	metadata []bool,
) ([]bool, error) {
	if paramCount < 0 {
		return nil, moerr.NewInvalidInputNoCtx("negative prepare parameter count")
	}
	if len(metadata) == 0 {
		return nil, nil
	}
	if paramCount == 0 {
		return nil, moerr.NewInvalidInputNoCtx("prepare parameter metadata without parameters")
	}
	if len(metadata) <= paramCount {
		return append([]bool(nil), metadata...), nil
	}
	if len(metadata) != paramCount*4 && len(metadata) != paramCount*12 {
		return nil, moerr.NewInvalidInputNoCtxf(
			"invalid prepare parameter metadata length %d for %d parameters",
			len(metadata), paramCount)
	}
	return append([]bool(nil), metadata[:paramCount]...), nil
}

// BinaryStringPrepareParamMetadataForRemote validates the v18-only prepared
// parameter binary-string field at both ends of the process wire boundary.
func BinaryStringPrepareParamMetadataForRemote(
	service string,
	paramCount int,
	metadata []bool,
) ([]bool, error) {
	if len(metadata) == 0 {
		return nil, nil
	}
	if paramCount <= 0 || len(metadata) != paramCount {
		return nil, moerr.NewInvalidInputNoCtxf(
			"invalid binary-string prepare parameter metadata length %d for %d parameters",
			len(metadata), paramCount)
	}
	hasBinaryString := false
	for _, binaryString := range metadata {
		hasBinaryString = hasBinaryString || binaryString
	}
	if !hasBinaryString {
		return nil, nil
	}
	if prepareParamProtocolVersion(service) < defines.MORPCVersion18 {
		return nil, moerr.NewNotSupportedNoCtxf(
			"binary string prepared parameters require MORPC protocol version %d",
			defines.MORPCVersion18)
	}
	return append([]bool(nil), metadata...), nil
}

// RuntimeStringDomainPrepareParamMetadataForRemote validates explicit per-row
// runtime string-domain metadata on the process wire boundary.
func RuntimeStringDomainPrepareParamMetadataForRemote(
	service string,
	paramCount int,
	metadata []uint32,
) ([]uint32, error) {
	if len(metadata) == 0 {
		return nil, nil
	}
	if paramCount <= 0 || len(metadata) != paramCount {
		return nil, moerr.NewInvalidInputNoCtxf(
			"invalid runtime string-domain prepare parameter metadata length %d for %d parameters",
			len(metadata), paramCount)
	}
	for i, encoded := range metadata {
		if encoded > uint32(types.RuntimeStringBinary) {
			return nil, moerr.NewInvalidInputNoCtxf(
				"invalid runtime string domain %d at parameter %d", encoded, i)
		}
	}
	if prepareParamProtocolVersion(service) < defines.MORPCVersion58 {
		return nil, moerr.NewNotSupportedNoCtxf(
			"runtime string domains in prepared parameters require MORPC protocol version %d",
			defines.MORPCVersion58)
	}
	return append([]uint32(nil), metadata...), nil
}

// StringSourcePrepareParamMetadataForRemote validates the independent source
// axis and gates non-default ownership on the protocol version that can carry
// it. Older peers receive the pre-source payload during rolling upgrades; a
// nil result also preserves the source-free protobuf fast path.
func StringSourcePrepareParamMetadataForRemote(
	service string,
	paramCount int,
	metadata []uint32,
) ([]uint32, error) {
	if len(metadata) == 0 {
		return nil, nil
	}
	if paramCount <= 0 || len(metadata) != paramCount {
		return nil, moerr.NewInvalidInputNoCtxf(
			"invalid string source prepare parameter metadata length %d for %d parameters",
			len(metadata), paramCount)
	}
	hasMetadata := false
	for i, encoded := range metadata {
		if encoded > uint32(types.StringSourceCOMStmt) {
			return nil, moerr.NewInvalidInputNoCtxf(
				"invalid string source %d at parameter %d", encoded, i)
		}
		source := types.StringSource(encoded)
		if !source.Valid() {
			return nil, moerr.NewInvalidInputNoCtxf(
				"invalid string source %d at parameter %d", encoded, i)
		}
		hasMetadata = hasMetadata || source != types.StringSourceExpression
	}
	if !hasMetadata {
		return nil, nil
	}
	if prepareParamProtocolVersion(service) < defines.MORPCVersion37 {
		return nil, nil
	}
	return append([]uint32(nil), metadata...), nil
}

func prepareParamProtocolVersion(service string) int64 {
	rt := runtime.ServiceRuntime(service)
	if rt == nil {
		return defines.MORPCMinVersion
	}
	v, ok := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	if !ok {
		return defines.MORPCMinVersion
	}
	switch version := v.(type) {
	case int64:
		return version
	case int:
		return int64(version)
	case uint64:
		return int64(version)
	default:
		return defines.MORPCMinVersion
	}
}

func (proc *Process) setPrepareParams(
	prepareParams *vector.Vector,
	isBin []bool,
	owned bool,
) {
	if proc.Base.prepareParams == prepareParams && proc.Base.prepareParamsOwned {
		owned = true
	}
	if proc.Base.prepareParamsOwned && proc.Base.prepareParams != nil && proc.Base.prepareParams != prepareParams {
		proc.Base.prepareParams.Free(proc.Mp())
	}
	proc.Base.prepareParams = prepareParams
	proc.Base.prepareParamsIsBin = isBin
	proc.Base.prepareParamsOwned = owned && prepareParams != nil
	// Typed values and the source describe one execution's parameters; a
	// caller installs them after the TEXT transport.
	proc.setTypedPrepareParams(nil, false)
	proc.Base.prepareParamsStringSource = types.StringSourceExpression
}

// SetPrepareParamsStringSource records where the current parameters came
// from. Set it after the TEXT transport, which resets it.
func (proc *Process) SetPrepareParamsStringSource(source types.StringSource) {
	proc.Base.prepareParamsStringSource = source
}

// GetPrepareParamsStringSource returns where the current parameters came from.
func (proc *Process) GetPrepareParamsStringSource() types.StringSource {
	return proc.Base.prepareParamsStringSource
}

// SetTypedPrepareParams installs the typed parameter values: a one-row batch
// whose column i holds parameter i in its source SQL type. Ownership of params
// moves to proc; replacing or clearing the prepare parameters releases it.
// Set it after the legacy text parameters, which clear it when set to nil.
func (proc *Process) SetTypedPrepareParams(params *batch.Batch) {
	proc.setTypedPrepareParams(params, true)
}

func (proc *Process) setTypedPrepareParams(params *batch.Batch, owned bool) {
	old := proc.Base.typedPrepareParams
	if old != nil && old != params && proc.Base.typedPrepareParamsOwned {
		old.Clean(proc.Mp())
	}
	proc.Base.typedPrepareParams = params
	proc.Base.typedPrepareParamsOwned = owned && params != nil
}

// GetTypedPrepareParam returns parameter i as a one-row vector of its source
// SQL type, or nil when no typed value was supplied for it.
func (proc *Process) GetTypedPrepareParam(i int) *vector.Vector {
	params := proc.Base.typedPrepareParams
	if params == nil || i < 0 || i >= len(params.Vecs) {
		return nil
	}
	return params.Vecs[i]
}

// GetTypedPrepareParams returns the typed parameter batch, or nil.
func (proc *Process) GetTypedPrepareParams() *batch.Batch {
	return proc.Base.typedPrepareParams
}

// PrepareParamsState keeps the complete prepare-parameter state while a
// Compile that shares the Process is being released.
type PrepareParamsState struct {
	prepareParams *vector.Vector
	isBin         []bool
	owned         bool
	typed         *batch.Batch
	typedOwned    bool
	source        types.StringSource
}

// DetachPrepareParams removes the prepare-parameter state without releasing
// the owned vector. The caller must restore the returned state so a later
// Process.Free can release it.
func (proc *Process) DetachPrepareParams() PrepareParamsState {
	state := PrepareParamsState{
		prepareParams: proc.Base.prepareParams,
		isBin:         proc.Base.prepareParamsIsBin,
		owned:         proc.Base.prepareParamsOwned,
		typed:         proc.Base.typedPrepareParams,
		typedOwned:    proc.Base.typedPrepareParamsOwned,
		source:        proc.Base.prepareParamsStringSource,
	}
	proc.Base.prepareParams = nil
	proc.Base.prepareParamsIsBin = nil
	proc.Base.prepareParamsOwned = false
	proc.Base.typedPrepareParams = nil
	proc.Base.typedPrepareParamsOwned = false
	return state
}

// BorrowPrepareParams exposes detached prepare parameters without transferring
// their ownership back to proc. It lets nested work use the parameters while
// Process.Free releases only resources owned by that nested work.
func (proc *Process) BorrowPrepareParams(state PrepareParamsState) {
	proc.setPrepareParams(state.prepareParams, state.isBin, false)
	proc.setTypedPrepareParams(state.typed, false)
	proc.Base.prepareParamsStringSource = state.source
}

// RestorePrepareParams restores state previously returned by
// DetachPrepareParams.
func (proc *Process) RestorePrepareParams(state PrepareParamsState) {
	proc.setPrepareParams(state.prepareParams, state.isBin, state.owned)
	proc.setTypedPrepareParams(state.typed, state.typedOwned)
	proc.Base.prepareParamsStringSource = state.source
}

func (proc *Process) OperatorOutofMemory(size int64) bool {
	return proc.Mp().Cap() < size
}

func (proc *Process) AllocVectorOfRows(typ types.Type, nele int, nsp *nulls.Nulls) (*vector.Vector, error) {
	vec := vector.NewVec(typ)
	err := vec.PreExtend(nele, proc.Mp())
	if err != nil {
		return nil, err
	}
	vec.SetLength(nele)
	if nsp != nil {
		vec.GetNulls().Or(nsp)
	}
	return vec, nil
}

func (proc *Process) NewBatchFromSrc(src *batch.Batch, preAllocSize int) (*batch.Batch, error) {
	return proc.NewBatchFromSrcWithAllocation(src, preAllocSize, nil)
}

// NewBatchFromSrcWithAllocation creates an empty off-heap destination whose
// first vector growth uses the supplied immutable allocation provenance.
func (proc *Process) NewBatchFromSrcWithAllocation(
	src *batch.Batch,
	preAllocSize int,
	selection *vector.AllocationAccountSelection,
) (_ *batch.Batch, retErr error) {
	if proc == nil || src == nil || preAllocSize < 0 {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	bat := batch.NewOffHeapWithSize(len(src.Vecs))
	defer func() {
		if retErr != nil {
			bat.Clean(proc.Mp())
		}
	}()
	bat.SetAttributes(src.Attrs)
	bat.Recursive = src.Recursive
	for i := range bat.Vecs {
		if src.Vecs[i] == nil {
			return nil, mpool.ErrAllocationAccountInvalid
		}
		bat.Vecs[i] = vector.NewOffHeapVecWithType(*src.Vecs[i].GetType())
	}
	if selection != nil {
		if err := bat.SetAllocationAccount(selection); err != nil {
			return nil, err
		}
	}
	for i := range bat.Vecs {
		v := bat.Vecs[i]
		if v.Capacity() < preAllocSize {
			err := v.PreExtend(preAllocSize, proc.Mp())
			if err != nil {
				return nil, err
			}
		}
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

func (proc *Process) GetSpillFileService() (fileservice.MutableFileService, error) {
	local, err := fileservice.Get[fileservice.MutableFileService](proc.Base.FileService, defines.LocalFileServiceName)
	if err != nil {
		return nil, err
	}

	// The spill directory is process-independent, idempotent initialization.
	// proc.Ctx is pipeline-scoped and may already be canceled (or be nil in a
	// lightweight Process) before a frontend spool asks for the service. The
	// actual spill I/O still receives and observes its caller-owned context.
	if err := local.EnsureDir(context.Background(), defines.SpillFileServiceName); err != nil {
		return nil, err
	}

	subPathFS := fileservice.SubPath(local, defines.SpillFileServiceName)
	mutablefs, ok := subPathFS.(fileservice.MutableFileService)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("subPathFS is not a MutableFileService")
	}
	return mutablefs, nil
}
