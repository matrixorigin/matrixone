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

package external

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/crt"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	OneBatchMaxRow   = int(options.DefaultBlockMaxRows)
	S3ParallelMaxnum = 10
)

const opName = "external"

func (external *External) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": external output")
}

func (external *External) OpType() vm.OpType {
	return vm.External
}

func (external *External) Prepare(proc *process.Process) error {
	_, span := trace.Start(proc.Ctx, "ExternalPrepare")
	defer span.End()

	if external.OpAnalyzer == nil {
		external.OpAnalyzer = process.NewAnalyzer(external.GetIdx(), external.IsFirst, external.IsLast, "external")
	} else {
		external.OpAnalyzer.Reset()
	}

	param := external.Es
	if param == nil {
		return moerr.NewInvalidInput(proc.Ctx, "external parameter is missing")
	}
	if err := validateParquetWholeFileFanoutProtocol(proc, param); err != nil {
		return err
	}
	if param.Fileparam == nil {
		return moerr.NewInvalidInput(proc.Ctx, "external file parameter is missing")
	}
	if proc.GetLim().MaxMsgSize == 0 {
		param.maxBatchSize = uint64(morpc.GetMessageSize())
	} else {
		param.maxBatchSize = proc.GetLim().MaxMsgSize
	}
	param.maxBatchSize = uint64(float64(param.maxBatchSize) * 0.6)

	if param.Extern == nil {
		if param.ForeignScan != nil {
			// Same rationale as datastream below: no file-backed ExternParam,
			// CreateSql is the foreign envelope, not JSON.
			param.Extern = ForeignExternParam(param.ForeignScan.Kind)
		} else if param.DatastreamScan != nil {
			// A datastream scan has no file-backed ExternParam; its CreateSql is
			// the datastream envelope, not JSON.  Rebuild the synthetic param
			// (remote-run decode arrives here with Extern == nil).
			param.Extern = DatastreamExternParam()
		} else if param.KafkaScan != nil {
			// Same rationale: CreateSql is the kafka envelope, not JSON.
			param.Extern = KafkaExternParam(param.KafkaScan)
		} else {
			param.Extern = &tree.ExternParam{}
			if err := json.Unmarshal([]byte(param.CreateSql), param.Extern); err != nil {
				return err
			}
			if err := plan2.InitS3Param(param.Extern); err != nil {
				return err
			}
			param.Extern.FileService = proc.Base.FileService
		}
	} else {
		// Parallel workers borrow the same configuration. Keep runtime context
		// and service writes, including those in reader.Open, local to this worker.
		privateExtern := *param.Extern
		param.Extern = &privateExtern
	}
	if param.Extern.FileService == nil {
		// Decoded remote parameters carry path/configuration but not the local
		// FileService interface. Install the executing CN's service before the
		// rollout gate so aliases are classified against that worker's backend.
		param.Extern.FileService = proc.Base.FileService
	}
	if param.ForeignScan != nil && param.ForeignScan.Kind == foreignScanKindESQL {
		param.ESQLTemporalUTC = true
	}
	if !loadFormatIsValid(param.Extern) {
		return moerr.NewNYIf(proc.Ctx, "load format '%s'", param.Extern.Format)
	}
	if param.Extern.Format == tree.ARROW &&
		(param.Extern.ExternType != int32(plan.ExternType_LOAD) ||
			param.ArrowExecutionScope != pipeline.ArrowExecutionScope_ArrowLoadData) {
		return moerr.NewNotSupported(proc.Ctx, "Arrow format is supported only by LOAD DATA")
	}
	if param.Extern.Format == tree.ARROW {
		// A remote External is reconstructed on the executing CN.  The compile
		// gate on the coordinator is therefore not sufficient: each worker must
		// enforce its own rollout configuration before it opens the source.
		settings, err := plan2.RequireArrowLoadEnabled(proc, param.Extern)
		if err != nil {
			return err
		}
		if param.ArrowDistributedExecution && !settings.DistributedEnabled {
			return moerr.NewNotSupported(proc.Ctx,
				"distributed Arrow LOAD is disabled by configuration")
		}
	}
	if param.Extern.ExternType == int32(plan.ExternType_LOAD) &&
		(param.Extern.Parallel || param.Extern.ParallelLoadRequested) {
		param.LoadEmptyNumericAsZero = true
	}

	// File list check
	if len(param.FileList) == 0 && param.Extern.ScanType != tree.INLINE {
		logutil.Warnf("no such file '%s'", param.Extern.Filepath)
		param.Fileparam.End = true
	}
	param.Fileparam.FileCnt = len(param.FileList)
	// INLINE mode has no FileList, set FileCnt=1 as virtual single file
	if param.Extern.ScanType == tree.INLINE {
		param.Fileparam.FileCnt = 1
	}
	param.Ctx = proc.Ctx
	if param.Extern.ScanType != tree.INLINE && !param.Extern.Local && param.ForeignScan == nil {
		param.Ctx = process.SQLFileAccessContext(proc.Ctx, proc)
	}
	param.Extern.Ctx = param.Ctx
	param.addParquetProfile(icebergParquetProfileStats(param))
	// Validate the physical output mapping before constructing a reader. A
	// failed mapping must not leave a reader or batch behind for the caller to
	// clean up after Prepare returns an error.
	if err := validateExternalOutputAttrs(proc.Ctx, param.Attrs, param.Cols); err != nil {
		return err
	}

	// Filter public preprocessing
	if param.Filter == nil {
		param.Filter = &FilterParam{}
	}
	name2ColIndex := make(map[string]int32, len(param.Cols))
	for i, col := range param.Cols {
		name2ColIndex[col.Name] = int32(i)
	}
	tableDef := &plan.TableDef{Name2ColIndex: name2ColIndex}
	param.Filter.columnMap, _, _, _ = plan2.GetColumnsByExpr(
		param.Filter.FilterExpr, tableDef)

	// Deep copy FilterExpr + one-time AssignAuxIdForExpr
	if param.Filter.FilterExpr != nil {
		param.Filter.FilterExpr = plan2.DeepCopyExpr(param.Filter.FilterExpr)
		param.Filter.AuxIdCnt = plan2.AssignAuxIdForExpr(param.Filter.FilterExpr, 0)
	} else {
		param.Filter.AuxIdCnt = 0
	}

	// Defensive close old reader
	if external.reader != nil {
		logutil.Debugf("external Prepare: closing leftover reader from previous execution")
		external.reader.Close()
		external.reader = nil
		external.fileOpened = false
	}

	// Error-mode columns are resolved from the pruned attribute list before any
	// reader is built, so every reader sees the same decision.
	resolveExternalErrorMode(param)
	param.colLoaders = nil
	param.outputMP = external.OutputAllocation

	// Create reader (single dispatch point)
	switch {
	case param.ForeignScan != nil:
		external.reader = NewForeignScanReader(param)
	case param.DatastreamScan != nil:
		external.reader = NewDataStreamReader(param)
	case param.KafkaScan != nil:
		external.reader = NewKafkaReader(param)
	case param.Extern.ExternType == int32(plan.ExternType_RESULT_SCAN):
		external.reader = NewZonemapReader(param, proc)
	case param.Extern.Format == tree.PARQUET:
		external.reader = NewParquetReader(param, proc)
	case param.Extern.Format == tree.ARROW:
		reader, err := NewArrowReader(param, proc, external.allocationAccount)
		if err != nil {
			return err
		}
		external.reader = reader
	default:
		r, err := NewCsvReader(param, proc)
		if err != nil {
			return err
		}
		external.reader = r
	}
	if err := external.prepareIcebergDeleteApply(proc); err != nil {
		return err
	}

	// Projection init
	if external.ProjectList != nil {
		err := external.PrepareProjection(proc)
		if err != nil {
			return err
		}
	}
	if external.ctr.buf == nil {
		attrs := make([]string, len(param.Attrs))
		for i, attr := range param.Attrs {
			attrs[i] = attr.ColName
		}
		external.ctr.buf = batch.NewWithSize(len(attrs))
		flag := param.ParallelLoad
		if param.Extern.Format == tree.PARQUET {
			flag = false
		}
		// Allocate output vectors in Attrs order, but resolve their physical
		// types through ColIndex. Generated or hidden columns can be omitted
		// from Attrs, so output position and table-column position differ.
		for i, attr := range param.Attrs {
			colIndex := int(attr.ColIndex)
			colType := &param.Cols[colIndex].Typ
			typ := makeType(colType, flag || loadTimeTextInput(param, types.T(colType.Id)) ||
				loadAttributedDecimalTextInput(param, colType))
			// Readers grow the output through param.outputPool.
			external.ctr.buf.Vecs[i] = vector.NewVec(typ, param.outputPool(proc))
		}
	}
	return nil
}

func validateParquetWholeFileFanoutProtocol(proc *process.Process, param *ExternalParam) error {
	if param == nil || !param.ParquetWholeFileFanout {
		return nil
	}
	if proc == nil || proc.Ctx == nil {
		return moerr.NewNotSupportedNoCtx(
			"Parquet whole-file fanout remote execution requires MORPC protocol version 45",
		)
	}
	if remote, _ := proc.Ctx.Value(defines.RemoteRunContext{}).(bool); !remote {
		return nil
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return moerr.NewNotSupported(proc.Ctx, "Parquet whole-file fanout remote execution requires MORPC protocol version 45")
	}
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return moerr.NewNotSupported(proc.Ctx, "Parquet whole-file fanout remote execution requires MORPC protocol version 45")
	}
	protocolVersion, ok := version.(int64)
	if !ok || protocolVersion < defines.MORPCVersion45 {
		return moerr.NewNotSupported(proc.Ctx, "Parquet whole-file fanout remote execution requires MORPC protocol version 45")
	}
	return nil
}

func validateExternalOutputAttrs(ctx context.Context, attrs []plan.ExternAttr, cols []*plan.ColDef) error {
	for _, attr := range attrs {
		colIndex := int(attr.ColIndex)
		if colIndex < 0 || colIndex >= len(cols) || cols[colIndex] == nil {
			return moerr.NewInvalidInputf(ctx, "external output column index %d is invalid", attr.ColIndex)
		}
	}
	return nil
}

func (external *External) checkLoadLockTableBinds(proc *process.Process) error {
	param := external.Es
	if param == nil ||
		param.Extern == nil ||
		param.Extern.ExternType != int32(plan.ExternType_LOAD) {
		return nil
	}

	txnOp := proc.GetTxnOperator()
	if txnOp == nil {
		return nil
	}
	return txnOp.CheckLockTableBinds(proc.Ctx)
}

func (external *External) Call(proc *process.Process) (vm.CallResult, error) {
	t := time.Now()
	ctx, span := trace.Start(proc.Ctx, "ExternalCall")
	t1 := time.Now()

	analyzer := external.OpAnalyzer
	param := external.Es
	defer func() {
		analyzer.AddScanTime(t1)
		param.flushParquetProfile(analyzer)
		span.End()
		v2.TxnStatementExternalScanDurationHistogram.Observe(time.Since(t).Seconds())
	}()

	result := vm.NewCallResult()
	if param.Fileparam.End {
		result.Status = vm.ExecStop
		return result, nil
	}

	// "Open file loop": skip empty files, find next non-empty file
	for !external.fileOpened {
		// INLINE mode has no FileList, skip file pointer advancement
		if param.Extern.ScanType != tree.INLINE {
			if param.Fileparam.FileIndex >= len(param.FileList) {
				param.Fileparam.End = true
				break
			}
			param.Fileparam.Filepath = param.FileList[param.Fileparam.FileIndex]
			param.Fileparam.FileIndex++
		}

		fileEmpty, err := external.reader.Open(param, proc)
		if err != nil {
			external.reader.Close()
			external.fileOpened = false
			param.Fileparam.End = true
			return result, err
		}
		if fileEmpty {
			external.reader.Close()
			external.finishCurrentFile(param)
			if param.Fileparam.End {
				break
			}
			continue
		}
		external.fileOpened = true
	}

	// All files processed (including all-empty case)
	if param.Fileparam.End && !external.fileOpened {
		result.Status = vm.ExecStop
		return result, nil
	}

	if err := external.checkLoadLockTableBinds(proc); err != nil {
		return result, err
	}

	// A publishing reader (Arrow) returns a batch it owns; every other reader
	// fills the operator's own buffer.
	out := external.ctr.buf
	var fileFinished bool
	var err error
	if publisher, ok := external.reader.(BatchPublisher); ok {
		out, fileFinished, err = publisher.PublishBatch(ctx, proc, analyzer)
	} else {
		if out != nil {
			out.CleanOnlyData()
		}
		fileFinished, err = external.reader.ReadBatch(ctx, out, proc, analyzer)
	}
	if err != nil {
		external.reader.Close()
		external.fileOpened = false
		param.Fileparam.End = true
		if external.ctr.buf != nil {
			external.ctr.buf.CleanOnlyData()
		}
		return result, err
	}
	if out != nil && out.RowCount() > 0 {
		if err := external.applyIcebergDeletes(ctx, out, proc); err != nil {
			external.reader.Close()
			external.fileOpened = false
			param.Fileparam.End = true
			return result, err
		}
	}

	if fileFinished {
		// Closing the file keeps a published batch: it is this call's output.
		if err := external.reader.Close(); err != nil {
			external.fileOpened = false
			param.Fileparam.End = true
			if external.ctr.buf != nil {
				external.ctr.buf.CleanOnlyData()
			}
			return result, err
		}
		external.finishCurrentFile(param)
	}

	analyzer.Input(out)

	result.Batch = out
	if out != nil {
		external.ctr.maxAllocSize = max(external.ctr.maxAllocSize, out.Size())
		param.addParquetProfile(process.ParquetProfileStats{PeakBatchBytes: int64(out.Size())})
		result.Batch.ShuffleIDX = int32(param.Idx)
	}

	return result, nil
}

// finishCurrentFile handles unified file completion.
func (external *External) finishCurrentFile(param *ExternalParam) {
	external.fileOpened = false
	param.Fileparam.FileFin++
	if param.Fileparam.FileFin >= param.Fileparam.FileCnt {
		param.Fileparam.End = true
	}
}

func isFileLevelColumn(node *plan.Node, col *plan.ColRef) bool {
	return colexec.FileLevelExecution.IsFileLevelColumn(node, col)
}

func isSafeFileLevelFunction(ref *plan.ObjectRef) bool {
	return colexec.FileLevelExecution.IsSafeFileLevelFunction(ref)
}

func classifyFileLevelColumns(node *plan.Node, expr *plan.Expr) (bool, bool) {
	return colexec.FileLevelExecution.ClassifyFileLevelColumns(node, expr)
}

func isFileLevelFilter(node *plan.Node, expr *plan.Expr) bool {
	return colexec.FileLevelExecution.IsFileLevelFilter(node, expr)
}

func makeFilepathBatch(node *plan.Node, proc *process.Process, fileList []string) (bat *batch.Batch, err error) {
	num := len(node.TableDef.Cols)
	bat = &batch.Batch{
		Vecs: make([]*vector.Vector, num),
	}

	mp := proc.GetMPool()
	for i := 0; i < num; i++ {
		name := node.TableDef.Cols[i].Name
		if i == num-1 && (catalog.ContainExternalHidenCol(name) ||
			catalog.IsForeignQueryCol(name, node.TableDef.Cols[i].ColId)) {
			typ := types.T_varchar.ToType()
			bat.Vecs[i], err = proc.AllocVectorOfRows(typ, len(fileList), nil)
			if err != nil {
				bat.Clean(mp)
				return nil, err
			}

			for j := 0; j < len(fileList); j++ {
				if err = vector.SetStringAt(bat.Vecs[i], j, fileList[j], mp); err != nil {
					bat.Clean(mp)
					return nil, err
				}
			}
		}
	}
	bat.SetRowCount(len(fileList))
	return bat, nil
}

func filterByAccountAndFilename(ctx context.Context, node *plan.Node, proc *process.Process, fileList []string, fileSize []int64) ([]string, []int64, []*plan.Expr, error) {
	_, span := trace.Start(ctx, "filterByAccountAndFilename")
	defer span.End()
	filterList := make([]*plan.Expr, 0)
	filterList2 := make([]*plan.Expr, 0)
	for i := 0; i < len(node.FilterList); i++ {
		if isFileLevelFilter(node, node.FilterList[i]) {
			filterList = append(filterList, node.FilterList[i])
		} else {
			filterList2 = append(filterList2, node.FilterList[i])
		}
	}
	if len(filterList) == 0 {
		return fileList, fileSize, filterList2, nil
	}
	bat, err := makeFilepathBatch(node, proc, fileList)
	if err != nil {
		return nil, nil, nil, err
	}
	defer bat.Clean(proc.Mp())
	filter := colexec.RewriteFilterExprList(filterList)

	executor, err := colexec.NewExpressionExecutor(proc, filter)
	if err != nil {
		return nil, nil, nil, err
	}
	defer executor.Free()
	vec, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return nil, nil, nil, err
	}

	fileListTmp := make([]string, 0)
	fileSizeTmp := make([]int64, 0)
	for i := 0; i < len(fileList); i++ {
		valuePos := i
		if vec.IsConst() {
			valuePos = 0
		}
		if !vec.GetNulls().Contains(uint64(valuePos)) && vector.GetFixedAtWithTypeCheck[bool](vec, i) {
			fileListTmp = append(fileListTmp, fileList[i])
			fileSizeTmp = append(fileSizeTmp, fileSize[i])
		}
	}
	return fileListTmp, fileSizeTmp, filterList2, nil
}

func FilterFileList(ctx context.Context, node *plan.Node, proc *process.Process, fileList []string, fileSize []int64) ([]string, []int64, []*plan.Expr, error) {
	return filterByAccountAndFilename(ctx, node, proc, fileList, fileSize)
}

func readFile(param *ExternalParam, proc *process.Process) (io.ReadCloser, error) {
	var fileOffsets []int64
	var fileSizeMax int64 = math.MaxInt64
	if param.Extern.ScanType == tree.INLINE || param.Extern.Local {
		return crt.GetIOReadCloser(proc, param.Extern, param.Extern.Data, fileOffsets, fileSizeMax)
	}

	// adjust read offset for parallel load.
	if 2*param.Idx >= len(param.FileOffsetTotal[param.Fileparam.FileIndex-1].Offset) {
		return nil, nil
	}
	param.FileOffset = param.FileOffsetTotal[param.Fileparam.FileIndex-1].Offset[2*param.Idx : 2*param.Idx+2]
	fileOffsets = param.FileOffset
	fileSizeMax = param.FileSize[param.Fileparam.FileIndex-1]

	return crt.GetIOReadCloser(proc, param.Extern, param.Fileparam.Filepath, fileOffsets, fileSizeMax)
}

func ReadFileOffset(param *tree.ExternParam, mcpu int, fileSize int64, visibleCols []*plan.ColDef) ([]int64, error) {
	ctx := param.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if crt.GetCompressType(param.CompressType, param.Filepath) != tree.NOCOMPRESS {
		return nil, moerr.NewInvalidInputf(ctx, "parallel read is not supported for compressed file %s", param.Filepath)
	}
	arr := make([]int64, 0)

	fs, readPath, err := plan2.GetForETLWithType(param, param.Filepath)
	if err != nil {
		return nil, err
	}
	// A field token can hide a physical line terminator, including one
	// completed by the following field. Keep such files in one range so
	// the CSV tokenizer retains the context needed to consume that token.
	if !param.Strict && nonStrictFieldTerminatorConflict(param) {
		if param.Ctx != nil {
			if err := param.Ctx.Err(); err != nil {
				return nil, err
			}
		}
		return []int64{param.FileStartOff, -1}, nil
	}
	var r io.ReadCloser
	vec := fileservice.IOVector{
		FilePath: readPath,
		Policy:   fileservice.SkipAllCache,
		Entries: []fileservice.IOEntry{
			0: {
				Offset:            0,
				Size:              -1,
				ReadCloserForRead: &r,
			},
		},
	}

	var offsets []int64

	offsets = append(offsets, param.FileStartOff)
	batchSize := (fileSize - param.FileStartOff) / int64(mcpu)
	legacyBoundary := param.Format != tree.CSV
	requiresVerifiedBoundaries := false

	for i := 1; i < mcpu; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if param.Strict && batchSize > 0 && offsets[i-1] > math.MaxInt64-batchSize {
			break
		}
		guess := offsets[i-1] + batchSize
		if guess >= fileSize {
			break
		}
		if !param.Strict {
			var boundary int64
			if !legacyBoundary {
				boundary, legacyBoundary, err = getNonStrictBoundary(param, fs, readPath, offsets[i-1], guess)
				if err == io.EOF {
					break
				}
				if err != nil {
					return nil, err
				}
			}
			if legacyBoundary {
				boundary, err = getNonStrictFallbackBoundary(param, fs, readPath, guess)
				if err == io.EOF {
					break
				}
				if err != nil {
					return nil, err
				}
			}
			if boundary >= fileSize {
				break
			}
			offsets = append(offsets, boundary)
			continue
		}
		cfg := strictCSVConfig(param)
		limit, ok := strictProbeInputLimit(&cfg, csvparser.BufferSizeScale)
		if !ok {
			break
		}
		attempt := &strictProbeAttempt{remaining: limit}
		vec.Entries[0].Offset = guess
		if err = fs.Read(ctx, &vec); err != nil {
			return nil, err
		}
		tailSize, err := getTailSizeStrictAttempt(param, visibleCols, r, &cfg, attempt)
		closeErr := r.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if err != nil {
			// Strict scanning retains its existing parser-resynchronization fallback.
			break
		}
		if tailSize <= 0 || guess < 0 || guess > math.MaxInt64-tailSize {
			break
		}
		next := guess + tailSize
		if next <= offsets[i-1] || next >= fileSize {
			break
		}
		// At the old cap, discovering real EOF can admit a candidate where
		// the capped reader failed. Verify equality too, then keep verifying
		// subsequent ranges even when their suffix probes consume less.
		requiresVerifiedBoundaries = requiresVerifiedBoundaries || limit-attempt.remaining >= strictProbeSoftBytes
		if requiresVerifiedBoundaries {
			if param.FileStartOff < 0 || next-param.FileStartOff > attempt.remaining {
				break
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			vec.Entries[0].Offset = param.FileStartOff
			if err := fs.Read(ctx, &vec); err != nil {
				return nil, err
			}
			err := verifyStrictTarget(param, &cfg, r, next-param.FileStartOff, attempt)
			closeErr := r.Close()
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			if closeErr != nil {
				return nil, closeErr
			}
			if err != nil {
				break
			}
		}
		offsets = append(offsets, next)
	}

	for i := 0; i < len(offsets); i++ {
		if i+1 < len(offsets) {
			arr = append(arr, offsets[i])
			arr = append(arr, offsets[i+1])
		} else {
			arr = append(arr, offsets[i])
			arr = append(arr, -1)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return arr, nil
}

func getTailSize(param *tree.ExternParam, cols []*plan.ColDef, r io.ReadCloser) (int64, error) {
	if param.Strict {
		return getTailSizeStrict(param, cols, r)
	}
	return getTailSizeNoStrict(param, r)
}

func lineTerminator(param *tree.ExternParam) string {
	terminator := "\n"
	if param.Tail != nil && param.Tail.Lines != nil {
		if terminated := param.Tail.Lines.TerminatedBy; terminated != nil && terminated.Value != "" {
			terminator = terminated.Value
		}
	}
	return terminator
}

func delimiterPrefix(terminator string) []int {
	prefix := make([]int, len(terminator))
	for i, matched := 1, 0; i < len(terminator); i++ {
		for matched > 0 && terminator[i] != terminator[matched] {
			matched = prefix[matched-1]
		}
		if terminator[i] == terminator[matched] {
			matched++
		}
		prefix[i] = matched
	}
	return prefix
}

// A raw line match is unsafe when it begins inside an earlier field token.
// Newline has priority at the same position in the CSV tokenizer, so only
// interior positions and suffixes completed by field data need consideration.
func nonStrictFieldTerminatorConflict(param *tree.ExternParam) bool {
	field := tree.DefaultFieldsTerminated
	if param.Tail != nil && param.Tail.Fields != nil {
		if terminated := param.Tail.Fields.Terminated; terminated != nil && terminated.Value != "" {
			field = terminated.Value
		}
	}
	if param.Format == tree.JSONLINE {
		field = "\t"
	}
	if len(field) < 2 {
		return false
	}
	line := lineTerminator(param)
	prefix := delimiterPrefix(line)
	matched := 0
	for i := 1; i < len(field); i++ {
		for matched > 0 && field[i] != line[matched] {
			matched = prefix[matched-1]
		}
		if field[i] == line[matched] {
			matched++
		}
		if matched == len(line) {
			return true
		}
	}
	return matched > 0
}

// getTailSizeNoStrict finds an unambiguously aligned physical line terminator. Like the
// non-strict fast split path, it does not recover quote/escape context from
// before the requested offset. ReadFileOffset excludes conflicting field tokens
// before probing. The reader is borrowed; ReadFileOffset owns it.
func getTailSizeNoStrict(param *tree.ExternParam, r io.Reader) (int64, error) {
	terminator := lineTerminator(param)
	bufR := bufio.NewReader(r)
	var size int64
	if len(terminator) == 1 {
		for {
			if param.Ctx != nil {
				if err := param.Ctx.Err(); err != nil {
					return size, err
				}
			}
			part, err := bufR.ReadSlice(terminator[0])
			size += int64(len(part))
			if err != bufio.ErrBufferFull {
				return size, err
			}
		}
	}

	// KMP keeps both time and memory bounded for long records and overlapping
	// delimiter prefixes, including delimiters spanning buffered reads.
	prefix := delimiterPrefix(terminator)
	bordered := prefix[len(terminator)-1] != 0
	var previousMatchEnd int64
	for matched := 0; ; {
		if size%4096 == 0 && param.Ctx != nil {
			if err := param.Ctx.Err(); err != nil {
				return size, err
			}
		}
		ch, err := bufR.ReadByte()
		if err != nil {
			return size, err
		}
		size++
		for matched > 0 && ch != terminator[matched] {
			matched = prefix[matched-1]
		}
		if ch == terminator[matched] {
			matched++
		}
		if matched == len(terminator) {
			start := size - int64(len(terminator))
			// A bordered delimiter (e.g. || or abab) can overlap the one
			// consumed before the probe started. Accept a match only when
			// every possible overlapping predecessor is visible and none
			// exists, so its alignment is independent of the initial phase.
			if !bordered || (start >= int64(len(terminator)-1) && previousMatchEnd <= start) {
				return size, nil
			}
			previousMatchEnd = size
			// Keep overlapping matches: skipping one could falsely certify
			// the next match as having no overlapping predecessor.
			matched = prefix[matched-1]
		}
	}
}

// getNonStrictBoundary scans from a known record start to the first record end
// at or after guess, using the same grammar as CsvReader. Starting at guess
// cannot recover quote state when it lands inside a multiline enclosed field.
// Across valid splits this adds one serial parse pass, plus per-reader read-ahead.
// No row-width or type validation applies to non-strict boundary selection.
//
// A syntax error requests legacy physical-line splitting for this and all later
// splits: a legacy boundary is no longer a known CSV record start. Other errors
// propagate. The reader is closed before the caller opens a fallback reader.
func getNonStrictBoundary(param *tree.ExternParam, fs fileservice.ETLFileService, readPath string, start, guess int64) (boundary int64, legacy bool, err error) {
	ctx := param.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	var r io.ReadCloser
	vec := fileservice.IOVector{
		FilePath: readPath,
		Policy:   fileservice.SkipAllCache,
		Entries:  []fileservice.IOEntry{{Offset: start, Size: -1, ReadCloserForRead: &r}},
	}
	if err := fs.Read(ctx, &vec); err != nil {
		return 0, false, err
	}
	defer func() {
		if closeErr := r.Close(); closeErr != nil && (err == nil || err == io.EOF) {
			err = closeErr
		}
	}()
	csvReader, err := newCSVParserFromReader(param, boundaryContextReader{ctx: ctx, Reader: r})
	if err != nil {
		return 0, false, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		err = csvReader.ScanRecord()
		if err == io.EOF {
			return start + csvReader.Pos(), false, io.EOF
		}
		if err != nil {
			if csvparser.IsSyntaxError(err) {
				return 0, true, nil
			}
			return 0, false, err
		}
		if start+csvReader.Pos() >= guess {
			return start + csvReader.Pos(), false, nil
		}
	}
}

// boundaryContextReader checks cancellation at read-block boundaries, including
// inside a long record. The file service owns cancellation of a blocked read.
type boundaryContextReader struct {
	ctx context.Context
	io.Reader
}

func (r boundaryContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

// getNonStrictFallbackBoundary uses the configured physical line terminator for
// malformed CSV and non-CSV formats, retaining bounded overlap-aware scanning.
func getNonStrictFallbackBoundary(param *tree.ExternParam, fs fileservice.ETLFileService, readPath string, guess int64) (boundary int64, err error) {
	ctx := param.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var r io.ReadCloser
	vec := fileservice.IOVector{
		FilePath: readPath,
		Policy:   fileservice.SkipAllCache,
		Entries:  []fileservice.IOEntry{{Offset: guess, Size: -1, ReadCloserForRead: &r}},
	}
	if err := fs.Read(ctx, &vec); err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := r.Close(); closeErr != nil && (err == nil || err == io.EOF) {
			err = closeErr
		}
	}()
	tailSize, err := getTailSizeNoStrict(param, boundaryContextReader{ctx: ctx, Reader: r})
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	return guess + tailSize, err
}

const (
	strictProbeSoftBytes  = 256 * 1024
	strictProbeMaxRecords = 256
	// CSVParser multiplies its requested block size by BufferSizeScale (5).
	// Keep reads small even when the record-size-derived hard cap is large.
	strictProbeBlockSize  = 16 * 1024
	strictProbeBufferSize = 4096
)

var errStrictProbeBudgetExceeded = moerr.NewInternalError(moerr.NoReportContext(), "strict CSV boundary probe budget exceeded")

// strictCSVConfig freezes the execution parser's effective record weight for
// both streams of one attempt. Ordinary execution keeps its existing defaults.
func strictCSVConfig(param *tree.ExternParam) csvparser.CSVConfig {
	cfg := *CSVConfigForLoad(param)
	if cfg.MaxRecordSize <= 0 {
		cfg.MaxRecordSize = int64(config.LargestEntryLimit)
	}
	return cfg
}

// strictProbeInputLimit bounds one initial fragment, one complete candidate,
// skipped input and parser read-ahead. Record weight prices content and each
// field, but not field delimiters or enclosing quotes. For C content bytes and
// F fields, C+F <= W and physical bytes <= C+(F-1)*D+2*F*Q+L.
// Thus R=W*max(1,D+2*Q)+L+P+4 also covers STARTING BY and an entry BOM.
// H=2*R+S+2*(B+U), with one explicitly sized bufio buffer U, is a consumption
// bound, never an allocation size. Infinite skipped input still stops at H.
// cfg must carry the effective MaxRecordSize resolved by strictCSVConfig.
func strictProbeInputLimit(cfg *csvparser.CSVConfig, scale int64) (int64, bool) {
	weight := cfg.MaxRecordSize
	if weight <= 0 || scale <= 0 || len(cfg.FieldsTerminatedBy) == 0 {
		return 0, false
	}
	field, quote := int64(len(cfg.FieldsTerminatedBy)), int64(len(cfg.FieldsEnclosedBy))
	if quote > (math.MaxInt64-field)/2 {
		return 0, false
	}
	factor := max(int64(1), field+2*quote)
	if weight > math.MaxInt64/factor {
		return 0, false
	}
	record := weight * factor
	for _, extra := range []int64{max(int64(2), int64(len(cfg.LinesTerminatedBy))), int64(len(cfg.LinesStartingBy)), 4} {
		if record > math.MaxInt64-extra {
			return 0, false
		}
		record += extra
	}
	if scale > math.MaxInt64/strictProbeBlockSize {
		return 0, false
	}
	block := strictProbeBlockSize * scale
	if block > int64(^uint(0)>>1) || block > (math.MaxInt64-strictProbeSoftBytes)/2-strictProbeBufferSize {
		return 0, false
	}
	overhead := int64(strictProbeSoftBytes) + 2*(block+strictProbeBufferSize)
	if record > (math.MaxInt64-overhead)/2 {
		return 0, false
	}
	return 2*record + overhead, true
}

// strictProbeReader borrows the stream. A non-EOF error prevents the parser
// from accepting an artificially truncated record as a complete final row.
type strictProbeReader struct {
	io.Reader
	remaining int64
}

func (r *strictProbeReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, errStrictProbeBudgetExceeded
	}
	if int64(len(p)) > r.remaining {
		p = p[:int(r.remaining)]
	}
	n, err := r.Reader.Read(p)
	r.remaining -= int64(n)
	return n, err
}

func getTailSizeStrict(param *tree.ExternParam, cols []*plan.ColDef, r io.ReadCloser) (int64, error) {
	cfg := strictCSVConfig(param)
	limit, ok := strictProbeInputLimit(&cfg, csvparser.BufferSizeScale)
	if !ok {
		return 0, errStrictProbeBudgetExceeded
	}
	return getTailSizeStrictWithLimit(param, cols, r, &cfg, limit)
}

func getTailSizeStrictWithLimit(param *tree.ExternParam, cols []*plan.ColDef, r io.Reader, cfg *csvparser.CSVConfig, limit int64) (int64, error) {
	return getTailSizeStrictAttempt(param, cols, r, cfg, &strictProbeAttempt{remaining: limit})
}

func getTailSizeStrictAttempt(param *tree.ExternParam, cols []*plan.ColDef, r io.Reader, cfg *csvparser.CSVConfig, attempt *strictProbeAttempt) (int64, error) {
	limit := attempt.remaining
	ctx := param.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if limit <= 0 {
		return 0, errStrictProbeBudgetExceeded
	}
	quietCtx := errutil.ContextWithNoReport(ctx, true)
	limited := &strictProbeReader{Reader: r, remaining: limit}
	defer func() { attempt.remaining = limited.remaining }()
	bufR := bufio.NewReaderSize(boundaryContextReader{ctx: ctx, Reader: limited}, strictProbeBufferSize)
	// ensure the first character is not field quote symbol
	skipCount := int64(0)
	for len(cfg.FieldsEnclosedBy) > 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		ch, err := bufR.ReadByte()
		if err != nil {
			return 0, err
		}
		if ch != cfg.FieldsEnclosedBy[0] {
			err = bufR.UnreadByte()
			if err != nil {
				return 0, err
			}
			break
		}
		skipCount++
		if skipCount == strictProbeSoftBytes {
			return 0, errStrictProbeBudgetExceeded
		}
	}
	csvReader, err := csvparser.NewCSVParser(cfg, bufR, strictProbeBlockSize, false)
	if err != nil {
		return 0, err
	}
	validators := newFieldValidators(quietCtx, param, cols)
	var fields []csvparser.Field
	for attempts := 0; attempts < strictProbeMaxRecords; attempts++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		// Allow an initial fragment and the following complete candidate to
		// finish, even for long records. Stop between subsequent candidates
		// once physical reads (including read-ahead) reach the soft budget.
		if attempts >= 2 && limit-limited.remaining >= strictProbeSoftBytes {
			return 0, errStrictProbeBudgetExceeded
		}
		if err := attempt.nextRecord(); err != nil {
			return 0, err
		}
		fields, err = csvReader.Read(fields)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		if err != nil {
			return 0, err
		}
		if len(fields) < len(cols) {
			continue
		}
		legal := isLegalLineWithValidators(quietCtx, param, validators, fields)
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if legal {
			return csvReader.Pos() + skipCount, nil
		}
	}
	return 0, errStrictProbeBudgetExceeded
}

func isLegalLine(param *tree.ExternParam, cols []*plan.ColDef, fields []csvparser.Field) bool {
	ctx := moerr.Context()
	return isLegalLineWithValidators(ctx, param, newFieldValidators(ctx, param, cols), fields)
}

// fieldValidator checks the field of one column during the strict probe,
// with the parse the loader uses, resolved once per probe.
type fieldValidator struct {
	name            string
	enumValues      string
	id              types.T
	enumOrSet, trim bool
	emptyIsNull     bool
	valid           func(ctx context.Context, field csvparser.Field) bool
}

func newFieldValidators(ctx context.Context, param *tree.ExternParam, cols []*plan.ColDef) []fieldValidator {
	validators := make([]fieldValidator, len(cols))
	for i, col := range cols {
		id := types.T(col.Typ.Id)
		typ := types.New(id, col.Typ.Width, col.Typ.Scale)
		isSet := id == types.T_uint64 && col.Typ.Enumvalues != ""
		isString := id == types.T_char || id == types.T_varchar || id == types.T_binary || id == types.T_varbinary ||
			id == types.T_blob || id == types.T_text || id == types.T_datalink
		v := fieldValidator{
			name:       col.Name,
			enumValues: col.Typ.Enumvalues,
			id:         id,
			enumOrSet:  id == types.T_enum || isSet,
		}
		// T_bit carries raw bytes (parsed byte-by-byte, not as text), so
		// whitespace bytes are data and must not be trimmed.
		v.trim = !v.enumOrSet && !isString && id != types.T_json && id != types.T_bit
		v.emptyIsNull = !v.enumOrSet && !isString && id != types.T_json
		switch {
		case isSet:
			v.valid = func(ctx context.Context, field csvparser.Field) bool {
				_, err := types.ParseSetWithContext(ctx, col.Typ.Enumvalues, field.Val)
				return err == nil
			}
		case id == types.T_enum:
			v.valid = func(ctx context.Context, field csvparser.Field) bool {
				_, err := types.ParseEnumWithContext(ctx, col.Typ.Enumvalues, field.Val)
				return err == nil
			}
		case id == types.T_bit:
			width := col.Typ.Width
			v.valid = func(_ context.Context, field csvparser.Field) bool {
				_, ok := bitFieldValue(field.Val, width)
				return ok
			}
		case isString:
			v.valid = func(context.Context, csvparser.Field) bool { return true }
		case id == types.T_json && param.Format != tree.CSV:
			v.valid = func(context.Context, csvparser.Field) bool { return true }
		default:
			// isLegalLine is only used for file offset calculation in
			// parallel LOAD DATA, not for loading: timestamps are read in
			// time.Local since the session is not available.
			opts := types.TextParseOptions{Ctx: ctx, Loc: time.Local, AllowTruncation: true}
			parse, err := types.ParseAnyFunc(typ, opts)
			if err != nil {
				v.valid = func(context.Context, csvparser.Field) bool { return false }
				break
			}
			// unquoted YEAR text is a number (see types.TextParseOptions)
			opts.NumericYear = true
			parseUnquoted, _ := types.ParseAnyFunc(typ, opts)
			trimQuotes := id == types.T_json
			v.valid = func(_ context.Context, field csvparser.Field) bool {
				text := field.Val
				if trimQuotes {
					text = strings.Trim(text, "\"")
				}
				p := parseUnquoted
				if field.HasStringQuote {
					p = parse
				}
				_, err := p(text)
				return err == nil
			}
		}
		validators[i] = v
	}
	return validators
}

func isLegalLineWithValidators(ctx context.Context, param *tree.ExternParam, validators []fieldValidator, fields []csvparser.Field) bool {
	for idx := range validators {
		if ctx.Err() != nil {
			return false
		}
		v := &validators[idx]
		field := fields[idx]
		emptyIsNull := false
		if v.enumOrSet {
			field.Val, emptyIsNull = normalizeEnumSetCSVValueWithContext(ctx, v.id, v.enumValues, field.Val)
		} else if v.trim {
			field.Val = strings.TrimSpace(field.Val)
		}
		isNullOrEmpty := field.IsNull || getNullFlag(param.NullMap, v.name, field.Val) || emptyIsNull ||
			(v.emptyIsNull && len(field.Val) == 0)
		if isNullOrEmpty {
			continue
		}
		if !v.valid(ctx, field) {
			return false
		}
	}
	return true
}

func makeType(typ *plan.Type, flag bool) types.Type {
	if flag && !isDirectParallelLoadType(types.T(typ.Id)) {
		return types.New(types.T_varchar, 0, 0)
	}
	return types.MustTypeFromPlan(*typ)
}

// isDirectParallelLoadType identifies types that must be decoded by the
// external scan even when LOAD DATA is parallel.  Decoding vector values as
// varchar first retains both the CSV representation and the binary vector in
// the pipeline while the project casts the value.  Vectors are already parsed
// by getColData in the non-parallel path, so keeping their target type here
// avoids that duplicate large allocation.
// appendFieldText appends a field's raw text. This is where file input
// becomes a typed value, so a native Unicode column admits only valid text in
// its charset.
func appendFieldText(vec *vector.Vector, text string, mp *mpool.MPool) error {
	value := []byte(text)
	if err := types.ValidateCollationValue(*vec.GetType(), value); err != nil {
		return err
	}
	return vector.AppendBytes(vec, value, false, mp)
}

func isDirectParallelLoadType(id types.T) bool {
	return id == types.T_array_float32 || id == types.T_array_float64
}

// getRealAttrCnt counts the attributes that must be present as FIELDS in the
// record. Synthesized columns — __mo_filepath and the error-mode columns — are
// produced by the scan, not read from the record, so they must not inflate the
// expected field count.
func getRealAttrCnt(attrs []plan.ExternAttr, cols []*plan.ColDef) int {
	cnt := 0
	for i := 0; i < len(attrs); i++ {
		if catalog.ContainExternalHidenCol(attrs[i].ColName) {
			cnt++
			continue
		}
		var colId uint64
		if idx := int(attrs[i].ColIndex); idx < len(cols) && cols[idx] != nil {
			colId = cols[idx].ColId
		}
		if catalog.IsExternalErrorCol(attrs[i].ColName, colId) {
			cnt++
		}
	}
	return len(attrs) - cnt
}

func checkLineValidRestrictive(param *ExternalParam, proc *process.Process, line []csvparser.Field, rowIdx int) error {
	if param.ClusterTable != nil && param.ClusterTable.GetIsClusterTable() {
		//the column account_id of the cluster table do need to be filled here
		if len(line)+1 != getRealAttrCnt(param.Attrs, param.Cols) {
			return moerr.NewInvalidInputf(proc.Ctx, "the data of row %d contained is not equal to input columns", rowIdx+1)
		}
	} else {
		if param.Extern.ExternType == int32(plan.ExternType_EXTERNAL_TB) {
			if len(line) < getRealAttrCnt(param.Attrs, param.Cols) {
				return moerr.NewInvalidInputf(proc.Ctx, "the data of row %d contained is less than input columns", rowIdx+1)
			}
			return nil
		}
		if len(line) != int(param.ColumnListLen) {
			if len(line) != int(param.ColumnListLen)+1 {
				return moerr.NewInvalidInputf(proc.Ctx, "the data of row %d contained is not equal to input columns", rowIdx+1)
			}
			field := line[len(line)-1]
			if field.Val != "" {
				return moerr.NewInvalidInputf(proc.Ctx, "the data of row %d contained is not equal to input columns", rowIdx+1)
			}
		}
	}
	return nil
}

func checkLineStrict(param *ExternalParam) bool {
	if param.Extern.Local || !param.StrictSqlMode {
		return false
	}
	return true
}

// JsonBinaryKey is the single-key object wrapper JSONLINE OUTFILE uses to
// export BINARY/VARBINARY/BLOB values: {"$binary":"<base64>"}. The wrapper
// lets the jsonline loader distinguish MO-encoded binary payloads from plain-
// text JSON strings: wrapped values are base64-decoded, bare strings pass
// through byte-exact so historical plain-text files keep loading as-is
// (matrixone#30635).
const JsonBinaryKey = "$binary"

func getNullFlag(nullMap map[string][]string, attr, field string) bool {
	if nullMap == nil || len(nullMap[attr]) == 0 {
		return false
	}
	for _, v := range nullMap[attr] {
		if v == field {
			return true
		}
	}
	return false
}

// isEnumMember reports, without allocating, whether name is a declared member
// of the ENUM definition, using the same comma split and case folding as
// types.ParseEnum.
func isEnumMember(enumValues, name string) bool {
	// A raw padded legacy label wins before the trailing-space spelling.
	for {
		remaining := enumValues
		for {
			item, rest, more := strings.Cut(remaining, ",")
			if strings.EqualFold(item, name) {
				return true
			}
			if !more {
				break
			}
			remaining = rest
		}
		trimmed := strings.TrimRight(name, " ")
		if trimmed == name {
			return false
		}
		name = trimmed
	}
}

// isPaddedSetInput reports whether val has outer whitespace, as
// strings.TrimSpace (unicode.IsSpace) would remove it, and is already valid
// input for the SET definition, as native INSERT parses it. Trimming such a
// value could select a different member (' a' or '\u00a0a' vs 'a'); padded
// non-member input keeps the CSV trimming policy. This guard only protects
// member names; numeric fallback follows that existing CSV policy, including
// whitespace normalization. Unpadded values return early without parsing.
func isPaddedSetInput(setValues, val string) bool {
	return isPaddedSetInputWithContext(moerr.Context(), setValues, val)
}

func isPaddedSetInputWithContext(ctx context.Context, setValues, val string) bool {
	if len(val) == 0 {
		return false
	}
	first, _ := utf8.DecodeRuneInString(val)
	last, _ := utf8.DecodeLastRuneInString(val)
	if !unicode.IsSpace(first) && !unicode.IsSpace(last) {
		return false
	}
	return types.MatchSetNameWithContext(ctx, setValues, val)
}

// normalizeEnumSetCSVValue shares the member-aware trim and empty-to-NULL
// policy between row conversion and strict parallel-read boundary detection.
// Callers use it only for ENUM or uint64 with SET domain metadata, and apply
// explicit NULL markers to the returned text before parsing it.
func normalizeEnumSetCSVValue(id types.T, definition, val string) (text string, emptyIsNull bool) {
	return normalizeEnumSetCSVValueWithContext(moerr.Context(), id, definition, val)
}

func normalizeEnumSetCSVValueWithContext(ctx context.Context, id types.T, definition, val string) (text string, emptyIsNull bool) {
	if id == types.T_enum {
		if isEnumMember(definition, val) {
			return val, false
		}
		val = strings.TrimSpace(val)
		return val, len(val) == 0 && !isEnumMember(definition, "")
	}
	if !isPaddedSetInputWithContext(ctx, definition, val) {
		val = strings.TrimSpace(val)
	}
	return val, false
}

func shouldLoadEmptyNumericAsZero(param *ExternalParam, id types.T) bool {
	if param == nil || param.Extern == nil ||
		param.Extern.ExternType != int32(plan.ExternType_LOAD) ||
		(!param.ParallelLoad && !param.LoadEmptyNumericAsZero && !shouldApplyLoadDataNonStrictAdjustments(param)) {
		return false
	}
	if !param.ParallelLoad && !param.LoadEmptyNumericAsZero {
		return isLoadNumericAdjustedValueType(id)
	}
	return isLoadNumericZeroFillType(id)
}

func shouldApplyLoadDataNonStrictAdjustments(param *ExternalParam) bool {
	return param != nil && param.Extern != nil &&
		param.Extern.ExternType == int32(plan.ExternType_LOAD) &&
		param.Extern.Local &&
		param.Extern.Format == tree.CSV &&
		!param.StrictSqlMode
}

func isLoadNumericZeroFillType(id types.T) bool {
	return id == types.T_bool || isLoadNumericAdjustedValueType(id)
}

func isLoadNumericAdjustedValueType(id types.T) bool {
	return id.IsInteger() || id.IsFloat() || id.IsDecimal()
}

func loadDataNonStrictNumericPrefix(val string) string {
	val = strings.TrimSpace(val)
	if val == "" {
		return "0"
	}

	i := 0
	if val[i] == '+' || val[i] == '-' {
		i++
	}
	digitsStart := i
	for i < len(val) && val[i] >= '0' && val[i] <= '9' {
		i++
	}
	digits := i > digitsStart
	if i < len(val) && val[i] == '.' {
		i++
		fracStart := i
		for i < len(val) && val[i] >= '0' && val[i] <= '9' {
			i++
		}
		digits = digits || i > fracStart
	}
	if !digits {
		return "0"
	}

	end := i
	if i < len(val) && (val[i] == 'e' || val[i] == 'E') {
		exp := i + 1
		if exp < len(val) && (val[exp] == '+' || val[exp] == '-') {
			exp++
		}
		expDigits := exp
		for exp < len(val) && val[exp] >= '0' && val[exp] <= '9' {
			exp++
		}
		if exp > expDigits {
			end = exp
		}
	}
	return val[:end]
}

func truncateLoadDataStringValue(val string, width int32) string {
	if width <= 0 || utf8.RuneCountInString(val) <= int(width) {
		return val
	}
	return string([]rune(val)[:width])
}

// resolveExternalErrorMode decides, once per scan, whether the error-mode
// columns survived column pruning. An attribute absent from param.Attrs was
// pruned, so a query that does not mention these columns behaves exactly as
// before and pays nothing per row.
func resolveExternalErrorMode(param *ExternalParam) {
	mode := ExternalErrorMode{}
	for _, attr := range param.Attrs {
		var colId uint64
		if int(attr.ColIndex) < len(param.Cols) && param.Cols[attr.ColIndex] != nil {
			colId = param.Cols[attr.ColIndex].ColId
		}
		if catalog.IsExternalErrorToleranceCol(attr.ColName, colId) {
			mode.Tolerate = true
		}
		if attr.ColName == catalog.ExternalFileLine && colId == catalog.ExternalFileLineColId {
			mode.WantLine = true
		}
	}
	param.ErrorMode = mode
}

// isSynthesizedAttr reports whether the scan produces this column itself
// rather than reading it out of the record.
func isSynthesizedAttr(attr plan.ExternAttr, colId uint64, param *ExternalParam) bool {
	switch {
	case catalog.ContainExternalHidenCol(attr.ColName):
		return true
	case param.ForeignScan != nil && attr.ColName == catalog.ExternalQuery:
		return true
	case attr.ColName == catalog.ExternalFileLine && colId == catalog.ExternalFileLineColId:
		return true
	}
	if param.KafkaScan != nil {
		if _, ok := kafkaMetaField(attr.ColName, param); ok {
			return true
		}
	}
	return false
}

func getFieldFromLine(line []csvparser.Field, colName string, colId uint64, param *ExternalParam, fieldIdx int32) csvparser.Field {
	// Error-mode columns are synthesized, never read from the record. On a row
	// that parsed, both error columns are NULL; the tolerant path overwrites
	// them for a row that did not. __mo_file_line is position metadata and is
	// filled whether or not the row parsed.
	//
	// Scoped by the reserved ColId, not by name: a table created before these
	// names were reserved can have a REAL user column called
	// __mo_error_message, and it has to keep reading its own data.
	switch {
	case colName == catalog.ExternalFileLine && colId == catalog.ExternalFileLineColId:
		if param.KafkaScan != nil {
			// A Kafka record has no line in a file; __mo_message_id is what
			// identifies it.
			return csvparser.Field{IsNull: true}
		}
		return csvparser.Field{Val: strconv.FormatInt(param.ErrorMode.RecordLine, 10)}
	case colName == catalog.ExternalErrorMessage && colId == catalog.ExternalErrorMessageColId,
		colName == catalog.ExternalErrorText && colId == catalog.ExternalErrorTextColId:
		return csvparser.Field{IsNull: true}
	}
	// __mo_filepath is synthesized by name (pre-existing behavior); __mo_query
	// only on foreign scans, so a real __mo_query data column in a
	// pre-existing generic external table still reads source data.
	if catalog.ContainExternalHidenCol(colName) ||
		(param.ForeignScan != nil && colName == catalog.ExternalQuery) {
		return csvparser.Field{Val: param.Fileparam.Filepath}
	}
	if param.KafkaScan != nil {
		if f, ok := kafkaMetaField(colName, param); ok {
			return f
		}
	}
	return line[fieldIdx]
}

// getOneRowData materializes one record into the batch.
//
// Without error mode this is exactly the historical behaviour: the first
// conversion failure aborts the statement.
//
// With error mode (the query kept __mo_error_message or __mo_error_text) a
// record that cannot be materialized must not fail the query. Columns are
// appended one at a time, so a failure part-way leaves the batch's vectors at
// unequal lengths; the row is therefore rolled back to the lengths captured
// before it and re-emitted with every user column NULL and the error columns
// describing the failure.
func getOneRowData(proc *process.Process, bat *batch.Batch, line []csvparser.Field, rowIdx int, param *ExternalParam) error {
	if !param.ErrorMode.Tolerate {
		return materializeOneRow(proc, bat, line, rowIdx, param)
	}

	mode := &param.ErrorMode
	if cap(mode.rowLens) < len(bat.Vecs) {
		mode.rowLens = make([]int, len(bat.Vecs))
	}
	lens := mode.rowLens[:len(bat.Vecs)]
	for i, vec := range bat.Vecs {
		lens[i] = vec.Length()
	}

	err := materializeOneRow(proc, bat, line, rowIdx, param)
	if err == nil {
		return nil
	}
	for i, vec := range bat.Vecs {
		vec.SetLength(lens[i])
	}
	return appendErrorRow(proc, bat, line, rowIdx, param, err)
}

// appendErrorRow emits the replacement row for a record that failed to
// materialize: every user column NULL, __mo_filepath and __mo_file_line as
// usual, and the two error columns describing the failure.
func appendErrorRow(proc *process.Process, bat *batch.Batch, line []csvparser.Field, rowIdx int, param *ExternalParam, cause error) error {
	mp := param.outputPool(proc)
	message := cause.Error()
	text := recordText(line, param)
	for _, attr := range param.Attrs {
		vec := bat.Vecs[attr.ColIndex]
		var colId uint64
		if idx := int(attr.ColIndex); idx < len(param.Cols) && param.Cols[idx] != nil {
			colId = param.Cols[idx].ColId
		}
		switch {
		case attr.ColName == catalog.ExternalErrorMessage && colId == catalog.ExternalErrorMessageColId:
			if err := vector.AppendBytes(vec, []byte(message), false, mp); err != nil {
				return err
			}
		case attr.ColName == catalog.ExternalErrorText && colId == catalog.ExternalErrorTextColId:
			if err := vector.AppendBytes(vec, []byte(text), false, mp); err != nil {
				return err
			}
		case isSynthesizedAttr(attr, colId, param):
			// Position and source metadata -- __mo_filepath, __mo_file_line,
			// the Kafka message columns -- describe where the record came
			// from, which is exactly what a failed record needs to be found.
			// They are synthesized, so they do not depend on the record
			// having parsed; the ordinary column path fills them, typed as
			// the catalog declares them.
			if err := getColData(bat, line, rowIdx, param, mp, attr, proc); err != nil {
				return err
			}
		default:
			// A record that failed to parse has no trustworthy value for any
			// user column, so all of them are NULL — including the ones that
			// happened to convert before the failure.
			if err := vector.AppendBytes(vec, nil, true, mp); err != nil {
				return err
			}
		}
	}
	return nil
}

// recordText rebuilds the failed record as text for __mo_error_text. The
// parser hands back decoded fields, so the reconstruction re-joins them with
// the configured terminator: quoting and escaping are normalized rather than
// byte-identical to the file.
func recordText(line []csvparser.Field, param *ExternalParam) string {
	if param.ErrorMode.RawText != "" {
		return param.ErrorMode.RawText
	}
	sep := ","
	if param.Extern != nil && param.Extern.Tail != nil && param.Extern.Tail.Fields != nil &&
		param.Extern.Tail.Fields.Terminated != nil && param.Extern.Tail.Fields.Terminated.Value != "" {
		sep = param.Extern.Tail.Fields.Terminated.Value
	}
	var sb strings.Builder
	for i, field := range line {
		if i > 0 {
			sb.WriteString(sep)
		}
		if !field.IsNull {
			sb.WriteString(field.Val)
		}
	}
	return sb.String()
}

func materializeOneRow(proc *process.Process, bat *batch.Batch, line []csvparser.Field, rowIdx int, param *ExternalParam) error {
	mp := param.outputPool(proc)
	loaders := param.columnLoaders(bat, proc)
	// Kafka JSONL object conversion already checks the declared key count.
	// Its field slice contains only projected data positions, so its length
	// cannot be used as the record width. Synthetic attrs are filled by
	// getColData even when no data column is projected.
	kafkaJSON := param.KafkaScan != nil && param.Extern != nil && param.Extern.Format == tree.JSONLINE
	if checkLineStrict(param) {
		if !kafkaJSON {
			if err := checkLineValidRestrictive(param, proc, line, rowIdx); err != nil {
				return err
			}
		}
		for i, attr := range param.Attrs {
			if err := loaders[i].load(bat.Vecs[attr.ColIndex], line, rowIdx, param, proc, mp); err != nil {
				return err
			}
		}
		return nil
	}

	if kafkaJSON || int32(len(line)) >= param.ColumnListLen {
		for i, attr := range param.Attrs {
			if err := loaders[i].load(bat.Vecs[attr.ColIndex], line, rowIdx, param, proc, mp); err != nil {
				return err
			}
		}
		return nil
	}

	for i, attr := range param.Attrs {
		if attr.ColFieldIndex < int32(len(line)) {
			if err := loaders[i].load(bat.Vecs[attr.ColIndex], line, rowIdx, param, proc, mp); err != nil {
				return err
			}
			continue
		}
		vec := bat.Vecs[attr.ColIndex]
		if err := vector.AppendBytes(vec, nil, true, mp); err != nil {
			return err
		}
	}
	return nil
}

func loadTimeTextInput(param *ExternalParam, id types.T) bool {
	return id == types.T_time && param.Extern != nil &&
		param.Extern.ExternType == int32(plan.ExternType_LOAD) &&
		param.Extern.Format != tree.PARQUET && param.Extern.Format != tree.ARROW
}

// isAttributedDecimalStagingTarget reports a decimal target carrying the
// unsigned attribute mask — the type contract the text-staging decisions
// read at allocation, field decoding and mapper rendering.
func isAttributedDecimalStagingTarget(typ *plan.Type) bool {
	return typ != nil && types.T(typ.Id).IsDecimal() &&
		typ.NumericAttributes&types.NumericAttributeUnsigned != 0
}

// loadAttributedDecimalTextInput is the attributed DECIMAL text staging
// predicate: the shared admission boundary classifies raw field text
// (CSV/JSONLINE) or mapper-rendered text (Parquet/Arrow) for LOAD targets
// carrying the unsigned attribute. Unlike TIME's textual input, this covers
// every physical format.
func loadAttributedDecimalTextInput(param *ExternalParam, typ *plan.Type) bool {
	if param == nil || param.Extern == nil ||
		param.Extern.ExternType != int32(plan.ExternType_LOAD) {
		return false
	}
	return isAttributedDecimalStagingTarget(typ)
}

// jsonNumberBitLexeme converts the canonical decimal lexeme of a nonnegative
// integer JSON NUMBER token into that BIT value's canonical big-endian byte
// encoding (single 0x00 for zero, no leading zero bytes, at most 8 bytes).
// It returns false for everything else -- digits with leading zeros, signs,
// fractions, exponents, values above uint64, or values above the BIT width
// -- and the caller keeps the token text, preserving today's byte conversion
// values and errors bit-for-bit.
func jsonNumberBitLexeme(lexeme string, width int32) (string, bool) {
	if len(lexeme) == 0 || len(lexeme) > 20 {
		return "", false
	}
	for i := 0; i < len(lexeme); i++ {
		if lexeme[i] < '0' || lexeme[i] > '9' {
			return "", false
		}
	}
	if len(lexeme) > 1 && lexeme[0] == '0' {
		return "", false
	}
	value, err := strconv.ParseUint(lexeme, 10, 64)
	if err != nil {
		return "", false
	}
	// A BIT(64) target accepts the whole uint64 domain; narrower widths are
	// clamped so out-of-width tokens keep the pre-existing conversion path.
	if width > 0 && width < 64 && value > 1<<uint(width)-1 {
		return "", false
	}
	var buf [8]byte
	for i := 7; i >= 0; i-- {
		buf[i] = byte(value)
		value >>= 8
	}
	n := 8
	for n > 1 && buf[8-n] == 0 {
		n--
	}
	return string(buf[8-n:]), true
}

func loadFormatIsValid(param *tree.ExternParam) bool {
	switch param.Format {
	case tree.JSONLINE, tree.CSV, tree.PARQUET, tree.ARROW:
		return true
	}
	return false
}
