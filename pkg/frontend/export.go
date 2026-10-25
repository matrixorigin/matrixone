// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/parquet-go/parquet-go"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	util2 "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ExportConfig struct {
	// configs from user input
	userConfig *tree.ExportParam

	// curFileSize
	CurFileSize uint64
	hasCSVData  bool // True when this file has accepted non-empty CSV data bytes.
	Rows        uint64
	FileCnt     uint
	ColumnFlag  []bool
	Symbol      [][]byte
	// default flush size
	DefaultBufSize int64

	//file service & buffer for the line
	writeParam
	FileService fileservice.FileService
	Ctx         context.Context
	AsyncReader *io.PipeReader
	AsyncWriter *io.PipeWriter
	AsyncGroup  *errgroup.Group
	mrs         *MysqlResultSet
	ctx         context.Context
	service     string
	// proc borrows the current statement identity; export never owns or changes it.
	proc *process.Process

	// Parquet writer for parquet format export
	parquetWriter *ParquetWriter
	// The statement stops submissions before joining workers or clearing metadata.
	cancel          context.CancelFunc
	workers         sync.WaitGroup
	stopFileCancel  func() bool
	fileCancelDone  chan struct{}
	ownsFileService bool
}

type writeParam struct {
	First      bool
	Index      atomic.Int32
	WriteIndex atomic.Int32
	ByteChan   chan *BatchByte
	BatchMap   map[int32]*BatchByte
}

type BatchByte struct {
	index     int32
	writeByte []byte
	rowEnds   []int
	rowCount  int
	err       error
}

type CloseExportData struct {
	stopExportData chan interface{}
	onceClose      sync.Once
}

func NewCloseExportData() *CloseExportData {
	return &CloseExportData{
		stopExportData: make(chan interface{}),
	}
}

func (cld *CloseExportData) Open() {
}

func (cld *CloseExportData) Close() {
	cld.onceClose.Do(func() {
		close(cld.stopExportData)
	})
}

// needExportToFile checks needing to export into file or not
func (ec *ExportConfig) needExportToFile() bool {
	return ec != nil && ec.userConfig != nil && ec.userConfig.Outfile
}

func initExportFileParam(ep *ExportConfig, mrs *MysqlResultSet) {
	ep.DefaultBufSize *= 1024 * 1024
	n := (int)(mrs.GetColumnCount())
	if n <= 0 {
		return
	}
	ep.Symbol = make([][]byte, n)
	for i := 0; i < n-1; i++ {
		ep.Symbol[i] = []byte(ep.userConfig.Fields.Terminated.Value)
	}
	ep.Symbol[n-1] = []byte(ep.userConfig.Lines.TerminatedBy.Value)
	columnsSet := make(map[string]int)
	for i := 0; i < len(mrs.Columns); i++ {
		columnsSet[mrs.Columns[i].Name()] = i
	}
	ep.ColumnFlag = make([]bool, len(mrs.Columns))
	for i := 0; i < len(ep.userConfig.ForceQuote); i++ {
		col, ok := columnsSet[ep.userConfig.ForceQuote[i]]
		if ok {
			ep.ColumnFlag[col] = true
		}
	}
}

var openNewFile = func(ctx context.Context, ep *ExportConfig, mrs *MysqlResultSet) error {
	if err := ep.validateCSVFraming(ctx); err != nil {
		return err
	}
	if err := validateOutfileHeaderPrefix(ctx, ep, mrs); err != nil {
		return err
	}
	var err error
	var filePath string
	ep.CurFileSize = 0
	ep.hasCSVData = false
	// openNewFile is re-entered on file rotation via writeToCSVFile with
	// ep.ctx, and some callers (for example MODUMP query_result) only set Ctx.
	// A nil ctx must never reach errgroup.WithContext below.
	if ctx == nil {
		ctx = ep.Ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// For parquet format, we don't use pipe-based writing
	// Parquet data is accumulated in memory and written at the end
	if ep.getExportFormat() == "parquet" {
		ep.Rows = 0
		return nil
	}

	if len(ep.userConfig.StageFilePath) != 0 {
		filePath = getExportFilePath(ep.userConfig.StageFilePath, ep.FileCnt)
	} else {
		filePath = getExportFilePath(ep.userConfig.FilePath, ep.FileCnt)
	}

	// fileservice is determined from filepath
	// if filepath is SHARED:/path, return getGlobalPu().FileService, filepath
	// otherwise, return fileservice.GetForETL(ctx, nil, filepath)
	fspath, err := fileservice.ParsePath(filePath)
	if err != nil {
		return err
	}

	if err := process.AuthorizeSQLFileAccessPath(ep.proc, fspath); err != nil {
		return err
	}
	var readPath string
	if fspath.Service == defines.SharedFileServiceName {
		ep.FileService = getPu(ep.service).FileService
		readPath = filePath

	} else {
		ep.ownsFileService = true
		ep.FileService, readPath, err = fileservice.GetForETLWithParsedPath(process.SQLFileAccessContext(ctx, ep.proc), nil, filePath, fspath)
		if err != nil {
			return err
		}

	}

	reader, writer := io.Pipe()
	ep.AsyncReader, ep.AsyncWriter = reader, writer
	fs := ep.FileService
	// Capture this file's endpoints: a split may install another pair later.
	ep.fileCancelDone = make(chan struct{})
	cancelDone := ep.fileCancelDone
	ep.stopFileCancel = context.AfterFunc(ctx, func() {
		defer close(cancelDone)
		_ = reader.CloseWithError(ctx.Err())
		_ = writer.CloseWithError(ctx.Err())
	})
	ep.AsyncGroup, _ = errgroup.WithContext(ctx)
	ep.AsyncGroup.Go(func() error {
		err := fs.Write(ctx, fileservice.IOVector{FilePath: readPath, Entries: []fileservice.IOEntry{{ReaderForWrite: reader, Size: -1}}})
		// Release a blocked producer even when the sink fails before reading.
		_ = reader.CloseWithError(err)
		return err
	})

	// Only write CSV header for CSV format (not for jsonline or parquet)
	exportFormat := ep.userConfig.ExportFormat
	if exportFormat == "" {
		exportFormat = "csv"
	}
	if ep.userConfig.Header && exportFormat == "csv" {
		var header string
		n := len(mrs.Columns)
		if n == 0 {
			return nil
		}
		for i := 0; i < n-1; i++ {
			header += mrs.Columns[i].Name() + ep.userConfig.Fields.Terminated.Value
		}
		header += mrs.Columns[n-1].Name() + ep.userConfig.Lines.TerminatedBy.Value
		maxSize := getEffectiveMaxFileSize(ep)
		if maxSize != 0 && uint64(len(header)) >= maxSize {
			return moerr.NewInternalError(ctx, "the header line size is over the maxFileSize/splitSize")
		}
		if err := writeDataToCSVFile(ep, []byte(header)); err != nil {
			return err
		}
	}

	ep.Rows = 0
	return nil
}

// formatSpecifierRegex matches printf-style integer format specifiers like %d, %5d, %05d
var formatSpecifierRegex = regexp.MustCompile(`%[0-9]*d`)

// getExportFilePath returns the file path for export, supporting printf-style format specifiers.
// If filename contains a format specifier like %d or %05d, it uses fmt.Sprintf to format the fileCnt.
// Otherwise, it falls back to legacy behavior: append ".N" suffix for fileCnt > 0.
func getExportFilePath(filename string, fileCnt uint) string {
	// Check if filename contains a valid format specifier
	if formatSpecifierRegex.MatchString(filename) {
		return fmt.Sprintf(filename, fileCnt)
	}
	// Legacy behavior
	if fileCnt == 0 {
		return filename
	}
	return fmt.Sprintf("%s.%d", filename, fileCnt)
}

// checkCSVValueRoundTrip rejects the value shapes LOAD can no longer tell
// apart from framing once the escape byte is not the historical backslash:
//
//   - A field of exactly `\N` matches LOAD's NULL token list (always ['\N'])
//     after unescaping, even inside quotes (quotedNullIsText is false), so a
//     literal \N string would silently come back as SQL NULL whenever
//     FIELDS ESCAPED BY is set to a custom or empty byte.
//   - LOAD's unescape translates the second byte of an escape pair from a
//     fixed letter table ({'0','b','n','r','t','Z'} -> control character,
//     everything else literal), whatever the configured escape byte is. When
//     the escape byte itself is one of those letters, the writer's doubling
//     (e.g. 'banana' with ESCAPED BY 'n' -> 'bannanna') decodes to a control
//     character instead of the letter: no encoding of such a value exists,
//     so it must be rejected. 'N' is exempt here (N+N stays literal N); it
//     breaks only the NULL marker, which formatOutputNull guards.
//   - With escaping disabled, the reader strips a trailing CR from the last
//     column's record when its (CRLF-normalized) terminator is '\n'
//     (csv_parser.go). That strip runs on the record buffer AFTER quotes are
//     removed, so an enclosure does not protect the CR; only an escape byte
//     (which rewrites it to a two-byte sequence the reader translates back)
//     does. Other terminators never trigger the strip.
//
// All three checks make the export produce either a file that reads back
// exactly or an upfront error — never silent corruption.
func checkCSVValueRoundTrip(oq *ExportConfig, tmp []byte, last bool) error {
	esc := oq.escapeChar()
	if esc != '\\' {
		if bytes.Equal(tmp, []byte(`\N`)) {
			return moerr.NewNotSupported(oq.ctx,
				"select into outfile (csv): a value of exactly \\N cannot round-trip unless FIELDS ESCAPED BY is the default '\\\\'")
		}
		if isAmbiguousCSVEscape(esc) && esc != 'N' && bytes.IndexByte(tmp, esc) >= 0 {
			return moerr.NewNotSupportedf(oq.ctx,
				"select into outfile (csv): a value containing '%c' cannot round-trip with FIELDS ESCAPED BY '%c': LOAD translates the doubled escape back to a control character", esc, esc)
		}
	}
	_, lineTerm, _, _ := oq.csvBoundaryBytes()
	if last && esc == 0 && lineTerm == '\n' && bytes.HasSuffix(tmp, []byte{'\r'}) {
		return moerr.NewNotSupported(oq.ctx,
			"select into outfile (csv): with FIELDS ESCAPED BY '' a value ending in CR in the last column cannot round-trip")
	}
	return nil
}

var formatOutputString = func(oq *ExportConfig, tmp, symbol []byte, enclosed byte, flag bool, last bool, buffer *bytes.Buffer) error {
	if err := checkCSVValueRoundTrip(oq, tmp, last); err != nil {
		return err
	}
	if !flag && enclosed != 0 && oq.escapeChar() != 0 {
		field, line, _, _ := oq.csvBoundaryBytes()
		// LOAD translates \n, \0, etc., and recognizes \N as NULL. An
		// ambiguous literal terminator or enclosure cannot be protected by
		// prefixing it; enclose the value instead, so addEscapeToString can
		// double the enclosure byte without creating a translated escape.
		// A separator beginning with ESCAPED BY also collides with bytes
		// introduced by encoding (e.g. CR becomes backslash-r), not just with
		// literal separator bytes in tmp. Frame the value before escaping it.
		// When enclosure and escape coincide, any generated escape (even a
		// leading CR's backslash-r) would otherwise open an unquoted field.
		flag = enclosed == oq.escapeChar() || field == oq.escapeChar() || line == oq.escapeChar() ||
			(isAmbiguousCSVEscape(field) && bytes.IndexByte(tmp, field) >= 0) ||
			(isAmbiguousCSVEscape(line) && bytes.IndexByte(tmp, line) >= 0) ||
			(isAmbiguousCSVEscape(enclosed) && bytes.IndexByte(tmp, enclosed) >= 0)
	}
	quote := byte(0)
	if flag {
		quote = enclosed
	}
	return formatOutputStringRaw(oq, addEscapeToString(oq, tmp, quote), symbol, enclosed, flag, buffer)
}

// NULL bypasses value escaping, but still needs enclosure when the escape
// prefix would otherwise become a separator or opening enclosure. Keep this
// check off the ordinary non-NULL raw writer path.
func formatOutputNull(oq *ExportConfig, symbol []byte, enclosed byte, buffer *bytes.Buffer) error {
	// The NULL marker always ends in 'N'. With FIELDS ESCAPED BY 'N' the
	// reader consumes the byte after the marker (separator or terminator)
	// as escape content, merging fields/records — no safe encoding exists,
	// so reject before writing a corrupt file.
	if oq.escapeChar() == 'N' {
		return moerr.NewNotSupported(oq.ctx,
			"select into outfile (csv): a NULL value cannot round-trip with FIELDS ESCAPED BY 'N'")
	}
	flag := false
	if enclosed != 0 && oq.escapeChar() != 0 {
		field, line, _, _ := oq.csvBoundaryBytes()
		flag = enclosed == oq.escapeChar() || field == oq.escapeChar() || line == oq.escapeChar()
	}
	return formatOutputStringRaw(oq, []byte{'\\', 'N'}, symbol, enclosed, flag, buffer)
}

// formatOutputStringRaw is also used for the genuine NULL marker. All non-NULL
// values go through formatOutputString, independently of their SQL type.
func formatOutputStringRaw(oq *ExportConfig, tmp, symbol []byte, enclosed byte, flag bool, buffer *bytes.Buffer) error {
	var err error
	if flag && enclosed != 0 {
		if _, err = buffer.Write([]byte{enclosed}); err != nil {
			return err
		}
	}
	if _, err = buffer.Write(tmp); err != nil {
		return err
	}
	if flag && enclosed != 0 {
		if _, err = buffer.Write([]byte{enclosed}); err != nil {
			return err
		}
	}
	if _, err = buffer.Write(symbol); err != nil {
		return err
	}
	return nil
}

// Close completes one file, including a split, without stopping conversion.
var Close = func(ep *ExportConfig) error {
	ep.FileCnt++
	return ep.closeFile(nil)
}

// Preserve the primary error's concrete type: transaction policy and MySQL
// error-code consumers still use direct *moerr.Error assertions.
func exportCleanupError(primary, cleanup error) error {
	if primary == nil {
		return cleanup
	}
	if cleanup != nil && !errors.Is(primary, cleanup) && !errors.Is(cleanup, io.ErrClosedPipe) && !errors.Is(cleanup, context.Canceled) {
		logutil.Warn("secondary export cleanup failure", zap.Error(cleanup), zap.NamedError("primary", primary))
	}
	return primary
}

func (ec *ExportConfig) closeFile(cause error) error {
	var err error
	if ec.AsyncWriter != nil {
		if cause != nil {
			err = ec.AsyncWriter.CloseWithError(cause)
			if ec.AsyncReader != nil {
				err = exportCleanupError(err, ec.AsyncReader.CloseWithError(cause))
			}
		} else {
			err = ec.AsyncWriter.Close()
		}
	}
	if ec.AsyncGroup != nil {
		err = exportCleanupError(err, ec.AsyncGroup.Wait())
	}
	if ec.AsyncReader != nil {
		err = exportCleanupError(err, ec.AsyncReader.Close())
	}
	if ec.stopFileCancel != nil && !ec.stopFileCancel() {
		<-ec.fileCancelDone
	}
	if ec.ownsFileService && ec.FileService != nil {
		ec.FileService.Close(context.Background())
	}
	ec.AsyncReader, ec.AsyncWriter, ec.AsyncGroup = nil, nil, nil
	ec.stopFileCancel, ec.fileCancelDone = nil, nil
	ec.FileService = nil
	ec.ownsFileService = false
	return err
}

var Write = func(ep *ExportConfig, output []byte) (int, error) {
	n, err := ep.AsyncWriter.Write(output)
	if err != nil {
		// Closing our own endpoint can surface ErrClosedPipe instead of the cause.
		if errors.Is(err, io.ErrClosedPipe) && ep.ctx != nil && ep.ctx.Err() != nil {
			err = ep.ctx.Err()
		}
		_ = ep.AsyncWriter.CloseWithError(err)
	}
	return n, err
}

// getEffectiveMaxFileSize returns the effective max file size for splitting.
// SplitSize takes precedence over MaxFileSize if both are set.
// SplitSize is in bytes, MaxFileSize is in bytes (already converted from KB in parser).
func getEffectiveMaxFileSize(ep *ExportConfig) uint64 {
	if ep.userConfig.SplitSize != 0 {
		return ep.userConfig.SplitSize
	}
	return ep.userConfig.MaxFileSize
}

// writeToCSVFile function may create a new file. Make sure the output buffer contains the complete CSV row to keep the CSV parser happy.
func writeToCSVFile(ep *ExportConfig, output []byte) error {
	maxSize := getEffectiveMaxFileSize(ep)
	isCSV := ep.getExportFormat() == "csv"
	if maxSize != 0 && exceedsFileSize(ep.CurFileSize, uint64(len(output)), maxSize) &&
		(!isCSV || (ep.hasCSVData && len(output) > 0)) {
		if err := Close(ep); err != nil {
			return err
		}
		if err := openNewFile(ep.ctx, ep, ep.mrs); err != nil {
			return err
		}
	}

	if err := writeDataToCSVFile(ep, output); err != nil {
		return err
	}
	if isCSV && len(output) > 0 {
		ep.hasCSVData = true
	}
	return nil
}

func exceedsFileSize(currentSize, additionalSize, maxSize uint64) bool {
	return currentSize > maxSize || additionalSize > maxSize-currentSize
}

func writeExportBatchToFile(ep *ExportConfig, batch *BatchByte) error {
	if ep.getExportFormat() != "csv" {
		return writeToCSVFile(ep, batch.writeByte)
	}
	if batch.rowCount == 0 {
		return nil
	}

	maxSize := getEffectiveMaxFileSize(ep)
	if maxSize == 0 {
		if err := writeToCSVFile(ep, batch.writeByte); err != nil {
			return err
		}
		ep.Rows += uint64(batch.rowCount)
		return nil
	}

	chunkStart, chunkEnd, chunkRows := 0, 0, 0
	flushChunk := func() error {
		if chunkRows == 0 {
			return nil
		}
		if err := writeToCSVFile(ep, batch.writeByte[chunkStart:chunkEnd]); err != nil {
			return err
		}
		ep.Rows += uint64(chunkRows)
		chunkStart = chunkEnd
		chunkRows = 0
		return nil
	}

	for _, rowEnd := range batch.rowEnds {
		if chunkRows == 0 || exceedsFileSize(ep.CurFileSize, uint64(rowEnd-chunkStart), maxSize) {
			if err := flushChunk(); err != nil {
				return err
			}
			rowSize := uint64(rowEnd - chunkStart)
			if rowSize > 0 && ep.hasCSVData && exceedsFileSize(ep.CurFileSize, rowSize, maxSize) {
				if err := Close(ep); err != nil {
					return err
				}
				if err := openNewFile(ep.ctx, ep, ep.mrs); err != nil {
					return err
				}
			}
		}
		chunkEnd = rowEnd
		chunkRows++
	}
	return flushChunk()
}

var writeDataToCSVFile = func(ep *ExportConfig, output []byte) error {

	for {
		if n, err := Write(ep, output); err != nil {
			return err
		} else if n == len(output) {
			break
		}
	}
	ep.CurFileSize += uint64(len(output))
	return nil
}

func escapeJSONControlChars(s string) string {
	var builder strings.Builder
	builder.Grow(len(s))

	// \\t
	// \\\\t
	// abc\t
	// {"a": "abc", "b": "abc\t"}
	for i := 0; i < len(s); {
		if i+2 > len(s) {
			builder.WriteString(s[i:])
			break
		}
		switch s[i : i+2] {
		case `\t`:
			i += 2
			builder.WriteString(`\\t`)
		case `\n`:
			i += 2
			builder.WriteString(`\\n`)
		case `\r`:
			i += 2
			builder.WriteString(`\\r`)
		case `\f`:
			i += 2
			builder.WriteString(`\\f`)
		case `\\`:
			i += 2
			builder.WriteString(`\\\\`)
		default:
			builder.WriteString(s[i : i+1])
			i++
		}
	}

	return builder.String()
}

func sendExportBatchByte(
	ctx context.Context,
	byteChan chan *BatchByte,
	value *BatchByte,
) bool {
	select {
	case byteChan <- value:
		return true
	case <-ctx.Done():
		return false
	}
}

func constructByte(ctx context.Context, obj FeSession, bat *batch.Batch, index int32, ByteChan chan *BatchByte, ep *ExportConfig) {
	var (
		ok      bool
		backSes *backSession
		ss      *Session
		mp      *mpool.MPool
	)

	if ss, ok = obj.(*Session); !ok {
		backSes = obj.(*backSession)
		mp = backSes.GetMemPool()
	} else {
		mp = ss.GetMemPool()
	}
	defer bat.Clean(mp)
	if ctx.Err() != nil {
		return
	}

	symbol := ep.Symbol
	closeby := ep.userConfig.Fields.EnclosedBy.Value
	flag := ep.ColumnFlag

	// LINES STARTING BY prefixes every data record (and only data records:
	// the CSV header stays unprefixed so the reader's startingBy line filter
	// skips it, mirroring the externalwrite writer and LOAD's readRecord).
	var startingBy []byte
	if l := ep.userConfig.Lines; l != nil && l.StartingBy != "" {
		startingBy = []byte(l.StartingBy)
	}

	buffer := &bytes.Buffer{}

	// formatOutputString can now reject a value LOAD would no longer read
	// back losslessly; record the first failure and abort the batch on it
	// instead of writing a corrupt file.
	var fmtErr error
	out := func(tmp, sym []byte, enclosed byte, quote bool, last bool) {
		if fmtErr != nil {
			return
		}
		fmtErr = formatOutputString(ep, tmp, sym, enclosed, quote, last, buffer)
	}

	rowCount := bat.RowCount()
	var rowEnds []int
	if getEffectiveMaxFileSize(ep) != 0 {
		rowEnds = make([]int, 0, rowCount)
	}
	for i := 0; i < rowCount; i++ {
		if len(startingBy) > 0 {
			buffer.Write(startingBy)
		}
		for j, vec := range bat.Vecs {
			if vec.GetNulls().Contains(uint64(i)) {
				if fmtErr == nil {
					fmtErr = formatOutputNull(ep, symbol[j], closeby, buffer)
				}
				continue
			}
			switch vec.GetType().Oid { //get col
			case types.T_json:
				val := types.DecodeJson(vec.GetBytesAt(i))
				value := []byte(val.String())
				out(value, symbol[j], closeby, true, j == len(bat.Vecs)-1)
			case types.T_bool:
				val := vector.GetFixedAtNoTypeCheck[bool](vec, i)
				if val {
					out([]byte("true"), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
				} else {
					out([]byte("false"), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
				}
			case types.T_bit:
				val := vector.GetFixedAtNoTypeCheck[uint64](vec, i)
				bitLength := vec.GetType().Width
				byteLength := (bitLength + 7) / 8
				b := types.EncodeUint64(&val)[:byteLength]
				slices.Reverse(b)
				out(b, symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_int8:
				val := vector.GetFixedAtNoTypeCheck[int8](vec, i)
				out([]byte(strconv.FormatInt(int64(val), 10)), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_int16:
				val := vector.GetFixedAtNoTypeCheck[int16](vec, i)
				out([]byte(strconv.FormatInt(int64(val), 10)), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_int32:
				val := vector.GetFixedAtNoTypeCheck[int32](vec, i)
				out([]byte(strconv.FormatInt(int64(val), 10)), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_int64:
				val := vector.GetFixedAtNoTypeCheck[int64](vec, i)
				out([]byte(strconv.FormatInt(int64(val), 10)), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_uint8:
				val := vector.GetFixedAtNoTypeCheck[uint8](vec, i)
				out([]byte(strconv.FormatUint(uint64(val), 10)), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_uint16:
				val := vector.GetFixedAtNoTypeCheck[uint16](vec, i)
				out([]byte(strconv.FormatUint(uint64(val), 10)), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_uint32:
				val := vector.GetFixedAtNoTypeCheck[uint32](vec, i)
				out([]byte(strconv.FormatUint(uint64(val), 10)), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_uint64:
				val := vector.GetFixedAtNoTypeCheck[uint64](vec, i)
				out([]byte(strconv.FormatUint(uint64(val), 10)), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_float32:
				val := vector.GetFixedAtNoTypeCheck[float32](vec, i)
				if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
					out([]byte(strconv.FormatFloat(float64(val), 'f', -1, 32)), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
				} else {
					out([]byte(strconv.FormatFloat(float64(val), 'f', int(vec.GetType().Scale), 64)), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
				}
			case types.T_float64:
				val := vector.GetFixedAtNoTypeCheck[float64](vec, i)
				if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
					out([]byte(strconv.FormatFloat(val, 'f', -1, 64)), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
				} else {
					out([]byte(strconv.FormatFloat(float64(val), 'f', int(vec.GetType().Scale), 64)), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
				}
			case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
				value := vec.GetBytesAt(i)
				out(value, symbol[j], closeby, true, j == len(bat.Vecs)-1)
			case types.T_geometry, types.T_geometry32:
				text, err := planfunction.GeometryPayloadToText(vec.GetBytesAt(i))
				if err != nil {
					sendExportBatchByte(ctx, ByteChan, &BatchByte{err: err})
					return
				}
				value := []byte(text)
				out(value, symbol[j], closeby, true, j == len(bat.Vecs)-1)
			case types.T_array_float32:
				arrStr := types.BytesToArrayToString[float32](vec.GetBytesAt(i))
				value := util2.UnsafeStringToBytes(arrStr)
				out(value, symbol[j], closeby, true, j == len(bat.Vecs)-1)
			case types.T_array_float64:
				arrStr := types.BytesToArrayToString[float64](vec.GetBytesAt(i))
				value := util2.UnsafeStringToBytes(arrStr)
				out(value, symbol[j], closeby, true, j == len(bat.Vecs)-1)
			case types.T_array_bf16:
				arrStr := types.BytesToArrayToString[types.BF16](vec.GetBytesAt(i))
				value := util2.UnsafeStringToBytes(arrStr)
				out(value, symbol[j], closeby, true, j == len(bat.Vecs)-1)
			case types.T_array_float16:
				arrStr := types.BytesToArrayToString[types.Float16](vec.GetBytesAt(i))
				value := util2.UnsafeStringToBytes(arrStr)
				out(value, symbol[j], closeby, true, j == len(bat.Vecs)-1)
			case types.T_array_int8:
				arrStr := types.BytesToArrayToString[int8](vec.GetBytesAt(i))
				value := util2.UnsafeStringToBytes(arrStr)
				out(value, symbol[j], closeby, true, j == len(bat.Vecs)-1)
			case types.T_array_uint8:
				// []uint8 aliases []byte; BytesToArrayToString[uint8] renders the values
				// as an array string ("[1, 2, 3]"), not the raw bytes as a string.
				arrStr := types.BytesToArrayToString[uint8](vec.GetBytesAt(i))
				value := util2.UnsafeStringToBytes(arrStr)
				out(value, symbol[j], closeby, true, j == len(bat.Vecs)-1)
			case types.T_date:
				val := vector.GetFixedAtNoTypeCheck[types.Date](vec, i)
				out([]byte(val.String()), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_datetime:
				scale := vec.GetType().Scale
				val := vector.GetFixedAtNoTypeCheck[types.Datetime](vec, i).String2(scale)
				out([]byte(val), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_time:
				scale := vec.GetType().Scale
				val := vector.GetFixedAtNoTypeCheck[types.Time](vec, i).String2(scale)
				out([]byte(val), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_timestamp:
				var timeZone *time.Location
				if ss != nil {
					timeZone = ss.GetTimeZone()
				} else {
					timeZone = backSes.GetTimeZone()
				}
				scale := vec.GetType().Scale
				val := vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, i).String2(timeZone, scale)
				out([]byte(val), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_year:
				val := vector.GetFixedAtNoTypeCheck[types.MoYear](vec, i).String()
				out([]byte(val), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_decimal64:
				scale := vec.GetType().Scale
				val := vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, i).Format(scale)
				out([]byte(val), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_decimal128:
				scale := vec.GetType().Scale
				val := vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, i).Format(scale)
				out([]byte(val), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_decimal256:
				scale := vec.GetType().Scale
				val := vector.GetFixedAtNoTypeCheck[types.Decimal256](vec, i).Format(scale)
				out([]byte(val), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_uuid:
				val := vector.GetFixedAtNoTypeCheck[types.Uuid](vec, i).String()
				out([]byte(val), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_Rowid:
				val := vector.GetFixedAtNoTypeCheck[types.Rowid](vec, i)
				out([]byte(val.String()), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_Blockid:
				val := vector.GetFixedAtNoTypeCheck[types.Blockid](vec, i)
				out([]byte(val.String()), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			case types.T_enum:
				val := vector.GetFixedAtNoTypeCheck[types.Enum](vec, i).String()
				out([]byte(val), symbol[j], closeby, flag[j], j == len(bat.Vecs)-1)
			default:
				if ss != nil {
					ss.Error(ctx,
						"Failed to construct byte due to unsupported type",
						zap.Int("typeOid", int(vec.GetType().Oid)))
				} else {
					backSes.Error(ctx,
						"Failed to construct byte due to unsupported type",
						zap.Int("typeOid", int(vec.GetType().Oid)))
				}

				// stop early if downstream already failed
				sendExportBatchByte(ctx, ByteChan, &BatchByte{
					err: moerr.NewInternalErrorf(ctx, "constructByte : unsupported type %d", vec.GetType().Oid),
				})
				return
			}
		}
		if fmtErr != nil {
			sendExportBatchByte(ctx, ByteChan, &BatchByte{err: fmtErr})
			return
		}
		if rowEnds != nil {
			rowEnds = append(rowEnds, buffer.Len())
		}
	}

	// copy data. byteBuffer.Bytes() is not able to pass to channel
	reslen := buffer.Len()
	result := make([]byte, reslen)
	copy(result, buffer.Bytes())
	buffer = nil

	if !sendExportBatchByte(ctx, ByteChan, &BatchByte{
		index:     index,
		writeByte: result,
		rowEnds:   rowEnds,
		rowCount:  rowCount,
		err:       nil,
	}) {
		return
	}

}

// escapeChar defaults to backslash; a configured zero disables prefix escaping.
// Doubling an enclosure is independent of prefix escaping.
func (ec *ExportConfig) escapeChar() byte {
	if ec == nil || ec.userConfig == nil {
		return '\\'
	}
	if f := ec.userConfig.Fields; f != nil && f.EscapedBy != nil {
		return f.EscapedBy.Value
	}
	return '\\'
}

// csvBoundaryBytes returns the first bytes of the configured separators and
// enclosure. The enclosure must also be protected in bare values (e.g. BIT),
// since a leading enclosure would otherwise be read as framing. For unenclosed
// output also protect LOAD's default double quote: its current reader falls
// back to that quote even for ENCLOSED BY ”. Prefixing it is valid with both
// an unenclosed MySQL reader and MatrixOne's default reader.
func (ec *ExportConfig) csvBoundaryBytes() (field, line, quote byte, defaultLine bool) {
	fieldTerm := ","
	lineTerm := "\n"
	quote = '"'
	if ec != nil && ec.userConfig != nil {
		if f := ec.userConfig.Fields; f != nil && f.Terminated != nil && f.Terminated.Value != "" {
			fieldTerm = f.Terminated.Value
		}
		if f := ec.userConfig.Fields; f != nil && f.EnclosedBy != nil && f.EnclosedBy.Value != 0 {
			quote = f.EnclosedBy.Value
		}
		if l := ec.userConfig.Lines; l != nil && l.TerminatedBy != nil && l.TerminatedBy.Value != "" {
			lineTerm = l.TerminatedBy.Value
		}
	}
	switch lineTerm {
	case "\n", "\r\n", "":
		return fieldTerm[0], '\n', quote, true
	default:
		return fieldTerm[0], lineTerm[0], quote, false
	}
}

// isAmbiguousCSVEscape includes translated bytes and N, which would turn a
// non-NULL value consisting of a single N into LOAD's escaped NULL marker.
func isAmbiguousCSVEscape(b byte) bool {
	switch b {
	case '0', 'b', 'n', 'r', 't', 'Z', 'N':
		return true
	default:
		return false
	}
}

func (ec *ExportConfig) validateCSVFraming(ctx context.Context) error {
	if ec.getExportFormat() != "csv" {
		return nil
	}
	field, line, quote, defaultLine := ec.csvBoundaryBytes()
	// LOAD recognizes both CR and LF in default/LF/normalized-CRLF mode,
	// even when CR is absent from the configured terminator. Neither can be
	// an enclosure; retain physical-prefix checks for other terminators too.
	physicalLine := line
	if l := ec.userConfig.Lines; l != nil && l.TerminatedBy != nil && l.TerminatedBy.Value != "" {
		physicalLine = l.TerminatedBy.Value[0]
	}
	noQuote := ec.userConfig.Fields == nil || ec.userConfig.Fields.EnclosedBy == nil || ec.userConfig.Fields.EnclosedBy.Value == 0
	if !noQuote && (quote == field || quote == line || quote == physicalLine || defaultLine && quote == '\r') {
		// LOAD may treat framing as a record boundary or a doubled enclosure.
		// Reject before opening a file, even with prefix escapes disabled:
		// enclosure doubling is independent of ESCAPED BY.
		return moerr.NewInvalidInput(ctx,
			"CSV ENCLOSED BY must not be the first byte of a field or line terminator or another recognized newline byte")
	}
	// Mirror the LOAD parser's config check: a prefix containing the record
	// terminator makes record boundaries ambiguous (csvparser normalizes CRLF
	// to LF the same way). Written output would never read back, so reject
	// before opening the file.
	if l := ec.userConfig.Lines; l != nil && l.StartingBy != "" && l.TerminatedBy != nil {
		terminator := l.TerminatedBy.Value
		if terminator == "\r\n" {
			terminator = "\n"
		}
		if terminator != "" && strings.Contains(l.StartingBy, terminator) {
			return moerr.NewInvalidInputNoCtx(fmt.Sprintf("STARTING BY '%s' cannot contain LINES TERMINATED BY '%s'", l.StartingBy, terminator))
		}
	}
	if ec.escapeChar() == 0 {
		return nil
	}
	if noQuote && (isAmbiguousCSVEscape(field) || isAmbiguousCSVEscape(line) || field == ec.escapeChar() || line == ec.escapeChar() || physicalLine == ec.escapeChar()) {
		return moerr.NewInvalidInput(ctx,
			"CSV terminators starting with ESCAPED BY or an ambiguous escape character (0, b, n, r, t, Z, N) require a nonempty ENCLOSED BY")
	}
	return nil
}

// validateOutfileHeaderPrefix applies LOAD's startingBy line filter to the
// CSV header before the file is opened. The reader keeps any line containing
// the prefix (dropping everything up to its first occurrence); only lines
// without it are skipped wholesale. The header line is written unprefixed on
// purpose below so the filter skips it — but if the raw header text happens
// to CONTAIN the prefix (for example a column named `name` with
// LINES STARTING BY 'n'), the filter instead keeps the mangled remainder as
// a data row. Reject that configuration up front: the alternatives are
// HEADER 'false' or a prefix that does not occur in the header.
func validateOutfileHeaderPrefix(ctx context.Context, ep *ExportConfig, mrs *MysqlResultSet) error {
	if ep.getExportFormat() != "csv" || !ep.userConfig.Header || mrs == nil {
		return nil
	}
	l := ep.userConfig.Lines
	if l == nil || l.StartingBy == "" {
		return nil
	}
	n := len(mrs.Columns)
	if n == 0 {
		return nil
	}
	var header strings.Builder
	for i := 0; i < n-1; i++ {
		header.WriteString(mrs.Columns[i].Name())
		header.WriteString(ep.userConfig.Fields.Terminated.Value)
	}
	header.WriteString(mrs.Columns[n-1].Name())
	if strings.Contains(header.String(), l.StartingBy) {
		return moerr.NewInvalidInputf(ctx,
			"LINES STARTING BY '%s' occurs in the CSV header: LOAD would import the remainder of the header as data; set HEADER 'false' or choose a prefix that does not occur in the header",
			l.StartingBy)
	}
	return nil
}

// addEscapeToString makes a value survive the CSV writer + reader round-trip.
//
// enclosed != 0: the value is written enclosed, so the terminators inside it
// are already protected by the enclosure bytes; the escape character and the
// enclosure byte itself are doubled, which the reader collapses back.
// When both bytes coincide, a literal escape needs four copies: LOAD collapses
// doubled enclosures before processing escape sequences.
//
// enclosed == 0: the value is written bare, so every byte that would
// otherwise terminate the field or the record (see csvBoundaryBytes) is
// prefixed with the escape character - the MySQL FIELDS ESCAPED BY
// convention, which LOAD DATA already unescapes. A zero escape byte disables
// prefix escaping (enclosure doubling is still possible).
func addEscapeToString(ep *ExportConfig, s []byte, enclosed byte) []byte {
	escape := ep.escapeChar()
	if escape == 0 && enclosed == 0 {
		return s
	}
	if escape != 0 && enclosed == 0 && bytes.Equal(s, []byte("NULL")) {
		// LOAD's default enclosure mode treats bare NULL as SQL NULL. Escape
		// its first N (not a translated escape) to retain literal text/BIT bytes.
		result := make([]byte, len(s)+1)
		result[0] = escape
		copy(result[1:], s)
		return result
	}
	field, line, quote, defaultLine := ep.csvBoundaryBytes()
	needsEscape := func(b byte) bool {
		return (escape != 0 && b == escape) || (enclosed != 0 && b == enclosed) ||
			(escape != 0 && defaultLine && b == '\r') ||
			(enclosed == 0 && (b == field || b == line || (quote != 0 && b == quote)))
	}
	extra := 0
	for _, b := range s {
		if needsEscape(b) {
			if enclosed != 0 && enclosed == escape && b == escape {
				// LOAD first collapses doubled enclosures, then unescapes.
				// Four bytes retain one literal escape through both stages.
				extra += 3
			} else if enclosed != 0 && enclosed == escape && defaultLine && b == '\r' {
				// A generated escape also crosses enclosure parsing. Double its
				// prefix so a following 'r' separator cannot close the field.
				extra += 2
			} else {
				extra++
			}
		}
	}
	if extra == 0 {
		return s // do not allocate/copy ordinary values
	}
	result := make([]byte, 0, len(s)+extra)
	for _, b := range s {
		if needsEscape(b) {
			if enclosed != 0 && enclosed == escape && b == escape {
				result = append(result, escape, escape, escape)
			} else if enclosed != 0 && b == enclosed && b != escape {
				result = append(result, b) // preserve doubled enclosure convention
			} else {
				result = append(result, escape)
				if b == '\r' && defaultLine {
					if enclosed != 0 && enclosed == escape {
						result = append(result, escape)
					}
					// The reader strips trailing raw CR before unescaping.
					b = 'r'
				}
			}
		}
		result = append(result, b)
	}
	return result
}

func exportDataFromResultSetToCSVFile(oq *ExportConfig) error {
	symbol := oq.Symbol
	closeby := oq.userConfig.Fields.EnclosedBy.Value
	flag := oq.ColumnFlag

	buffer := &bytes.Buffer{}

	// One call builds one record; prefix it with LINES STARTING BY exactly as
	// the batch writer (constructByte) does.
	if l := oq.userConfig.Lines; l != nil && l.StartingBy != "" {
		buffer.WriteString(l.StartingBy)
	}

	for i := uint64(0); i < oq.mrs.GetColumnCount(); i++ {

		column, err := oq.mrs.GetColumn(oq.ctx, i)
		if err != nil {
			return err
		}
		mysqlColumn, ok := column.(*MysqlColumn)
		if !ok {
			return moerr.NewInternalError(oq.ctx, "sendColumn need MysqlColumn")
		}
		if isNil, err := oq.mrs.ColumnIsNull(oq.ctx, 0, i); err != nil {
			return err
		} else if isNil {
			//NULL is output as \N
			if err = formatOutputNull(oq, symbol[i], closeby, buffer); err != nil {
				return err
			}
			continue
		}

		switch mysqlColumn.ColumnType() {
		case defines.MYSQL_TYPE_DECIMAL:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_BOOL:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_BIT:
			value, err := oq.mrs.GetUint64(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			byteLength := (mysqlColumn.Length() + 7) / 8
			encoded := types.EncodeUint64(&value)[:byteLength]
			slices.Reverse(encoded)
			if err = formatOutputString(oq, encoded, symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_YEAR:
			value, err := oq.mrs.GetInt64(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if mysqlColumn.ColumnType() == defines.MYSQL_TYPE_YEAR {
				if value == 0 {
					if err = formatOutputString(oq, []byte("0000"), symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
						return err
					}
				} else {
					var lineStr []byte
					lineStr = strconv.AppendInt(lineStr, value, 10)
					if err = formatOutputString(oq, lineStr, symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
						return err
					}
				}
			} else {
				var lineStr []byte
				lineStr = strconv.AppendInt(lineStr, value, 10)
				if err = formatOutputString(oq, lineStr, symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_FLOAT, defines.MYSQL_TYPE_DOUBLE:
			value, err := oq.mrs.GetFloat64(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			lineStr := []byte(fmt.Sprintf("%v", value))
			if err = formatOutputString(oq, lineStr, symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_LONGLONG:
			if uint32(mysqlColumn.Flag())&defines.UNSIGNED_FLAG != 0 {
				if value, err := oq.mrs.GetUint64(oq.ctx, 0, i); err != nil {
					return err
				} else {
					var lineStr []byte
					lineStr = strconv.AppendUint(lineStr, value, 10)
					if err = formatOutputString(oq, lineStr, symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
						return err
					}
				}
			} else {
				if value, err := oq.mrs.GetInt64(oq.ctx, 0, i); err != nil {
					return err
				} else {
					var lineStr []byte
					lineStr = strconv.AppendInt(lineStr, value, 10)
					if err = formatOutputString(oq, lineStr, symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
						return err
					}
				}
			}
		// Binary/varbinary has mysql_type_varchar.
		case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING,
			defines.MYSQL_TYPE_BLOB, defines.MYSQL_TYPE_TINY_BLOB, defines.MYSQL_TYPE_MEDIUM_BLOB, defines.MYSQL_TYPE_LONG_BLOB,
			defines.MYSQL_TYPE_TEXT, defines.MYSQL_TYPE_GEOMETRY:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if arr, ok := value.([]float32); ok {
				// this is for T_array_float32 type
				value = []byte(types.ArrayToString[float32](arr))
			} else if arr, ok := value.([]float64); ok {
				// this is for T_array_float64 type
				value = []byte(types.ArrayToString[float64](arr))
			} else if arr, ok := value.([]types.BF16); ok {
				// this is for T_array_bf16 type
				value = []byte(types.ArrayToString[types.BF16](arr))
			} else if arr, ok := value.([]types.Float16); ok {
				// this is for T_array_float16 type
				value = []byte(types.ArrayToString[types.Float16](arr))
			} else if arr, ok := value.([]int8); ok {
				// this is for T_array_int8 type
				value = []byte(types.ArrayToString[int8](arr))
			} else if s, ok := value.(string); ok {
				// this is for T_array_uint8 (stored as its display string in
				// extractRowFromVector, since []uint8 is indistinguishable from
				// raw []byte) and any other string-valued varchar column
				value = []byte(s)
			}

			if err = formatOutputString(oq, value.([]byte), symbol[i], closeby, true, i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_DATE:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value.(types.Date).String()), symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_TIME:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value.(types.Time).String()), symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_DATETIME:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value.(string)), symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_TIMESTAMP:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_JSON:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			jsonStr := value.(bytejson.ByteJson).String()
			if err = formatOutputString(oq, []byte(jsonStr), symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_UUID:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_ENUM:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i], i == oq.mrs.GetColumnCount()-1, buffer); err != nil {
				return err
			}
		default:
			return moerr.NewInternalErrorf(oq.ctx, "unsupported column type %d ", mysqlColumn.ColumnType())
		}
	}

	// write the complete CSV row to CSV Writer
	err := writeToCSVFile(oq, buffer.Bytes())
	if err != nil {
		return err
	}
	oq.Rows++
	return err
}

// acceptExportBatch and flushExportBatches have one ordered-consumer owner.
func acceptExportBatch(ep *ExportConfig, value *BatchByte) error {
	if value.err != nil {
		return value.err
	}
	ep.BatchMap[value.index] = value
	return flushExportBatches(ep)
}

func flushExportBatches(ep *ExportConfig) error {
	for {
		if ep.ctx != nil && ep.ctx.Err() != nil {
			return ep.ctx.Err()
		}
		next := ep.WriteIndex.Load() + 1
		value, ok := ep.BatchMap[next]
		if !ok {
			return nil
		}
		if err := writeExportBatchToFile(ep, value); err != nil {
			return err
		}
		delete(ep.BatchMap, next)
		ep.WriteIndex.Store(next)
	}
}

func exportDataFromBatchToCSVFile(ep *ExportConfig) error {
	select {
	case value, ok := <-ep.ByteChan:
		if !ok {
			return moerr.NewInternalErrorNoCtx("export result channel closed before finalization")
		}
		return acceptExportBatch(ep, value)
	default:
		return flushExportBatches(ep)
	}
}

func exportAllDataFromBatches(ep *ExportConfig) error {
	if ep.getExportFormat() == "parquet" {
		return finalizeParquetExport(ep)
	}
	ctx := ep.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	// Submissions have stopped. Canceled workers need not publish their indices.
	target := ep.Index.Load()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := flushExportBatches(ep); err != nil {
			return err
		}
		if ep.WriteIndex.Load() == target {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case value, ok := <-ep.ByteChan:
			if !ok {
				return moerr.NewInternalErrorNoCtx("export result channel closed with missing batches")
			}
			if err := acceptExportBatch(ep, value); err != nil {
				return err
			}
		}
	}
}

// finalizeParquetExport closes the parquet writer and writes the complete parquet file
func finalizeParquetExport(ep *ExportConfig) error {
	if ep.parquetWriter == nil {
		// A writer that was just closed at a split boundary represents a
		// complete export. Do not create a trailing empty split; only an export
		// with no rows needs an explicitly initialized empty writer.
		if ep.Rows > 0 {
			return nil
		}
		if ep.mrs == nil {
			return moerr.NewInternalError(ep.ctx, "mrs is nil for parquet export")
		}
		writer, err := NewParquetWriter(ep.ctx, ep.mrs)
		if err != nil {
			return err
		}
		ep.parquetWriter = writer
	}
	return ep.flushParquetFile()
}

// writeParquetToFile writes the complete parquet data to the output file
func writeParquetToFile(ep *ExportConfig, data []byte) error {
	var filePath string
	if len(ep.userConfig.StageFilePath) != 0 {
		filePath = getExportFilePath(ep.userConfig.StageFilePath, ep.FileCnt)
	} else {
		filePath = getExportFilePath(ep.userConfig.FilePath, ep.FileCnt)
	}

	// Parse file path to determine file service
	fspath, err := fileservice.ParsePath(filePath)
	if err != nil {
		return err
	}

	if err := process.AuthorizeSQLFileAccessPath(ep.proc, fspath); err != nil {
		return err
	}
	var fs fileservice.FileService
	var readPath string
	if fspath.Service == defines.SharedFileServiceName {
		fs = getPu(ep.service).FileService
		readPath = filePath
	} else {
		fs, readPath, err = fileservice.GetForETLWithParsedPath(process.SQLFileAccessContext(ep.ctx, ep.proc), nil, filePath, fspath)
		if err != nil {
			return err
		}
	}

	// Write the parquet data to file service
	vec := fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{
			{
				Data: data,
				Size: int64(len(data)),
			},
		},
	}

	return fs.Write(ep.ctx, vec)
}

var _ CsvWriter = &ExportConfig{}

func (ec *ExportConfig) init() {
	ctx := ec.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	ec.ctx, ec.cancel = context.WithCancel(ctx)
	ec.ByteChan = make(chan *BatchByte, 10)
	ec.BatchMap = make(map[int32]*BatchByte)
	ec.Index.Store(0)
	ec.WriteIndex.Store(0)
}

func (ec *ExportConfig) Write(execCtx *ExecCtx, crs *perfcounter.CounterSet, bat *batch.Batch) error {
	if ec.ctx != nil && ec.ctx.Err() != nil {
		return ec.ctx.Err()
	}
	// CSV and JSON conversion runs asynchronously and can outlive runner.Run.
	// The worker copy therefore belongs to the export pipeline, not to the
	// producing statement's allocation account.
	copied, err := cloneExportWorkerBatch(bat, execCtx.ses.GetMemPool())
	if err != nil {
		return err
	}

	idx := ec.Index.Add(1)
	// Parquet is synchronous; CSV/JSON copies belong to joined conversion workers.
	convert := func(fn func(context.Context, FeSession, *batch.Batch, int32, chan *BatchByte, *ExportConfig)) {
		ec.workers.Add(1)
		go func() { defer ec.workers.Done(); fn(ec.ctx, execCtx.ses, copied, idx, ec.ByteChan, ec) }()
	}
	// Choose the appropriate constructor based on export format
	switch ec.getExportFormat() {
	case "jsonline":
		convert(constructJSONLine)
	case "parquet":
		// Parquet export: write batch to parquet writer
		return ec.writeParquet(execCtx, copied)
	default: // csv
		convert(constructByte)
	}

	if err = exportDataFromBatchToCSVFile(ec); err != nil {
		execCtx.ses.Error(execCtx.reqCtx,
			"Error occurred while exporting to file",
			zap.Error(err))
		return err
	}
	return nil
}

func cloneExportWorkerBatch(
	bat *batch.Batch,
	mp *mpool.MPool,
) (*batch.Batch, error) {
	return bat.DupWithoutAllocationAccount(mp)
}

// writeParquet writes a batch to the parquet writer
func (ec *ExportConfig) writeParquet(execCtx *ExecCtx, bat *batch.Batch) error {
	if bat == nil {
		return nil
	}
	defer bat.Clean(execCtx.ses.GetMemPool())
	if bat.RowCount() == 0 {
		return nil
	}

	// Initialize parquet writer if not already done
	if ec.parquetWriter == nil {
		var err error
		if ec.mrs == nil {
			return moerr.NewInternalError(execCtx.reqCtx, "mrs is nil for parquet export")
		}
		ec.parquetWriter, err = NewParquetWriter(execCtx.reqCtx, ec.mrs)
		if err != nil {
			return err
		}
	}

	// Get timezone from session
	var timeZone *time.Location
	if ss, ok := execCtx.ses.(*Session); ok {
		timeZone = ss.GetTimeZone()
	} else {
		timeZone = time.UTC
	}

	// Check if we need to split the file
	splitSize := getEffectiveMaxFileSize(ec)
	if splitSize == 0 {
		if err := ec.parquetWriter.WriteBatch(bat, execCtx.ses.GetMemPool(), timeZone); err != nil {
			return err
		}
		ec.Rows += uint64(bat.RowCount())
		return nil
	}

	// Accumulate bounded row groups across executor batches. Flush ends a row
	// group, so flushing every row causes large metadata overhead and exhausts
	// parquet-go's per-file row-group limit long before SPLITSIZE is reached.
	for row := 0; row < bat.RowCount(); {
		if ec.parquetWriter == nil {
			var err error
			ec.parquetWriter, err = NewParquetWriter(execCtx.reqCtx, ec.mrs)
			if err != nil {
				return err
			}
		}
		writer := ec.parquetWriter
		groupBudget := min(uint64(maxParquetRowGroupBytes), splitSize-uint64(writer.Size()))
		next, err := writer.writeBatchChunk(bat, row, bat.RowCount(), groupBudget, timeZone)
		if err != nil {
			return err
		}
		ec.Rows += uint64(next - row)
		row = next
		if writer.bufferedBytes < groupBudget && writer.bufferedRows < maxParquetRowGroupRows {
			continue
		}
		if err := writer.Flush(); err != nil {
			return err
		}
		// A row group (and the final footer) can exceed the target. Rotate at
		// parquet-go's format limit too, even for highly compressible/NULL data.
		if shouldSplitParquetFile(uint64(writer.Size()), splitSize) || writer.rowGroups == parquet.MaxRowGroups {
			if err := ec.flushParquetFile(); err != nil {
				return err
			}
			ec.FileCnt++
		}
	}

	return nil
}

// shouldSplitParquetFile checks if the parquet file should be split based on current size
func shouldSplitParquetFile(currentSize, splitSize uint64) bool {
	if splitSize == 0 {
		return false
	}
	return currentSize >= splitSize
}

// flushParquetFile closes the current parquet writer and writes data to file
func (ec *ExportConfig) flushParquetFile() error {
	if ec.parquetWriter == nil {
		return nil
	}

	writer := ec.parquetWriter
	// Release the writer before writing the completed bytes. A failed
	// fileservice write must not leave a closed writer attached to the session.
	ec.parquetWriter = nil

	// Close the parquet writer to get the complete parquet data (including footer)
	parquetData, err := writer.Close()
	if err != nil {
		return err
	}

	// Write the parquet data to file
	if err := writeParquetToFile(ec, parquetData); err != nil {
		return err
	}

	return nil
}

// finish is called after the producer and ordered consumer stop accessing ec.
func (ec *ExportConfig) finish(err error) error {
	if ec == nil {
		return err
	}
	if err == nil && ec.ctx != nil {
		err = ec.ctx.Err()
	}
	if err != nil {
		if ec.cancel != nil {
			ec.cancel()
		}
		err = exportCleanupError(err, ec.closeFile(err))
	}
	ec.workers.Wait()
	if err == nil {
		if ec.AsyncWriter != nil || ec.AsyncGroup != nil {
			err = Close(ec)
		}
		if err == nil && ec.ctx != nil {
			err = ec.ctx.Err()
		}
	}
	if ec.cancel != nil {
		ec.cancel()
	}
	// No worker can publish or read request fields beyond this point.
	ec.ByteChan, ec.BatchMap = nil, nil
	ec.mrs, ec.ctx, ec.cancel = nil, nil, nil
	ec.Ctx, ec.parquetWriter = nil, nil
	ec.proc = nil
	return err
}

func (ec *ExportConfig) Close() {
	if ec != nil {
		_ = ec.finish(context.Canceled)
	}
}

// getExportFormat returns the export format, defaulting to "csv" if not specified
func (ec *ExportConfig) getExportFormat() string {
	if ec.userConfig.ExportFormat == "" {
		return "csv"
	}
	return ec.userConfig.ExportFormat
}

// inferFormatFromSuffix infers the export format from file suffix.
// Returns empty string if suffix is not recognized.
func inferFormatFromSuffix(filePath string) string {
	lowerPath := strings.ToLower(filePath)

	// Check for known suffixes
	switch {
	case strings.HasSuffix(lowerPath, ".csv"):
		return "csv"
	case strings.HasSuffix(lowerPath, ".jsonl"),
		strings.HasSuffix(lowerPath, ".jsonline"),
		strings.HasSuffix(lowerPath, ".ndjson"):
		return "jsonline"
	case strings.HasSuffix(lowerPath, ".parquet"):
		return "parquet"
	default:
		return ""
	}
}

// validateExportFormat validates that FORMAT matches file suffix,
// or infers FORMAT from suffix if not specified.
func validateExportFormat(ctx context.Context, ep *tree.ExportParam) error {
	if ep == nil {
		return nil
	}

	inferredFormat := inferFormatFromSuffix(ep.FilePath)

	// Case 1: No FORMAT specified - infer from suffix
	if ep.ExportFormat == "" {
		if inferredFormat != "" {
			ep.ExportFormat = inferredFormat
		} else {
			// Unknown suffix, default to csv
			ep.ExportFormat = "csv"
		}
		return nil
	}

	// Case 2: FORMAT specified - validate against suffix if suffix is recognized
	if inferredFormat != "" && ep.ExportFormat != inferredFormat {
		// Get the actual suffix for error message
		lowerPath := strings.ToLower(ep.FilePath)
		var suffix string
		for _, s := range []string{".csv", ".jsonl", ".jsonline", ".ndjson", ".parquet"} {
			if strings.HasSuffix(lowerPath, s) {
				suffix = s
				break
			}
		}
		return moerr.NewInternalErrorf(ctx, "format '%s' does not match file suffix '%s'", ep.ExportFormat, suffix)
	}

	// Case 3: FORMAT specified with unknown suffix - allow
	return nil
}

// constructJSONLine constructs JSONLINE format output from a batch
func constructJSONLine(ctx context.Context, obj FeSession, bat *batch.Batch, index int32, ByteChan chan *BatchByte, ep *ExportConfig) {
	var (
		ok      bool
		backSes *backSession
		ss      *Session
		mp      *mpool.MPool
	)

	if ss, ok = obj.(*Session); !ok {
		backSes = obj.(*backSession)
		mp = backSes.GetMemPool()
	} else {
		mp = ss.GetMemPool()
	}
	defer bat.Clean(mp)
	if ctx.Err() != nil {
		return
	}

	buffer := &bytes.Buffer{}
	columnNames := make([]string, len(ep.mrs.Columns))
	for i, col := range ep.mrs.Columns {
		columnNames[i] = col.Name()
	}
	// A JSON object cannot carry two members with the same name. Two SELECT
	// aliases that collide would otherwise silently write one value over the
	// other on every row, so reject the export up front instead of returning
	// success and dropping columns (#30703). CSV and parquet exports are not
	// affected: they write values positionally.
	if dup, found := duplicatedJSONLineColumn(columnNames); found {
		err := moerr.NewInvalidInputNoCtxf(
			"jsonline export cannot write duplicate column names ('%s'); give every output column a unique alias",
			dup)
		if !sendExportBatchByte(ctx, ByteChan, &BatchByte{
			err: err,
		}) {
			return
		}
		return
	}

	for i := 0; i < bat.RowCount(); i++ {
		if ctx.Err() != nil {
			return
		}
		row := make(map[string]interface{})
		for j, vec := range bat.Vecs {
			colName := columnNames[j]
			if vec.GetNulls().Contains(uint64(i)) {
				row[colName] = nil
				continue
			}
			val, err := vectorValueToJSON(vec, i, ss, backSes)
			if err != nil {
				if !sendExportBatchByte(ctx, ByteChan, &BatchByte{
					err: err,
				}) {
					return
				}
				return
			}
			row[colName] = val
		}
		jsonBytes, err := json.Marshal(row)
		if err != nil {
			if !sendExportBatchByte(ctx, ByteChan, &BatchByte{
				err: moerr.NewInternalErrorf(ctx, "failed to marshal JSON: %v", err),
			}) {
				return
			}
			return
		}
		buffer.Write(jsonBytes)
		buffer.WriteByte('\n')
	}

	reslen := buffer.Len()
	result := make([]byte, reslen)
	copy(result, buffer.Bytes())
	buffer = nil

	if !sendExportBatchByte(ctx, ByteChan, &BatchByte{
		index:     index,
		writeByte: result,
		err:       nil,
	}) {
		return
	}

}

// vectorValueToJSON converts a vector value at index i to a JSON-compatible Go value
func vectorValueToJSON(vec *vector.Vector, i int, ss *Session, backSes *backSession) (interface{}, error) {
	switch vec.GetType().Oid {
	case types.T_json:
		val := types.DecodeJson(vec.GetBytesAt(i))
		// Return as raw JSON string to be embedded
		return json.RawMessage(val.String()), nil
	case types.T_bool:
		return vector.GetFixedAtNoTypeCheck[bool](vec, i), nil
	case types.T_bit:
		val := vector.GetFixedAtNoTypeCheck[uint64](vec, i)
		return val, nil
	case types.T_int8:
		return vector.GetFixedAtNoTypeCheck[int8](vec, i), nil
	case types.T_int16:
		return vector.GetFixedAtNoTypeCheck[int16](vec, i), nil
	case types.T_int32:
		return vector.GetFixedAtNoTypeCheck[int32](vec, i), nil
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](vec, i), nil
	case types.T_uint8:
		return vector.GetFixedAtNoTypeCheck[uint8](vec, i), nil
	case types.T_uint16:
		return vector.GetFixedAtNoTypeCheck[uint16](vec, i), nil
	case types.T_uint32:
		return vector.GetFixedAtNoTypeCheck[uint32](vec, i), nil
	case types.T_uint64:
		return vector.GetFixedAtNoTypeCheck[uint64](vec, i), nil
	case types.T_float32:
		return vector.GetFixedAtNoTypeCheck[float32](vec, i), nil
	case types.T_float64:
		return vector.GetFixedAtNoTypeCheck[float64](vec, i), nil
	case types.T_char, types.T_varchar, types.T_text, types.T_datalink:
		value := vec.GetBytesAt(i)
		if !utf8.Valid(value) {
			return nil, moerr.NewNotSupported(context.Background(),
				"JSONLINE OUTFILE cannot encode text containing invalid UTF-8; use CSV to preserve raw bytes")
		}
		return string(value), nil
	case types.T_binary, types.T_varbinary, types.T_blob:
		// #30635: wrap base64 in {"$binary":...} so the jsonline loader can
		// distinguish MO-encoded binary payloads from plain-text JSON
		// strings; bare strings keep their byte-exact passthrough on load.
		return map[string]any{external.JsonBinaryKey: base64.StdEncoding.EncodeToString(vec.GetBytesAt(i))}, nil
	case types.T_geometry, types.T_geometry32:
		return planfunction.GeometryPayloadToText(vec.GetBytesAt(i))
	case types.T_array_float32:
		return types.BytesToArray[float32](vec.GetBytesAt(i)), nil
	case types.T_array_float64:
		return types.BytesToArray[float64](vec.GetBytesAt(i)), nil
	case types.T_array_bf16:
		// bf16/f16 are uint16-backed; widen to float32 so JSON emits their float values
		// (a raw []BF16 would marshal the uint16 bit patterns).
		return types.BF16ToFloat32Slice(types.BytesToArray[types.BF16](vec.GetBytesAt(i))), nil
	case types.T_array_float16:
		return types.Float16ToFloat32Slice(types.BytesToArray[types.Float16](vec.GetBytesAt(i))), nil
	case types.T_array_int8:
		return types.BytesToArray[int8](vec.GetBytesAt(i)), nil
	case types.T_array_uint8:
		// []uint8 aliases []byte and json.Marshal would base64-encode it; widen to
		// []uint16 so it serializes as a JSON number array like the other vectors.
		src := types.BytesToArray[uint8](vec.GetBytesAt(i))
		out := make([]uint16, len(src))
		for k, v := range src {
			out[k] = uint16(v)
		}
		return out, nil
	case types.T_date:
		val := vector.GetFixedAtNoTypeCheck[types.Date](vec, i)
		return val.String(), nil
	case types.T_datetime:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Datetime](vec, i).String2(scale)
		return val, nil
	case types.T_time:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Time](vec, i).String2(scale)
		return val, nil
	case types.T_timestamp:
		var timeZone *time.Location
		if ss != nil {
			timeZone = ss.GetTimeZone()
		} else if backSes != nil {
			timeZone = backSes.GetTimeZone()
		} else {
			timeZone = time.UTC
		}
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, i).String2(timeZone, scale)
		return val, nil
	case types.T_decimal64:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, i).Format(scale)
		return val, nil
	case types.T_decimal128:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, i).Format(scale)
		return val, nil
	case types.T_decimal256:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Decimal256](vec, i).Format(scale)
		return val, nil
	case types.T_uuid:
		val := vector.GetFixedAtNoTypeCheck[types.Uuid](vec, i).String()
		return val, nil
	case types.T_Rowid:
		val := vector.GetFixedAtNoTypeCheck[types.Rowid](vec, i)
		return val.String(), nil
	case types.T_Blockid:
		val := vector.GetFixedAtNoTypeCheck[types.Blockid](vec, i)
		return val.String(), nil
	case types.T_enum:
		val := vector.GetFixedAtNoTypeCheck[types.Enum](vec, i).String()
		return val, nil
	default:
		return nil, moerr.NewInternalErrorf(context.Background(), "vectorValueToJSON: unsupported type %d", vec.GetType().Oid)
	}
}

// duplicatedJSONLineColumn returns the first repeated column name and whether
// one was found. Empty names are valid result metadata and can also repeat.
func duplicatedJSONLineColumn(names []string) (string, bool) {
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, dup := seen[name]; dup {
			return name, true
		}
		seen[name] = struct{}{}
	}
	return "", false
}
