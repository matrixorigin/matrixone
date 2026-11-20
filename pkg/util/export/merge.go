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

package export

import (
	"bufio"
	"container/list"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

const LoggerNameETLMerge = "ETLMerge"
const LoggerNameContentReader = "ETLContentReader"

const MAX_MERGE_INSERT_TIME = 10 * time.Second

const defaultMaxFileSize = 32 * mpool.MB

// ========================
// handle merge
// ========================

// Merge like a compaction, merge input files into one/two/... files.
// - NewMergeService init merge as service, with serviceInited to avoid multi init.
// - MergeTaskExecutorFactory drive by Cron TaskService.
// - NewMerge handle merge obj init.
// - Merge.Start() as service loop, trigger Merge.Main()
// - Merge.Main() handle main job.
//  1. foreach account, build `rootPath` with tuple {account, date, Table }
//  2. call Merge.doMergeFiles() with all files in `rootPath`,  do merge job
//
// - Merge.doMergeFiles handle one job flow: read each file, merge in cache, write into file.
type Merge struct {
	service     string
	task        task.AsyncTask          // set by WithTask
	table       *table.Table            // set by WithTable
	fs          fileservice.FileService // set by WithFileService
	pathBuilder table.PathBuilder       // const as table.NewAccountDatePathBuilder()

	// MaxFileSize the total filesize to trigger doMergeFiles()，default: 32 MB
	// Deprecated
	MaxFileSize int64 // set by WithMaxFileSize
	// MaxMergeJobs 允许进行的 Merge 的任务个数，default: 1
	MaxMergeJobs int64 // set by WithMaxMergeJobs

	// logger
	logger *log.MOLogger
	// mp for TAEReader if needed.
	mp *mpool.MPool
	// runningJobs control task concurrency, init with MaxMergeJobs cnt
	runningJobs chan struct{}

	// flow ctrl
	ctx        context.Context
	cancelFunc context.CancelFunc
}

type MergeOption func(*Merge)

func (opt MergeOption) Apply(m *Merge) {
	opt(m)
}

func WithTask(task task.AsyncTask) MergeOption {
	return MergeOption(func(m *Merge) {
		m.task = task
	})
}
func WithTable(tbl *table.Table) MergeOption {
	return MergeOption(func(m *Merge) {
		m.table = tbl
	})
}
func WithFileService(fs fileservice.FileService) MergeOption {
	return MergeOption(func(m *Merge) {
		m.fs = fs
	})
}
func WithMaxFileSize(filesize int64) MergeOption {
	return MergeOption(func(m *Merge) {
		m.MaxFileSize = filesize
	})
}
func WithMaxMergeJobs(jobs int64) MergeOption {
	return MergeOption(func(m *Merge) {
		m.MaxMergeJobs = jobs
	})
}

// serviceInited handle Merge as service
var serviceInited uint32

func NewMergeService(
	ctx context.Context,
	service string,
	opts ...MergeOption,
) (*Merge, bool, error) {
	// fix multi-init in standalone
	if !atomic.CompareAndSwapUint32(&serviceInited, 0, 1) {
		return nil, true, nil
	}
	m, err := NewMerge(ctx, service, opts...)
	return m, false, err
}

var poolMux sync.Mutex
var ETLMergeTaskPool *mpool.MPool

func getMpool() (*mpool.MPool, error) {
	poolMux.Lock()
	defer poolMux.Unlock()
	if ETLMergeTaskPool == nil {
		mp, err := mpool.NewMPool("etl_merge_task", 0, mpool.NoFixed)
		if err != nil {
			return nil, err
		}
		ETLMergeTaskPool = mp
	}
	return ETLMergeTaskPool, nil
}

func NewMerge(
	ctx context.Context,
	service string,
	opts ...MergeOption,
) (*Merge, error) {
	var err error
	m := &Merge{
		service:      service,
		pathBuilder:  table.NewAccountDatePathBuilder(),
		MaxFileSize:  defaultMaxFileSize,
		MaxMergeJobs: 1,
		logger:       runtime.ServiceRuntime(service).Logger().WithContext(ctx).Named(LoggerNameETLMerge),
	}
	m.ctx, m.cancelFunc = context.WithCancel(ctx)
	for _, opt := range opts {
		opt(m)
	}
	if m.mp, err = getMpool(); err != nil {
		return nil, err
	}
	m.validate(ctx)
	m.runningJobs = make(chan struct{}, m.MaxMergeJobs)
	return m, nil
}

// validate check missing init elems. Panic with has missing elems.
func (m *Merge) validate(ctx context.Context) {
	if m.table == nil {
		panic(moerr.NewInternalError(ctx, "merge task missing input 'table'"))
	}
	if m.fs == nil {
		panic(moerr.NewInternalError(ctx, "merge task missing input 'FileService'"))
	}
}

// Start for service Loop
func (m *Merge) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.Main(ctx)
		case <-m.ctx.Done():
			return
		}
	}
}

// Stop should call only once
func (m *Merge) Stop() {
	m.cancelFunc()
}

// =======================
// main logic
// =======================

type FileMeta struct {
	FilePath string
	FileSize int64
}

// Main do list all accounts, all dates which belong to m.table.GetName()
func (m *Merge) Main(ctx context.Context) error {
	var files = make([]*FileMeta, 0, 1000)
	var totalSize int64

	m.logger.Debug(fmt.Sprintf("merge task with max file: %v MB", m.MaxFileSize/mpool.MB))
	for account, err := range m.fs.List(ctx, "/") {
		if err != nil {
			return err
		}
		if !account.IsDir {
			m.logger.Warn(fmt.Sprintf("path is not dir: %s", account.Name))
			continue
		}
		// build targetPath like "${account}/logs/*/*/*/${table_name}"
		targetPath := m.pathBuilder.Build(account.Name, table.MergeLogTypeLogs, table.ETLParamTSAll, m.table.GetDatabase(), m.table.GetName())

		// search all paths like:
		// 0: ${account}/logs/2023/05/31/${table_name}
		// 1: ${account}/logs/2023/06/01/${table_name}
		// 2: ...
		rootPaths, err := m.getAllTargetPath(ctx, targetPath)
		if err != nil {
			return err
		}

		// get all file entry
		for _, rootPath := range rootPaths {
			m.logger.Info("start merge", logutil.TableField(m.table.GetIdentify()), logutil.PathField(rootPath),
				zap.String("metadata.ID", m.task.Metadata.ID))

			files = files[:0]
			totalSize = 0
			for f, err := range m.fs.List(ctx, rootPath) {
				if err != nil {
					return err
				}
				if f.IsDir || !isETLFile(f.Name) {
					continue
				}
				filepath := path.Join(rootPath, f.Name)
				totalSize += f.Size
				files = append(files, &FileMeta{filepath, f.Size})
				if totalSize > m.MaxFileSize {
					if err = m.doMergeFiles(ctx, files); err != nil {
						m.logger.Error(fmt.Sprintf("merge task meet error: %v", err))
					}
					files = files[:0]
					totalSize = 0
				}
			}

			if len(files) > 0 {
				if err = m.doMergeFiles(ctx, files); err != nil {
					m.logger.Warn(fmt.Sprintf("merge task meet error: %v", err))
				}
			}
		}
	}

	return nil
}

func (m *Merge) getAllTargetPath(ctx context.Context, filePath string) ([]string, error) {
	sep := "/"
	pathDir := strings.Split(filePath, sep)
	l := list.New()
	if pathDir[0] == "" {
		l.PushBack(sep)
	} else {
		l.PushBack(pathDir[0])
	}

	for i := 1; i < len(pathDir); i++ {
		length := l.Len()
		for j := 0; j < length; j++ {
			elem := l.Remove(l.Front())
			prefix := elem.(string)
			for entry, err := range m.fs.List(ctx, prefix) {
				if err != nil {
					return nil, err
				}
				if !entry.IsDir && i+1 != len(pathDir) {
					continue
				}
				matched, err := path.Match(pathDir[i], entry.Name)
				if err != nil {
					return nil, err
				}
				if !matched {
					continue
				}
				l.PushBack(path.Join(prefix, entry.Name))
			}
		}
	}

	length := l.Len()
	fileList := make([]string, 0, length)
	for idx := 0; idx < length; idx++ {
		fileList = append(fileList, l.Remove(l.Front()).(string))
	}
	return fileList, nil
}

// doMergeFiles handle merge (read->write->delete) ops for all files in the target directory.
// Handle the files one by one, act uploadFile and do the deletion if upload is success.
// Upload the files to SQL table
// Delete the files from FileService
func (m *Merge) doMergeFiles(ctx context.Context, files []*FileMeta) error {
	ctx, span := trace.Start(ctx, "doMergeFiles")
	defer span.End()

	// Control task concurrency
	select {
	case m.runningJobs <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		<-m.runningJobs
	}()

	// Step 3. do simple merge
	var uploadFile = func(ctx context.Context, fp *FileMeta) error {
		reader, err := newETLReader(ctx, m.service, m.table, m.fs, fp.FilePath, fp.FileSize, m.mp)
		if err != nil {
			v2.TraceETLMergeOpenFailedCounter.Inc()
			return err
		}
		defer reader.Close()
		read := func() ([]string, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			row, err := reader.ReadLine()
			if err != nil {
				v2.TraceETLMergeReadFailedCounter.Inc()
			}
			return row, err
		}
		first, err := read()
		if err != nil {
			return err
		}
		if first != nil {
			done, err := db_holder.IsRecordExisted(ctx, first, m.table, db_holder.GetOrInitDBConn)
			if err != nil {
				v2.TraceETLMergeExistFailedCounter.Inc()
				return err
			}
			if done {
				v2.TraceETLMergeExistCounter.Inc()
			} else if err := db_holder.WriteMergeFile(ctx, m.table, first, read, MAX_MERGE_INSERT_TIME); err != nil {
				v2.TraceETLMergeWriteFailedCounter.Inc()
				return err
			}
		}
		if err := m.fs.Delete(ctx, fp.FilePath); err != nil {
			v2.TraceETLMergeDeleteFailedCounter.Inc()
			return err
		}
		v2.TraceETLMergeSuccessCounter.Inc()
		return nil
	}
	var firstErr error
	successCnt := 0
	for _, fp := range files {
		if err := ctx.Err(); err != nil {
			return errors.Join(firstErr, err)
		}
		if err := uploadFile(ctx, fp); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			timer := time.NewTimer(10 * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return errors.Join(firstErr, ctx.Err())
			case <-timer.C:
			}
			m.logger.Error("failed to upload file to MO",
				logutil.TableField(m.table.GetIdentify()),
				logutil.PathField(fp.FilePath),
				zap.Error(err),
			)
		} else {
			successCnt++
		}
	}
	m.logger.Error("upload files success",
		logutil.TableField(m.table.GetIdentify()),
		zap.Int("total", len(files)),
		zap.Int("success", successCnt),
	)

	return firstErr
}

func SubStringPrefixLimit(str string, length int) string {
	if length <= 0 {
		return ""
	}

	if len(str) < length {
		return str
	} else {
		return str[:length] + "..."
	}
}

// ContentReader borrows a single CSV record until the next ReadLine.
type ContentReader struct {
	ctx         context.Context
	raw         io.ReadCloser
	input       *recordInput
	buffer      *bufio.Reader
	reader      *csv.Reader
	recordLimit int64
}

type recordInput struct {
	ctx       context.Context
	raw       io.Reader
	offset    int64
	remaining int64 // negative allows incremental reads while skipping comments
}

func (r *recordInput) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.remaining == 0 {
		return 0, moerr.NewInvalidInput(r.ctx, "ETL CSV record exceeds the input byte limit")
	}
	if r.remaining > 0 && int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.raw.Read(p)
	r.offset += int64(n)
	if r.remaining > 0 {
		r.remaining -= int64(n)
	}
	return n, err
}

func newContentReader(ctx context.Context, raw io.ReadCloser) *ContentReader {
	input := &recordInput{ctx: ctx, raw: raw, remaining: -1}
	buffer := bufio.NewReader(input)
	reader := csv.NewReader(buffer)
	reader.Comma = table.CommonCsvOptions.FieldTerminator
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.ReuseRecord = true
	return &ContentReader{ctx: ctx, raw: raw, input: input, buffer: buffer, reader: reader, recordLimit: int64(config.LargestEntryLimit)}
}

func (s *ContentReader) ReadLine() ([]string, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	// Skip physical comments and blank lines only between logical records.
	// This keeps long comments out of the CSV parser's record scratch space.
	for {
		prefix, err := s.buffer.Peek(1)
		if err == io.EOF {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		switch prefix[0] {
		case '#':
			for {
				_, err := s.buffer.ReadSlice('\n')
				if errors.Is(err, bufio.ErrBufferFull) {
					continue
				}
				if err == io.EOF {
					return nil, nil
				}
				if err != nil {
					return nil, err
				}
				break
			}
			continue
		case '\n':
			_, _ = s.buffer.Discard(1)
			continue
		case '\r':
			pair, err := s.buffer.Peek(2)
			if err != nil && err != io.EOF {
				return nil, err
			}
			if len(pair) == 2 && pair[1] == '\n' {
				_, _ = s.buffer.Discard(2)
				continue
			}
		}
		break
	}

	start := s.input.offset - int64(s.buffer.Buffered())
	// Allow one byte of lookahead for exact-limit records ending at EOF.
	s.input.remaining = max(0, s.recordLimit+1-int64(s.buffer.Buffered()))
	row, err := s.reader.Read()
	s.input.remaining = -1
	if err != nil && err != io.EOF {
		return nil, err
	}
	if s.input.offset-int64(s.buffer.Buffered())-start > s.recordLimit {
		return nil, moerr.NewInvalidInput(s.ctx, "ETL CSV record exceeds the input byte limit")
	}
	return row, nil
}

func (s *ContentReader) ReadRow(row *table.Row) error {
	panic("NOT implement")
}

func (s *ContentReader) Close() {
	_ = s.raw.Close()
}

func newETLReader(
	ctx context.Context,
	service string,
	tbl *table.Table,
	fs fileservice.FileService,
	filePath string,
	size int64,
	mp *mpool.MPool,
) (ETLReader, error) {
	switch path.Ext(filePath) {
	case table.CsvExtension:
		return NewCSVReader(ctx, service, fs, filePath)
	case table.TaeExtension:
		r, err := etl.NewTaeReader(ctx, tbl, filePath, size, fs, mp)
		if err != nil {
			return nil, err
		}
		_, err = r.ReadAll(ctx)
		if err != nil {
			r.Close()
			return nil, err
		}
		return r, nil
	default:
		return nil, moerr.NewNotSupportedf(ctx, "unsupported ETL file: %s", filePath)
	}
}

func isETLFile(filePath string) bool {
	ext := path.Ext(filePath)
	return ext == table.CsvExtension || ext == table.TaeExtension
}

// NewCSVReader create new csv reader.
// success case return: ok_reader, nil error
// failed case return: nil_reader, error
func NewCSVReader(
	ctx context.Context,
	_ string,
	fs fileservice.FileService,
	path string,
) (ETLReader, error) {
	// external.ReadFile
	var reader io.ReadCloser
	vec := fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{
			0: {
				Offset:            0,
				Size:              -1,
				ReadCloserForRead: &reader,
			},
		},
	}
	// open file reader
	if err := fs.Read(ctx, &vec); err != nil {
		return nil, err
	}

	return newContentReader(ctx, reader), nil
}

func LongRunETLMerge(
	ctx context.Context,
	service string,
	task task.AsyncTask,
	logger *log.MOLogger,
	opts ...MergeOption,
) error {
	// should init once in/with schema-init.
	tables := table.GetAllTables()
	if len(tables) == 0 {
		logger.Info("empty tables")
		return nil
	}

	var newOptions []MergeOption
	newOptions = append(newOptions, opts...)
	newOptions = append(newOptions, WithTask(task))
	newOptions = append(newOptions, WithTable(tables[0]))
	merge, err := NewMerge(ctx, service, newOptions...)
	if err != nil {
		return err
	}

	logger.Info("start LongRunETLMerge")
	v2.TraceETLMergeJobCounter.Inc()
	// handle today
	for _, tbl := range tables {
		merge.table = tbl
		if err = merge.Main(ctx); err != nil {
			logger.Error("merge metric failed", zap.Error(err))
		}
	}

	return nil
}

func MergeTaskExecutorFactory(
	service string,
	opts ...MergeOption,
) func(ctx context.Context, task task.Task) error {
	CronMerge := func(ctx context.Context, t task.Task) error {
		asyncTask, ok := t.(*task.AsyncTask)
		if !ok {
			return moerr.NewInternalError(ctx, "invalid task type")
		}
		ctx, span := trace.Start(ctx, "CronMerge")
		defer span.End()

		args := asyncTask.Metadata.Context
		ts := time.Now()
		logger := runtime.ServiceRuntime(service).Logger().WithContext(ctx).Named(LoggerNameETLMerge)
		fields := []zap.Field{
			zap.String("args", util.UnsafeBytesToString(args)),
			zap.Time("start", ts),
			zap.Uint64("taskID", asyncTask.ID),
			zap.Int64("create", asyncTask.CreateAt),
			zap.String("metadataID", asyncTask.Metadata.ID),
		}
		logger.Info("start merge", fields...)
		defer logger.Info("done merge", fields...)

		// task run long time
		if len(args) != 0 {
			logger.Warn("ETLMergeTask should have empty args", zap.Int("cnt", len(args)))
		}
		if err := LongRunETLMerge(ctx, service, *asyncTask, logger, opts...); err != nil {
			return err
		}
		return nil
	}
	return CronMerge
}

// MergeTaskCronExpr support sec level
// Deprecated
var MergeTaskCronExpr = MergeTaskCronExprEvery4Hour

const MergeTaskCronExprEvery15Sec = "*/15 * * * * *"
const MergeTaskCronExprEveryMin = "0 * * * * *"
const MergeTaskCronExprEvery05Min = "0 */5 * * * *"
const MergeTaskCronExprEvery15Min = "0 */15 * * * *"
const MergeTaskCronExprEvery1Hour = "0 0 */1 * * *"
const MergeTaskCronExprEvery2Hour = "0 0 */2 * * *"
const MergeTaskCronExprEvery4Hour = "0 0 4,8,12,16,20 * * *"
const MergeTaskCronExprYesterday = "0 5 0 * * *"
const MergeTaskToday = "today"
const MergeTaskYesterday = "yesterday"
const ParamSeparator = " "

// MergeTaskMetadata handle args like: "{db_tbl_name} [date, default: today]"
func MergeTaskMetadata(id task.TaskCode, args ...string) task.TaskMetadata {
	return task.TaskMetadata{
		ID:       path.Join(etl.ETLMergeTask, path.Join(args...)),
		Executor: id,
		Context:  []byte(strings.Join(args, ParamSeparator)),
		Options:  task.TaskOptions{Concurrency: 1},
	}
}

func CreateCronTask(
	ctx context.Context,
	service string,
	executorID task.TaskCode,
	taskService taskservice.TaskService,
) error {
	var err error
	ctx, span := trace.Start(ctx, "ETLMerge.CreateCronTask")
	defer span.End()
	ctx = defines.AttachAccount(ctx, catalog.System_Account, catalog.System_User, catalog.System_Role)
	logger := runtime.ServiceRuntime(service).Logger().WithContext(ctx)
	logger.Info(fmt.Sprintf("init merge task with CronExpr: %s", MergeTaskCronExpr))
	if err = taskService.CreateCronTask(ctx, MergeTaskMetadata(executorID), MergeTaskCronExpr); err != nil {
		return err
	}
	return nil
}

// InitCronExpr support min interval 5 min, max 12 hour
func InitCronExpr(ctx context.Context, duration time.Duration) error {
	if duration < 0 || duration > 12*time.Hour {
		return moerr.NewNotSupportedf(ctx, "export cron expr not support cycle: %v", duration)
	}
	if duration < 5*time.Minute {
		MergeTaskCronExpr = fmt.Sprintf("@every %.0fs", duration.Seconds())
	} else if duration < time.Hour {
		const unit = 5 * time.Minute
		duration = (duration + unit - 1) / unit * unit
		switch duration {
		case 5 * time.Minute:
			MergeTaskCronExpr = MergeTaskCronExprEvery05Min
		case 15 * time.Minute:
			MergeTaskCronExpr = MergeTaskCronExprEvery15Min
		default:
			MergeTaskCronExpr = fmt.Sprintf("@every %.0fm", duration.Minutes())
		}
	} else {
		minHour := duration / time.Hour
		switch minHour {
		case 1:
			MergeTaskCronExpr = MergeTaskCronExprEvery1Hour
		case 2:
			MergeTaskCronExpr = MergeTaskCronExprEvery2Hour
		case 4:
			MergeTaskCronExpr = MergeTaskCronExprEvery4Hour
		default:
			var hours = make([]string, 0, 12)
			for h := minHour; h < 24; h += minHour {
				hours = append(hours, strconv.Itoa(int(h)))
			}
			MergeTaskCronExpr = fmt.Sprintf("0 0 %s * * *", strings.Join(hours, ","))
		}
	}
	return nil
}

func InitMerge(ctx context.Context, SV *config.ObservabilityParameters) error {
	var err error
	mergeCycle := SV.MergeCycle.Duration
	if mergeCycle > 0 {
		err = InitCronExpr(ctx, mergeCycle)
		if err != nil {
			return err
		}
	}
	return nil
}
