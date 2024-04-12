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
	"container/list"
	"context"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/trace"

	"github.com/matrixorigin/simdcsv"
	"go.uber.org/zap"
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

func NewMergeService(ctx context.Context, opts ...MergeOption) (*Merge, bool, error) {
	// fix multi-init in standalone
	if !atomic.CompareAndSwapUint32(&serviceInited, 0, 1) {
		return nil, true, nil
	}
	m, err := NewMerge(ctx, opts...)
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

func NewMerge(ctx context.Context, opts ...MergeOption) (*Merge, error) {
	var err error
	m := &Merge{
		pathBuilder:  table.NewAccountDatePathBuilder(),
		MaxFileSize:  defaultMaxFileSize,
		MaxMergeJobs: 1,
		logger:       runtime.ProcessLevelRuntime().Logger().WithContext(ctx).Named(LoggerNameETLMerge),
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

	accounts, err := m.fs.List(ctx, "/")
	if err != nil {
		return err
	}
	if len(accounts) == 0 {
		m.logger.Info("merge find empty data")
		return nil
	}
	m.logger.Debug(fmt.Sprintf("merge task with max file: %v MB", m.MaxFileSize/mpool.MB))
	for _, account := range accounts {
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

			fileEntrys, err := m.fs.List(ctx, rootPath)
			if err != nil {
				// fixme: m.logger.Error()
				return err
			}
			files = files[:0]
			totalSize = 0
			for _, f := range fileEntrys {
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

	return err
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
			entries, err := m.fs.List(ctx, prefix)
			if err != nil {
				return nil, err
			}
			for _, entry := range entries {
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
	m.runningJobs <- struct{}{}
	defer func() {
		<-m.runningJobs
	}()

	// Step 3. do simple merge
	var uploadFile = func(ctx context.Context, fp *FileMeta) error {
		row := m.table.GetRow(ctx)
		defer row.Free()
		// open reader
		reader, err := newETLReader(ctx, m.table, m.fs, fp.FilePath, fp.FileSize, m.mp)
		if err != nil {
			m.logger.Error(fmt.Sprintf("merge file meet read failed: %v", err))
			return err
		}
		defer reader.Close()

		cacheFileData := &SliceCache{}
		defer cacheFileData.Reset()

		// Read the first line to check if the record already exists
		var existed bool
		firstLine, err := reader.ReadLine()
		if err != nil {
			m.logger.Error("failed to read the first line of the file",
				logutil.PathField(fp.FilePath), zap.Error(err))
			return err
		}

		if firstLine != nil {
			if err = row.ParseRow(firstLine); err != nil {
				m.logger.Error("parse first ETL row failed",
					logutil.TableField(m.table.GetIdentify()),
					logutil.PathField(fp.FilePath),
					logutil.VarsField(SubStringPrefixLimit(fmt.Sprintf("%v", firstLine), 102400)),
				)
				return err
			}

			// Check if the first record already exists in the database
			existed, err = db_holder.IsRecordExisted(ctx, firstLine, m.table, db_holder.GetOrInitDBConn)
			if err != nil {
				m.logger.Error("error checking if the first record exists",
					logutil.TableField(m.table.GetIdentify()),
					logutil.PathField(fp.FilePath),
					logutil.ErrorField(err),
				)
				return err
			}

			// Process the first line since it doesn't exist in the database
			if !existed {
				cacheFileData.Put(row)
			}
		}

		// read all content if not existed
		if !existed {
			var line []string
			line, err = reader.ReadLine()
			for ; line != nil && err == nil; line, err = reader.ReadLine() {
				if err = row.ParseRow(line); err != nil {
					m.logger.Error("parse ETL rows failed",
						logutil.TableField(m.table.GetIdentify()),
						logutil.PathField(fp.FilePath),
						logutil.VarsField(SubStringPrefixLimit(fmt.Sprintf("%v", line), 102400)),
					)
					return err
				}

				cacheFileData.Put(row)
			}
			if err != nil {
				m.logger.Warn("failed to read file",
					logutil.PathField(fp.FilePath), zap.Error(err))
				return err
			}
		}

		// sql insert
		if cacheFileData.Size() > 0 {
			if err = cacheFileData.Flush(m.table); err != nil {
				return err
			}
			cacheFileData.Reset()
		}
		// delete empty file or file already uploaded
		if cacheFileData.Size() == 0 {
			if err = m.fs.Delete(ctx, fp.FilePath); err != nil {
				m.logger.Warn("failed to delete file", zap.Error(err))
				return err
			}
		}
		return nil
	}
	var err error

	for _, fp := range files {
		if err = uploadFile(ctx, fp); err != nil {
			// todo: adjust the sleep settings
			// Sleep 10 seconds to wait for the database to recover
			time.Sleep(10 * time.Second)
			m.logger.Error("failed to upload file to MO",
				logutil.TableField(m.table.GetIdentify()),
				logutil.PathField(fp.FilePath),
				zap.Error(err),
			)
		}
	}
	logutil.Info("upload files success", logutil.TableField(m.table.GetIdentify()), zap.Int("file count", len(files)))

	return err
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

type ContentReader struct {
	ctx     context.Context
	path    string
	idx     int
	length  int
	content [][]string

	logger *log.MOLogger
	reader *simdcsv.Reader
	raw    io.ReadCloser
}

// BatchReadRows ~= 20MB rawlog file has about 3700+ rows
const BatchReadRows = 4000

func NewContentReader(ctx context.Context, path string, reader *simdcsv.Reader, raw io.ReadCloser) *ContentReader {
	logger := runtime.ProcessLevelRuntime().Logger().WithContext(ctx).Named(LoggerNameContentReader)
	return &ContentReader{
		ctx:     ctx,
		path:    path,
		length:  0,
		content: make([][]string, BatchReadRows),
		logger:  logger,
		reader:  reader,
		raw:     raw,
	}
}

func (s *ContentReader) ReadLine() ([]string, error) {
	if s.idx == s.length && s.reader != nil {
		var cnt int
		var err error
		s.content, cnt, err = s.reader.Read(BatchReadRows, s.ctx, s.content)
		if err != nil {
			return nil, err
		} else if s.content == nil {
			s.logger.Error("ContentReader.ReadLine.nil", logutil.PathField(s.path),
				zap.Bool("nil", s.content == nil),
				zap.Error(s.ctx.Err()),
				zap.Bool("SupportedCPU", simdcsv.SupportedCPU()),
			)
			return nil, moerr.NewInternalError(s.ctx, "read files meet context Done")
		}
		if cnt < BatchReadRows {
			//s.reader.Close() // DO NOT call, because it is a forever loop with empty op.
			s.reader = nil
			s.raw.Close()
			s.raw = nil
			s.logger.Debug("ContentReader.ReadLine.EOF", logutil.PathField(s.path), zap.Int("rows", cnt))
		}
		s.idx = 0
		s.length = cnt
		s.logger.Debug("ContentReader.ReadLine", logutil.PathField(s.path), zap.Int("rows", cnt),
			zap.Bool("SupportedCPU", simdcsv.SupportedCPU()),
		)
	}
	if s.idx < s.length {
		idx := s.idx
		s.idx++
		if s.content == nil || len(s.content) == 0 {
			s.logger.Error("ContentReader.ReadLine.nil",
				logutil.PathField(s.path),
				zap.Bool("nil", s.content == nil),
				zap.Int("cached", len(s.content)),
				zap.Int("idx", idx),
				zap.Bool("SupportedCPU", simdcsv.SupportedCPU()),
			)
		}
		return s.content[idx], nil
	}
	return nil, nil
}

func (s *ContentReader) ReadRow(row *table.Row) error {
	panic("NOT implement")
}

func (s *ContentReader) Close() {
	capLen := cap(s.content)
	s.content = s.content[:capLen]
	for idx := range s.content {
		s.content[idx] = nil
	}
	if s.raw != nil {
		_ = s.raw.Close()
		s.raw = nil
	}
}

func newETLReader(ctx context.Context, tbl *table.Table, fs fileservice.FileService, path string, size int64, mp *mpool.MPool) (ETLReader, error) {
	if strings.LastIndex(path, table.CsvExtension) > 0 {
		return NewCSVReader(ctx, fs, path)
	} else if strings.LastIndex(path, table.TaeExtension) > 0 {
		r, err := etl.NewTaeReader(ctx, tbl, path, size, fs, mp)
		if err != nil {
			r.Close()
			return nil, err
		}
		_, err = r.ReadAll(ctx)
		if err != nil {
			r.Close()
			return nil, err
		}
		return r, nil
	} else {
		panic("NOT Implements")
	}
}

// NewCSVReader create new csv reader.
// success case return: ok_reader, nil error
// failed case return: nil_reader, error
func NewCSVReader(ctx context.Context, fs fileservice.FileService, path string) (ETLReader, error) {
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

	// parse csv content
	simdCsvReader := simdcsv.NewReaderWithOptions(reader,
		table.CommonCsvOptions.FieldTerminator,
		'#',
		true,
		true)

	// return content Reader
	return NewContentReader(ctx, path, simdCsvReader, reader), nil
}

type Cache interface {
	Put(*table.Row)
	Size() int64
	Flush(*table.Table) error
	Reset()
	IsEmpty() bool
}

type SliceCache struct {
	m    [][]string
	size int64
}

func (c *SliceCache) Flush(tbl *table.Table) error {
	_, err := db_holder.WriteRowRecords(c.m, tbl, MAX_MERGE_INSERT_TIME)
	c.Reset()
	return err
}

func (c *SliceCache) Reset() {
	for idx := range c.m {
		c.m[idx] = nil
	}
	c.m = c.m[:0]
	c.size = 0
}

func (c *SliceCache) IsEmpty() bool {
	return len(c.m) == 0
}

func (c *SliceCache) Put(r *table.Row) {
	c.m = append(c.m, r.GetCsvStrings())
	c.size += r.Size()
}

func (c *SliceCache) Size() int64 { return c.size }

func LongRunETLMerge(ctx context.Context, task task.AsyncTask, logger *log.MOLogger, opts ...MergeOption) error {
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
	merge, err := NewMerge(ctx, newOptions...)
	if err != nil {
		return err
	}

	logger.Info("start LongRunETLMerge")
	// handle today
	for _, tbl := range tables {
		merge.table = tbl
		if err = merge.Main(ctx); err != nil {
			logger.Error("merge metric failed", zap.Error(err))
		}
	}

	return nil
}

func MergeTaskExecutorFactory(opts ...MergeOption) func(ctx context.Context, task task.Task) error {

	CronMerge := func(ctx context.Context, t task.Task) error {
		asyncTask, ok := t.(*task.AsyncTask)
		if !ok {
			return moerr.NewInternalError(ctx, "invalid task type")
		}
		ctx, span := trace.Start(ctx, "CronMerge")
		defer span.End()

		args := asyncTask.Metadata.Context
		ts := time.Now()
		logger := runtime.ProcessLevelRuntime().Logger().WithContext(ctx).Named(LoggerNameETLMerge)
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
		if err := LongRunETLMerge(ctx, *asyncTask, logger, opts...); err != nil {
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
		ID:       path.Join("ETLMergeTask", path.Join(args...)),
		Executor: id,
		Context:  []byte(strings.Join(args, ParamSeparator)),
		Options:  task.TaskOptions{Concurrency: 1},
	}
}

func CreateCronTask(ctx context.Context, executorID task.TaskCode, taskService taskservice.TaskService) error {
	var err error
	ctx, span := trace.Start(ctx, "ETLMerge.CreateCronTask")
	defer span.End()
	ctx = defines.AttachAccount(ctx, catalog.System_Account, catalog.System_User, catalog.System_Role)
	logger := runtime.ProcessLevelRuntime().Logger().WithContext(ctx)
	logger.Info(fmt.Sprintf("init merge task with CronExpr: %s", MergeTaskCronExpr))
	if err = taskService.CreateCronTask(ctx, MergeTaskMetadata(executorID), MergeTaskCronExpr); err != nil {
		return err
	}
	return nil
}

// InitCronExpr support min interval 5 min, max 12 hour
func InitCronExpr(ctx context.Context, duration time.Duration) error {
	if duration < 0 || duration > 12*time.Hour {
		return moerr.NewNotSupported(ctx, "export cron expr not support cycle: %v", duration)
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
