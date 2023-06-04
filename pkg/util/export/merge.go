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
	"bytes"
	"container/list"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
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

// ========================
// handle merge
// ========================

// Merge like a compaction, merge input files into one/two/... files.
//   - `NewMergeService` init merge as service, with param `serviceInited` to avoid multi init.
//   - `MergeTaskExecutorFactory` drive by Cron TaskService.
//   - `NewMerge` handle merge obj init.
//   - `Merge::Start` as service loop, trigger `Merge::Main` each cycle
//   - `Merge::Main` handle handle job,
//     1. foreach account, build `rootPath` with tuple {account, date, Table }
//     2. call `Merge::doMergeFiles` with all files in `rootPath`,  do merge job
//   - `Merge::doMergeFiles` handle one job flow: read each file, merge in cache, write into file.
type Merge struct {
	Task task.Task

	Table       *table.Table            // WithTable
	FS          fileservice.FileService // WithFileService
	datetime    time.Time               // see Main
	pathBuilder table.PathBuilder       // const as NewAccountDatePathBuilder()

	// MaxFileSize 控制合并后最大文件大小，default: 128 MB
	MaxFileSize int64 // WithMaxFileSize
	// MaxMergeJobs 允许进行的 Merge 的任务个数，default: 16
	MaxMergeJobs int64 // WithMaxMergeJobs
	// MinFilesMerge 控制 Merge 最少合并文件个数，default：2
	//
	// Deprecated: useless in Merge all in one file
	MinFilesMerge int // WithMinFilesMerge
	// FileCacheSize 控制 Merge 过程中，允许缓存的文件大小，default: 32 MB
	FileCacheSize int64

	// logger
	logger *log.MOLogger

	mp *mpool.MPool

	// flow ctrl
	ctx        context.Context
	cancelFunc context.CancelFunc

	runningJobs chan struct{}
}

type MergeOption func(*Merge)

func (opt MergeOption) Apply(m *Merge) {
	opt(m)
}

func WithTable(tbl *table.Table) MergeOption {
	return MergeOption(func(m *Merge) {
		m.Table = tbl
	})
}
func WithFileService(fs fileservice.FileService) MergeOption {
	return MergeOption(func(m *Merge) {
		m.FS = fs
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

func WithMinFilesMerge(files int) MergeOption {
	return MergeOption(func(m *Merge) {
		m.MinFilesMerge = files
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
		datetime:      time.Now(),
		pathBuilder:   table.NewAccountDatePathBuilder(),
		MaxFileSize:   128 * mpool.MB,
		MaxMergeJobs:  16,
		MinFilesMerge: 1,
		FileCacheSize: 32 * mpool.MB,
		logger:        runtime.ProcessLevelRuntime().Logger().WithContext(ctx).Named(LoggerNameETLMerge),
	}
	m.ctx, m.cancelFunc = context.WithCancel(ctx)
	for _, opt := range opts {
		opt(m)
	}
	if m.mp, err = getMpool(); err != nil {
		return nil, err
	}
	m.valid(ctx)
	m.runningJobs = make(chan struct{}, m.MaxMergeJobs)
	return m, nil
}

// valid check missing init elems. Panic with has missing elems.
func (m *Merge) valid(ctx context.Context) {
	if m.Table == nil {
		panic(moerr.NewInternalError(ctx, "merge task missing input 'Table'"))
	}
	if m.FS == nil {
		panic(moerr.NewInternalError(ctx, "merge task missing input 'FileService'"))
	}
}

// Start for service Loop
func (m *Merge) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case ts := <-ticker.C:
			m.Main(ctx, ts)
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

// Main handle cron job
// foreach all
func (m *Merge) Main(ctx context.Context, ts time.Time) error {
	var files = make([]*FileMeta, 0, 1000)
	var totalSize int64

	m.datetime = ts
	if m.datetime.IsZero() {
		return moerr.NewInternalError(ctx, "Merge Task missing input 'datetime'")
	}
	accounts, err := m.FS.List(ctx, "/")
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
		rootPath := m.pathBuilder.Build(account.Name, table.MergeLogTypeLogs, m.datetime, m.Table.GetDatabase(), m.Table.GetName())
		// get all file entry

		fileEntrys, err := m.FS.List(ctx, rootPath)
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
				if err = m.doMergeFiles(ctx, account.Name, files, totalSize); err != nil {
					m.logger.Error(fmt.Sprintf("merge task meet error: %v", err))
				}
				files = files[:0]
				totalSize = 0
			}
		}

		if len(files) > 0 {
			if err = m.doMergeFiles(ctx, account.Name, files, 0); err != nil {
				m.logger.Warn(fmt.Sprintf("merge task meet error: %v", err))
			}
		}
	}

	return err
}

// ListRange do list all accounts, all dates which belong to m.Table.GetName()
func (m *Merge) ListRange(ctx context.Context) error {
	var files = make([]*FileMeta, 0, 1000)
	var totalSize int64

	accounts, err := m.FS.List(ctx, "/")
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
		targetPath := m.pathBuilder.Build(account.Name, table.MergeLogTypeLogs, table.ETLParamTSAll, m.Table.GetDatabase(), m.Table.GetName())

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
			m.logger.Info("start merge", logutil.TableField(m.Table.GetIdentify()), logutil.PathField(rootPath),
				zap.String("metadata.ID", m.Task.Metadata.ID))

			fileEntrys, err := m.FS.List(ctx, rootPath)
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
					if err = m.doMergeFiles(ctx, account.Name, files, totalSize); err != nil {
						m.logger.Error(fmt.Sprintf("merge task meet error: %v", err))
					}
					files = files[:0]
					totalSize = 0
				}
			}

			if len(files) > 0 {
				if err = m.doMergeFiles(ctx, account.Name, files, 0); err != nil {
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
			entries, err := m.FS.List(ctx, prefix)
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

// doMergeFiles handle merge{read, write, delete} ops
// Upload the files to SQL table
// Delete the files from local disk
func (m *Merge) doMergeFiles(ctx context.Context, account string, files []*FileMeta, bufferSize int64) error {
	ctx, span := trace.Start(ctx, "doMergeFiles")
	defer span.End()

	// Control task concurrency
	m.runningJobs <- struct{}{}
	defer func() {
		<-m.runningJobs
	}()

	// Step 3. do simple merge
	var uploadFile = func(ctx context.Context, fp *FileMeta) error {
		row := m.Table.GetRow(ctx)
		defer row.Free()
		cacheFileData := &SliceCache{}
		defer cacheFileData.Reset()
		// open reader
		reader, err := newETLReader(ctx, m.Table, m.FS, fp.FilePath, fp.FileSize, m.mp)
		defer reader.Close()
		if err != nil {
			m.logger.Error(fmt.Sprintf("merge file meet read failed: %v", err))
			return err
		}

		// read all content
		var line []string
		line, err = reader.ReadLine()
		for ; line != nil && err == nil; line, err = reader.ReadLine() {
			if err = row.ParseRow(line); err != nil {
				m.logger.Error("parse ETL rows failed",
					logutil.TableField(m.Table.GetIdentify()),
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

		// sql insert
		if cacheFileData.Size() > 0 {
			if err = cacheFileData.Flush(m.Table); err != nil {
				return err
			}
			cacheFileData.Reset()
		}
		// delete empty file or file already uploaded
		if cacheFileData.Size() == 0 {
			if err = m.FS.Delete(ctx, fp.FilePath); err != nil {
				m.logger.Warn("failed to delete file", zap.Error(err))
				return err
			}
		}
		return nil
	}

	for _, fp := range files {
		if err := uploadFile(ctx, fp); err != nil {
			// todo: adjust the sleep settings
			// Sleep 10 seconds to wait for the database to recover
			time.Sleep(10 * time.Second)
			m.logger.Error("failed to write sql",
				logutil.TableField(m.Table.GetIdentify()),
				logutil.PathField(fp.FilePath),
				zap.Error(err),
			)
		}
	}
	logutil.Info("upload files success", logutil.TableField(m.Table.GetIdentify()), zap.Int("file count", len(files)))

	return nil
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

var _ ETLWriter = (*ContentWriter)(nil)

type ContentWriter struct {
	writer io.StringWriter
	buf    *bytes.Buffer
	parser *csv.Writer
}

func (w *ContentWriter) WriteRow(row *table.Row) error {
	panic("not implement")
}

func NewContentWriter(writer io.StringWriter, buffer []byte) *ContentWriter {
	buf := bytes.NewBuffer(buffer)
	return &ContentWriter{
		writer: writer,
		buf:    buf,
		parser: csv.NewWriter(buf),
	}
}

func (w *ContentWriter) WriteStrings(record []string) error {
	return nil
}

func (w *ContentWriter) FlushAndClose() (int, error) {
	return w.writer.WriteString(w.buf.String())
}

func newETLWriter(ctx context.Context, fs fileservice.FileService, filePath string, buf []byte, tbl *table.Table, mp *mpool.MPool) (ETLWriter, error) {

	if strings.LastIndex(filePath, table.TaeExtension) > 0 {
		writer := etl.NewTAEWriter(ctx, tbl, mp, filePath, fs)
		return writer, nil
	} else {
		// CSV
		fsWriter := etl.NewFSWriter(ctx, fs, etl.WithFilePath(filePath))
		return etl.NewCSVWriter(ctx, fsWriter), nil
	}
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

func LongRunETLMerge(ctx context.Context, task task.Task, logger *log.MOLogger, opts ...MergeOption) error {
	// should init once in/with schema-init.
	tables := table.GetAllTable()
	if len(tables) == 0 {
		logger.Info("empty tables")
		return nil
	}

	newOptions := []MergeOption{WithMaxFileSize(maxFileSize.Load())}
	newOptions = append(newOptions, opts...)
	newOptions = append(newOptions, WithTable(tables[0]))
	merge, err := NewMerge(ctx, newOptions...)
	if err != nil {
		return err
	}
	merge.Task = task

	logger.Info("start LongRunETLMerge")
	// handle today
	for _, tbl := range tables {
		merge.Table = tbl
		if err = merge.ListRange(ctx); err != nil {
			logger.Error("merge metric failed", zap.Error(err))
		}
	}

	return nil
}

func MergeTaskExecutorFactory(opts ...MergeOption) func(ctx context.Context, task task.Task) error {

	CronMerge := func(ctx context.Context, task task.Task) error {
		ctx, span := trace.Start(ctx, "CronMerge")
		defer span.End()

		args := task.Metadata.Context
		ts := time.Now()
		logger := runtime.ProcessLevelRuntime().Logger().WithContext(ctx).Named(LoggerNameETLMerge)
		logger.Info(fmt.Sprintf("start merge '%s' at %v, ID: %d, CreateAt: %d, Metadata.ID: %s", args, ts,
			task.ID, task.CreateAt, task.Metadata.ID))
		defer logger.Info(fmt.Sprintf("done merge '%s', ID: %d, CreateAt: %d, Metadata.ID: %s", args,
			task.ID, task.CreateAt, task.Metadata.ID))

		// new branch: task run long time
		if len(args) == 0 {
			if err := LongRunETLMerge(ctx, task, logger, opts...); err != nil {
				return err
			}
			return nil
		}

		// old branch
		elems := strings.Split(string(args), ParamSeparator)
		id := elems[0]
		table, exist := table.GetTable(id)
		if !exist {
			return moerr.NewNotSupported(ctx, "merge task not support table: %s", id)
		}
		if !table.PathBuilder.SupportMergeSplit() {
			logger.Info("not support merge task", logutil.TableField(table.GetIdentify()))
			return nil
		}
		if len(elems) == 2 {
			date := elems[1]
			switch date {
			case MergeTaskToday:
			case MergeTaskYesterday:
				ts = ts.Add(-24 * time.Hour)
			default:
				var err error
				// try to parse date format like '2021-01-01'
				if ts, err = time.Parse("2006-01-02", date); err != nil {
					return moerr.NewNotSupported(ctx, "merge task not support args: %s", args)
				}
			}
		}

		// handle metric
		newOptions := []MergeOption{WithMaxFileSize(maxFileSize.Load())}
		newOptions = append(newOptions, opts...)
		newOptions = append(newOptions, WithTable(table))
		merge, err := NewMerge(ctx, newOptions...)
		if err != nil {
			return err
		}
		merge.Task = task
		if err = merge.Main(ctx, ts); err != nil {
			logger.Error(fmt.Sprintf("merge metric failed: %v", err))
			return err
		}

		return nil
	}
	return CronMerge
}

// MergeTaskCronExpr support sec level
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
		ID:       path.Join("ETL_merge_task", path.Join(args...)),
		Executor: id,
		Context:  []byte(strings.Join(args, ParamSeparator)),
		Options:  task.TaskOptions{Concurrency: 1},
	}
}

func CreateCronTask(ctx context.Context, executorID task.TaskCode, taskService taskservice.TaskService) error {
	var err error
	ctx, span := trace.Start(ctx, "ETLMerge.CreateCronTask")
	defer span.End()
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

var maxFileSize atomic.Int64

func InitMerge(ctx context.Context, SV *config.ObservabilityParameters) error {
	var err error
	mergeCycle := SV.MergeCycle.Duration
	filesize := SV.MergeMaxFileSize
	if mergeCycle > 0 {
		err = InitCronExpr(ctx, mergeCycle)
		if err != nil {
			return err
		}
	}
	maxFileSize.Store(int64(filesize * mpool.MB))
	return nil
}
