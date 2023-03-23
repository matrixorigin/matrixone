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
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/trace"

	"github.com/matrixorigin/simdcsv"
	"go.uber.org/zap"
)

const LoggerNameETLMerge = "ETLMerge"
const LoggerNameContentReader = "ETLContentReader"

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
	accounts, err := m.FS.List(m.ctx, "/")
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

		fileEntrys, err := m.FS.List(m.ctx, rootPath)
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

// doMergeFiles handle merge{read, write, delete} ops
// Step 1. find new timestamp_start, timestamp_end.
// Step 2. make new filename, file writer
// Step 3. read file data(valid format), and write down new file
// Step 4. delete old files.
func (m *Merge) doMergeFiles(ctx context.Context, account string, files []*FileMeta, bufferSize int64) error {

	var err error
	ctx, span := trace.Start(ctx, "doMergeFiles")
	defer span.End()

	// Control task concurrency
	m.runningJobs <- struct{}{}
	defer func() {
		<-m.runningJobs
	}()

	if len(files) < m.MinFilesMerge {
		return moerr.NewInternalError(ctx, "file cnt %d less then threshold %d", len(files), m.MinFilesMerge)
	}

	// Step 1. group by node_uuid, find target timestamp
	timestamps := make([]string, 0, len(files))
	var p table.Path
	for _, f := range files {
		p, err = m.pathBuilder.ParsePath(ctx, f.FilePath)
		if err != nil {
			return err
		}
		ts := p.Timestamp()
		if len(ts) == 0 {
			m.logger.Warn(fmt.Sprintf("merge file meet unknown file: %s", f.FilePath))
			continue
		}
		timestamps = append(timestamps, ts[0])
	}
	if len(timestamps) == 0 {
		return moerr.NewNotSupported(ctx, "csv merge: NO timestamp for merge")
	}
	timestampStart := timestamps[0]
	timestampEnd := timestamps[len(timestamps)-1]

	// new buffer
	if bufferSize <= 0 {
		bufferSize = m.MaxFileSize
	}
	var buf []byte = nil
	if mergedExtension == table.CsvExtension {
		buf = make([]byte, 0, bufferSize)
	}

	// Step 2. new filename, file writer
	prefix := m.pathBuilder.Build(account, table.MergeLogTypeMerged, m.datetime, m.Table.GetDatabase(), m.Table.GetName())
	mergeFilename := m.pathBuilder.NewMergeFilename(timestampStart, timestampEnd, mergedExtension)
	mergeFilepath := path.Join(prefix, mergeFilename)
	newFileWriter, _ := newETLWriter(m.ctx, m.FS, mergeFilepath, buf, m.Table, m.mp)

	// Step 3. do simple merge
	cacheFileData := newRowCache(m.Table)
	row := m.Table.GetRow(ctx)
	defer row.Free()
	defer cacheFileData.Reset()
	var reader ETLReader
	for _, path := range files {
		// open reader
		reader, err = newETLReader(m.ctx, m.Table, m.FS, path.FilePath, path.FileSize, m.mp)
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
					logutil.PathField(path.FilePath),
					logutil.VarsField(SubStringPrefixLimit(fmt.Sprintf("%v", line), 102400)),
				)
				reader.Close()
				return err
			}
			cacheFileData.Put(row)
		}
		if err != nil {
			m.logger.Warn("failed to read file",
				logutil.PathField(path.FilePath), zap.Error(err))
			reader.Close()
			return err
		}

		// flush cache data
		if cacheFileData.Size() > m.FileCacheSize {
			if err = cacheFileData.Flush(newFileWriter); err != nil {
				m.logger.Warn("failed to write merged etl file",
					logutil.PathField(mergeFilepath), zap.Error(err))
				reader.Close()
				return err
			}
		}
		reader.Close()
	}
	// flush cache data
	if !cacheFileData.IsEmpty() {
		if err = cacheFileData.Flush(newFileWriter); err != nil {
			m.logger.Warn("failed to write merged etl file",
				logutil.PathField(mergeFilepath), zap.Error(err))
			return err
		}
	}
	// close writer
	if _, err = newFileWriter.FlushAndClose(); err != nil {
		m.logger.Warn("failed to write merged file",
			logutil.PathField(mergeFilepath), zap.Error(err))
		return err
	}

	// step 4. delete old files
	paths := make([]string, len(files))
	for idx, f := range files {
		paths[idx] = f.FilePath
	}
	if err = m.FS.Delete(m.ctx, paths...); err != nil {
		m.logger.Warn("failed to delete input files", zap.Error(err))
		return err
	}

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
	if err := w.parser.Write(record); err != nil {
		return err
	}
	w.parser.Flush()
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
		return NewContentWriter(fsWriter, buf), nil
	}

}

type Cache interface {
	Put(*table.Row)
	Size() int64
	Flush(ETLWriter) error
	Reset()
	IsEmpty() bool
}

type SliceCache struct {
	m    [][]string
	size int64
}

func (c *SliceCache) Flush(writer ETLWriter) error {
	for _, record := range c.m {
		if err := writer.WriteStrings(record); err != nil {
			return err
		}
	}
	c.Reset()
	return nil
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

func (c *MapCache) Size() int64 { return c.size }

type MapCache struct {
	m    map[string][]string
	size int64
}

// Flush will do Reset
func (c *MapCache) Flush(writer ETLWriter) error {
	for _, record := range c.m {
		if err := writer.WriteStrings(record); err != nil {
			return err
		}
	}
	c.Reset()
	return nil
}

func (c *MapCache) Reset() {
	c.size = 0
	for key := range c.m {
		delete(c.m, key)
	}
}

func (c *MapCache) IsEmpty() bool {
	return len(c.m) == 0
}

func (c *MapCache) Put(r *table.Row) {
	c.m[r.CsvPrimaryKey()] = r.GetCsvStrings()
	c.size += r.Size()
}

func newRowCache(tbl *table.Table) Cache {
	if len(tbl.PrimaryKeyColumn) == 0 {
		return &SliceCache{}
	} else {
		return &MapCache{m: make(map[string][]string)}
	}
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
	// should init once in/with schema-init.
	tables := table.GetAllTable()
	logger.Info(fmt.Sprintf("init merge task with CronExpr: %s", MergeTaskCronExpr))
	for _, tbl := range tables {
		logger.Debug(fmt.Sprintf("init table merge task: %s", tbl.GetIdentify()))
		if err = taskService.CreateCronTask(ctx, MergeTaskMetadata(executorID, tbl.GetIdentify()), MergeTaskCronExpr); err != nil {
			return err
		}
		if err = taskService.CreateCronTask(ctx, MergeTaskMetadata(executorID, tbl.GetIdentify(), MergeTaskYesterday), MergeTaskCronExprYesterday); err != nil {
			return err
		}
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
var mergedExtension = table.GetExtension(table.CsvExtension)

func InitMerge(ctx context.Context, SV *config.ObservabilityParameters) error {
	var err error
	mergeCycle := SV.MergeCycle.Duration
	filesize := SV.MergeMaxFileSize
	ext := SV.MergedExtension
	if mergeCycle > 0 {
		err = InitCronExpr(ctx, mergeCycle)
		if err != nil {
			return err
		}
	}
	maxFileSize.Store(int64(filesize * mpool.MB))
	mergedExtension = table.GetExtension(ext)
	return nil
}
