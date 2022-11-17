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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"

	"github.com/matrixorigin/simdcsv"
)

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
	Table       *Table                  // WithTable
	FS          fileservice.FileService // WithFileService
	FSName      string                  // WithFileServiceName, cooperate with FS
	datetime    time.Time               // see Main
	pathBuilder PathBuilder             // const as NewAccountDatePathBuilder()

	// MaxFileSize 控制合并后最大文件大小, default: 128 MB
	MaxFileSize int64 // WithMaxFileSize
	// MaxMergeJobs 允许进行的Merge的任务个数，default: 16
	MaxMergeJobs int64 // WithMaxMergeJobs
	// MinFilesMerge 控制Merge最少合并文件个数，default：2
	//
	// Deprecated: useless in Merge all in one file
	MinFilesMerge int // WithMinFilesMerge
	// FileCacheSize 控制Merge 过程中, 允许缓存的文件大小，default: 32 MB
	FileCacheSize int64

	// flow ctrl
	ctx        context.Context
	cancelFunc context.CancelFunc

	runningJobs chan struct{}
}

type MergeOption func(*Merge)

func (opt MergeOption) Apply(m *Merge) {
	opt(m)
}

func WithTable(tbl *Table) MergeOption {
	return MergeOption(func(m *Merge) {
		m.Table = tbl
	})
}
func WithFileService(fs fileservice.FileService) MergeOption {
	return MergeOption(func(m *Merge) {
		m.FS = fs
	})
}
func WithFileServiceName(name string) MergeOption {
	return MergeOption(func(m *Merge) {
		m.FSName = name
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

func NewMergeService(ctx context.Context, opts ...MergeOption) (*Merge, bool) {
	// fix multi-init in standalone
	if !atomic.CompareAndSwapUint32(&serviceInited, 0, 1) {
		return nil, true
	}
	return NewMerge(ctx, opts...), false
}

func NewMerge(ctx context.Context, opts ...MergeOption) *Merge {
	m := &Merge{
		FSName:        defines.ETLFileServiceName,
		datetime:      time.Now(),
		pathBuilder:   NewAccountDatePathBuilder(),
		MaxFileSize:   128 * mpool.MB,
		MaxMergeJobs:  16,
		MinFilesMerge: 1,
		FileCacheSize: 32 * mpool.MB,
	}
	m.ctx, m.cancelFunc = context.WithCancel(ctx)
	for _, opt := range opts {
		opt(m)
	}
	if fs, err := fileservice.Get[fileservice.FileService](m.FS, m.FSName); err == nil {
		m.FS = fs
	}
	m.valid()
	m.runningJobs = make(chan struct{}, m.MaxMergeJobs)
	return m
}

// valid check missing init elems. Panic with has missing elems.
func (m *Merge) valid() {
	if m.Table == nil {
		panic(moerr.NewInternalError("merge task missing input 'Table'"))
	}
	if m.FS == nil {
		panic(moerr.NewInternalError("merge task missing input 'FileService'"))
	}
}

// Start for service Loop
func (m *Merge) Start(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case ts := <-ticker.C:
			m.Main(ts)
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

// Main handle cron job
// foreach all
func (m *Merge) Main(ts time.Time) error {
	var files = make([]string, 0, 1000)
	var totalSize int64

	m.datetime = ts
	if m.datetime.IsZero() {
		return moerr.NewInternalError("Merge Task missing input 'datetime'")
	}
	accounts, err := m.FS.List(m.ctx, "/")
	if err != nil {
		return err
	}
	if len(accounts) == 0 {
		logutil.Info("merge find empty data")
		return nil
	}
	logutil.Debugf("merge task with max file: %v MB", m.MaxFileSize/mpool.MB)
	for _, account := range accounts {
		if !account.IsDir {
			logutil.Warnf("path is not dir: %s", account.Name)
			continue
		}
		rootPath := m.pathBuilder.Build(account.Name, MergeLogTypeLogs, m.datetime, m.Table.GetDatabase(), m.Table.GetName())
		// get all file entry

		fileEntrys, err := m.FS.List(m.ctx, rootPath)
		if err != nil {
			// fixme: logutil.Error()
			return err
		}
		files = files[:0]
		totalSize = 0
		for _, f := range fileEntrys {
			filepath := path.Join(rootPath, f.Name)
			totalSize += f.Size
			files = append(files, filepath)
			if totalSize > m.MaxFileSize {
				if err = m.doMergeFiles(account.Name, files, totalSize); err != nil {
					logutil.Errorf("merge task meet error: %v", err)
				}
				files = files[:0]
				totalSize = 0
			}
		}

		if len(files) > 0 {
			if err = m.doMergeFiles(account.Name, files, 0); err != nil {
				logutil.Errorf("merge task meet error: %v", err)
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
func (m *Merge) doMergeFiles(account string, paths []string, bufferSize int64) error {

	// Control task concurrency
	m.runningJobs <- struct{}{}
	defer func() {
		<-m.runningJobs
	}()

	if len(paths) < m.MinFilesMerge {
		return moerr.NewInternalError("file cnt(%d) less then threshold(%d)", len(paths), m.MinFilesMerge)
	}

	// Step 1. group by node_uuid, find target timestamp
	timestamps := make([]string, 0, len(paths))
	for _, path_ := range paths {
		p, err := m.pathBuilder.ParsePath(path_)
		if err != nil {
			return err
		}
		ts := p.Timestamp()
		if len(ts) == 0 {
			logutil.Warnf("merge file meet unknown file: %s", path_)
			continue
		}
		timestamps = append(timestamps, ts[0])
	}
	if len(timestamps) == 0 {
		return moerr.NewNotSupported("csv merge: NO timestamp for merge")
	}
	timestampStart := timestamps[0]
	timestampEnd := timestamps[len(timestamps)-1]

	// new buffer
	if bufferSize <= 0 {
		bufferSize = m.MaxFileSize
	}
	buf := make([]byte, 0, bufferSize)

	// Step 2. new filename, file writer
	prefix := m.pathBuilder.Build(account, MergeLogTypeMerged, m.datetime, m.Table.GetDatabase(), m.Table.GetName())
	mergeFilename := m.pathBuilder.NewMergeFilename(timestampStart, timestampEnd)
	mergeFilepath := path.Join(prefix, mergeFilename)
	newFileWriter, _ := NewCSVWriter(m.ctx, m.FS, mergeFilepath, buf)

	// Step 3. do simple merge
	cacheFileData := m.Table.NewRowCache()
	row := m.Table.GetRow()
	for _, path := range paths {
		reader, err := NewCSVReader(m.ctx, m.FS, path)
		if err != nil {
			logutil.Errorf("merge file meet read failed: %v", err)
			return err
		}
		var line []string
		for line, err = reader.ReadLine(); line != nil && err == nil; line, err = reader.ReadLine() {
			if err = row.ParseRow(line); err != nil {
				continue
			}
			cacheFileData.Put(row)
		}
		if err != nil {
			return err
		}
		// check cache size
		if cacheFileData.Size() > m.FileCacheSize {
			if err = cacheFileData.Flush(newFileWriter); err != nil {
				logutil.Errorf("merge file meet flush failed: %v", err)
				return err
			}
			cacheFileData.Reset()
		}
		reader.Close()
	}
	if !cacheFileData.IsEmpty() {
		if err := cacheFileData.Flush(newFileWriter); err != nil {
			logutil.Errorf("merge file meet flush failed: %v", err)
			return err
		}
		cacheFileData.Reset()
	}
	if err := newFileWriter.FlushAndClose(); err != nil {
		logutil.Errorf("merge file meet write failed: %v", err)
		return err
	}

	// step 4. delete old files
	if err := m.FS.Delete(m.ctx, paths...); err != nil {
		logutil.Errorf("merge file meet delete failed: %v", err)
		return err
	}

	return nil
}

type CSVReader interface {
	ReadLine() ([]string, error)
	Close()
}

type ContentReader struct {
	ctx     context.Context
	idx     int
	length  int
	content [][]string

	reader *simdcsv.Reader
	raw    io.ReadCloser
}

// BatchReadRows ~= 20MB rawlog file has about 3700+ rows
const BatchReadRows = 4000

func NewContentReader(ctx context.Context, reader *simdcsv.Reader, raw io.ReadCloser) *ContentReader {
	return &ContentReader{
		ctx:     ctx,
		length:  0,
		content: make([][]string, BatchReadRows),
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
		}
		if cnt < BatchReadRows {
			//s.reader.Close() // just empty op and never end.
			s.reader = nil
			s.raw.Close()
			s.raw = nil
		}
		s.idx = 0
		s.length = cnt
	}
	if s.idx < s.length {
		idx := s.idx
		s.idx++
		return s.content[idx], nil
	}
	return nil, nil
}

func (s *ContentReader) Close() {
	capLen := cap(s.content)
	s.content = s.content[:capLen]
	for idx := range s.content {
		s.content[idx] = nil
	}
}

func NewCSVReader(ctx context.Context, fs fileservice.FileService, path string) (CSVReader, error) {
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
		CommonCsvOptions.FieldTerminator,
		'#',
		true,
		true)

	// return content Reader
	return NewContentReader(ctx, simdCsvReader, reader), nil
}

type CSVWriter interface {
	// WriteStrings write record as one line into csv file
	WriteStrings(record []string) error
	// FlushAndClose flush its buffer and close.
	FlushAndClose() error
}

var _ CSVWriter = (*ContentWriter)(nil)

type ContentWriter struct {
	writer io.StringWriter
	buf    *bytes.Buffer
	parser *csv.Writer
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

func (w *ContentWriter) FlushAndClose() error {
	_, err := w.writer.WriteString(w.buf.String())
	return err
}

func NewCSVWriter(ctx context.Context, fs fileservice.FileService, path string, buf []byte) (CSVWriter, error) {

	factory := GetFSWriterFactory(fs, "", "")
	fsWriter := factory(ctx, "", nil, WithFilePath(path))

	return NewContentWriter(fsWriter, buf), nil
}

type Cache interface {
	Put(*Row)
	Size() int64
	Flush(CSVWriter) error
	Reset()
	IsEmpty() bool
}

type SliceCache struct {
	m    [][]string
	size int64
}

func (c *SliceCache) Flush(writer CSVWriter) error {
	for _, record := range c.m {
		if err := writer.WriteStrings(record); err != nil {
			return err
		}
	}
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

func (c *SliceCache) Put(r *Row) {
	c.m = append(c.m, r.ToRawStrings())
	c.size += r.Size()
}

func (c *SliceCache) Size() int64 { return c.size }

func (c *MapCache) Size() int64 { return c.size }

type MapCache struct {
	m    map[string][]string
	size int64
}

func (c *MapCache) Flush(writer CSVWriter) error {
	for _, record := range c.m {
		if err := writer.WriteStrings(record); err != nil {
			return err
		}
	}
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

func (c *MapCache) Put(r *Row) {
	c.m[r.PrimaryKey()] = r.ToRawStrings()
	c.size += r.Size()
}

func (tbl *Table) NewRowCache() Cache {
	if len(tbl.PrimaryKeyColumn) == 0 {
		return &SliceCache{}
	} else {
		return &MapCache{m: make(map[string][]string)}
	}
}

func (r *Row) ParseRow(cols []string) error {
	r.Columns = cols
	return nil
}

func (r *Row) PrimaryKey() string {
	if len(r.Table.PrimaryKeyColumn) == 0 {
		return ""
	}
	if len(r.Table.PrimaryKeyColumn) == 1 {
		return r.Columns[r.Name2ColumnIdx[r.Table.PrimaryKeyColumn[0].Name]]
	}
	sb := strings.Builder{}
	for _, col := range r.Table.PrimaryKeyColumn {
		sb.WriteString(r.Columns[r.Name2ColumnIdx[col.Name]])
		sb.WriteRune('-')
	}
	return sb.String()
}

func (r *Row) Size() (size int64) {
	for _, col := range r.Columns {
		size += int64(len(col))
	}
	return
}

func MergeTaskExecutorFactory(opts ...MergeOption) func(ctx context.Context, task task.Task) error {

	return func(ctx context.Context, task task.Task) error {

		args := task.Metadata.Context
		ts := time.Now()
		logutil.Infof("start merge '%s' at %v", args, ts)

		elems := strings.Split(string(args), ParamSeparator)
		id := elems[0]
		table, exist := gTable[id]
		if !exist {
			return moerr.NewNotSupported("merge task not support table: %s", id)
		}
		if !table.PathBuilder.SupportMergeSplit() {
			logutil.Info("not support merge task", logutil.TableField(table.GetIdentify()))
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
					return moerr.NewNotSupported("merge task not support args: %s", args)
				}
			}
		}

		// handle metric
		newOptions := []MergeOption{WithMaxFileSize(maxFileSize.Load())}
		newOptions = append(newOptions, opts...)
		newOptions = append(newOptions, WithTable(table))
		merge := NewMerge(ctx, newOptions...)
		if err := merge.Main(ts); err != nil {
			logutil.Errorf("merge metric failed: %v", err)
			return err
		}

		return nil
	}
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
		Executor: uint32(id),
		Context:  []byte(strings.Join(args, ParamSeparator)),
	}
}

func CreateCronTask(ctx context.Context, executorID task.TaskCode, taskService taskservice.TaskService) error {
	var err error
	// should init once in/with schema-init.
	tables := GetAllTable()
	logutil.Infof("init merge task with CronExpr: %s", MergeTaskCronExpr)
	for _, tbl := range tables {
		logutil.Debugf("init table merge task: %s", tbl.GetIdentify())
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
func InitCronExpr(duration time.Duration) error {
	if duration < 0 || duration > 12*time.Hour {
		return moerr.NewNotSupported("export cron expr not support cycle: %v", duration)
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

func InitMerge(mergeCycle time.Duration, filesize int) error {
	var err error
	if mergeCycle > 0 {
		err = InitCronExpr(mergeCycle)
		if err != nil {
			return err
		}
	}
	maxFileSize.Store(int64(filesize * mpool.MB))
	return nil
}
