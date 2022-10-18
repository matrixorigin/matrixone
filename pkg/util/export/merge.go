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
	"io"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/simdcsv"
)

// ========================
// handle merge
// ========================

// Merge like a compaction, merge input files into one/two/... files.
type Merge struct {
	Table       *Table                  // see WithTable
	DB          string                  // see WithDB
	FS          fileservice.FileService // see WithFileService
	FSName      string                  // see WithFileServiceName, cooperate with FS
	datetime    time.Time               // see Main
	pathBuilder PathBuilder             // const as NewMetricLogPathBuilder()

	// MaxFileSize 控制合并后最大文件大小, default: 128 MB
	MaxFileSize int64 // see WithMaxFileSize
	// MaxMergeJobs 允许进行的Merge的任务个数，default: 16
	MaxMergeJobs int64 // see WithMaxMergeJobs
	// MinFilesMerge 控制Merge最少合并文件个数，default：2
	//
	// Deprecated: useless in Merge all in one file
	MinFilesMerge int // see WithMinFilesMerge
	// FileCacheSize 控制Merge 过程中, 允许缓存的文件大小，default: 16 MB
	//
	// Deprecated: useless while NOT support multiParts upload
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
func WithDB(db string) MergeOption {
	return MergeOption(func(m *Merge) {
		m.DB = db
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
		FSName:        etlFileServiceName,
		datetime:      time.Now(),
		pathBuilder:   NewMetricLogPathBuilder(),
		MaxFileSize:   128 * mpool.MB,
		MaxMergeJobs:  16,
		MinFilesMerge: 2,
		FileCacheSize: mpool.PB, // disable it by set very large
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
		panic(moerr.NewInternalError("Merge Task missing input 'Table'"))
	}
	if m.FS == nil {
		panic(moerr.NewInternalError("Merge Task missing input 'FileService'"))
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
	logutil.Debugf("Merge start on %s.%s, %v", m.DB, m.Table.GetName(), m.datetime)
	accounts, err := m.FS.List(m.ctx, "/")
	if err != nil {
		return err
	}
	for _, account := range accounts {
		if !account.IsDir {
			logutil.Warnf("path is not dir: %s", account.Name)
			continue
		}
		rootPath := m.pathBuilder.Build(account.Name, MergeLogTypeLog, m.datetime, m.DB, m.Table.GetName())
		// get all file entry

		fileEntrys, err := m.FS.List(m.ctx, rootPath)
		if err != nil {
			// fixme: logutil.Error()
			return err
		}
		files = files[:0]
		for _, f := range fileEntrys {
			filepath := path.Join(rootPath, f.Name)
			totalSize += f.Size
			files = append(files, filepath)
		}

		if err := m.doMergeFiles(account.Name, files); err != nil {
			logutil.Errorf("err: %v\n", err)
		}
	}

	return err
}

// doMergeFiles handle merge{read, write, delete} ops
// Step 1. find new timestamp_start, timestamp_end.
// Step 2. make new filename, file writer
// Step 3. read file data(valid format), and write down new file
// Step 4. delete old files.
func (m *Merge) doMergeFiles(account string, paths []string) error {

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
	for _, path := range paths {
		p, err := m.pathBuilder.ParsePath(path)
		if err != nil {
			return err
		}
		ts := p.Timestamp()
		if len(ts) == 0 {
			// fixme: logutil.Warn
			continue
		}
		timestamps = append(timestamps, ts[0])
	}
	if len(timestamps) <= 1 {
		return moerr.NewInternalError("CSVMerge: only one timestamp")
	}
	timestampStart := timestamps[0]
	timestampEnd := timestamps[len(timestamps)-1]

	// Step 2. new filename, file writer
	prefix := m.pathBuilder.Build(account, MergeLogTypeMerged, m.datetime, m.DB, m.Table.GetName())
	mergeFilename := m.pathBuilder.NewMergeFilename(timestampStart, timestampEnd)
	mergeFilepath := path.Join(prefix, mergeFilename)
	newFileWriter, _ := NewCSVWriter(m.ctx, m.FS, mergeFilepath)

	// Step 3. do simple merge
	cacheFileData := m.Table.NewRowCache()
	for _, path := range paths {
		reader, err := NewCSVReader(m.ctx, m.FS, path)
		if err != nil {
			// fixme: handle this path ? just return
			// errorFileHandler(m.ctx, m.FS, path) without continue
			continue
		}
		for line := reader.ReadLine(); line != nil; line = reader.ReadLine() {

			row := m.Table.ParseRow(line)
			// fixme: if !obj.Valid() { continue }
			cacheFileData.Put(row) // if table_name == "statement_info", try to save last record.
		}
		// fixme: reader.Close()
		if cacheFileData.Size() > m.FileCacheSize {
			if err := cacheFileData.Flush(newFileWriter); err != nil {
				// fixme: handle error situation
				logutil.Errorf("merge file meet flush error: %v", err)
			}
			cacheFileData.Reset()
		}
	}
	if !cacheFileData.IsEmpty() {
		if err := cacheFileData.Flush(newFileWriter); err != nil {
			// fixme: handle error situation
			logutil.Errorf("merge file meet flush error: %v", err)
		}
		cacheFileData.Reset()
	}
	newFileWriter.FlushAndClose()

	// step 4. delete old files
	err := m.FS.Delete(m.ctx, paths...)

	return err
}

type CSVReader interface {
	ReadLine() []string
}

type ContentReader struct {
	idx     int
	length  int
	content [][]string
}

func NewContentReader(content [][]string) *ContentReader {
	return &ContentReader{
		length:  len(content),
		content: content,
	}
}

func (s *ContentReader) ReadLine() []string {
	if s.idx < s.length {
		idx := s.idx
		s.idx++
		return s.content[idx]
	}
	return nil
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
	defer reader.Close()

	// parse csv content
	simdCsvReader := simdcsv.NewReaderWithOptions(reader,
		CommonCsvOptions.FieldTerminator,
		'#',
		true,
		true)
	defer simdCsvReader.Close()
	content, err := simdCsvReader.ReadAll(ctx)
	if err != nil {
		return nil, err
	}

	// return content Reader
	return NewContentReader(content), nil
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

func NewContentWriter(writer io.StringWriter) *ContentWriter {
	buf := bytes.NewBuffer(nil)
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

func NewCSVWriter(ctx context.Context, fs fileservice.FileService, path string) (CSVWriter, error) {

	factory := GetFSWriterFactory(fs, "", "")
	fsWriter := factory(ctx, "", nil, WithFilePath(path))

	return NewContentWriter(fsWriter), nil
}

type Cache interface {
	Put(*Row)
	Size() int64
	Flush(CSVWriter) error
	Reset()
	IsEmpty() bool
}

type SliceCache struct {
	m    []*Row
	size int64
}

func (c *SliceCache) Flush(writer CSVWriter) error {
	for _, record := range c.m {
		if err := writer.WriteStrings(record.ToStrings()); err != nil {
			return err
		}
	}
	return nil
}

func (c *SliceCache) Reset() {
	c.m = c.m[:]
	c.size = 0
}

func (c *SliceCache) IsEmpty() bool {
	return len(c.m) == 0
}

func (c *SliceCache) Put(r *Row) {
	c.m = append(c.m, r)
	c.size += r.Size()
}

func (c *SliceCache) Size() int64 { return c.size }

func (c *MapCache) Size() int64 { return c.size }

type MapCache struct {
	m    map[string]*Row
	size int64
}

func (c *MapCache) Flush(writer CSVWriter) error {
	for _, record := range c.m {
		if err := writer.WriteStrings(record.ToStrings()); err != nil {
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
	c.m[r.PrimaryKey()] = r
	c.size += r.Size()
}

func (tbl *Table) NewRowCache() Cache {
	if len(tbl.PrimaryKeyColumn) == 0 {
		return &SliceCache{}
	} else {
		return &MapCache{m: make(map[string]*Row)}
	}
}

func (tbl *Table) ParseRow(cols []string) *Row {
	r := tbl.GetRow()
	copy(r.Columns[:], cols[:])
	return r
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
