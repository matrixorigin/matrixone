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

package trace

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export"
)

// ========================
// handle merge
// ========================

// FileName handle filename maker logic.
type FileName interface {
	Name() string
}

type CSVWriter interface {
	SetWriter(io.StringWriter)
	// WriteLine format all elem with default format and write one line into csv file
	WriteLine(elems []any) error
	// WriteStrings write all string elems as one line into csv file
	WriteStrings(elems []string) error
	// FlushAndClose flush its buffer and close.
	FlushAndClose() error
}

// =======================
// main logic
// =======================

// Merge like a compaction, merge input files into one/two/... files.
type Merge struct {
	Table       *Table                  // see With?
	Datetime    time.Time               // see With?
	FS          fileservice.FileService // see With?
	pathBuilder export.PathBuilder

	// MaxFileSize 控制合并后最大文件大小, default: 128 MB
	MaxFileSize int64 // see With?
	// MaxMergeJobs 允许进行的Merge的任务个数，default: 16
	MaxMergeJobs int64 // see With?
	// MinFilesMerge 控制Merge最少合并文件个数，default：2
	//
	// Deprecated: useless in Merge all in one file
	MinFilesMerge int //
	// FileCacheSize 控制Merge 过程中, 允许缓存的文件大小，default: 16 MB
	FileCacheSize int64 // see With?

	// flow ctrl

	ctx        context.Context
	cancelFunc context.CancelFunc
}

type MergeOption func(*Merge)

func (opt MergeOption) Apply(m *Merge) {
	opt(m)
}

func NewMerge(ctx context.Context, opts ...MergeOption) *Merge {
	m := &Merge{}
	m.ctx, m.cancelFunc = context.WithCancel(ctx)
	for _, opt := range opts {
		opt(m)
	}
	m.valid()
	// fixme: m.pathBuilder = ?? // init with Table and Datetime
	return m
}

// valid check missing init elems. Panic with has missing elems.
func (m *Merge) valid() {
	if m.Table == nil {
		panic(moerr.NewInternalError("Merge Task missing input 'Table'"))
	}
	if m.Datetime.IsZero() {
		panic(moerr.NewInternalError("Merge Task missing input 'Datetime'"))
	}
	if m.FS == nil {
		panic(moerr.NewInternalError("Merge Task missing input 'FileService'"))
	}
}

// Stop should call only once
func (m *Merge) Stop() {
	m.cancelFunc()
}

// Main handle cron job
// foreach all
func (m *Merge) Main() error {
	var files = make([]string, 1000)
	var totalSize int64

	accounts, err := m.FS.List(m.ctx, "/")
	if err != nil {
		return err
	}
	for _, account := range accounts {
		if !account.IsDir {
			logutil.Warnf("path is not dir: %s", account.Name)
			continue
		}
		rootPath := m.pathBuilder.Build(account.Name, export.MergeLogTypeLog, m.Datetime, m.Table)
		// get all file entry
		fileEntry, err := m.FS.List(m.ctx, rootPath)
		if err != nil {
			// fixme: logutil.Error()
			return err
		}
		for _, f := range fileEntry {
			filepath := m.pathBuilder.Join(f.Name)
			totalSize += f.Size
			files = append(files, filepath)
		}

		go m.doMergeFiles(account.Name, files, m.pathBuilder.Clone())

	}

	return err
}

var runningJobs int64

// doMergeFiles handle merge{read, write, delete} ops
// Step 1. find new timestamp_start, timestamp_end.
// Step 2. make new filename, file writer
// Step 3. read file data(valid format), and write down new file
// Step 4. delete old files.
func (m *Merge) doMergeFiles(account string, paths []string, pathBuilder export.PathBuilder) error {

	// fixme: Control task concurrency
	for runningJobs > m.MaxMergeJobs {
		// todo: wait
		time.Sleep(time.Minute)
		runningJobs--
	}

	if len(paths) < m.MinFilesMerge {
		return moerr.NewInternalError("file cnt(%d) less then threshold(%d)", len(paths), m.MinFilesMerge)
	}

	// Step 1. group by node_uuid, find target timestamp
	timestamps := []string{}
	for _, path := range paths {
		p, err := pathBuilder.ParsePath(path)
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
		// fixme
		return moerr.NewInternalError("CSVMerge: only one timestamp")
	}
	timestamp_start := timestamps[0]
	timestamp_end := timestamps[len(timestamps)-1]

	// Step 2. new filename, file writer
	pathBuilder.Build(account, export.MergeLogTypeMerged, m.Datetime, m.Table)
	merge_filename := pathBuilder.NewMergeFilename(timestamp_start, timestamp_end)
	merge_filepath := pathBuilder.Join(merge_filename)
	new_file_writer := NewCSVWriter(m.FS, WithPath(merge_filepath))

	// Step 3. do simple merge
	cacheFileData := m.Table.NewRowCache()
	for _, path := range paths {
		reader := NewCSVReader(m.FS, WithPath(path))
		for line := reader.ReadLine(); line != nil; line = reader.ReadLine() {

			row := m.Table.ParseRow(line)
			// fixme: if !obj.Valid() { continue }
			cacheFileData.Put(row) // if table_name == "statement_info", try to save last record.
			if cacheFileData.Size() > m.FileCacheSize {
				cacheFileData.Flush(new_file_writer)
				cacheFileData.Reset()
			}
		}
	}
	if !cacheFileData.IsEmpty() {
		cacheFileData.Flush(new_file_writer)
		cacheFileData.Reset()
	}
	new_file_writer.FlushAndClose()

	// step 4. delete old files
	err := m.FS.Delete(m.ctx, paths...)

	return err
}

type CSVReader interface {
	ReadLine() []string
}

func NewCSVReader(fs fileservice.FileService, path interface{}) CSVReader {
	panic("not implement")
}

func WithPath(filepath string) interface{} {
	panic("not implement")
}

func NewCSVWriter(fs fileservice.FileService, i interface{}) CSVWriter {
	panic("not implement")
}

type Cache interface {
	Put(*Row)
	Size() int64
	Flush(CSVWriter)
	Reset()
	IsEmpty() bool
}

type SliceCache struct {
	m    []*Row
	size int64
}

func (c *SliceCache) Flush(writer CSVWriter) {
	//TODO implement me
	panic("implement me")
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

func (c *MapCache) Flush(writer CSVWriter) {
	//TODO implement me
	panic("implement me")
}

func (c *MapCache) Reset() {
	c.size = 0
	c.m = make(map[string]*Row, len(c.m))
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
