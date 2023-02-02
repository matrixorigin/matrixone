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

package table

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type MergeLogType string

func (t MergeLogType) String() string { return string(t) }

const MergeLogTypeMerged MergeLogType = "merged"
const MergeLogTypeLogs MergeLogType = "logs"
const MergeLogTypeALL MergeLogType = "*"

const FilenameSeparator = "_"
const CsvExtension = ".csv"
const TaeExtension = ".tae"

const ETLParamTypeAll = MergeLogTypeALL
const ETLParamAccountAll = "*"

const AccountAll = ETLParamAccountAll

var ETLParamTSAll = time.Time{}

// PathBuilder hold strategy to build filepath
type PathBuilder interface {
	// Build directory path
	Build(account string, typ MergeLogType, ts time.Time, db string, name string) string
	// BuildETLPath return path for EXTERNAL table 'infile' options
	//
	// like: {account}/merged/*/*/*/{name}/*.csv
	BuildETLPath(db, name, account string) string
	// ParsePath
	//
	// switch path {
	// case "{timestamp_writedown}_{node_uuid}_{ndoe_type}.csv":
	// case "{timestamp_start}_{timestamp_end}_merged.csv"
	// }
	ParsePath(ctx context.Context, path string) (Path, error)
	NewMergeFilename(timestampStart, timestampEnd, extension string) string
	NewLogFilename(name, nodeUUID, nodeType string, ts time.Time, extension string) string
	// SupportMergeSplit const. if false, not support SCV merge|split task
	SupportMergeSplit() bool
	// SupportAccountStrategy const
	SupportAccountStrategy() bool
	// GetName const
	GetName() string
}

type Path interface {
	Table() string
	Timestamp() []string
}

var _ Path = (*ETLPath)(nil)

type ETLPath struct {
	// path raw data
	path string
	// table parsed from path
	table string
	// filename
	filename string
	// timestamps parsed from filename in path
	timestamps []string
	// fileType, val in [log, merged]
	fileType MergeLogType
}

const PathElems = 7
const PathIdxFilename = 6
const PathIdxTable = 5
const PathIdxAccount = 0
const FilenameElems = 3
const FilenameElemsV2 = 4
const FilenameIdxType = 2

// NewETLPath
//
// path like: sys/[log|merged]/yyyy/mm/dd/table/***.csv
// ##    idx: 0   1            2    3  4  5     6
// filename like: {timestamp}_{node_uuid}_{node_type}.csv
// ##         or: {timestamp_start}_{timestamp_end}_merged.csv
func NewETLPath(path string) *ETLPath {
	return &ETLPath{path: path}
}

func (p *ETLPath) Parse(ctx context.Context) error {
	// parse path => filename, table
	elems := strings.Split(p.path, "/")
	if len(elems) != PathElems {
		return moerr.NewInternalError(ctx, "invalid etl path: %s", p.path)
	}
	p.filename = elems[PathIdxFilename]
	p.table = elems[PathIdxTable]

	// parse filename => fileType, timestamps
	filename := strings.Trim(p.filename, CsvExtension)
	fnElems := strings.Split(filename, FilenameSeparator)
	if len(fnElems) != FilenameElems && len(fnElems) != FilenameElemsV2 {
		return moerr.NewInternalError(ctx, "invalid etl filename: %s", p.filename)
	}
	if fnElems[FilenameIdxType] == string(MergeLogTypeMerged) {
		p.fileType = MergeLogTypeMerged
		p.timestamps = fnElems[:2]
	} else {
		p.fileType = MergeLogTypeLogs
		p.timestamps = fnElems[:1]
	}

	return nil
}

func (p *ETLPath) Table() string {
	return p.table
}

func (p *ETLPath) Timestamp() []string {
	return p.timestamps
}

type PathBuilderConfig struct {
	withDatabase bool
}

type PathBuilderOption func(*PathBuilderConfig)

func (opt PathBuilderOption) Apply(cfg *PathBuilderConfig) {
	opt(cfg)
}

func WithDatabase(with bool) PathBuilderOption {
	return PathBuilderOption(func(cfg *PathBuilderConfig) {
		cfg.withDatabase = with
	})
}

var _ PathBuilder = (*AccountDatePathBuilder)(nil)

type AccountDatePathBuilder struct {
	PathBuilderConfig
}

func NewAccountDatePathBuilder(opts ...PathBuilderOption) *AccountDatePathBuilder {
	builder := &AccountDatePathBuilder{}
	for _, opt := range opts {
		opt.Apply(&builder.PathBuilderConfig)
	}
	return builder
}

func (b *AccountDatePathBuilder) Build(account string, typ MergeLogType, ts time.Time, db string, tblName string) string {
	identify := tblName
	if b.withDatabase {
		identify = fmt.Sprintf("%s.%s", db, tblName)
	}
	if ts != ETLParamTSAll {
		return path.Join(account,
			typ.String(),
			fmt.Sprintf("%d", ts.Year()),
			fmt.Sprintf("%02d", ts.Month()),
			fmt.Sprintf("%02d", ts.Day()),
			identify,
		)
	} else {
		return path.Join(account, typ.String(), "*/*/*" /*All datetime*/, identify)
	}
}

// BuildETLPath implement PathBuilder
//
// #     account | typ | ts   | table | filename
// like: *       /*    /*/*/* /metric /*.csv
func (b *AccountDatePathBuilder) BuildETLPath(db, name, account string) string {
	etlDirectory := b.Build(account, ETLParamTypeAll, ETLParamTSAll, db, name)
	etlFilename := "*"
	return path.Join("/", etlDirectory, etlFilename)
}

func (b *AccountDatePathBuilder) ParsePath(ctx context.Context, path string) (Path, error) {
	p := NewETLPath(path)
	return p, p.Parse(ctx)
}

var timeMu sync.Mutex

var NSecString = func() string {
	timeMu.Lock()
	nsec := time.Now().Nanosecond()
	timeMu.Unlock()
	return fmt.Sprintf("%09d", nsec)
}

func (b *AccountDatePathBuilder) NewMergeFilename(timestampStart, timestampEnd, extension string) string {
	seq := NSecString()
	return strings.Join([]string{timestampStart, timestampEnd, string(MergeLogTypeMerged), seq}, FilenameSeparator) + extension
}

func (b *AccountDatePathBuilder) NewLogFilename(name, nodeUUID, nodeType string, ts time.Time, extension string) string {
	seq := NSecString()
	return strings.Join([]string{fmt.Sprintf("%d", ts.Unix()), nodeUUID, nodeType, seq}, FilenameSeparator) + extension
}

func (b *AccountDatePathBuilder) SupportMergeSplit() bool      { return true }
func (b *AccountDatePathBuilder) SupportAccountStrategy() bool { return true }
func (b *AccountDatePathBuilder) GetName() string              { return "AccountDate" }

var _ PathBuilder = (*DBTablePathBuilder)(nil)

type DBTablePathBuilder struct{}

// BuildETLPath implement PathBuilder
//
// like: system/metric_*.csv
func (m *DBTablePathBuilder) BuildETLPath(db, name, account string) string {
	return fmt.Sprintf("%s/%s_*", db, name) + CsvExtension
}

func NewDBTablePathBuilder() *DBTablePathBuilder {
	return &DBTablePathBuilder{}
}

func (m *DBTablePathBuilder) Build(account string, typ MergeLogType, ts time.Time, db string, name string) string {
	return db
}

func (m *DBTablePathBuilder) ParsePath(ctx context.Context, path string) (Path, error) {
	panic("not implement")
}

func (m *DBTablePathBuilder) NewMergeFilename(timestampStart, timestampEnd, extension string) string {
	panic("not implement")
}

func (m *DBTablePathBuilder) NewLogFilename(name, nodeUUID, nodeType string, ts time.Time, extension string) string {
	return fmt.Sprintf(`%s_%s_%s_%s`, name, nodeUUID, nodeType, ts.Format("20060102.150405.000000")) + extension
}

func (m *DBTablePathBuilder) SupportMergeSplit() bool      { return false }
func (m *DBTablePathBuilder) SupportAccountStrategy() bool { return false }
func (m *DBTablePathBuilder) GetName() string              { return "DBTable" }

func PathBuilderFactory(pathBuilder string) PathBuilder {
	switch pathBuilder {
	case (*DBTablePathBuilder)(nil).GetName():
		return NewDBTablePathBuilder()
	case (*AccountDatePathBuilder)(nil).GetName():
		return NewAccountDatePathBuilder()
	default:
		return nil
	}
}

func GetExtension(ext string) string {
	switch ext {
	case CsvExtension, TaeExtension:
		return ext
	case "csv":
		return CsvExtension
	case "tae":
		return TaeExtension
	default:
		panic("unknown type of ext")
	}
}

func String2Bytes(s string) (ret []byte) {
	sliceHead := (*reflect.SliceHeader)(unsafe.Pointer(&ret))
	strHead := (*reflect.StringHeader)(unsafe.Pointer(&s))

	sliceHead.Data = strHead.Data
	sliceHead.Len = strHead.Len
	sliceHead.Cap = strHead.Len
	return
}

type RowWriter interface {
	WriteRow(row *Row) error
	// GetContent get buffer content
	GetContent() string
	// FlushAndClose flush its buffer and close.
	FlushAndClose() (int, error)
}

type RowField interface {
	GetTable() *Table
	FillRow(context.Context, *Row)
}

type WriteRequest interface {
	Handle() (int, error)
	GetContent() string
}

type ExportRequests []WriteRequest

type RowRequest struct {
	writer RowWriter
}

func NewRowRequest(writer RowWriter) *RowRequest {
	return &RowRequest{writer}
}

func (r *RowRequest) Handle() (int, error) {
	if r.writer == nil {
		return 0, nil
	}
	return r.writer.FlushAndClose()
}

func (r *RowRequest) GetContent() string {
	return r.writer.GetContent()
}

type WriterFactory func(ctx context.Context, account string, tbl *Table, ts time.Time) RowWriter

type FilePathCfg struct {
	NodeUUID  string
	NodeType  string
	Extension string
}

func (c *FilePathCfg) LogsFilePathFactory(account string, tbl *Table, ts time.Time) string {
	filename := tbl.PathBuilder.NewLogFilename(tbl.Table, c.NodeUUID, c.NodeType, ts, c.Extension)
	dir := tbl.PathBuilder.Build(account, MergeLogTypeLogs, ts, tbl.Database, tbl.Table)
	return path.Join(dir, filename)
}
