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
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type MergeLogType string

func (t MergeLogType) String() string { return string(t) }

const MergeLogTypeMerged MergeLogType = "merged"
const MergeLogTypeLogs MergeLogType = "logs"
const MergeLogTypeALL MergeLogType = "*"

const FilenameSeparator = "_"
const CsvExtension = ".csv"

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
	ParsePath(ctx context.Context, path string) (CSVPath, error)
	NewMergeFilename(timestampStart, timestampEnd string) string
	NewLogFilename(name, nodeUUID, nodeType string, ts time.Time) string
	// SupportMergeSplit const. if false, not support SCV merge|split task
	SupportMergeSplit() bool
	// SupportAccountStrategy const
	SupportAccountStrategy() bool
	// GetName const
	GetName() string
}

type CSVPath interface {
	Table() string
	Timestamp() []string
}

var _ CSVPath = (*MetricLogPath)(nil)

type MetricLogPath struct {
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
const FilenameIdxType = 2

// NewMetricLogPath
//
// path like: sys/[log|merged]/yyyy/mm/dd/table/***.csv
// ##    idx: 0   1            2    3  4  5     6
// filename like: {timestamp}_{node_uuid}_{node_type}.csv
// ##         or: {timestamp_start}_{timestamp_end}_merged.csv
func NewMetricLogPath(path string) *MetricLogPath {
	return &MetricLogPath{path: path}
}

func (p *MetricLogPath) Parse(ctx context.Context) error {
	// parse path => filename, table
	elems := strings.Split(p.path, "/")
	if len(elems) != PathElems {
		return moerr.NewInternalError(ctx, "metric/log invalid path: %s", p.path)
	}
	p.filename = elems[PathIdxFilename]
	p.table = elems[PathIdxTable]

	// parse filename => fileType, timestamps
	filename := strings.Trim(p.filename, CsvExtension)
	fnElems := strings.Split(filename, FilenameSeparator)
	if len(fnElems) != FilenameElems {
		return moerr.NewInternalError(ctx, "metric/log invalid filename: %s", p.path)
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

func (p *MetricLogPath) Table() string {
	return p.table
}

func (p *MetricLogPath) Timestamp() []string {
	return p.timestamps
}

var _ PathBuilder = (*AccountDatePathBuilder)(nil)

type AccountDatePathBuilder struct{}

func NewAccountDatePathBuilder() *AccountDatePathBuilder {
	return &AccountDatePathBuilder{}
}

func (b *AccountDatePathBuilder) Build(account string, typ MergeLogType, ts time.Time, db string, name string) string {
	if ts != ETLParamTSAll {
		return path.Join(account,
			typ.String(),
			fmt.Sprintf("%d", ts.Year()),
			fmt.Sprintf("%02d", ts.Month()),
			fmt.Sprintf("%02d", ts.Day()),
			name,
		)
	} else {
		return path.Join(account, typ.String(), "*/*/*" /*All datetime*/, name)
	}
}

// BuildETLPath implement PathBuilder
//
// #     account | typ | ts   | table | filename
// like: *       /*    /*/*/* /metric /*.csv
func (b *AccountDatePathBuilder) BuildETLPath(db, name, account string) string {
	etlDirectory := b.Build(account, ETLParamTypeAll, ETLParamTSAll, db, name)
	etlFilename := "*" + CsvExtension
	return path.Join("/", etlDirectory, etlFilename)
}

func (b *AccountDatePathBuilder) ParsePath(ctx context.Context, path string) (CSVPath, error) {
	p := NewMetricLogPath(path)
	return p, p.Parse(ctx)
}

func (b *AccountDatePathBuilder) NewMergeFilename(timestampStart, timestampEnd string) string {
	return strings.Join([]string{timestampStart, timestampEnd, string(MergeLogTypeMerged)}, FilenameSeparator) + CsvExtension
}

func (b *AccountDatePathBuilder) NewLogFilename(name, nodeUUID, nodeType string, ts time.Time) string {
	return strings.Join([]string{fmt.Sprintf("%d", ts.Unix()), nodeUUID, nodeType}, FilenameSeparator) + CsvExtension
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

func (m *DBTablePathBuilder) ParsePath(ctx context.Context, path string) (CSVPath, error) {
	panic("not implement")
}

func (m *DBTablePathBuilder) NewMergeFilename(timestampStart, timestampEnd string) string {
	panic("not implement")
}

func (m *DBTablePathBuilder) NewLogFilename(name, nodeUUID, nodeType string, ts time.Time) string {
	return fmt.Sprintf(`%s_%s_%s_%s`, name, nodeUUID, nodeType, ts.Format("20060102.150405.000000")) + CsvExtension
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
