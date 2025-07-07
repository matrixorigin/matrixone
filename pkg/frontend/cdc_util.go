// Copyright 2023 Matrix Origin
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

package frontend

import (
	"context"
	"database/sql"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

const (
	CDCShowOutputColumnsNameTaskID_Idx     = 0
	CDCShowOutputColumnsNameTaskName_Idx   = 1
	CDCShowOutputColumnsNameSourceURI_Idx  = 2
	CDCShowOutputColumnsNameSinkURI_Idx    = 3
	CDCShowOutputColumnsNameState_Idx      = 4
	CDCShowOutputColumnsNameErrMsg_Idx     = 5
	CDCShowOutputColumnsNameCheckpoint_Idx = 6
	CDCShowOutputColumnsNameTimestamp_Idx  = 7

	CDCShowOutputColumnsCount = 8
)

var CDCShowOutputColumnsSpec = map[int]struct {
	name       string
	columnType defines.MysqlType
}{
	CDCShowOutputColumnsNameTaskID_Idx:     {name: "task_id", columnType: defines.MYSQL_TYPE_VARCHAR},
	CDCShowOutputColumnsNameTaskName_Idx:   {name: "task_name", columnType: defines.MYSQL_TYPE_VARCHAR},
	CDCShowOutputColumnsNameSourceURI_Idx:  {name: "source_uri", columnType: defines.MYSQL_TYPE_TEXT},
	CDCShowOutputColumnsNameSinkURI_Idx:    {name: "sink_uri", columnType: defines.MYSQL_TYPE_TEXT},
	CDCShowOutputColumnsNameState_Idx:      {name: "state", columnType: defines.MYSQL_TYPE_VARCHAR},
	CDCShowOutputColumnsNameErrMsg_Idx:     {name: "err_msg", columnType: defines.MYSQL_TYPE_VARCHAR},
	CDCShowOutputColumnsNameCheckpoint_Idx: {name: "checkpoint", columnType: defines.MYSQL_TYPE_VARCHAR},
	CDCShowOutputColumnsNameTimestamp_Idx:  {name: "timestamp", columnType: defines.MYSQL_TYPE_VARCHAR},
}

var CDCShowOutputColumns = [CDCShowOutputColumnsCount]Column{
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameTaskID_Idx].name,
			columnType: CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameTaskID_Idx].columnType,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameTaskName_Idx].name,
			columnType: CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameTaskName_Idx].columnType,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameSourceURI_Idx].name,
			columnType: CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameSourceURI_Idx].columnType,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameSinkURI_Idx].name,
			columnType: CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameSinkURI_Idx].columnType,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameState_Idx].name,
			columnType: CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameState_Idx].columnType,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameErrMsg_Idx].name,
			columnType: CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameErrMsg_Idx].columnType,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameCheckpoint_Idx].name,
			columnType: CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameCheckpoint_Idx].columnType,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameTimestamp_Idx].name,
			columnType: CDCShowOutputColumnsSpec[CDCShowOutputColumnsNameTimestamp_Idx].columnType,
		},
	},
}

func GetCDCShowOutputResultSet() *MysqlResultSet {
	var rs MysqlResultSet
	for _, column := range CDCShowOutputColumns {
		rs.AddColumn(column)
	}
	return &rs
}

func TransformStdTimeString(tsStr string) (string, error) {
	ts, err := timestamp.ParseTimestamp(tsStr)
	if err != nil {
		return "", err
	}
	return ts.ToStdTime().In(time.Local).String(), nil
}

func ExecuteAndGetRowsAffected(
	ctx context.Context,
	tx taskservice.SqlExecutor,
	query string,
	args ...interface{},
) (int64, error) {
	exec, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	rows, err := exec.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

func WithBackgroundExec(
	ctx context.Context,
	ses *Session,
	fn func(context.Context, *Session, BackgroundExec) error,
) (err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	return fn(ctx, ses, bh)
}

// PitrConfig represents the PITR configuration for a specific level
type PitrConfig struct {
	Level  string
	Length int64
	Unit   string
	Exists bool
}

// NewPitrConfig creates a new PitrConfig instance
func NewPitrConfig(level string) *PitrConfig {
	return &PitrConfig{
		Level: level,
	}
}

// IsValid checks if the PITR configuration meets the minimum requirements
func (pc *PitrConfig) IsValid(minLength int64) bool {
	if !pc.Exists {
		return false
	}
	return !(pc.Unit == "h" && pc.Length < minLength)
}

// CDCCheckPitrGranularity checks if the PITR (Point-in-Time Recovery) granularity settings
// meet the minimum requirements for CDC tasks at different levels (cluster/account/db/table)
// It verifies the PITR configuration in descending order of priority (cluster > account > database > table)
// to ensure that at least one level satisfies the minimum time requirement (2 hours).
//
// Parameters:
// - ctx: Context for managing the lifecycle of the function.
// - bh: Background handler for executing database queries.
// - accName: The account name associated with the CDC task.
// - pts: Pattern tuples representing the database and table patterns to be checked.
// - minLength: the minimum time requirement of pitr
//
// Returns:
// - error: Returns an error if no PITR configuration meets the minimum requirement, otherwise nil.
var CDCCheckPitrGranularity = func(
	ctx context.Context,
	bh BackgroundExec,
	accName string,
	pts *cdc.PatternTuples,
	minLength ...int64,
) error {
	var minPitrLen int64 = 2
	if len(minLength) > 1 {
		return moerr.NewInternalErrorf(ctx, "only one length parameter allowed")
	}
	if len(minLength) > 0 {
		minPitrLen = max(minLength[0]+1, minPitrLen)
	}

	// Helper function to get PITR config for a specific level
	getPitrConfig := func(level, dbName, tblName string) (*PitrConfig, error) {
		config := NewPitrConfig(level)
		length, unit, ok, err := getPitrLengthAndUnit(ctx, bh, level, accName, dbName, tblName)
		if err != nil {
			return nil, err
		}
		config.Length = length
		config.Unit = unit
		config.Exists = ok
		return config, nil
	}

	// Check cluster level first
	if config, err := getPitrConfig(cdc.CDCPitrGranularity_Cluster, "", ""); err != nil {
		return err
	} else if config.IsValid(minPitrLen) {
		return nil
	}

	// Check other levels for each pattern tuple
	for _, pt := range pts.Pts {
		dbName := pt.Source.Database
		tblName := pt.Source.Table

		// Determine the level based on pattern
		level := cdc.CDCPitrGranularity_Table
		if dbName == cdc.CDCPitrGranularity_All && tblName == cdc.CDCPitrGranularity_All {
			level = cdc.CDCPitrGranularity_Account
		} else if tblName == cdc.CDCPitrGranularity_All {
			level = cdc.CDCPitrGranularity_DB
		}

		// Check account level
		if config, err := getPitrConfig(cdc.CDCPitrGranularity_Account, dbName, tblName); err != nil {
			return err
		} else if config.IsValid(minPitrLen) {
			continue
		}

		// Check DB level if needed
		if level == cdc.CDCPitrGranularity_DB || level == cdc.CDCPitrGranularity_Table {
			if config, err := getPitrConfig(cdc.CDCPitrGranularity_DB, dbName, tblName); err != nil {
				return err
			} else if config.IsValid(minPitrLen) {
				continue
			}
		}

		// Check table level if needed
		if level == cdc.CDCPitrGranularity_Table {
			if config, err := getPitrConfig(cdc.CDCPitrGranularity_Table, dbName, tblName); err != nil {
				return err
			} else if config.IsValid(minPitrLen) {
				continue
			}
		}

		return moerr.NewInternalErrorf(ctx,
			"no valid PITR configuration found for pattern: %s, minimum required length: %d hours",
			pt.OriginString, minPitrLen)
	}
	return nil
}

var (
	getGlobalPuWrapper = getPu
)

var initAesKeyBySqlExecutor = func(
	ctx context.Context,
	executor taskservice.SqlExecutor,
	accountId uint32,
	service string,
) (err error) {
	if len(cdc.AesKey) > 0 {
		return nil
	}

	var (
		encryptedKey string
		cnt          int64
	)
	querySql := cdc.CDCSQLBuilder.GetDataKeySQL(uint64(accountId), cdc.InitKeyId)

	if cnt, err = ForeachQueriedRow(
		ctx,
		executor,
		querySql,
		func(ctx context.Context, rows *sql.Rows) (bool, error) {
			if cnt > 0 {
				return false, nil
			}
			if err2 := rows.Scan(&encryptedKey); err2 != nil {
				return false, err2
			}
			cnt++
			return true, nil
		},
	); err != nil {
		return
	} else if cnt == 0 {
		return moerr.NewInternalError(ctx, "no data key")
	}

	cdc.AesKey, err = cdc.AesCFBDecodeWithKey(
		ctx,
		encryptedKey,
		[]byte(getGlobalPuWrapper(service).SV.KeyEncryptionKey),
	)
	logutil.Infof("DEBUG-1: %v:%v", encryptedKey, err)
	return
}

func CDCStrToTime(tsStr string, tz *time.Location) (ts time.Time, err error) {
	if tsStr == "" {
		return
	}

	if tz != nil {
		if ts, err = time.ParseInLocation(
			time.DateTime, tsStr, tz,
		); err == nil {
			return
		}
	}

	ts, err = time.Parse(time.RFC3339, tsStr)
	return
}

func CDCStrToTS(tsStr string) (types.TS, error) {
	if tsStr == "" {
		return types.TS{}, nil
	}
	t, err := CDCStrToTime(tsStr, nil)
	if err != nil {
		return types.TS{}, err
	}

	return types.BuildTS(t.UnixNano(), 0), nil
}

// CDCParseTableInfo parses a string to get account,database,table info
//
// input format:
//
//	DbLevel: database
//	TableLevel: database.table
//
// There must be no special characters (','  '.'  ':' '`') in database name & table name.
func CDCParseTableInfo(
	ctx context.Context,
	input string,
	level string,
) (db string, table string, err error) {
	parts := strings.Split(strings.TrimSpace(input), ".")
	if level == cdc.CDCPitrGranularity_DB && len(parts) != 1 {
		err = moerr.NewInternalErrorf(ctx, "invalid databases format: %s", input)
		return
	} else if level == cdc.CDCPitrGranularity_Table && len(parts) != 2 {
		err = moerr.NewInternalErrorf(ctx, "invalid tables format: %s", input)
		return
	}

	db = strings.TrimSpace(parts[0])
	if !dbNameIsLegal(db) {
		err = moerr.NewInternalErrorf(ctx, "invalid database name: %s", db)
		return
	}

	if level == cdc.CDCPitrGranularity_Table {
		table = strings.TrimSpace(parts[1])
		if !tableNameIsLegal(table) {
			err = moerr.NewInternalErrorf(ctx, "invalid table name: %s", table)
			return
		}
	} else {
		table = cdc.CDCPitrGranularity_All
	}
	return
}

// CDCParsePitrGranularity parses a comma-separated list of table patterns into a PatternTuples struct.
// The level parameter specifies whether patterns are at account/db/table level.
// For account level, returns a single pattern matching all DBs and tables.
// For db/table level, parses each pattern into source and sink components.
func CDCParsePitrGranularity(
	ctx context.Context,
	level string,
	tables string,
) (pts *cdc.PatternTuples, err error) {
	pts = &cdc.PatternTuples{}

	if level == cdc.CDCPitrGranularity_Account {
		pts.Append(&cdc.PatternTuple{
			Source: cdc.PatternTable{
				Database: cdc.CDCPitrGranularity_All,
				Table:    cdc.CDCPitrGranularity_All,
			},
			Sink: cdc.PatternTable{
				Database: cdc.CDCPitrGranularity_All,
				Table:    cdc.CDCPitrGranularity_All,
			},
		})
		return
	}

	// split tables by ',' => table pair
	var pt *cdc.PatternTuple
	tablePairs := strings.Split(strings.TrimSpace(tables), ",")
	dup := make(map[string]struct{})
	for _, pair := range tablePairs {
		if pt, err = CDCParseGranularityTuple(
			ctx, level, pair, dup,
		); err != nil {
			return
		}
		pts.Append(pt)
	}
	return
}

// CDCParseGranularityTuple pattern example:
//
//	db1
//	db1:db2
//	db1.t1
//	db1.t1:db2.t2
//
// There must be no special characters (','  '.'  ':' '`') in database name & table name.
func CDCParseGranularityTuple(
	ctx context.Context,
	level string,
	pattern string,
	dup map[string]struct{},
) (pt *cdc.PatternTuple, err error) {
	splitRes := strings.Split(strings.TrimSpace(pattern), ":")
	if len(splitRes) > 2 {
		err = moerr.NewInternalErrorf(
			ctx, "invalid pattern format: %s, must be `source` or `source:sink`.", pattern,
		)
		return
	}

	pt = &cdc.PatternTuple{OriginString: pattern}

	// handle source part
	if pt.Source.Database, pt.Source.Table, err = CDCParseTableInfo(
		ctx, splitRes[0], level,
	); err != nil {
		return
	}
	key := cdc.GenDbTblKey(pt.Source.Database, pt.Source.Table)
	if _, ok := dup[key]; ok {
		err = moerr.NewInternalErrorf(
			ctx, "one db/table: %s can't be used as multi sources in a cdc task", key,
		)
		return
	}
	dup[key] = struct{}{}

	// handle sink part
	if len(splitRes) > 1 {
		if pt.Sink.Database, pt.Sink.Table, err = CDCParseTableInfo(
			ctx, splitRes[1], level,
		); err != nil {
			return
		}
	} else {
		// if not specify sink, then sink = source
		pt.Sink.Database = pt.Source.Database
		pt.Sink.Table = pt.Source.Table
	}
	return
}

// only accept positive integers that end with m or h
func isValidFrequency(freq string) bool {
	if !strings.HasSuffix(freq, "m") && !strings.HasSuffix(freq, "h") {
		return false
	}

	numPart := strings.TrimSuffix(strings.TrimSuffix(freq, "m"), "h")

	matched, _ := regexp.MatchString(`^[1-9]\d*$`, numPart)
	if !matched {
		return false
	}

	num, err := strconv.Atoi(numPart)
	if err != nil {
		return false
	}

	if num < 0 {
		return false
	}

	if num > 10000000 {
		return false
	}

	return true
}

// e.g. 1h -> 1
//
//	30m -> 1
//	60m -> 1
//	61m -> 2
//
// call isValidFrequency before calling this function
func transformIntoHours(freq string) int64 {
	if strings.HasSuffix(freq, "h") {
		hoursStr := strings.TrimSuffix(freq, "h")
		hours, _ := strconv.Atoi(hoursStr)
		return int64(hours)
	}

	if strings.HasSuffix(freq, "m") {
		minStr := strings.TrimSuffix(freq, "m")
		minutes, _ := strconv.Atoi(minStr)

		hours := int(math.Ceil(float64(minutes) / 60.0))

		return int64(hours)
	}

	return 0
}
