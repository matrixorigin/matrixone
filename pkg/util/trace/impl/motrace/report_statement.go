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

package motrace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"

	"github.com/google/uuid"
)

var NilStmtID [16]byte
var NilTxnID [16]byte
var NilSesID [16]byte

// StatementInfo implement export.IBuffer2SqlItem and export.CsvFields

var _ IBuffer2SqlItem = (*StatementInfo)(nil)

func StatementInfoNew(i Item, ctx context.Context) Item {
	if s, ok := i.(*StatementInfo); ok {
		// process the execplan
		s.ExecPlan2Json(ctx)
		// remove the plan
		s.jsonByte = nil
		s.ExecPlan = nil

		// remove the TransacionID
		s.TransactionID = NilTxnID
		s.StatementTag = ""
		s.StatementFingerprint = ""
		s.Error = nil
		s.RowsRead = 0
		s.BytesScan = 0
		s.ResultCount = 0
		s.AggrCount = 1
		return s
	}
	return nil
}
func StatementInfoUpdate(existing, new Item) {

	e := existing.(*StatementInfo)
	n := new.(*StatementInfo)
	// update the stats
	e.Duration += n.Duration
	e.Statement = e.Statement + "; " + n.Statement
	e.AggrCount += 1
	// reponseAt is the last response time
	e.ResponseAt = n.ResponseAt
	// TODO: update the stats still json here
	n.ExecPlan2Json(context.Background())
	//[1 80335 1800 0 0]
	//[1 147960 1800 0 0]
	err := mergeStats(e, n)

	if err != nil {
		// handle error
		log.Printf("Failed to merge stats: %v", err)
	}
}

func StatementInfoFilter(i Item) bool {
	// Attempt to perform a type assertion to *StatementInfo
	statementInfo, ok := i.(*StatementInfo)

	if !ok {
		// The item couldn't be cast to *StatementInfo
		return false
	}

	if statementInfo.Status == StatementStatusRunning {
		return false
	}

	// Check SqlSourceType
	switch statementInfo.SqlSourceType {
	case "internal_sql", "external_sql", "non_cloud_user":
		// Check StatementType
		switch statementInfo.StatementType {
		case "Insert", "Update", "Delete", "Execute":
			return true
		case "Select":
			// For 'select', also check if Duration is longer than 200 milliseconds
			if statementInfo.Duration < GetTracerProvider().selectAggrThreshold {
				return true
			}
		}
	}

	// If no conditions matched, return false
	return false
}

type StatementInfo struct {
	StatementID          [16]byte  `json:"statement_id"`
	TransactionID        [16]byte  `json:"transaction_id"`
	SessionID            [16]byte  `jons:"session_id"`
	Account              string    `json:"account"`
	User                 string    `json:"user"`
	Host                 string    `json:"host"`
	RoleId               uint32    `json:"role_id"`
	Database             string    `json:"database"`
	Statement            string    `json:"statement"`
	StatementFingerprint string    `json:"statement_fingerprint"`
	StatementTag         string    `json:"statement_tag"`
	SqlSourceType        string    `json:"sql_source_type"`
	RequestAt            time.Time `json:"request_at"` // see WithRequestAt

	StatementType string `json:"statement_type"`
	QueryType     string `json:"query_type"`

	// after
	Status     StatementInfoStatus `json:"status"`
	Error      error               `json:"error"`
	ResponseAt time.Time           `json:"response_at"`
	Duration   time.Duration       `json:"duration"` // unit: ns
	// new ExecPlan
	ExecPlan SerializableExecPlan `json:"-"` // set by SetSerializableExecPlan
	// RowsRead, BytesScan generated from ExecPlan
	RowsRead  int64 `json:"rows_read"`  // see ExecPlan2Json
	BytesScan int64 `json:"bytes_scan"` // see ExecPlan2Json
	AggrCount int64 `json:"aggr_count"` // see EndStatement

	ResultCount int64 `json:"result_count"` // see EndStatement

	// flow ctrl
	// #		|case 1 |case 2 |case 3 |case 4|
	// end		| false | false | true  | true |  (set true at EndStatement)
	// exported	| false | true  | false | true |  (set true at function FillRow, set false at function EndStatement)
	//
	// case 1: first gen statement_info record
	// case 2: statement_info exported as `status=Running` record
	// case 3: while query done, call EndStatement mark statement need to be exported again
	// case 4: done final export
	//
	// normally    flow: case 1->2->3->4
	// query-quick flow: case 1->3->4
	end bool // cooperate with mux
	mux sync.Mutex
	// mark reported
	reported bool
	// mark exported
	exported bool

	// keep []byte as elem
	jsonByte, statsJsonByte []byte
}

type Key struct {
	SessionID     [16]byte
	StatementType string
	Window        time.Time
	Status        StatementInfoStatus
}

var stmtPool = sync.Pool{
	New: func() any {
		return &StatementInfo{}
	},
}

func NewStatementInfo() *StatementInfo {
	return stmtPool.Get().(*StatementInfo)
}

type Statistic struct {
	RowsRead  int64
	BytesScan int64
}

func (s *StatementInfo) Key(duration time.Duration) interface{} {
	return Key{SessionID: s.SessionID, StatementType: s.StatementType, Window: s.RequestAt.Truncate(duration), Status: s.Status}
}

func (s *StatementInfo) GetName() string {
	return SingleStatementTable.GetName()
}

// deltaContentLength approximate value that may gen as table record
// stmtID, txnID, sesID: 36 * 3
// timestamp: 26 * 2
// status: 7
// spanInfo: 36+16
const deltaStmtContentLength = int64(36*3 + 26*2 + 7 + 36 + 16)
const jsonByteLength = int64(4096)

func (s *StatementInfo) Size() int64 {
	num := int64(unsafe.Sizeof(s)) + deltaStmtContentLength + int64(
		len(s.Account)+len(s.User)+len(s.Host)+
			len(s.Database)+len(s.Statement)+len(s.StatementFingerprint)+len(s.StatementTag)+
			len(s.SqlSourceType)+len(s.StatementType)+len(s.QueryType)+len(s.jsonByte)+len(s.statsJsonByte),
	)
	if s.jsonByte == nil {
		return num + jsonByteLength
	}
	return num
}

func (s *StatementInfo) Free() {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.end && s.exported { // cooperate with s.mux
		s.RoleId = 0
		s.Statement = ""
		s.StatementFingerprint = ""
		s.StatementTag = ""
		if s.ExecPlan != nil {
			s.ExecPlan.Free()
		}
		s.RequestAt = time.Time{}
		s.ResponseAt = time.Time{}
		s.ExecPlan = nil
		s.Status = StatementStatusRunning
		s.Error = nil
		s.RowsRead = 0
		s.BytesScan = 0
		s.ResultCount = 0
		s.end = false
		s.reported = false
		s.exported = false
		// clean []byte
		s.jsonByte = nil
		s.statsJsonByte = nil
		stmtPool.Put(s)
	}
}

func (s *StatementInfo) GetTable() *table.Table { return SingleStatementTable }

func (s *StatementInfo) FillRow(ctx context.Context, row *table.Row) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.exported = true
	row.Reset()
	row.SetColumnVal(stmtIDCol, table.UuidField(s.StatementID[:]))
	if !s.IsZeroTxnID() {
		row.SetColumnVal(txnIDCol, table.UuidField(s.TransactionID[:]))
	}
	row.SetColumnVal(sesIDCol, table.UuidField(s.SessionID[:]))
	row.SetColumnVal(accountCol, table.StringField(s.Account))
	row.SetColumnVal(roleIdCol, table.Int64Field(int64(s.RoleId)))
	row.SetColumnVal(userCol, table.StringField(s.User))
	row.SetColumnVal(hostCol, table.StringField(s.Host))
	row.SetColumnVal(dbCol, table.StringField(s.Database))
	row.SetColumnVal(stmtCol, table.StringField(s.Statement))
	row.SetColumnVal(stmtTagCol, table.StringField(s.StatementTag))
	row.SetColumnVal(sqlTypeCol, table.StringField(s.SqlSourceType))
	row.SetColumnVal(stmtFgCol, table.StringField(s.StatementFingerprint))
	row.SetColumnVal(nodeUUIDCol, table.StringField(GetNodeResource().NodeUuid))
	row.SetColumnVal(nodeTypeCol, table.StringField(GetNodeResource().NodeType))
	row.SetColumnVal(reqAtCol, table.TimeField(s.RequestAt))
	row.SetColumnVal(respAtCol, table.TimeField(s.ResponseAt))
	row.SetColumnVal(durationCol, table.Uint64Field(uint64(s.Duration)))
	row.SetColumnVal(statusCol, table.StringField(s.Status.String()))
	if s.Error != nil {
		var moError *moerr.Error
		errCode := moerr.ErrInfo
		if errors.As(s.Error, &moError) {
			errCode = moError.ErrorCode()
		}
		row.SetColumnVal(errCodeCol, table.StringField(fmt.Sprintf("%d", errCode)))
		row.SetColumnVal(errorCol, table.StringField(fmt.Sprintf("%s", s.Error)))
	}
	execPlan, stats := s.ExecPlan2Json(ctx)
	if GetTracerProvider().disableSqlWriter {
		// Be careful, this two string is unsafe, will be free after Free
		row.SetColumnVal(execPlanCol, table.StringField(util.UnsafeBytesToString(execPlan)))
		row.SetColumnVal(statsCol, table.StringField(util.UnsafeBytesToString(stats)))
	} else {
		row.SetColumnVal(execPlanCol, table.BytesField(execPlan))
		row.SetColumnVal(statsCol, table.BytesField(stats))
	}
	row.SetColumnVal(rowsReadCol, table.Int64Field(s.RowsRead))
	row.SetColumnVal(bytesScanCol, table.Int64Field(s.BytesScan))
	row.SetColumnVal(stmtTypeCol, table.StringField(s.StatementType))
	row.SetColumnVal(queryTypeCol, table.StringField(s.QueryType))
	row.SetColumnVal(aggrCntCol, table.Int64Field(s.AggrCount))
	row.SetColumnVal(resultCntCol, table.Int64Field(s.ResultCount))
}

func getStatsValues(jsonData []byte, duration uint64) ([]uint64, error) {
	type Statistic struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
		Unit  string `json:"unit"`
	}

	type Statistics struct {
		Time    []Statistic `json:"Time"`
		Memory  []Statistic `json:"Memory"`
		IO      []Statistic `json:"IO"`
		Network []Statistic `json:"Network"`
	}

	type StatJson struct {
		Statistics Statistics `json:"statistics"`
		TotalStats Statistic  `json:"totalStats"`
	}

	var stats StatJson
	err := json.Unmarshal(jsonData, &stats)
	if err != nil {
		return nil, err
	}

	var timeConsumed, memorySize, s3IOInputCount, s3IOOutputCount uint64

	for _, stat := range stats.Statistics.Time {
		if stat.Name == "Time Consumed" {
			timeConsumed = uint64(stat.Value)
			break
		}
	}

	for _, stat := range stats.Statistics.Memory {
		if stat.Name == "Memory Size" {
			memorySize = uint64(stat.Value)
			break
		}
	}

	for _, stat := range stats.Statistics.IO {
		if stat.Name == "S3 IO Input Count" {
			s3IOInputCount = uint64(stat.Value)
		} else if stat.Name == "S3 IO Output Count" {
			s3IOOutputCount = uint64(stat.Value)
		}
	}
	statsValues := make([]uint64, 5)
	statsValues[0] = 1 // this is the version number

	statsValues[1] = timeConsumed
	statsValues[2] = memorySize * duration
	statsValues[3] = s3IOInputCount
	statsValues[4] = s3IOOutputCount

	return statsValues, nil
}

func mergeStats(e, n *StatementInfo) error {
	// Convert statsJsonByte back to string and trim the square brackets
	eStatsStr := strings.Trim(string(e.statsJsonByte), "[]")
	nStatsStr := strings.Trim(string(n.statsJsonByte), "[]")

	// Split the strings by comma to get the individual elements
	eStatsElements := strings.Split(eStatsStr, ",")
	nStatsElements := strings.Split(nStatsStr, ",")

	// Ensure both arrays have the same length
	if len(eStatsElements) != len(nStatsElements) {
		return moerr.NewInternalError(context.Background(), "statsJsonByte length mismatch")
	}

	// Parse the strings to integers and add the values together
	for i := 1; i < len(eStatsElements); i++ {
		eVal, err := strconv.Atoi(strings.TrimSpace(eStatsElements[i]))
		if err != nil {
			return err
		}

		nVal, err := strconv.Atoi(strings.TrimSpace(nStatsElements[i]))
		if err != nil {
			return err
		}

		// Store the sum back in eStatsElements
		eStatsElements[i] = strconv.Itoa(eVal + nVal)
	}

	// Join eStatsElements with commas and convert back to byte slice
	e.statsJsonByte = []byte("[" + strings.Join(eStatsElements, ", ") + "]")

	return nil
}

// ExecPlan2Json return ExecPlan Serialized json-str
// and set RowsRead, BytesScan from ExecPlan
//
// please used in s.mux.Lock()
func (s *StatementInfo) ExecPlan2Json(ctx context.Context) ([]byte, []byte) {
	var stats Statistic

	if s.jsonByte != nil {
		goto endL
	} else if s.ExecPlan == nil {
		uuidStr := uuid.UUID(s.StatementID).String()
		return []byte(fmt.Sprintf(`{"code":200,"message":"NO ExecPlan Serialize function","steps":null,"success":false,"uuid":%q}`, uuidStr)),
			[]byte(`{"code":200,"message":"NO ExecPlan"}`)
	} else {
		s.jsonByte, s.statsJsonByte, stats = s.ExecPlan.Marshal(ctx)
		s.RowsRead, s.BytesScan = stats.RowsRead, stats.BytesScan
		//if queryTime := GetTracerProvider().longQueryTime; queryTime > int64(s.Duration) {
		//	// get nil ExecPlan json-str
		//	jsonByte, _, _ = s.SerializeExecPlan(ctx, nil, uuid.UUID(s.StatementID))
		//}
	}
	if len(s.statsJsonByte) == 0 {
		s.statsJsonByte = []byte("[]")
	} else {
		// Convert statsJsonByte to four key values array
		var err error
		statsValues, err := getStatsValues(s.statsJsonByte, uint64(s.Duration))
		if err != nil {
			return nil, nil
		}

		// Convert statsValues to string and then to byte array
		statsValuesStr := fmt.Sprintf("%v", statsValues)
		s.statsJsonByte = []byte(statsValuesStr)
	}
endL:
	return s.jsonByte, s.statsJsonByte
}

func GetLongQueryTime() time.Duration {
	return time.Duration(GetTracerProvider().longQueryTime)
}

type SerializeExecPlanFunc func(ctx context.Context, plan any, uuid2 uuid.UUID) (jsonByte []byte, statsJson []byte, stats Statistic)

type SerializableExecPlan interface {
	Marshal(context.Context) ([]byte, []byte, Statistic)
	Free()
}

func (s *StatementInfo) SetSerializableExecPlan(execPlan SerializableExecPlan) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.ExecPlan = execPlan
}

func (s *StatementInfo) SetTxnID(id []byte) {
	copy(s.TransactionID[:], id)
}

func (s *StatementInfo) IsZeroTxnID() bool {
	return bytes.Equal(s.TransactionID[:], NilTxnID[:])
}

func (s *StatementInfo) Report(ctx context.Context) {
	if s.Status == StatementStatusRunning && GetTracerProvider().skipRunningStmt {
		return
	}
	s.reported = true
	ReportStatement(ctx, s)
}

var EndStatement = func(ctx context.Context, err error, sentRows int64) {
	if !GetTracerProvider().IsEnable() {
		return
	}
	s := StatementFromContext(ctx)
	if s == nil {
		panic(moerr.NewInternalError(ctx, "no statement info in context"))
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	if !s.end { // cooperate with s.mux
		// do report
		s.end = true
		s.ResultCount = sentRows
		s.AggrCount = 0
		s.ResponseAt = time.Now()
		s.Duration = s.ResponseAt.Sub(s.RequestAt)
		s.Status = StatementStatusSuccess
		if err != nil {
			s.Error = err
			s.Status = StatementStatusFailed
		}
		if !s.reported || s.exported { // cooperate with s.mux
			s.exported = false
			s.Report(ctx)
		}
	}
}

type StatementInfoStatus int

const (
	StatementStatusRunning StatementInfoStatus = iota
	StatementStatusSuccess
	StatementStatusFailed
)

func (s StatementInfoStatus) String() string {
	switch s {
	case StatementStatusSuccess:
		return "Success"
	case StatementStatusRunning:
		return "Running"
	case StatementStatusFailed:
		return "Failed"
	}
	return "Unknown"
}

type StatementOption interface {
	Apply(*StatementInfo)
}

type StatementOptionFunc func(*StatementInfo)

var ReportStatement = func(ctx context.Context, s *StatementInfo) error {
	if !GetTracerProvider().IsEnable() {
		return nil
	}
	// Filter out the MO_LOGGER SQL statements
	if s.User == db_holder.MOLoggerUser {
		return nil
	}
	// Filter out part of the internal SQL statements
	// Todo: review how to aggregate the internal SQL statements logging
	if s.User == "internal" {
		if s.StatementType == "Commit" || s.StatementType == "Start Transaction" || s.StatementType == "Use" {
			return nil
		}
	}

	return GetGlobalBatchProcessor().Collect(ctx, s)
}
