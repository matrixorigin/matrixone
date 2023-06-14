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
	"errors"
	"fmt"
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
	row.SetColumnVal(resultCntCol, table.Int64Field(s.ResultCount))
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
		s.statsJsonByte = []byte("{}")
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
