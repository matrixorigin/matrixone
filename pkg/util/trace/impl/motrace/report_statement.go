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
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

var NilStmtID [16]byte
var NilTxnID [16]byte
var NilSesID [16]byte

// StatementInfo implement export.IBuffer2SqlItem and export.CsvFields

var _ IBuffer2SqlItem = (*StatementInfo)(nil)

const Decimal128Width = 38
const Decimal128Scale = 0

func convertFloat64ToDecimal128(val float64) (types.Decimal128, error) {
	return types.Decimal128FromFloat64(val, Decimal128Width, Decimal128Scale)
}
func mustDecimal128(v types.Decimal128, err error) types.Decimal128 {
	if err != nil {
		logutil.Panic("mustDecimal128", zap.Error(err))
	}
	return v
}

func StatementInfoNew(i Item, ctx context.Context) Item {
	stmt := NewStatementInfo() // Get a new statement from the pool
	if s, ok := i.(*StatementInfo); ok {

		// execute the stat plan
		s.ExecPlan2Stats(ctx)

		// remove the plan, s will be free
		s.jsonByte = nil
		s.FreeExecPlan()
		s.exported = true

		// copy value
		stmt.StatementID = s.StatementID
		stmt.SessionID = s.SessionID
		stmt.TransactionID = s.TransactionID
		stmt.Account = s.Account
		stmt.User = s.User
		stmt.Host = s.Host
		stmt.RoleId = s.RoleId
		stmt.StatementType = s.StatementType
		stmt.QueryType = s.QueryType
		stmt.SqlSourceType = s.SqlSourceType
		stmt.Statement = s.Statement
		stmt.StmtBuilder.WriteString(s.Statement)
		stmt.Status = s.Status
		stmt.Duration = s.Duration
		stmt.ResultCount = s.ResultCount
		stmt.RowsRead = s.RowsRead
		stmt.BytesScan = s.BytesScan
		stmt.ConnType = s.ConnType
		stmt.statsArray = s.statsArray
		stmt.RequestAt = s.RequestAt
		stmt.ResponseAt = s.ResponseAt
		stmt.end = s.end
		stmt.StatementTag = s.StatementTag
		stmt.StatementFingerprint = s.StatementFingerprint
		stmt.Error = s.Error
		stmt.Database = s.Database
		stmt.AggrMemoryTime = s.AggrMemoryTime

		// initialize the AggrCount as 0 here since aggr is not started
		stmt.AggrCount = 0
		return stmt
	}
	return nil
}

func StatementInfoUpdate(ctx context.Context, existing, new Item) {

	e := existing.(*StatementInfo)
	n := new.(*StatementInfo)
	// nil aggregated stmt record's txn-id, if including diff transactions.
	if e.TransactionID != n.TransactionID {
		e.TransactionID = NilTxnID
	}
	if e.AggrCount == 0 {
		// initialize the AggrCount as 1 here since aggr is started
		windowSize, _ := ctx.Value(DurationKey).(time.Duration)
		e.StatementTag = ""
		e.StatementFingerprint = ""
		e.Error = nil
		e.Database = ""
		duration := e.Duration
		e.AggrMemoryTime = mustDecimal128(convertFloat64ToDecimal128(e.statsArray.GetMemorySize() * float64(duration)))
		e.RequestAt = e.ResponseAt.Truncate(windowSize)
		e.ResponseAt = e.RequestAt.Add(windowSize)
		e.AggrCount = 1
	}
	// update the stats
	if GetTracerProvider().enableStmtMerge {
		e.StmtBuilder.WriteString(";\n")
		e.StmtBuilder.WriteString(n.Statement)
	}
	e.AggrCount += 1
	e.Duration += n.Duration
	e.ResultCount += n.ResultCount
	// responseAt is the last response time
	n.ExecPlan2Stats(context.Background())
	if err := mergeStats(e, n); err != nil {
		// handle error
		logutil.Error("Failed to merge stats", logutil.ErrorField(err))
	}
	n.FreeExecPlan()
	// NO need n.mux.Lock()
	// Because of this op is between EndStatement and FillRow.
	// This function is called in Aggregator, and StatementInfoFilter must return true.
	n.exported = true
}

func StatementInfoFilter(i Item) bool {
	// Attempt to perform a type assertion to *StatementInfo
	statementInfo, ok := i.(*StatementInfo)

	if !ok {
		// The item couldn't be cast to *StatementInfo
		return false
	}

	// Do not aggr the running statement
	if statementInfo.Status == StatementStatusRunning {
		return false
	}

	// for #14926
	if statementInfo.statsArray.GetCU() < 0 {
		return false
	}

	// Check SqlSourceType
	switch statementInfo.SqlSourceType {
	case constant.InternalSql, constant.ExternSql, constant.CloudNoUserSql:
		// Check StatementType
		switch statementInfo.StatementType {
		case "Insert", "Update", "Delete", "Execute", "Select":
			if statementInfo.Duration <= GetTracerProvider().selectAggrThreshold {
				return true
			}
		}
	}
	// If no conditions matched, return false
	return false
}

type StatementInfo struct {
	StatementID          [16]byte `json:"statement_id"`
	TransactionID        [16]byte `json:"transaction_id"`
	SessionID            [16]byte `jons:"session_id"`
	Account              string   `json:"account"`
	User                 string   `json:"user"`
	Host                 string   `json:"host"`
	RoleId               uint32   `json:"role_id"`
	Database             string   `json:"database"`
	Statement            string   `json:"statement"`
	StmtBuilder          strings.Builder
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

	// AggrMemoryTime
	AggrMemoryTime types.Decimal128

	ResultCount int64 `json:"result_count"` // see EndStatement

	ConnType statistic.ConnType `json:"-"` // see frontend.RecordStatement

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
	// reported mark reported
	// set by ReportStatement
	reported bool
	// exported mark exported
	// set by FillRow or StatementInfoUpdate
	exported bool

	// keep []byte as elem
	jsonByte   []byte
	statsArray statistic.StatsArray
	stated     bool

	// skipTxnOnce, readonly, for flow control
	// see more on NeedSkipTxn() and SkipTxnId()
	skipTxnOnce bool
	skipTxnID   []byte
}

type Key struct {
	SessionID     [16]byte
	StatementType string
	Window        time.Time
	Status        StatementInfoStatus
	SqlSourceType string
}

var stmtPool = sync.Pool{
	New: func() any {
		return &StatementInfo{}
	},
}

func NewStatementInfo() *StatementInfo {
	s := stmtPool.Get().(*StatementInfo)
	s.statsArray.Reset()
	s.stated = false
	return s
}

type Statistic struct {
	RowsRead  int64
	BytesScan int64
}

func (s *StatementInfo) Key(duration time.Duration) interface{} {
	return Key{SessionID: s.SessionID, StatementType: s.StatementType, Window: s.ResponseAt.Truncate(duration), Status: s.Status, SqlSourceType: s.SqlSourceType}
}

func (s *StatementInfo) GetName() string {
	return SingleStatementTable.GetName()
}

func (s *StatementInfo) IsMoLogger() bool {
	return s.Account == "sys" && s.User == db_holder.MOLoggerUser
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
			len(s.SqlSourceType)+len(s.StatementType)+len(s.QueryType)+len(s.jsonByte)+len(s.statsArray)*8,
	)
	if s.jsonByte == nil {
		return num + jsonByteLength
	}
	return num
}

// FreeExecPlan will free StatementInfo.ExecPlan.
// Please make sure it called after StatementInfo.ExecPlan2Stats
func (s *StatementInfo) FreeExecPlan() {
	if s.ExecPlan != nil {
		s.ExecPlan.Free()
		s.ExecPlan = nil
	}
}

func (s *StatementInfo) Free() {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.end && s.exported { // cooperate with s.mux
		s.free()
	}
}

// freeNoLocked will free StatementInfo if StatementInfo.end is true.
// Please make sure it called after EndStatement.
func (s *StatementInfo) freeNoLocked() {
	if s.end {
		s.free()
	}
}

func (s *StatementInfo) free() {
	s.StatementID = NilStmtID
	s.TransactionID = NilTxnID
	s.SessionID = NilSesID
	s.Account = ""
	s.User = ""
	s.Host = ""
	s.RoleId = 0
	s.Database = ""
	s.Statement = ""
	s.StmtBuilder.Reset()
	s.StatementFingerprint = ""
	s.StatementTag = ""
	s.SqlSourceType = ""
	s.RequestAt = time.Time{}
	s.StatementType = ""
	s.QueryType = ""
	s.Status = StatementStatusRunning
	s.Error = nil
	s.ResponseAt = time.Time{}
	s.Duration = 0
	s.FreeExecPlan() // handle s.ExecPlan
	s.RowsRead = 0
	s.BytesScan = 0
	s.AggrCount = 0
	// s.AggrMemoryTime // skip
	s.ResultCount = 0
	s.ConnType = 0
	s.end = false
	s.reported = false
	s.exported = false
	// clean []byte
	s.jsonByte = nil
	s.statsArray.Reset()
	s.stated = false
	// clean skipTxn ctrl
	s.skipTxnOnce = false
	s.skipTxnID = nil
	stmtPool.Put(s)
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
	execPlan := s.ExecPlan2Json(ctx)
	if s.AggrCount > 0 {
		float64Val := calculateAggrMemoryBytes(s.AggrMemoryTime, float64(s.Duration))
		s.statsArray.WithMemorySize(float64Val)
	}
	// stats := s.ExecPlan2Stats(ctx) // deprecated
	stats := s.GetStatsArrayBytes()
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

// calculateAggrMemoryBytes return scale = statistic.Decimal128ToFloat64Scale float64 val
func calculateAggrMemoryBytes(dividend types.Decimal128, divisor float64) float64 {
	scale := int32(statistic.Decimal128ToFloat64Scale)
	divisorD := mustDecimal128(types.Decimal128FromFloat64(divisor, Decimal128Width, scale))
	val, valScale, err := dividend.Div(divisorD, 0, scale)
	val = mustDecimal128(val, err)
	return types.Decimal128ToFloat64(val, valScale)
}

// mergeStats n (new one) into e (existing one)
// All data generated in ExecPlan2Stats should be handled here, including:
// - statsArray
// - RowRead
// - BytesScan
func mergeStats(e, n *StatementInfo) error {
	e.statsArray.Add(&n.statsArray)
	val, _, err := e.AggrMemoryTime.Add(
		mustDecimal128(convertFloat64ToDecimal128(n.statsArray.GetMemorySize()*float64(n.Duration))),
		Decimal128Scale,
		Decimal128Scale,
	)
	e.AggrMemoryTime = mustDecimal128(val, err)
	e.RowsRead += n.RowsRead
	e.BytesScan += n.BytesScan
	return nil
}

var noExecPlan = []byte(`{}`)

// ExecPlan2Json return ExecPlan Serialized json-str //
// please used in s.mux.Lock()
func (s *StatementInfo) ExecPlan2Json(ctx context.Context) []byte {
	if s.jsonByte != nil {
		goto endL
	} else if s.ExecPlan == nil {
		return noExecPlan
	} else {
		s.jsonByte = s.ExecPlan.Marshal(ctx)
		//if queryTime := GetTracerProvider().longQueryTime; queryTime > int64(s.Duration) {
		//	// get nil ExecPlan json-str
		//	jsonByte, _, _ = s.SerializeExecPlan(ctx, nil, uuid.UUID(s.StatementID))
		//}
	}
endL:
	return s.jsonByte
}

// ExecPlan2Stats return Stats Serialized int array str
// and set RowsRead, BytesScan from ExecPlan
// and CalculateCU
func (s *StatementInfo) ExecPlan2Stats(ctx context.Context) error {
	var stats Statistic
	var statsArray statistic.StatsArray

	if s.ExecPlan != nil && !s.stated {
		statsArray, stats = s.ExecPlan.Stats(ctx)
		if s.statsArray.GetTimeConsumed() > 0 {
			logutil.GetSkip1Logger().Error("statsArray.GetTimeConsumed() > 0",
				zap.String("statement_id", uuid.UUID(s.StatementID).String()),
			)
		}
		s.statsArray.InitIfEmpty().Add(&statsArray)
		s.statsArray.WithConnType(s.ConnType)
		s.RowsRead = stats.RowsRead
		s.BytesScan = stats.BytesScan
		s.stated = true
	}
	cu := CalculateCU(s.statsArray, int64(s.Duration))
	s.statsArray.WithCU(cu)
	return nil
}

func (s *StatementInfo) GetStatsArrayBytes() []byte {
	return s.statsArray.ToJsonString()
}

// SetSkipTxn set skip txn flag, cooperate with SetSkipTxnId()
// usage:
// Step1: SetSkipTxn(true)
// Step2:
//
//	if NeedSkipTxn() {
//		SetSkipTxn(false)
//		SetSkipTxnId(target_txn_id)
//	} else SkipTxnId(current_txn_id) {
//		// record current txn id
//	}
func (s *StatementInfo) SetSkipTxn(skip bool)   { s.skipTxnOnce = skip }
func (s *StatementInfo) SetSkipTxnId(id []byte) { s.skipTxnID = id }
func (s *StatementInfo) NeedSkipTxn() bool      { return s.skipTxnOnce }
func (s *StatementInfo) SkipTxnId(id []byte) bool {
	// s.skipTxnID == nil, means NO skipTxnId
	return s.skipTxnID != nil && bytes.Equal(s.skipTxnID, id)
}

func GetLongQueryTime() time.Duration {
	return time.Duration(GetTracerProvider().longQueryTime)
}

type SerializeExecPlanFunc func(ctx context.Context, plan any, uuid2 uuid.UUID) (jsonByte []byte, statsJson statistic.StatsArray, stats Statistic)

type SerializableExecPlan interface {
	Marshal(context.Context) []byte
	Free()
	Stats(ctx context.Context) (statistic.StatsArray, Statistic)
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
	ReportStatement(ctx, s)
}

func (s *StatementInfo) MarkResponseAt() {
	if s.ResponseAt.IsZero() {
		s.ResponseAt = time.Now()
		s.Duration = s.ResponseAt.Sub(s.RequestAt)
	}
}

// TcpIpv4HeaderSize default tcp header bytes.
const TcpIpv4HeaderSize = 66

// ResponseErrPacketSize avg prefix size for mysql packet response error.
// 66: default tcp header bytes.
// 13: avg payload prefix of err response
const ResponseErrPacketSize = TcpIpv4HeaderSize + 13

func EndStatement(ctx context.Context, err error, sentRows int64, outBytes int64, outPacket int64) {
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
		s.MarkResponseAt()
		// --- Start of metric part
		// duration is filled in s.MarkResponseAt()
		incStatementCounter(s.Account, s.QueryType)
		addStatementDurationCounter(s.Account, s.QueryType, s.Duration)
		// --- END of metric part
		if err != nil {
			outBytes += ResponseErrPacketSize + int64(len(err.Error()))
		}
		if GetTracerProvider().tcpPacket {
			outBytes += TcpIpv4HeaderSize * outPacket
		}
		s.statsArray.InitIfEmpty().WithOutTrafficBytes(float64(outBytes)).WithOutPacketCount(float64(outPacket))
		s.ExecPlan2Stats(ctx)
		if s.statsArray.GetCU() < 0 {
			logutil.Warnf("negative cu: %f, %s", s.statsArray.GetCU(), uuid.UUID(s.StatementID).String())
			v2.GetTraceNegativeCUCounter("cu").Inc()
		} else {
			metric.StatementCUCounter(s.Account, s.SqlSourceType).Add(s.statsArray.GetCU())
		}
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

func addStatementDurationCounter(tenant, queryType string, duration time.Duration) {
	metric.StatementDuration(tenant, queryType).Add(float64(duration))
}
func incStatementCounter(tenant, queryType string) {
	metric.StatementCounter(tenant, queryType).Inc()
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
	// Filter out the Running record.
	if s.Status == StatementStatusRunning && GetTracerProvider().skipRunningStmt {
		return nil
	}
	// Filter out the MO_LOGGER SQL statements
	//if s.User == db_holder.MOLoggerUser {
	//	goto DiscardAndFreeL
	//}

	// Filter out the statement is empty
	if s.Statement == "" {
		goto DiscardAndFreeL
	}

	// Filter out exported or reported statement
	if s.exported || s.reported {
		goto DiscardAndFreeL
	}

	// Filter out part of the internal SQL statements
	// Todo: review how to aggregate the internal SQL statements logging
	if s.User == "internal" && s.Account == "sys" {
		if s.StatementType == "Commit" || s.StatementType == "Start Transaction" || s.StatementType == "Use" {
			goto DiscardAndFreeL
		}
	}

	// logging the statement that should not be here anymore
	if s.exported || s.reported || s.Statement == "" {
		logutil.Error("StatementInfo should not be here anymore", zap.String("StatementInfo", s.Statement), zap.String("statement_id", uuid.UUID(s.StatementID).String()), zap.String("user", s.User), zap.Bool("exported", s.exported), zap.Bool("reported", s.reported))
	}

	s.reported = true
	return GetGlobalBatchProcessor().Collect(ctx, s)
DiscardAndFreeL:
	s.freeNoLocked()
	return nil
}
