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
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var nilTxnID [16]byte

// StatementInfo implement export.IBuffer2SqlItem and export.CsvFields
type StatementInfo struct {
	StatementID          [16]byte  `json:"statement_id"`
	TransactionID        [16]byte  `json:"transaction_id"`
	SessionID            [16]byte  `jons:"session_id"`
	Account              string    `json:"account"`
	User                 string    `json:"user"`
	Host                 string    `json:"host"`
	Database             string    `json:"database"`
	Statement            string    `json:"statement"`
	StatementFingerprint string    `json:"statement_fingerprint"`
	StatementTag         string    `json:"statement_tag"`
	RequestAt            time.Time `json:"request_at"` // see WithRequestAt

	// after
	Status     StatementInfoStatus `json:"status"`
	Error      error               `json:"error"`
	ResponseAt time.Time           `json:"response_at"`
	Duration   time.Duration       `json:"duration"` // unit: ns
	ExecPlan   any                 `json:"exec_plan"`
	// RowsRead, BytesScan generated from ExecPlan
	RowsRead  int64 `json:"rows_read"`  // see ExecPlan2Json
	BytesScan int64 `json:"bytes_scan"` // see ExecPlan2Json
	// SerializeExecPlan
	SerializeExecPlan SerializeExecPlanFunc // see SetExecPlan, ExecPlan2Json

	// flow ctrl
	end bool
	mux sync.Mutex
	// mark reported
	reported bool
	// mark exported
	exported bool
}

func (s *StatementInfo) GetName() string {
	return SingleStatementTable.GetName()
}

func (s *StatementInfo) Size() int64 {
	return int64(unsafe.Sizeof(s)) + int64(
		len(s.Account)+len(s.User)+len(s.Host)+
			len(s.Database)+len(s.Statement)+len(s.StatementFingerprint)+len(s.StatementTag),
	)
}

func (s *StatementInfo) Free() {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.end { // cooperate with s.mux
		s.Statement = ""
		s.StatementFingerprint = ""
		s.StatementTag = ""
		s.ExecPlan = nil
		s.Error = nil
	}
}

func (s *StatementInfo) GetRow() *table.Row { return SingleStatementTable.GetRow(DefaultContext()) }

func (s *StatementInfo) CsvFields(ctx context.Context, row *table.Row) []string {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.exported = true
	row.Reset()
	row.SetColumnVal(stmtIDCol, uuid.UUID(s.StatementID).String())
	row.SetColumnVal(txnIDCol, uuid.UUID(s.TransactionID).String())
	row.SetColumnVal(sesIDCol, uuid.UUID(s.SessionID).String())
	row.SetColumnVal(accountCol, s.Account)
	row.SetColumnVal(userCol, s.User)
	row.SetColumnVal(hostCol, s.Host)
	row.SetColumnVal(dbCol, s.Database)
	row.SetColumnVal(stmtCol, s.Statement)
	row.SetColumnVal(stmtTagCol, s.StatementTag)
	row.SetColumnVal(stmtFgCol, s.StatementFingerprint)
	row.SetColumnVal(nodeUUIDCol, GetNodeResource().NodeUuid)
	row.SetColumnVal(nodeTypeCol, GetNodeResource().NodeType)
	row.SetColumnVal(reqAtCol, Time2DatetimeString(s.RequestAt))
	row.SetColumnVal(respAtCol, Time2DatetimeString(s.ResponseAt))
	row.SetColumnVal(durationCol, fmt.Sprintf("%d", s.Duration))
	row.SetColumnVal(statusCol, s.Status.String())
	if s.Error != nil {
		var moError *moerr.Error
		errCode := moerr.ErrInfo
		if errors.As(s.Error, &moError) {
			errCode = moError.ErrorCode()
		}
		row.SetColumnVal(errCodeCol, fmt.Sprintf("%d", errCode))
		row.SetColumnVal(errorCol, fmt.Sprintf("%s", s.Error))
	}
	row.SetColumnVal(execPlanCol, s.ExecPlan2Json(ctx))
	row.SetColumnVal(rowsReadCol, fmt.Sprintf("%d", s.RowsRead))
	row.SetColumnVal(bytesScanCol, fmt.Sprintf("%d", s.BytesScan))

	return row.ToStrings()
}

// ExecPlan2Json return ExecPlan Serialized json-str
// and set RowsRead, BytesScan from ExecPlan
//
// please used in s.mux.Lock()
func (s *StatementInfo) ExecPlan2Json(ctx context.Context) string {
	var jsonByte []byte
	if s.SerializeExecPlan == nil {
		// use defaultSerializeExecPlan
		if f := getDefaultSerializeExecPlan(); f == nil {
			uuidStr := uuid.UUID(s.StatementID).String()
			return fmt.Sprintf(`{"code":200,"message":"NO ExecPlan Serialize function","steps":null,"success":false,"uuid":%q}`, uuidStr)
		} else {
			jsonByte, s.RowsRead, s.BytesScan = f(ctx, s.ExecPlan, uuid.UUID(s.StatementID))
		}
	} else {
		// use s.SerializeExecPlan
		// get real ExecPlan json-str
		jsonByte, s.RowsRead, s.BytesScan = s.SerializeExecPlan(ctx, s.ExecPlan, uuid.UUID(s.StatementID))
		if queryTime := GetTracerProvider().longQueryTime; queryTime > int64(s.Duration) {
			// get nil ExecPlan json-str
			jsonByte, _, _ = s.SerializeExecPlan(ctx, nil, uuid.UUID(s.StatementID))
		}
	}
	return string(jsonByte)
}

var defaultSerializeExecPlan atomic.Value

type SerializeExecPlanFunc func(ctx context.Context, plan any, uuid2 uuid.UUID) (jsonByte []byte, rows int64, bytes int64)

func SetDefaultSerializeExecPlan(f SerializeExecPlanFunc) {
	defaultSerializeExecPlan.Store(f)
}

func getDefaultSerializeExecPlan() SerializeExecPlanFunc {
	if defaultSerializeExecPlan.Load() == nil {
		return nil
	} else {
		return defaultSerializeExecPlan.Load().(SerializeExecPlanFunc)
	}
}

// SetExecPlan record execPlan should be TxnComputationWrapper.plan obj, which support 2json.
func (s *StatementInfo) SetExecPlan(execPlan any, SerializeFunc SerializeExecPlanFunc) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.ExecPlan = execPlan
	s.SerializeExecPlan = SerializeFunc
}

func (s *StatementInfo) SetTxnID(id []byte) {
	copy(s.TransactionID[:], id)
}

func (s *StatementInfo) IsZeroTxnID() bool {
	return bytes.Equal(s.TransactionID[:], nilTxnID[:])
}

func (s *StatementInfo) Report(ctx context.Context) {
	s.reported = true
	ReportStatement(ctx, s)
}

var EndStatement = func(ctx context.Context, err error) {
	if !GetTracerProvider().IsEnable() {
		return
	}
	s := StatementFromContext(ctx)
	if s == nil {
		panic(moerr.NewInternalError(ctx, "no statement info in context"))
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	if !s.end {
		// do report
		s.end = true
		s.ResponseAt = time.Now()
		s.Duration = s.ResponseAt.Sub(s.RequestAt)
		s.Status = StatementStatusSuccess
		if err != nil {
			s.Error = err
			s.Status = StatementStatusFailed
		}
		if !s.reported || s.exported { // cooperate with s.mux
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
	return GetGlobalBatchProcessor().Collect(ctx, s)
}
