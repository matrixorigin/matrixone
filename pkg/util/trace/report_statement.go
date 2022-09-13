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
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/export"
)

var _ IBuffer2SqlItem = (*StatementInfo)(nil)
var _ CsvFields = (*StatementInfo)(nil)

type StatementInfo struct {
	StatementID          [16]byte      `json:"statement_id"`
	TransactionID        [16]byte      `json:"transaction_id"`
	SessionID            [16]byte      `jons:"session_id"`
	Account              string        `json:"account"`
	User                 string        `json:"user"`
	Host                 string        `json:"host"`
	Database             string        `json:"database"`
	Statement            string        `json:"statement"`
	StatementFingerprint string        `json:"statement_fingerprint"`
	StatementTag         string        `json:"statement_tag"`
	RequestAt            util.TimeNano `json:"request_at"` // see WithRequestAt
	ExecPlan             any           `json:"exec_plan"`

	// flow ctrl
	end bool
}

func (s StatementInfo) GetName() string {
	return MOStatementType
}

func (s StatementInfo) Size() int64 {
	return int64(unsafe.Sizeof(s)) + int64(
		len(s.Account)+len(s.User)+len(s.Host)+
			len(s.Database)+len(s.Statement)+len(s.StatementFingerprint)+len(s.StatementTag),
	)
}

func (s StatementInfo) Free() {}

func (s StatementInfo) CsvOptions() *CsvOptions {
	return CommonCsvOptions
}

func (s StatementInfo) CsvFields() []string {
	var result []string
	result = append(result, uuid.UUID(s.StatementID).String())
	result = append(result, uuid.UUID(s.TransactionID).String())
	result = append(result, uuid.UUID(s.SessionID).String())
	result = append(result, s.Account)
	result = append(result, s.User)
	result = append(result, s.Host)
	result = append(result, s.Database)
	result = append(result, s.Statement)
	result = append(result, s.StatementTag)
	result = append(result, s.StatementFingerprint)
	result = append(result, GetNodeResource().NodeUuid)
	result = append(result, GetNodeResource().NodeType)
	result = append(result, nanoSec2DatetimeString(s.RequestAt))
	result = append(result, s.ExecPlan2Json())
	return result
}

func (s *StatementInfo) ExecPlan2Json() string {
	json, err := json.Marshal(s.ExecPlan)
	if err != nil {
		panic(moerr.NewPanicError(err))
	}
	return string(json)
}

// SetExecPlan record execPlan should be TxnComputationWrapper.plan obj, which support 2json.
func (s *StatementInfo) SetExecPlan(execPlan any) {
	s.ExecPlan = execPlan
}

func (s *StatementInfo) End(ctx context.Context) {
	s.end = true
	ReportStatement(ctx, s)
}

func EndStatement(ctx context.Context, err error) time.Time {
	s := StatementFromContext(ctx)
	if s == nil {
		panic(moerr.NewPanicError(fmt.Errorf("no statement info in context")))
	}
	endTime := util.NowNS()
	if !s.end {
		s.End(ctx)
	}
	status := "Success"
	if err != nil {
		status = fmt.Sprintf("Failed. %s", err.Error())
	}

	ReportStats(ctx, &MOStatsInfo{
		StatementID:       s.StatementID,
		Account:           s.Account,
		Timestamp:         endTime,
		Operator:          "status",
		OperateObjectDesc: status,
		NodeId:            0,
		InputRows:         0,
		OutputRows:        0,
		TimeConsumed:      int64((endTime - s.RequestAt) / 1e3),
		InputSize:         0,
		OutputSize:        0,
		MemorySize:        0,
	})

	return util.Time(endTime)
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

func ReportStatement(ctx context.Context, s *StatementInfo) error {
	if !GetTracerProvider().IsEnable() {
		return nil
	}
	return export.GetGlobalBatchProcessor().Collect(ctx, s)
}
