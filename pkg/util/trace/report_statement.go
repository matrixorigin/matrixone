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
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/export"
)

var _ IBuffer2SqlItem = (*StatementInfo)(nil)
var _ CsvFields = (*StatementInfo)(nil)

type StatementInfo struct {
	StatementID          [16]byte      `json:"statement_id"`
	TransactionID        [16]byte      `json:"transaction_id"`
	SessionID            [16]byte      `jons:"session_id"`
	TenantID             uint32        `json:"tenant_id"`
	UserID               uint32        `json:"user_id"`
	Account              string        `json:"account"`
	User                 string        `json:"user"`
	Host                 string        `json:"host"`
	Database             string        `json:"database"`
	Statement            string        `json:"statement"`
	StatementFingerprint string        `json:"statement_fingerprint"`
	StatementTag         string        `json:"statement_tag"`
	RequestAt            util.TimeNano `json:"request_at"` // see WithRequestAt
	ExecPlan             string        `json:"exec_plan"`
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
	return commonCsvOptions
}

func (s StatementInfo) CsvFields() []string {
	var uuid = make([]byte, 36)
	var result []string
	bytes2Uuid(uuid, s.StatementID)
	result = append(result, string(uuid[:]))
	bytes2Uuid(uuid, s.TransactionID)
	result = append(result, string(uuid[:]))
	bytes2Uuid(uuid, s.SessionID)
	result = append(result, string(uuid[:]))
	result = append(result, fmt.Sprintf("%d", s.TenantID))
	result = append(result, fmt.Sprintf("%d", s.UserID))
	result = append(result, s.Account)
	result = append(result, s.User)
	result = append(result, s.Host)
	result = append(result, s.Database)
	result = append(result, s.Statement)
	result = append(result, s.StatementTag)
	result = append(result, s.StatementFingerprint)
	result = append(result, GetNodeResource().NodeUuid)
	result = append(result, GetNodeResource().NodeType.String())
	result = append(result, nanoSec2DatetimeString(s.RequestAt))
	result = append(result, s.ExecPlan)
	return result
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
	if !gTracerProvider.IsEnable() {
		return nil
	}
	return export.GetGlobalBatchProcessor().Collect(ctx, s)
}
