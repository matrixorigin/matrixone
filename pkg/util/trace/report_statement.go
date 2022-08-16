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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/export"
)

var _ IBuffer2SqlItem = &StatementInfo{}

type StatementInfo struct {
	StatementID          uint64              `json:"statement_id"`
	TransactionID        uint64              `json:"transaction_id"`
	SessionID            uint64              `jons:"session_id"`
	Account              string              `json:"account"`
	User                 string              `json:"user"`
	Host                 string              `json:"host"`
	Database             string              `json:"database"`
	Statement            string              `json:"statement"`
	StatementFingerprint string              `json:"statement_fingerprint"`
	StatementTag         string              `json:"statement_tag"`
	RequestAt            util.TimeNano       `json:"request_at"` // see WithRequestAt
	Status               StatementInfoStatus `json:"status"`
	ExecPlan             string              `json:"exec_plan"`
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
