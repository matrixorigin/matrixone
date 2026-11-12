// Copyright 2026 Matrix Origin
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

package embed

import (
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestAsyncDatalinkIndexProgress(t *testing.T) {
	target := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	for _, tc := range []struct {
		name       string
		tail       int64
		tailErr    error
		state      int8
		watermark  string
		status     string
		jobErr     error
		want       bool
		parseError bool
		diagnostic string
	}{
		{name: "all fences reached", tail: 8, state: iscp.ISCPJobState_Completed, watermark: target.DebugString(), want: true},
		{name: "tail unchanged", tail: 7, state: iscp.ISCPJobState_Completed, watermark: target.DebugString()},
		{name: "running is not committed", tail: 8, state: iscp.ISCPJobState_Running, watermark: target.DebugString()},
		{name: "failed job", tail: 8, state: iscp.ISCPJobState_Error, watermark: target.DebugString(), status: `{"ErrorCode":10000}`, diagnostic: "error_code=10000"},
		{name: "watermark behind", tail: 8, state: iscp.ISCPJobState_Completed, watermark: "100-6"},
		{name: "invalid watermark", tail: 8, state: iscp.ISCPJobState_Completed, watermark: "invalid", parseError: true, diagnostic: "parse_error="},
		{name: "tail query error", tailErr: errors.New("tail query failed"), state: iscp.ISCPJobState_Completed, watermark: target.DebugString(), diagnostic: "tail_error=tail query failed"},
		{name: "job query error", tail: 8, jobErr: errors.New("job query failed"), diagnostic: "job_error=job query failed"},
		{name: "status is diagnostic only", tail: 8, state: iscp.ISCPJobState_Completed, watermark: target.DebugString(), status: "invalid", want: true, diagnostic: "status_error="},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			conn, err := db.Conn(t.Context())
			require.NoError(t, err)
			defer conn.Close()
			tail := mock.ExpectQuery(regexp.QuoteMeta("select tail"))
			if tc.tailErr != nil {
				tail.WillReturnError(tc.tailErr)
			} else {
				tail.WillReturnRows(sqlmock.NewRows([]string{"tail"}).AddRow(tc.tail))
			}
			job := mock.ExpectQuery(regexp.QuoteMeta(asyncDatalinkJobProgressSQL)).WithArgs(uint64(42), uint64(5))
			if tc.jobErr != nil {
				job.WillReturnError(tc.jobErr)
			} else {
				job.WillReturnRows(sqlmock.NewRows([]string{"job_state", "watermark", "job_status"}).AddRow(tc.state, tc.watermark, tc.status))
			}
			sample := readAsyncDatalinkIndexProgress(t.Context(), conn, "select tail", 42, 5)
			require.Equal(t, tc.want, sample.caughtUp(7, target))
			require.Equal(t, tc.parseError, sample.parseErr != nil)
			var progress asyncDatalinkIndexProgress
			progress.record(sample)
			if tc.diagnostic != "" {
				require.Contains(t, fmt.Sprint(&progress), tc.diagnostic)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestAsyncDatalinkIndexProgressRetainsLastObservation(t *testing.T) {
	var progress asyncDatalinkIndexProgress
	progress.record(asyncDatalinkIndexSample{tail: 8, state: iscp.ISCPJobState_Error, watermark: "100-6"})
	progress.record(asyncDatalinkIndexSample{tailErr: errors.New("tail unavailable"), jobErr: errors.New("job unavailable")})
	diagnostic := fmt.Sprintf("last observation: %s", &progress)
	for _, part := range []string{"tail_observed=true", "tail=8", "tail_error=tail unavailable", "job_observed=true", "state=4", `watermark="100-6"`, "job_error=job unavailable"} {
		require.Contains(t, diagnostic, part)
	}
	progress.record(asyncDatalinkIndexSample{tail: 9, state: iscp.ISCPJobState_Running, watermark: "100-7"})
	diagnostic = fmt.Sprint(&progress)
	for _, part := range []string{"tail_error=<nil>", "last_tail_error=tail unavailable", "job_error=<nil>", "last_job_error=job unavailable"} {
		require.Contains(t, diagnostic, part)
	}
}
