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

package types

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"

	"github.com/stretchr/testify/require"
)

func TestTemporalStringKindPreservesConversion(t *testing.T) {
	for _, tc := range []struct {
		input           string
		kind            TemporalStringKind
		conversionError bool
		value           string
	}{
		{"20240228", TemporalStringDate, false, "2024-02-28 00:00:00.000000"},
		{"20240228233000.999999", TemporalStringDatetime, false, "2024-02-28 23:30:00.999999"},
		{"20240228000000", TemporalStringDatetime, false, "2024-02-28 00:00:00.000000"},
		{"2024-02-28T00:00:00", TemporalStringDatetime, false, "2024-02-28 00:00:00.000000"},
		{"2024-02-28 23:59:59.9999999", TemporalStringDatetime, false, "2024-02-29 00:00:00.000000"},
		{"2024-00-28", TemporalStringDate, true, ""},
		{"2024-00-28 00:00:00", TemporalStringDatetime, true, ""},
		{"24-0-28 0:0:0", TemporalStringDatetime, true, ""},
		{"20240028000000", TemporalStringDatetime, true, ""},
		{"0000-00-00", TemporalStringDate, false, "0000-00-00 00:00:00.000000"},
		{"0000-00-00T00:00:00", TemporalStringDatetime, false, "0000-00-00 00:00:00.000000"},
		{"00000000000000", TemporalStringDatetime, false, "0000-00-00 00:00:00.000000"},
		{"2024-02-28 25:00:00", TemporalStringUnknown, true, ""},
		{"2024-00-28 25:00:00", TemporalStringUnknown, true, ""},
		{"2024-02-30 00:00:00", TemporalStringUnknown, true, ""},
		{"2024-02-30", TemporalStringUnknown, true, ""},
		{"9999-12-31 23:59:59.9999999", TemporalStringUnknown, true, ""},
		{"not-a-date", TemporalStringUnknown, true, ""},
		{"2024-02-28 00:00:00.", TemporalStringUnknown, true, ""},
		{"2024-02-28T00:00:00.x", TemporalStringUnknown, true, ""},
		{"20240228000000.x", TemporalStringUnknown, true, ""},
		{"20240228000000.", TemporalStringUnknown, true, ""},
		{"24-2-28 0:0:0.", TemporalStringUnknown, true, ""},
		{"24-0-28 0:0:0.x", TemporalStringUnknown, true, ""},
	} {
		t.Run(tc.input, func(t *testing.T) {
			var reports atomic.Int32
			kind := func() TemporalStringKind {
				previous := errutil.GetReportErrorFunc()
				errutil.SetErrorReporter(func(context.Context, error, int) { reports.Add(1) })
				defer errutil.SetErrorReporter(previous)
				return ClassifyTemporalString(tc.input)
			}()
			require.Zero(t, reports.Load(), "classification must not report discarded parser errors")
			require.Equal(t, tc.kind, kind)
			value, err := ParseDatetime(tc.input, 6)
			if tc.conversionError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.value, value.String2(6))
			}
		})
	}
}

// Ordinary conversion retains both its nested date-parser report and the final
// datetime error; observation must not change the conversion diagnostic owner.
func TestTemporalConversionReportsErrors(t *testing.T) {
	var reports atomic.Int32
	previous := errutil.GetReportErrorFunc()
	errutil.SetErrorReporter(func(context.Context, error, int) { reports.Add(1) })
	defer errutil.SetErrorReporter(previous)

	_, err := ParseDatetime("not-a-date", 6)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.EqualError(t, err, "invalid input: invalid datetime value not-a-date")
	require.Equal(t, int32(2), reports.Load())
}
