// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"go.uber.org/zap"
)

var (
	legacyConnectionCloseEvents = ConnectionCloseEvents{
		Expected: Event{Name: "connection.close.expected", Message: "connection closed during normal lifecycle"},
		Failed:   Event{Name: "connection.close.failed", Message: "connection close or I/O failed"},
	}
)

// ConnectionCloseEvents gives one stable event population to each terminal
// connection operation and outcome. Callers define it once at package scope.
type ConnectionCloseEvents struct {
	Expected Event
	Failed   Event
}

func ConnectionIdField(val uint32) zap.Field { return zap.Uint32("connection_id", val) }
func QueryField(val string) zap.Field        { return zap.String("query", val) }
func StatementField(val string) zap.Field    { return zap.String("statement", val) }
func VarsField(val string) zap.Field         { return zap.String("vars", val) }
func StatusField(val string) zap.Field       { return zap.String("status", val) }
func TableField(val string) zap.Field        { return zap.String("table", val) } // table name
func PathField(val string) zap.Field         { return zap.String("path", val) }

func SessionIdField(val string) zap.Field   { return zap.String("session_id", val) }
func TxnIdField(val string) zap.Field       { return zap.String("txn_id", val) }
func TxnInfoField(val string) zap.Field     { return zap.String("txn_info", val) }
func StatementIdField(val string) zap.Field { return zap.String("statement_id", val) }

func NoReportFiled() zap.Field { return zap.Bool(MOInternalFiledKeyNoopReport, true) }
func Discardable() zap.Field   { return zap.Bool(MOInternalFiledKeyDiscardable, true) }

func ErrorField(err error) zap.Field {
	if isDisallowedError(err) {
		panic(fmt.Sprintf("this error should not be logged: %v", err))
	}
	return zap.Error(err)
}

func isDisallowedError(err error) bool {
	switch {
	case errors.Is(err, io.EOF):
		// io.EOF should be handled by the caller, should never be logged
		return true
	}
	return false
}

// IsExpectedConnectionCloseError checks if the error is an expected error when closing connections.
// These errors occur during normal connection lifecycle and should be logged at DEBUG level.
// Currently only "use of closed network connection" is considered as expected.
func IsExpectedConnectionCloseError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "use of closed network connection")
}

// LogConnectionCloseEvent logs a connection operation at an appropriate level.
// Expected errors (like "use of closed network connection") use Expected at
// DEBUG, while unexpected errors use Failed at ERROR.
func LogConnectionCloseEvent(events ConnectionCloseEvents, err error, fields ...zap.Field) {
	build := func() []zap.Field {
		out := append([]zap.Field(nil), fields...)
		return append(out, ErrorFingerprintFields("error", err)...)
	}
	if IsExpectedConnectionCloseError(err) {
		events.Expected.DebugLazy(build)
	} else {
		events.Failed.ErrorLazy(build)
	}
}

// LogConnectionCloseError is kept for compatibility with callers that have
// not yet supplied a stable operation-specific Event pair.
//
// Deprecated: use LogConnectionCloseEvent with package-level ConnectionCloseEvents.
func LogConnectionCloseError(msg string, err error, fields ...zap.Field) {
	out := append([]zap.Field(nil), fields...)
	out = append(out, StringFingerprintFields("operation", msg)...)
	LogConnectionCloseEvent(legacyConnectionCloseEvents, err, out...)
}
