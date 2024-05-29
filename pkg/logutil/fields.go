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

	"go.uber.org/zap"
)

func ConnectionIdField(val uint32) zap.Field { return zap.Uint32("connection_id", val) }
func QueryField(val string) zap.Field        { return zap.String("query", val) }
func StatementField(val string) zap.Field    { return zap.String("statement", val) }
func VarsField(val string) zap.Field         { return zap.String("vars", val) }
func StatusField(val string) zap.Field       { return zap.String("status", val) }
func TableField(val string) zap.Field        { return zap.String("table", val) } // table name
func PathField(val string) zap.Field         { return zap.String("path", val) }

func SessionIdField(val string) zap.Field   { return zap.String("session_id", val) }
func TxnIdField(val string) zap.Field       { return zap.String("txn_id", val) }
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
