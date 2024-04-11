// Copyright 2024 Matrix Origin
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

package frontend

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"go.uber.org/zap"
)

type statementStatus int

const (
	success statementStatus = iota
	fail
	sessionId = "session_id"

	txnId       = "txn_id"
	statementId = "statement_id"
)

func (s statementStatus) String() string {
	switch s {
	case success:
		return "success"
	case fail:
		return "fail"
	}
	return "running"
}

// logStatementStatus prints the status of the statement into the log.
func logStatementStatus(ctx context.Context, ses FeSession, stmt tree.Statement, status statementStatus, err error) {
	var stmtStr string
	stm := motrace.StatementFromContext(ctx)
	if stm == nil {
		fmtCtx := tree.NewFmtCtx(dialect.MYSQL)
		stmt.Format(fmtCtx)
		stmtStr = fmtCtx.String()
	} else {
		stmtStr = stm.Statement
	}
	logStatementStringStatus(ctx, ses, stmtStr, status, err)
}

func logStatementStringStatus(ctx context.Context, ses FeSession, stmtStr string, status statementStatus, err error) {
	str := SubStringFromBegin(stmtStr, int(gPu.SV.LengthOfQueryPrinted))
	outBytes, outPacket := ses.GetMysqlProtocol().CalculateOutTrafficBytes(true)
	if status == success {
		logDebug(ses, ses.GetDebugString(), "query trace status", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.StatementField(str), logutil.StatusField(status.String()), trace.ContextField(ctx))
		err = nil // make sure: it is nil for EndStatement
	} else {
		logError(ses, ses.GetDebugString(), "query trace status", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.StatementField(str), logutil.StatusField(status.String()), logutil.ErrorField(err), trace.ContextField(ctx))
	}

	// pls make sure: NO ONE use the ses.tStmt after EndStatement
	if !ses.IsBackgroundSession() {
		motrace.EndStatement(ctx, err, ses.SendRows(), outBytes, outPacket)
	}

	// need just below EndStatement
	ses.SetTStmt(nil)
}

var logger *log.MOLogger
var loggerOnce sync.Once

func getLogger() *log.MOLogger {
	loggerOnce.Do(initLogger)
	return logger
}

func initLogger() {
	rt := moruntime.ProcessLevelRuntime()
	if rt == nil {
		rt = moruntime.DefaultRuntime()
	}
	logger = rt.Logger().Named("frontend")
}

func appendSessionField(fields []zap.Field, ses FeSession) []zap.Field {
	if ses != nil {
		if ses.GetStmtInfo() != nil {
			fields = append(fields, zap.String(sessionId, uuid.UUID(ses.GetStmtInfo().SessionID).String()))
			fields = append(fields, zap.String(statementId, uuid.UUID(ses.GetStmtInfo().StatementID).String()))
			txnInfo := ses.GetTxnInfo()
			if txnInfo != "" {
				fields = append(fields, zap.String(txnId, txnInfo))
			}
		} else {
			fields = append(fields, zap.String(sessionId, uuid.UUID(ses.GetUUID()).String()))
		}
	}
	return fields
}

func logInfo(ses FeSession, info string, msg string, fields ...zap.Field) {
	if ses != nil && ses.GetTenantInfo() != nil && ses.GetTenantInfo().User == db_holder.MOLoggerUser {
		return
	}
	fields = append(fields, zap.String("session_info", info))
	fields = appendSessionField(fields, ses)
	getLogger().Log(msg, log.DefaultLogOptions().WithLevel(zap.InfoLevel).AddCallerSkip(1), fields...)
}

func logInfof(info string, msg string, fields ...zap.Field) {
	if logutil.GetSkip1Logger().Core().Enabled(zap.InfoLevel) {
		fields = append(fields, zap.String("session_info", info))
		getLogger().Log(msg, log.DefaultLogOptions().WithLevel(zap.InfoLevel).AddCallerSkip(1), fields...)
	}
}

func logDebug(ses FeSession, info string, msg string, fields ...zap.Field) {
	if ses != nil && ses.GetTenantInfo() != nil && ses.GetTenantInfo().User == db_holder.MOLoggerUser {
		return
	}
	fields = append(fields, zap.String("session_info", info))
	fields = appendSessionField(fields, ses)
	getLogger().Log(msg, log.DefaultLogOptions().WithLevel(zap.DebugLevel).AddCallerSkip(1), fields...)
}

func logError(ses FeSession, info string, msg string, fields ...zap.Field) {
	if ses != nil && ses.GetTenantInfo() != nil && ses.GetTenantInfo().User == db_holder.MOLoggerUser {
		return
	}
	fields = append(fields, zap.String("session_info", info))
	fields = appendSessionField(fields, ses)
	getLogger().Log(msg, log.DefaultLogOptions().WithLevel(zap.ErrorLevel).AddCallerSkip(1), fields...)
}

// todo: remove this function after all the logDebugf are replaced by logDebug
func logDebugf(info string, msg string, fields ...interface{}) {
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		fields = append(fields, info)
		logutil.Debugf(msg+" %s", fields...)
	}
}
