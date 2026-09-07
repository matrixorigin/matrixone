// Copyright 2021 Matrix Origin
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	pbstats "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	MaxPrepareNumberInOneSession atomic.Uint32
)

func currentProtocolVersion(proc *process.Process) int64 {
	if proc == nil {
		return defines.MORPCLatestVersion
	}
	value, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return defines.MORPCVersion4
	}
	version, ok := value.(int64)
	if !ok {
		return defines.MORPCVersion4
	}
	return version
}

func logtailReadBarrierSupported(ses *Session) bool {
	rt := moruntime.ServiceRuntime(ses.GetService())
	if rt == nil {
		return false
	}
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	version, ok := value.(int64)
	return ok && version >= defines.MORPCVersion39
}

func (ses *Session) acquireLogtailReadBarrier(
	ctx context.Context,
) (timestamp.Timestamp, error) {
	pu := getPuIfPresent(ses.GetService())
	if pu == nil {
		return timestamp.Timestamp{}, moerr.NewInternalError(
			ctx, "missing parameter unit for logtail read barrier")
	}
	if pu.StorageEngine == nil {
		return timestamp.Timestamp{}, moerr.NewInternalError(
			ctx, "missing storage engine for logtail read barrier")
	}
	barrier, ok := pu.StorageEngine.(engine.LogtailReadBarrier)
	if !ok {
		return timestamp.Timestamp{}, moerr.NewInternalError(
			ctx, "storage engine does not support logtail read barrier")
	}
	return barrier.AcquireLogtailReadBarrier(ctx)
}

// reusablePlanGenerationSupported reports whether every live service in the
// rollout understands the logical-plan generation snapshot carried by remote
// pipeline and lock requests. Deployment keeps MOProtocolVersion at the oldest
// live service, so cross-transaction plan reuse must remain disabled until the
// version 32 wire contract is active cluster-wide.
func reusablePlanGenerationSupported(proc *process.Process) bool {
	return currentProtocolVersion(proc) >= defines.MORPCVersion32
}

func init() {
	MaxPrepareNumberInOneSession.Store(100000)
}

// TODO: this variable should be configure by set variable
const MoDefaultErrorCount = 64

type ShowStatementType int

const (
	NotShowStatement ShowStatementType = 0
	ShowTableStatus  ShowStatementType = 1
)

type ConnType int

const (
	ConnTypeUnset    ConnType = 0
	ConnTypeInternal ConnType = 1
	ConnTypeExternal ConnType = 2
)

type TS string

const (
	// Created
	TSCreatedStart TS = "TSCreatedStart"
	TSCreatedEnd   TS = "TSCreatedEnd"

	// Handler
	TSEstablishStart  TS = "TSEstablishStart"
	TSEstablishEnd    TS = "TSEstablishEnd"
	TSUpgradeTLSStart TS = "TSUpgradeTLSStart"
	TSUpgradeTLSEnd   TS = "TSUpgradeTLSEnd"

	// mysql protocol
	TSAuthenticateStart  TS = "TSAuthenticateStart"
	TSAuthenticateEnd    TS = "TSAuthenticateEnd"
	TSSendErrPacketStart TS = "TSSendErrPacketStart"
	TSSendErrPacketEnd   TS = "TSSendErrPacketEnd"
	TSSendOKPacketStart  TS = "TSSendOKPacketStart"
	TSSendOKPacketEnd    TS = "TSSendOKPacketEnd"

	// session
	TSCheckTenantStart      TS = "TSCheckTenantStart"
	TSCheckTenantEnd        TS = "TSCheckTenantEnd"
	TSCheckUserStart        TS = "TSCheckUserStart"
	TSCheckUserEnd          TS = "TSCheckUserEnd"
	TSCheckRoleStart        TS = "TSCheckRoleStart"
	TSCheckRoleEnd          TS = "TSCheckRoleEnd"
	TSCheckDbNameStart      TS = "TSCheckDbNameStart"
	TSCheckDbNameEnd        TS = "TSCheckDbNameEnd"
	TSInitGlobalSysVarStart TS = "TSInitGlobalSysVarStart"
	TSInitGlobalSysVarEnd   TS = "TSInitGlobalSysVarEnd"
)

type Session struct {
	feSessionImpl

	logger     *log.MOLogger
	logLevel   zapcore.Level
	loggerOnce sync.Once

	//cmd from the client
	cmd CommandType

	// the process of the session
	proc *process.Process

	isInternal bool

	data            [][]interface{}
	ep              *ExportConfig
	showStmtType    ShowStatementType
	userDefinedVars map[string]*UserDefinedVar
	// migrationSystemVarReplayable records whether the latest assignment for a
	// session system variable is known to be captured by the proxy's raw SET
	// replay stream. A prepared assignment is not visible to that stream.
	migrationSystemVarReplayable map[string]bool
	// tempTables records the relationship between the temporary table created by the session and the actual table.
	// Key: dbName.alias, Value: realName
	tempTables map[string]string
	// tempTablesRev records the reverse relationship.
	// Key: realName, Value: dbName.alias
	tempTablesRev map[string]string
	// tempTableIdentities preserves the database and alias as separate values.
	// The legacy tempTables key is intentionally kept for lookup compatibility,
	// but it cannot be split safely when quoted identifiers contain dots. Index
	// table aliases are marked internal so connection migration clones only the
	// user-visible table; cloning that table recreates its hidden index tables.
	tempTableIdentities map[string]tempTableIdentity
	// tempTableVersion changes whenever the session's temporary-table name
	// resolution changes. Prepared statements use it to invalidate plans that
	// were built against an older temporary-table mapping.
	tempTableVersion uint64
	// tempTableTxnJournals mirrors transaction and statement rollback for the
	// session-local alias map. Physical temporary relations live in the engine
	// workspace, so publishing an alias without journaling it would let a later
	// rollback discard the relation while retaining an unusable session name.
	// The outer key is the engine transaction ID. Entries are allocated lazily,
	// only when a transaction actually changes a temporary-table alias.
	tempTableTxnJournals map[string]*tempTableTxnJournal
	// ddlVersion changes after every successful session DDL. It covers
	// transaction-local catalog writes that are not visible in CatalogCache.
	ddlVersion      atomic.Uint64
	hasLockedTables atomic.Bool

	prepareStmts map[string]*PrepareStmt
	lastStmtId   uint32

	// preparedCursorBytes accounts for rows retained by all active server-side
	// cursors in this session. Cursor results live on the prepared statement,
	// so a session-level budget is required in addition to a per-cursor bound.
	preparedCursorBytes atomic.Uint64
	preparedCursorLimit atomic.Uint64

	priv *privilege

	ddlOwnerRoleID uint32

	errInfo *errInfo

	cache       *privilegeCache
	ruleCache   map[string]string // rewrite rule cache, nil means not loaded
	ruleCacheMu sync.RWMutex      // protects ruleCache

	// foreignConns caches connections to foreign data sources (Elasticsearch,
	// external SQL databases) opened by esql_tvf_connect / sql_tvf_connect and
	// consumed by esql_tvf / sql_tvf. It is session-scoped: every connection is
	// closed when the session ends (see closeForeignConns in Close). See
	// session_foreignconn.go for the process.ForeignConnCache implementation.
	foreignConnMu sync.Mutex
	foreignConns  map[string]process.ForeignConn // handle -> connection
	// foreignConnsClosed is the terminal tombstone set by closeForeignConns:
	// a connector racing with session close (KILL CONNECTION during a slow
	// connect handshake) must have its late connection rejected and closed,
	// not silently re-admitted into a cache nobody will ever clean up again.
	foreignConnsClosed bool

	// lastKafkaMessageID is the offset of the last message a completed Kafka
	// external-table scan returned in this session; read back by
	// LAST_KAFKA_MESSAGE_ID(). See session_kafka.go.
	lastKafkaMessageMu  sync.Mutex
	lastKafkaMessageID  int64
	lastKafkaMessageSet bool
	// kafkaProgressQueue holds drained Kafka scans' deferred progress
	// finalizers until the statement terminal (see session_kafka.go).
	kafkaProgressQueue []func(publish bool)

	// rewriteEnabled caches the enable_remap_hint system variable state
	// to avoid expensive GetSessionSysVar calls on every SQL query
	rewriteEnabled atomic.Bool

	mu sync.Mutex

	lastInsertID uint64

	// lastAffectedRows records the rows affected by the previous statement,
	// consumed by the ROW_COUNT() builtin. MySQL semantics: -1 after a
	// result-set statement (SELECT/SHOW...), 0 after DDL, affected rows after DML.
	lastAffectedRows int64

	// lastFoundRows records the result count exposed by FOUND_ROWS() for the
	// previous result-set statement.
	lastFoundRows uint64

	// tStmt is used only to record the StatementInfo
	// QueryResult please use feSessionImpl.stmtProfile instead.
	tStmt *motrace.StatementInfo
	// responseAccounting keeps failed statement completion open until the
	// terminal protocol response has actually been written. It is owned by the
	// routine goroutine and deliberately does not participate in session locks.
	responseAccounting     bool
	pendingStatementFailed bool
	pendingStatementError  error
	responseOutputWait     *responseOutputWaitTracker

	ast tree.Statement

	queryId []string

	blockIdx int

	p *plan.Plan

	limitResultSize float64 // MB

	curResultSize float64 // MB

	savedRowCount uint64 //count of rows saved in the query result
	queryRowCount uint64 //count of rows generated by the query

	// sentRows used to record rows it sent to client for motrace.StatementInfo.
	// If there is NO exec_plan, sentRows will be 0.
	sentRows atomic.Int64
	// writeBytes count of bytes send back to client.
	writeBytes int
	// packetCounter count the tcp packet send to client.
	packetCounter atomic.Int64
	// payloadCounter count the payload send by `load data LOCAL infile`
	payloadCounter int64

	// sqlModeNoAutoValueOnZero caches whether sql_mode contains NO_AUTO_VALUE_ON_ZERO
	// -1: unknown, 0: false, 1: true
	sqlModeNoAutoValueOnZero int32

	createdTime time.Time

	expiredTime time.Time

	planCache *planCache

	statsCacheMu       sync.Mutex
	statsCache         *plan2.StatsCache
	statsCacheVersions map[uint64]optimizerStatsCacheTag
	seqCurValues       map[uint64]string

	/*
		CORNER CASE:

		create sequence seq1;
		set @@a = (select nextval(seq1)); // a = 1
		select currval('seq1');// 1
		select lastval('seq1');// right value is 1

		We execute the expr of 'set var = expr' in a background session,
		the last value of the seq1 is saved in the background session.

		If we want to get the right value the lastval('seq1'), we need save
		the last value of the seq1 in the session that starts the background session.

		So, we define the type of seqLastValue as *string for updating its value conveniently.

		TODO: we need to reimplement the sequence in some extent traced by issue #9847.
	*/
	seqLastValue *string

	sqlHelper *SqlHelper

	rm *RoutineManager

	rt *Routine

	// requestLabel is the CN label info requested from client.
	requestLabel map[string]string
	// connTyp indicates the type of connection. Default is ConnTypeUnset.
	// If it is internal connection, the value will be ConnTypeInternal, otherwise,
	// the value will be ConnTypeExternal.
	connType ConnType

	// startedAt is the session start time.
	startedAt time.Time

	// queryEnd is the time when the query ends
	queryEnd time.Time
	// queryInProgress indicates whether the query is in progress
	queryInProgress atomic.Bool
	// queryInExecute indicates whether the query is in execute
	queryInExecute atomic.Bool

	// timestampMap record timestamp for statistical purposes
	timestampMap map[TS]time.Time

	// FromProxy denotes whether the session is dispatched from proxy
	fromProxy bool
	// If the connection is from proxy, client address is the real address of client.
	clientAddr string
	proxyAddr  string

	disableTrace bool

	// disableAgg co-operate with RecordStatement
	// more can see Benchmark_RecordStatement_IsTrue()
	disableAgg bool

	// mysql parser
	mysqlParser mysql.MySQLParser

	// create version
	createVersion string
}

type tempTableAliasState struct {
	realName string
	identity tempTableIdentity
	exists   bool
}

type tempTableIdentity struct {
	dbName   string
	alias    string
	internal bool
}

// A migration snapshot carries identifiers only, not table data. Keep its
// count bounded nevertheless: every entry becomes a CREATE ... CLONE statement
// on the target and all entries share the fixed connection-transfer deadline.
const maxMigrateTempTableCount = 1024

type tempTableTxnJournal struct {
	before     map[string]tempTableAliasState
	statements map[string]map[string]tempTableAliasState
}

func tempTableTxnKey(txnOp TxnOperator) string {
	if txnOp == nil {
		return ""
	}
	txnID := txnOp.Txn().ID
	if len(txnID) == 0 {
		return ""
	}
	return string(txnID)
}

func tempTableStatementKey(ses FeSession, sharedTxn bool) string {
	if sharedTxn {
		if owner := upstreamUserSession(ses); owner != nil {
			stmtID := owner.GetStmtId()
			return string(stmtID[:])
		}
	}
	stmtID := ses.GetStmtId()
	return string(stmtID[:])
}

func tempTableMutationKeys(ses FeSession) (string, string) {
	if ses == nil || ses.GetTxnHandler() == nil {
		return "", ""
	}
	txnHandler := ses.GetTxnHandler()
	txnKey := tempTableTxnKey(txnHandler.GetTxn())
	if txnKey == "" {
		return "", ""
	}
	return txnKey, tempTableStatementKey(ses, txnHandler.IsShareTxn())
}

func (ses *Session) GetMySQLParser() *mysql.MySQLParser {
	return &ses.mysqlParser
}

func (ses *Session) InitSystemVariables(ctx context.Context, bh BackgroundExec) (err error) {
	var sv *SystemVariables
	if sv, err = GSysVarsMgr.Get(ses.GetTenantInfo().TenantID, ses, ctx, bh); err != nil {
		return
	}
	return ses.initSystemVariablesFromGlobal(ctx, sv)
}

func (ses *Session) initSystemVariablesFromGlobal(ctx context.Context, sv *SystemVariables) (err error) {
	if sv == nil {
		return moerr.NewInternalError(ctx, "global system variables are not initialized")
	}
	sessionVars := sv.Clone()
	// A fresh session generation must initialize runtime state as well as the
	// values visible through @@session. time_zone is the only registered
	// variable whose setter owns additional Session state.
	if value := sessionVars.Get("time_zone"); value != nil {
		if _, ok := value.(string); !ok {
			return moerr.NewInternalErrorf(ctx, "invalid time_zone value %T", value)
		}
		if err = updateTimeZone(ctx, ses, sessionVars, "time_zone", value); err != nil {
			return err
		}
	}
	transactionIsolationValue := sessionVars.Get(transactionIsolationSystemVariable)
	if transactionIsolationValue == nil {
		transactionIsolationValue = gSysVarsDefs[transactionIsolationSystemVariable].Default
	}
	normalizedTransactionIsolation, transactionIsolation, err :=
		normalizeTxnIsolationSystemValue(ctx, ses.service, transactionIsolationValue)
	if err != nil {
		return err
	}
	sessionVars.Set(transactionIsolationSystemVariable, normalizedTransactionIsolation)

	ses.mu.Lock()
	ses.gSysVars = sv
	ses.sesSysVars = sessionVars
	txnHandler := ses.txnHandler
	ses.mu.Unlock()
	atomic.StoreInt32(&ses.sqlModeNoAutoValueOnZero, -1)

	// Initialize rewriteEnabled cache
	if v := sessionVars.Get("enable_remap_hint"); v != nil {
		if on, convErr := valueIsBoolTrue(v); convErr == nil {
			ses.rewriteEnabled.Store(on)
		}
	}
	if txnHandler != nil {
		txnHandler.setSessionTxnIsolation(transactionIsolation)
	}
	return
}

func (ses *Session) GetTxnHandler() *TxnHandler {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.txnHandler
}

func (ses *Session) GetTenantInfo() *TenantInfo {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.tenant
}

func (ses *Session) SendRows() int64 {
	return ses.sentRows.Load()
}

func (ses *Session) GetStmtInfo() *motrace.StatementInfo {
	return ses.tStmt
}

func (ses *Session) getNextProcessId() string {
	/*
		temporary method:
		routineId + sqlCount
	*/
	routineId := ses.GetResponser().GetU32(CONNID)
	// Optimize: use strconv instead of fmt.Sprintf
	var buf [24]byte
	b := strconv.AppendUint(buf[:0], uint64(routineId), 10)
	b = strconv.AppendUint(b, ses.GetSqlCount(), 10)
	return string(b)
}

// SetUserDefinedVar sets the user defined variable to the value in session
func (ses *Session) SetUserDefinedVar(name string, value interface{}, sql string) error {
	return ses.setUserDefinedVar(name, value, sql, false)
}

func (ses *Session) setUserDefinedVar(name string, value interface{}, sql string, isBin bool) error {
	return ses.setUserDefinedVarWithTypeAndKind(
		name, value, sql, isBin, inferUserDefinedVarType(value), prepareParamKindFromValue(value))
}

func (ses *Session) setUserDefinedVarWithType(name string, value interface{}, sql string, isBin bool, typ plan.Type) error {
	return ses.setUserDefinedVarWithTypeAndKind(
		name, value, sql, isBin, typ, prepareParamKindFromType(types.T(typ.Id)))
}

func (ses *Session) setUserDefinedVarWithKind(
	name string,
	value interface{},
	sql string,
	isBin bool,
	kind vector.PrepareParamKind,
) error {
	return ses.setUserDefinedVarWithTypeAndKind(
		name, value, sql, isBin, inferUserDefinedVarType(value), kind)
}

func (ses *Session) setUserDefinedVarWithKindAndReplayability(
	name string,
	value interface{},
	sql string,
	isBin bool,
	kind vector.PrepareParamKind,
	replayable bool,
) error {
	return ses.setUserDefinedVarWithTypeAndKindAndReplayability(
		name, value, sql, isBin, inferUserDefinedVarType(value), kind, replayable)
}

func (ses *Session) setUserDefinedVarWithTypeAndKind(
	name string,
	value interface{},
	sql string,
	isBin bool,
	typ plan.Type,
	kind vector.PrepareParamKind,
) error {
	return ses.setUserDefinedVarWithTypeAndKindAndReplayability(
		name, value, sql, isBin, typ, kind, false)
}

func (ses *Session) setUserDefinedVarWithTypeAndKindAndReplayability(
	name string,
	value interface{},
	sql string,
	isBin bool,
	typ plan.Type,
	kind vector.PrepareParamKind,
	replayable bool,
) error {
	if typ.Id == 0 {
		typ = inferUserDefinedVarType(value)
	}
	ses.mu.Lock()
	key := strings.ToLower(name)
	if previous := ses.userDefinedVars[key]; previous != nil && !previous.Replayable {
		replayable = false
	}
	ses.userDefinedVars[key] = &UserDefinedVar{
		Value:            value,
		Sql:              sql,
		IsBin:            isBin,
		Type:             typ,
		PrepareParamKind: kind,
		Replayable:       replayable,
	}
	ses.mu.Unlock()
	// User-variable references are typed at bind time. A later assignment can
	// change that type, so cached plans containing @vars must be rebound.
	ses.cleanCache()
	return nil
}

func (ses *Session) markMigrationSystemVarReplayable(name string, replayable bool) {
	name = canonicalSystemVariableName(name)
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.migrationSystemVarReplayable == nil {
		ses.migrationSystemVarReplayable = make(map[string]bool)
	}
	ses.migrationSystemVarReplayable[name] = replayable
}

func (ses *Session) getMigrationSystemVarReplayability(name string) (bool, bool) {
	name = canonicalSystemVariableName(name)
	ses.mu.Lock()
	defer ses.mu.Unlock()
	replayable, tracked := ses.migrationSystemVarReplayable[name]
	return replayable, tracked
}

func (ses *Session) restoreMigrationSystemVarReplayability(
	name string, replayable, tracked bool,
) {
	name = canonicalSystemVariableName(name)
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if !tracked {
		delete(ses.migrationSystemVarReplayable, name)
		return
	}
	if ses.migrationSystemVarReplayable == nil {
		ses.migrationSystemVarReplayable = make(map[string]bool)
	}
	ses.migrationSystemVarReplayable[name] = replayable
}

func (ses *Session) hasUnreplayableMigrationSystemVars() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	for _, replayable := range ses.migrationSystemVarReplayable {
		if !replayable {
			return true
		}
	}
	return false
}

// GetUserDefinedVar gets value of the user defined variable
func (ses *Session) GetUserDefinedVar(name string) (*UserDefinedVar, error) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	val, ok := ses.userDefinedVars[strings.ToLower(name)]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtxf(errorUserVariableDoesNotExist(), name)
	}
	return val, nil
}

// AddTempTable adds the temporary table to the session
func (ses *Session) AddTempTable(dbName, alias, realName string) {
	txnKey, stmtKey := tempTableMutationKeys(ses)
	ses.addTempTableWithIdentity(dbName, alias, realName, false, txnKey, stmtKey)
}

func (ses *Session) addTempTable(dbName, alias, realName, txnKey, stmtKey string) {
	ses.addTempTableWithIdentity(dbName, alias, realName, false, txnKey, stmtKey)
}

// AddTempIndexTable records a hidden physical index table owned by a temporary
// table. It remains resolvable and participates in session cleanup, but the
// parent table's CLONE recreates it during connection migration.
func (ses *Session) AddTempIndexTable(dbName, alias, realName string) {
	txnKey, stmtKey := tempTableMutationKeys(ses)
	ses.addTempTableWithIdentity(dbName, alias, realName, true, txnKey, stmtKey)
}

func (ses *Session) addTempIndexTable(dbName, alias, realName, txnKey, stmtKey string) {
	ses.addTempTableWithIdentity(dbName, alias, realName, true, txnKey, stmtKey)
}

func (ses *Session) addTempTableWithIdentity(
	dbName, alias, realName string,
	internal bool,
	txnKey, stmtKey string,
) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	key := dbName + "." + alias
	if ses.tempTableIdentities == nil {
		ses.tempTableIdentities = make(map[string]tempTableIdentity)
	}
	identity := tempTableIdentity{
		dbName: dbName, alias: alias, internal: internal,
	}
	if oldRealName, ok := ses.tempTables[key]; ok {
		if oldRealName == realName {
			if ses.tempTableIdentityLocked(key) != identity {
				ses.recordTempTableMutationLocked(txnKey, stmtKey, key)
				ses.tempTableIdentities[key] = identity
			}
			return
		}
		ses.recordTempTableMutationLocked(txnKey, stmtKey, key)
		delete(ses.tempTablesRev, oldRealName)
	} else {
		ses.recordTempTableMutationLocked(txnKey, stmtKey, key)
	}
	ses.tempTables[key] = realName
	ses.tempTablesRev[realName] = key
	ses.tempTableIdentities[key] = identity
	ses.tempTableVersion++
}

func (ses *Session) tempTableIdentityLocked(key string) tempTableIdentity {
	if identity, ok := ses.tempTableIdentities[key]; ok {
		return identity
	}
	dbName, alias, ok := strings.Cut(key, ".")
	if !ok {
		return tempTableIdentity{alias: key}
	}
	return tempTableIdentity{dbName: dbName, alias: alias}
}

func (ses *Session) snapshotTempTables() []*query.MigrateTempTable {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	keys := make([]string, 0, len(ses.tempTables))
	for key := range ses.tempTables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]*query.MigrateTempTable, 0, len(keys))
	for _, key := range keys {
		identity := ses.tempTableIdentityLocked(key)
		if identity.internal {
			continue
		}
		result = append(result, &query.MigrateTempTable{
			Database:     identity.dbName,
			Alias:        identity.alias,
			PhysicalName: ses.tempTables[key],
		})
	}
	return result
}

// snapshotTempTablesForMigration applies the same wire-size limit used by the
// typed variable snapshots. The source must reject an oversized snapshot before
// proxy starts a handoff: the old session remains authoritative and no target
// clone can be left behind by a transfer that cannot complete in one attempt.
func (ses *Session) snapshotTempTablesForMigration(ctx context.Context) ([]*query.MigrateTempTable, error) {
	result := ses.snapshotTempTables()
	if len(result) > maxMigrateTempTableCount {
		return nil, moerr.NewInternalErrorf(ctx,
			"temporary tables exceed the connection migration size limit (table limit %d)",
			maxMigrateTempTableCount)
	}
	if (&query.MigrateConnToRequest{TempTables: result}).ProtoSize() > maxMigrateUserDefinedVarsSize {
		return nil, moerr.NewInternalError(ctx,
			"temporary tables exceed the connection migration size limit")
	}
	return result, nil
}

// GetTempTable gets the real name of the temporary table
func (ses *Session) GetTempTable(dbName, alias string) (string, bool) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	val, ok := ses.tempTables[dbName+"."+alias]
	return val, ok
}

// GetTempTableVersion returns the version of the session's temporary-table
// name mapping.
func (ses *Session) GetTempTableVersion() uint64 {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.tempTableVersion
}

func (ses *Session) getDDLVersion() uint64 {
	return ses.ddlVersion.Load()
}

func (ses *Session) advanceDDLVersion() {
	ses.ddlVersion.Add(1)
}

// RemoveTempTable removes the temporary table alias
func (ses *Session) RemoveTempTable(dbName, alias string) {
	txnKey, stmtKey := tempTableMutationKeys(ses)
	ses.removeTempTable(dbName, alias, txnKey, stmtKey)
}

// RemoveTempTablesByDatabase removes every temporary-table alias owned by a
// database that has been dropped. The mutation is journaled so a failed
// statement or rolled-back transaction restores the aliases with their
// original physical identities.
func (ses *Session) RemoveTempTablesByDatabase(dbName string) {
	txnKey, stmtKey := tempTableMutationKeys(ses)
	ses.removeTempTablesByDatabase(dbName, txnKey, stmtKey)
}

func (ses *Session) removeTempTablesByDatabase(dbName, txnKey, stmtKey string) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	changed := false
	for key, realName := range ses.tempTables {
		identity, tracked := ses.tempTableIdentities[key]
		if tracked {
			if identity.dbName != dbName {
				continue
			}
		} else if !strings.HasPrefix(key, dbName+".") {
			continue
		}
		ses.recordTempTableMutationLocked(txnKey, stmtKey, key)
		delete(ses.tempTables, key)
		delete(ses.tempTablesRev, realName)
		delete(ses.tempTableIdentities, key)
		changed = true
	}
	if changed {
		ses.tempTableVersion++
	}
}

func (ses *Session) removeTempTable(dbName, alias, txnKey, stmtKey string) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	key := dbName + "." + alias
	if realName, ok := ses.tempTables[key]; ok {
		ses.recordTempTableMutationLocked(txnKey, stmtKey, key)
		delete(ses.tempTables, key)
		delete(ses.tempTablesRev, realName)
		delete(ses.tempTableIdentities, key)
		ses.tempTableVersion++
	}
}

// RemoveTempTableByRealName removes the temporary table alias by its real name
func (ses *Session) RemoveTempTableByRealName(realName string) {
	txnKey, stmtKey := tempTableMutationKeys(ses)
	ses.removeTempTableByRealName(realName, txnKey, stmtKey)
}

func (ses *Session) removeTempTableByRealName(realName, txnKey, stmtKey string) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if alias, ok := ses.tempTablesRev[realName]; ok {
		ses.recordTempTableMutationLocked(txnKey, stmtKey, alias)
		delete(ses.tempTables, alias)
		delete(ses.tempTablesRev, realName)
		delete(ses.tempTableIdentities, alias)
		ses.tempTableVersion++
	}
}

func (ses *Session) recordTempTableMutationLocked(txnKey, stmtKey, alias string) {
	if txnKey == "" {
		return
	}
	if ses.tempTableTxnJournals == nil {
		ses.tempTableTxnJournals = make(map[string]*tempTableTxnJournal)
	}
	journal := ses.tempTableTxnJournals[txnKey]
	if journal == nil {
		journal = &tempTableTxnJournal{
			before:     make(map[string]tempTableAliasState),
			statements: make(map[string]map[string]tempTableAliasState),
		}
		ses.tempTableTxnJournals[txnKey] = journal
	}
	state := tempTableAliasState{}
	if realName, ok := ses.tempTables[alias]; ok {
		state = tempTableAliasState{
			realName: realName,
			identity: ses.tempTableIdentityLocked(alias),
			exists:   true,
		}
	}
	if _, ok := journal.before[alias]; !ok {
		journal.before[alias] = state
	}
	statement := journal.statements[stmtKey]
	if statement == nil {
		statement = make(map[string]tempTableAliasState)
		journal.statements[stmtKey] = statement
	}
	if _, ok := statement[alias]; !ok {
		statement[alias] = state
	}
}

func (ses *Session) commitTempTableStatement(txnKey, stmtKey string) {
	if txnKey == "" {
		return
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if journal := ses.tempTableTxnJournals[txnKey]; journal != nil {
		delete(journal.statements, stmtKey)
	}
}

func (ses *Session) rollbackTempTableStatement(txnKey, stmtKey string) {
	if txnKey == "" {
		return
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if journal := ses.tempTableTxnJournals[txnKey]; journal != nil {
		ses.restoreTempTableAliasesLocked(journal.statements[stmtKey])
		delete(journal.statements, stmtKey)
	}
}

func (ses *Session) commitTempTableTransaction(txnKey string) {
	if txnKey == "" {
		return
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	delete(ses.tempTableTxnJournals, txnKey)
}

func (ses *Session) rollbackTempTableTransaction(txnKey string) {
	if txnKey == "" {
		return
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if journal := ses.tempTableTxnJournals[txnKey]; journal != nil {
		ses.restoreTempTableAliasesLocked(journal.before)
		delete(ses.tempTableTxnJournals, txnKey)
	}
}

func (ses *Session) restoreTempTableAliasesLocked(before map[string]tempTableAliasState) {
	changed := false
	for alias, state := range before {
		current, exists := ses.tempTables[alias]
		currentIdentity := ses.tempTableIdentityLocked(alias)
		if exists == state.exists && (!exists ||
			(current == state.realName && currentIdentity == state.identity)) {
			continue
		}
		changed = true
		if exists {
			delete(ses.tempTablesRev, current)
		}
		delete(ses.tempTables, alias)
		delete(ses.tempTableIdentities, alias)
		if state.exists {
			if ses.tempTableIdentities == nil {
				ses.tempTableIdentities = make(map[string]tempTableIdentity)
			}
			ses.tempTables[alias] = state.realName
			ses.tempTablesRev[state.realName] = alias
			ses.tempTableIdentities[alias] = state.identity
		}
	}
	if changed {
		ses.tempTableVersion++
	}
}

func (ses *Session) SetPlan(plan *plan.Plan) {
	ses.p = plan
}

func (ses *Session) GetProc() *process.Process {
	return ses.proc
}

func (ses *Session) GetStatsCache() *plan2.StatsCache {
	ses.statsCacheMu.Lock()
	defer ses.statsCacheMu.Unlock()
	return ses.statsCache
}

func (ses *Session) optimizerStatsKey(tableID uint64) optimizerStatsTableKey {
	return optimizerStatsTableKey{
		accountID: ses.GetAccountId(),
		tableID:   tableID,
	}
}

type optimizerStatsCacheTag struct {
	key               optimizerStatsTableKey
	version           uint64
	tableDefVersion   uint32
	tableVersionBound bool
}

func (ses *Session) getStatsCacheWithVersion(key optimizerStatsTableKey) (*plan2.StatsCache, uint64) {
	return ses.getStatsCacheForTableDefVersion(key, nil)
}

func (ses *Session) getStatsCacheForTableDefVersion(
	key optimizerStatsTableKey,
	tableDefVersion *uint32,
) (*plan2.StatsCache, uint64) {
	ses.statsCacheMu.Lock()
	defer ses.statsCacheMu.Unlock()
	ses.initStatsCacheLocked()
	version := currentOptimizerStatsVersion(ses.GetService(), key)
	wrapper := ses.statsCache.Get(key.tableID)
	tag, tagged := ses.statsCacheVersions[key.tableID]
	if !wrapper.Exists() {
		delete(ses.statsCacheVersions, key.tableID)
	} else if !tagged && version == 0 {
		// Accept caches created before version tracking only in the initial
		// generation. Once any publication has happened, an untagged entry is
		// conservatively stale.
		ses.statsCacheVersions[key.tableID] = optimizerStatsCacheTag{key: key, version: version}
	} else if tag.key != key || tag.version != version ||
		(tag.tableVersionBound &&
			(tableDefVersion == nil || tag.tableDefVersion != *tableDefVersion)) ||
		(tableDefVersion != nil && !tag.tableVersionBound) {
		ses.statsCache.Delete(key.tableID)
		delete(ses.statsCacheVersions, key.tableID)
	}
	return ses.statsCache, version
}

func (ses *Session) cacheStatsIfCurrent(
	key optimizerStatsTableKey,
	version uint64,
	stats *pbstats.StatsInfo,
) bool {
	return ses.cacheStatsForTableDefVersionIfCurrent(key, version, nil, stats)
}

func (ses *Session) cacheStatsForTableDefVersionIfCurrent(
	key optimizerStatsTableKey,
	version uint64,
	tableDefVersion *uint32,
	stats *pbstats.StatsInfo,
) bool {
	ses.statsCacheMu.Lock()
	defer ses.statsCacheMu.Unlock()
	if currentOptimizerStatsVersion(ses.GetService(), key) != version {
		return false
	}
	ses.initStatsCacheLocked()
	if ses.statsCache.SetAndReportReset(key.tableID, stats) {
		clear(ses.statsCacheVersions)
	}
	tag := optimizerStatsCacheTag{key: key, version: version}
	if tableDefVersion != nil {
		tag.tableDefVersion = *tableDefVersion
		tag.tableVersionBound = true
	}
	ses.statsCacheVersions[key.tableID] = tag
	return true
}

func (ses *Session) cachePublishedStatsForTableDefVersion(
	key optimizerStatsTableKey,
	version uint64,
	tableDefVersion *uint32,
	stats *pbstats.StatsInfo,
) {
	ses.statsCacheMu.Lock()
	defer ses.statsCacheMu.Unlock()
	ses.initStatsCacheLocked()
	if ses.statsCache.SetAndReportReset(key.tableID, stats) {
		clear(ses.statsCacheVersions)
	}
	tag := optimizerStatsCacheTag{key: key, version: version}
	if tableDefVersion != nil {
		tag.tableDefVersion = *tableDefVersion
		tag.tableVersionBound = true
	}
	ses.statsCacheVersions[key.tableID] = tag
}

func (ses *Session) initStatsCacheLocked() {
	if ses.statsCache == nil {
		ses.statsCache = plan2.NewStatsCache()
	}
	if ses.statsCacheVersions == nil {
		ses.statsCacheVersions = make(map[uint64]optimizerStatsCacheTag)
	}
}

func (ses *Session) GetSessionStart() time.Time {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.startedAt
}

func (ses *Session) SetQueryEnd(t time.Time) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.queryEnd = t
}

func (ses *Session) GetQueryEnd() time.Time {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.queryEnd
}

func (ses *Session) SetQueryInProgress(b bool) {
	ses.queryInProgress.Store(b)
}

func (ses *Session) GetQueryInProgress() bool {
	return ses.queryInProgress.Load()
}

func (ses *Session) SetQueryInExecute(b bool) {
	ses.queryInExecute.Store(b)
}

func (ses *Session) GetQueryInExecute() bool {
	return ses.queryInExecute.Load()
}

func (ses *Session) setRoutineManager(rm *RoutineManager) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.rm = rm
}

func (ses *Session) getRoutineManager() *RoutineManager {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.rm
}

func (ses *Session) setRoutine(rt *Routine) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.rt = rt
}

func (ses *Session) getRoutine() *Routine {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.rt
}

func (ses *Session) SetSeqLastValue(proc *process.Process) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	*ses.seqLastValue = proc.GetSessionInfo().SeqLastValue[0]
}

func (ses *Session) DeleteSeqValues(proc *process.Process) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	for _, k := range proc.GetSessionInfo().SeqDeleteKeys {
		delete(ses.seqCurValues, k)
	}
}

func (ses *Session) AddSeqValues(proc *process.Process) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	for k, v := range proc.GetSessionInfo().SeqAddValues {
		ses.seqCurValues[k] = v
	}
}

func (ses *Session) GetSeqLastValue() string {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return *ses.seqLastValue
}

func (ses *Session) CopySeqToProc(proc *process.Process) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	for k, v := range ses.seqCurValues {
		proc.GetSessionInfo().SeqCurValues[k] = v
	}
	proc.GetSessionInfo().SeqLastValue[0] = *ses.seqLastValue
}

func (ses *Session) InheritSequenceData(other *Session) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.seqCurValues = other.seqCurValues
	ses.seqLastValue = other.seqLastValue
}

func (ses *Session) GetSqlHelper() *SqlHelper {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.sqlHelper
}

func (ses *Session) CountPayload(length int) {
	if ses == nil {
		return
	}
	ses.payloadCounter += int64(length)
}

// CountFlushPackage records MySQL protocol packets whose bytes were fully
// accepted by the connection writer.
func (ses *Session) CountFlushPackage(delta int64) {
	if ses == nil {
		return
	}
	ses.packetCounter.Add(delta)
}

func (ses *Session) GetFlushPacketCnt() int64 {
	if ses == nil {
		return 0
	}
	return ses.packetCounter.Load()
}

func (ses *Session) ResetPacketCounter() {
	if ses == nil {
		return
	}
	ses.packetCounter.Store(0)
	ses.payloadCounter = 0
	ses.writeBytes = 0
}
func (ses *Session) CountOutputBytes(delta int) {
	if ses == nil {
		return
	}
	ses.writeBytes += delta
}
func (ses *Session) GetOutputBytes() int {
	return ses.writeBytes
}

// SetTStmt do set the Session.tStmt
// 1. init-set at RecordStatement, which means the statement is started.
// 2. reset nil, means the statement is finished.
//   - case 1: logStatementStringStatus()
//   - case 2: RecordParseErrorStatement()
func (ses *Session) SetTStmt(stmt *motrace.StatementInfo) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.tStmt = stmt
}

const saveQueryIdCnt = 10

func (ses *Session) pushQueryId(uuid string) {
	if len(ses.queryId) > saveQueryIdCnt {
		ses.queryId = ses.queryId[1:]
	}
	ses.queryId = append(ses.queryId, uuid)
}

func (ses *Session) getQueryId(internalSql bool) []string {
	if internalSql {
		cnt := len(ses.queryId)
		//the last one is cnt-1
		if cnt > 0 {
			return ses.queryId[:cnt-1]
		} else {
			return ses.queryId[:cnt]
		}
	}
	return ses.queryId
}

func (ses *Session) GetSqlModeNoAutoValueOnZero() (bool, bool) {
	v := atomic.LoadInt32(&ses.sqlModeNoAutoValueOnZero)
	if v == 1 {
		return true, true
	}
	if v == 0 {
		return false, true
	}
	if ses.sesSysVars == nil {
		return false, false
	}
	val, err := ses.GetSessionSysVar("sql_mode")
	if err != nil {
		return false, false
	}
	has, ok := parseNoAutoValueOnZero(val)
	if !ok {
		return false, false
	}
	if has {
		atomic.StoreInt32(&ses.sqlModeNoAutoValueOnZero, 1)
	} else {
		atomic.StoreInt32(&ses.sqlModeNoAutoValueOnZero, 0)
	}
	return has, true
}

func (ses *Session) updateSqlModeNoAutoValueOnZero(val interface{}) {
	has, ok := parseNoAutoValueOnZero(val)
	if !ok {
		atomic.StoreInt32(&ses.sqlModeNoAutoValueOnZero, -1)
		return
	}
	if has {
		atomic.StoreInt32(&ses.sqlModeNoAutoValueOnZero, 1)
	} else {
		atomic.StoreInt32(&ses.sqlModeNoAutoValueOnZero, 0)
	}
}

func (ses *Session) sqlModeHasMatrixOneNative() bool {
	if ses == nil {
		return false
	}
	value, err := ses.GetSessionSysVar("sql_mode")
	if err != nil {
		return false
	}
	has, ok := sqlModeHasMatrixOneNativeValue(value)
	return ok && has
}

func (ses *Session) sqlModeHasOnlyFullGroupBy() bool {
	if ses == nil {
		return false
	}
	value, err := ses.GetSessionSysVar("sql_mode")
	if err != nil {
		return false
	}
	has, ok := sqlModeHasOnlyFullGroupByValue(value)
	return ok && has
}

func (ses *Session) sqlModeHasEnableBoolSumAvg() bool {
	if ses == nil {
		return false
	}
	value, err := ses.GetSessionSysVar("sql_mode")
	if err != nil {
		return false
	}
	has, ok := sqlModeHasEnableBoolSumAvgValue(value)
	return ok && has
}

// updateSqlModeCaches evicts cached plans when a sql_mode token that shapes
// the plan changes membership. Every token the planner reads at bind time
// must be compared here: the cache is keyed by SQL text alone.
func (ses *Session) updateSqlModeCaches(oldNative, oldOnlyFullGroupBy, oldBoolSumAvg bool, val interface{}) {
	ses.updateSqlModeNoAutoValueOnZero(val)
	newNative, ok := sqlModeHasMatrixOneNativeValue(val)
	if !ok {
		return
	}
	newOnlyFullGroupBy, ok := sqlModeHasOnlyFullGroupByValue(val)
	if !ok {
		return
	}
	newBoolSumAvg, ok := sqlModeHasEnableBoolSumAvgValue(val)
	if !ok {
		return
	}
	if oldNative != newNative || oldOnlyFullGroupBy != newOnlyFullGroupBy ||
		oldBoolSumAvg != newBoolSumAvg {
		ses.cleanCache()
	}
}

func parseNoAutoValueOnZero(val interface{}) (bool, bool) {
	mode, ok := val.(string)
	if !ok {
		return false, false
	}
	return strings.Contains(strings.ToUpper(mode), "NO_AUTO_VALUE_ON_ZERO"), true
}

type errInfo struct {
	codes         []uint16
	msgs          []string
	levels        []string
	maxCnt        int
	totalWarnings uint64
}

func (e *errInfo) push(code uint16, msg string) {
	e.pushWithLevel(code, msg, "Error")
}

func (e *errInfo) pushWithLevel(code uint16, msg, level string) {
	if !strings.EqualFold(level, "Error") {
		e.totalWarnings++
	}
	e.pushStored(code, msg, level)
}

func (e *errInfo) pushStored(code uint16, msg, level string) {
	if e.maxCnt > 0 && len(e.codes) >= e.maxCnt {
		e.codes = e.codes[1:]
		e.msgs = e.msgs[1:]
		e.levels = e.levels[1:]
	}
	e.codes = append(e.codes, code)
	e.msgs = append(e.msgs, msg)
	e.levels = append(e.levels, level)
}

func (e *errInfo) appendWarningBatch(total uint64, codes []uint16, msgs []string) {
	e.totalWarnings += total
	for i := 0; i < len(codes) && i < len(msgs); i++ {
		e.pushStored(codes[i], msgs[i], "Warning")
	}
}

func (e *errInfo) reset() {
	e.codes = e.codes[:0]
	e.msgs = e.msgs[:0]
	e.levels = e.levels[:0]
	e.totalWarnings = 0
}

func (e *errInfo) snapshot() errInfo {
	return errInfo{
		codes:         append([]uint16(nil), e.codes...),
		msgs:          append([]string(nil), e.msgs...),
		levels:        append([]string(nil), e.levels...),
		maxCnt:        e.maxCnt,
		totalWarnings: e.totalWarnings,
	}
}

func (e errInfo) length() int {
	return len(e.codes)
}

func (e errInfo) warningCount() uint16 {
	if e.totalWarnings > 0 {
		if e.totalWarnings >= uint64(^uint16(0)) {
			return ^uint16(0)
		}
		return uint16(e.totalWarnings)
	}
	count := 0
	for i := range e.codes {
		level := "Error"
		if i < len(e.levels) && e.levels[i] != "" {
			level = e.levels[i]
		}
		if !strings.EqualFold(level, "Error") {
			count++
			if count >= int(^uint16(0)) {
				return ^uint16(0)
			}
		}
	}
	return uint16(count)
}

func NewSession(
	connCtx context.Context,
	service string,
	proto MysqlRrWr,
	mp *mpool.MPool,
) *Session {
	//if the sharedTxnHandler exists,we use its txnCtx and txnOperator in this session.
	//Currently, we only use the sharedTxnHandler in the background session.
	var txnOp TxnOperator
	var err error
	txnHandler := InitTxnHandler(service, getPu(service).StorageEngine, connCtx, txnOp)
	ses := &Session{
		feSessionImpl: feSessionImpl{
			pool:       mp,
			txnHandler: txnHandler,
			//TODO:fix database name after the catalog is ready
			txnCompileCtx:  InitTxnCompilerContext(proto.GetStr(DBNAME)),
			outputCallback: getDataFromPipeline,
			timeZone:       time.Local,
			respr:          NewMysqlResp(proto),
			service:        service,
		},
		errInfo: &errInfo{
			codes:  make([]uint16, 0, MoDefaultErrorCount),
			msgs:   make([]string, 0, MoDefaultErrorCount),
			maxCnt: MoDefaultErrorCount,
		},
		cache:     &privilegeCache{},
		blockIdx:  0,
		planCache: newPlanCache(100),
		startedAt: time.Now(),
		connType:  ConnTypeUnset,

		timestampMap:       map[TS]time.Time{},
		statsCache:         plan2.NewStatsCache(),
		statsCacheVersions: make(map[uint64]optimizerStatsCacheTag),
	}
	atomic.StoreInt32(&ses.sqlModeNoAutoValueOnZero, -1)

	ses.userDefinedVars = make(map[string]*UserDefinedVar)
	ses.migrationSystemVarReplayable = make(map[string]bool)
	ses.tempTables = make(map[string]string)
	ses.tempTablesRev = make(map[string]string)
	ses.tempTableIdentities = make(map[string]tempTableIdentity)
	ses.prepareStmts = make(map[string]*PrepareStmt)
	// For seq init values.
	ses.seqCurValues = make(map[uint64]string)
	ses.seqLastValue = new(string)

	ses.buf = buffer.New()
	ses.sqlHelper = &SqlHelper{ses: ses}
	u, _ := util.FastUuid()
	ses.uuid = uuid.UUID(u)
	pu := getPu(service)
	if ses.pool == nil {
		// If no mp, we create one for session.  Use GuestMmuLimitation as cap.
		// fixed pool size can be another param, or should be computed from cap,
		// but here, too lazy, just use Mid.
		//
		// XXX MPOOL
		// We don't have a way to close a session, so the only sane way of creating
		// a mpool is to use NoFixed
		ses.pool, err = mpool.NewMPool("pipeline-"+ses.GetUUIDString(), pu.SV.GuestMmuLimitation, mpool.NoFixed)
		if err != nil {
			panic(err)
		}
	}
	ses.proc = process.NewTopProcess(
		context.TODO(),
		ses.pool,
		pu.TxnClient,
		nil,
		pu.FileService,
		pu.LockService,
		pu.QueryClient,
		pu.HAKeeperClient,
		pu.UdfService,
		getAicm(service),
		getPu(ses.GetService()).GetTaskService())

	ses.proc.Base.Lim.Size = pu.SV.ProcessLimitationSize
	ses.proc.Base.Lim.SpillSize = pu.SV.ProcessLimitationSpillSize
	ses.proc.Base.Lim.BatchRows = pu.SV.ProcessLimitationBatchRows
	ses.proc.Base.Lim.MaxMsgSize = pu.SV.MaxMessageSize
	ses.proc.Base.Lim.PartitionRows = pu.SV.ProcessLimitationPartitionRows

	ses.proc.SetStmtProfile(&ses.stmtProfile)
	ses.proc.Session = ses
	setRowCount(ses, ses.proc, -1)
	// ses.proc.SetResolveVariableFunc(ses.txnCompileCtx.ResolveVariable)

	runtime.SetFinalizer(ses, func(ss *Session) {
		ss.Close()
	})
	return ses
}

// ReserveConnAndClose closes the session with the connection is reserved.
func (ses *Session) ReserveConnAndClose() {
	rm := ses.getRoutineManager()
	rm.sessionManager.RemoveSession(ses)
	ses.ReserveConn()
	ses.Close()
}

type sessionTempTable struct {
	aliasKey string
	dbName   string
	realName string
	identity tempTableIdentity
}

func (ses *Session) takeTempTables() ([]sessionTempTable, *TenantInfo) {
	ses.mu.Lock()
	tempTables := make([]sessionTempTable, 0, len(ses.tempTables))
	for key, realName := range ses.tempTables {
		identity := ses.tempTableIdentityLocked(key)
		tempTables = append(tempTables, sessionTempTable{
			aliasKey: key,
			dbName:   identity.dbName,
			realName: realName,
			identity: identity,
		})
	}
	ses.tempTables = nil
	ses.tempTablesRev = nil
	ses.tempTableIdentities = nil
	ses.tempTableTxnJournals = nil
	tenant := ses.tenant
	ses.mu.Unlock()
	if tenant != nil {
		tenant = tenant.Copy()
	}
	return tempTables, tenant
}

func dropSessionTempTables(
	ctx context.Context,
	service string,
	timeZone *time.Location,
	tenant *TenantInfo,
	tempTables []sessionTempTable,
) error {
	if len(tempTables) == 0 {
		return nil
	}
	serviceRuntime := moruntime.ServiceRuntime(service)
	if serviceRuntime == nil {
		return moerr.NewInternalError(ctx, "failed to clean temporary tables: service runtime is not ready")
	}
	v, ok := serviceRuntime.GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return moerr.NewInternalError(ctx, "failed to clean temporary tables: internal SQL executor is not ready")
	}
	exec, ok := v.(executor.SQLExecutor)
	if !ok {
		return moerr.NewInternalError(ctx, "failed to clean temporary tables: invalid internal SQL executor")
	}
	opts := executor.Options{}.WithTimeZone(timeZone)
	if tenant != nil {
		opts = opts.WithAccountID(tenant.GetTenantID()).WithStatementOption(
			executor.StatementOption{}.
				WithAccountID(tenant.GetTenantID()).
				WithUserID(tenant.GetUserID()).
				WithRoleID(tenant.GetDefaultRoleID()),
		)
	}
	var cleanupErr error
	for _, tbl := range tempTables {
		dropSQL := "DROP TABLE IF EXISTS " + sqlquote.QualifiedIdent(tbl.dbName, tbl.realName)
		res, err := exec.Exec(ctx, dropSQL, opts)
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		res.Close()
	}
	return cleanupErr
}

// resetTempTables synchronously removes every physical temporary table before
// a replacement session generation is published. Session.Close may clean up
// asynchronously because a disconnected client has no generation to reuse.
func (ses *Session) resetTempTables(ctx context.Context) error {
	tempTables, tenant := ses.takeTempTables()
	if err := dropSessionTempTables(ctx, ses.GetService(), ses.GetTimeZone(), tenant, tempTables); err != nil {
		// Preserve a retryable owner on failure. DROP IF EXISTS makes entries that
		// were already removed safe to execute again on the next reset attempt.
		ses.mu.Lock()
		ses.tempTables = make(map[string]string, len(tempTables))
		ses.tempTablesRev = make(map[string]string, len(tempTables))
		ses.tempTableIdentities = make(map[string]tempTableIdentity, len(tempTables))
		for _, tbl := range tempTables {
			ses.tempTables[tbl.aliasKey] = tbl.realName
			ses.tempTablesRev[tbl.realName] = tbl.aliasKey
			ses.tempTableIdentities[tbl.aliasKey] = tbl.identity
		}
		ses.mu.Unlock()
		return err
	}
	return nil
}

func (ses *Session) Close() {
	// The disconnect path has no next borrower, so temporary-table cleanup can
	// be asynchronous. Reset uses resetTempTables before it reaches Close.
	tempTables, tenantInfo := ses.takeTempTables()
	if len(tempTables) > 0 {
		service := ses.GetService()
		timeZone := ses.GetTimeZone()
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			_, _ = ExecuteFuncWithRecover(func() error {
				if err := dropSessionTempTables(ctx, service, timeZone, tenantInfo, tempTables); err != nil {
					logutil.Errorf("failed to clean temporary tables: %v", err)
				}
				return nil
			})
		}()
	}

	if ses.proc != nil {
		if ses.userLevelLocksMigrated {
			function.DiscardMigratedUserLevelLocks(ses.proc)
		} else {
			function.ReleaseUserLevelLocksOnSessionClose(ses.proc)
		}
	}

	// Close any esql_tvf / sql_tvf foreign-data connections opened by this
	// session so their sockets and driver pools do not outlive it.
	ses.closeForeignConns()
	// a session closing mid-statement must not advance the kafka chain
	ses.FinalizeKafkaProgress(false)

	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.feSessionImpl.Close()
	ses.feSessionImpl.Clear()
	ses.respr = nil
	ses.mrs = nil
	ses.data = nil
	ses.ep = nil
	if ses.txnHandler != nil {
		ses.txnHandler.Close()
	}
	if ses.txnCompileCtx != nil {
		ses.txnCompileCtx.execCtx = nil
		ses.txnCompileCtx = nil
	}
	ses.sql = ""
	ses.userDefinedVars = nil
	ses.gSysVars = nil
	for _, stmt := range ses.prepareStmts {
		stmt.Close()
	}
	ses.prepareStmts = nil
	ses.preparedCursorBytes.Store(0)
	ses.preparedCursorLimit.Store(0)
	ses.allResultSet = nil
	ses.tenant = nil
	ses.priv = nil
	ses.errInfo = nil
	ses.cache = nil
	ses.debugStr = ""
	ses.tStmt = nil
	ses.ast = nil
	ses.rs = nil
	ses.queryId = nil
	ses.p = nil
	ses.releasePlanCache()
	ses.planCache = nil
	ses.seqCurValues = nil
	ses.seqLastValue = nil
	if ses.sqlHelper != nil {
		ses.sqlHelper.ses = nil
		ses.sqlHelper = nil
	}
	ses.ClearStmtProfile()

	ses.proc.Free()
	ses.proc = nil

	for _, bat := range ses.resultBatches {
		bat.Clean(ses.pool)
	}

	if ses.buf != nil {
		ses.buf.Free()
		ses.buf = nil
	}

	//  The mpool cleanup must be placed at the end,
	// and you must wait for all resources to be cleaned up before you can delete the mpool
	pool := ses.GetMemPool()
	mpool.DeleteMPool(pool)
	ses.SetMemPool(nil)

	ses.timestampMap = nil
	ses.upstream = nil
	ses.rm = nil
	ses.rt = nil
}

func (ses *Session) Clear() {
	ses.feSessionImpl.Clear()
}

func (ses *Session) GetIncBlockIdx() int {
	ses.blockIdx++
	return ses.blockIdx
}

func (ses *Session) ResetBlockIdx() {
	ses.blockIdx = 0
}

func (ses *Session) IsBackgroundSession() bool {
	return false
}

func (ses *Session) cachePlan(sql string, stmts []tree.Statement, plans []*plan.Plan, versions ...int64) {
	ses.cachePlanWithSnapshotsAndStatsVersions(
		sql, stmts, plans, make([]timestamp.Timestamp, len(plans)),
		make([]map[optimizerStatsTableKey]uint64, len(plans)), versions...)
}

func (ses *Session) cachePlanWithStatsVersions(
	sql string,
	stmts []tree.Statement,
	plans []*plan.Plan,
	statsVersions map[optimizerStatsTableKey]uint64,
	versions ...int64,
) {
	ses.cachePlanWithSnapshotsAndStatsVersions(
		sql, stmts, plans, make([]timestamp.Timestamp, len(plans)),
		planStatsVersionsFromAggregate(len(plans), statsVersions), versions...)
}

func (ses *Session) cachePlanWithSnapshots(
	sql string,
	stmts []tree.Statement,
	plans []*plan.Plan,
	planSnapshotTS []timestamp.Timestamp,
	versions ...int64,
) {
	ses.cachePlanWithSnapshotsAndStatsVersions(
		sql, stmts, plans, planSnapshotTS,
		make([]map[optimizerStatsTableKey]uint64, len(plans)), versions...)
}

func (ses *Session) cachePlanWithSnapshotsAndStatsVersions(
	sql string,
	stmts []tree.Statement,
	plans []*plan.Plan,
	planSnapshotTS []timestamp.Timestamp,
	planStatsVersions []map[optimizerStatsTableKey]uint64,
	versions ...int64,
) {
	if len(sql) == 0 {
		return
	}
	statsVersions, versionsConsistent := aggregatePlanStatsVersions(planStatsVersions)
	if !versionsConsistent || !optimizerStatsVersionsCurrent(ses.GetService(), statsVersions) {
		// The plan crossed a statistics publication boundary while compiling.
		// It may execute, but must not enter the cache with stale dependencies.
		freeStmts(stmts)
		return
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.planCache == nil {
		freeStmts(stmts)
		return
	}
	protocolVersion := currentProtocolVersion(ses.proc)
	if len(versions) > 0 {
		protocolVersion = versions[0]
	}
	ses.planCache.cacheWithPlanSnapshotsAndStatsVersions(
		sql, stmts, plans, planSnapshotTS, planStatsVersions, protocolVersion)
}

func (ses *Session) getCachedPlan(sql string) *cachedPlan {
	if len(sql) == 0 {
		return nil
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.planCache == nil {
		return nil
	}
	cached := ses.planCache.get(sql)
	if cached != nil && (cached.protocolVersion != currentProtocolVersion(ses.proc) ||
		!optimizerStatsVersionsCurrent(ses.GetService(), cached.statsVersions)) {
		ses.planCache.remove(sql)
		return nil
	}
	return cached
}

func (ses *Session) isCached(sql string) bool {
	if len(sql) == 0 {
		return false
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.planCache == nil {
		return false
	}
	if !ses.planCache.isCached(sql) {
		return false
	}
	cached := ses.planCache.cachePool[sql].Value.(*cachedPlan)
	if cached.protocolVersion != currentProtocolVersion(ses.proc) ||
		!optimizerStatsVersionsCurrent(ses.GetService(), cached.statsVersions) {
		// isCached is also queried while wrappers still borrow the cached AST at
		// the end of execution. Report staleness without releasing that owner;
		// the next getCachedPlan lookup removes it after all borrowers are gone.
		return false
	}
	return true
}

func (ses *Session) removeCachedPlan(sql string) {
	if len(sql) == 0 {
		return
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.planCache != nil {
		ses.planCache.remove(sql)
	}
}

func (ses *Session) updateCachedPlanGeneration(
	sql string,
	index int,
	expectedPlan *plan.Plan,
	newPlan *plan.Plan,
	planSnapshotTS timestamp.Timestamp,
	statsVersions map[optimizerStatsTableKey]uint64,
) bool {
	if len(sql) == 0 {
		return false
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.planCache == nil {
		return false
	}
	return ses.planCache.updatePlanGeneration(
		sql, index, expectedPlan, newPlan, planSnapshotTS, statsVersions)
}

func (ses *Session) invalidateCachedPlanGeneration(
	sql string,
	index int,
	expectedPlan *plan.Plan,
) {
	if len(sql) == 0 {
		return
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.planCache != nil {
		ses.planCache.invalidatePlanGeneration(sql, index, expectedPlan)
	}
}

func (ses *Session) cleanCache() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.planCache != nil {
		ses.planCache.clean()
	}
}

// releasePlanCache is an internal method. The caller MUST hold ses.mu
// (currently only called from Session.Close which holds the lock).
func (ses *Session) releasePlanCache() {
	if ses.planCache != nil {
		ses.planCache.clean()
	}
}

func (ses *Session) UpdateDebugString() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	sb := bytes.Buffer{}
	//option connection id , ip
	if ses.respr != nil {
		sb.WriteString(fmt.Sprintf("connectionId %d", ses.respr.GetU32(CONNID)))
		sb.WriteByte('|')
		sb.WriteString(ses.respr.GetStr(PEER))
	}
	sb.WriteByte('|')
	//account info
	if ses.tenant != nil {
		sb.WriteString(fmt.Sprintf("account %s:%s", ses.tenant.GetTenant(), ses.tenant.GetUser()))
	} else {
		acc := getDefaultAccount()
		sb.WriteString(fmt.Sprintf("account %s:%s", acc.GetTenant(), acc.GetUser()))
	}
	sb.WriteByte('|')
	//go routine id
	if ses.rt != nil {
		sb.WriteString(fmt.Sprintf("goRoutineId %d", ses.rt.getGoroutineId()))
		sb.WriteByte('|')
		if ses.rt.mc != nil {
			sb.WriteString(fmt.Sprintf("migrate-goRoutineId %d", ses.rt.mc.getGoroutineId()))
			sb.WriteByte('|')
		}
	}
	//session id
	sb.WriteString(ses.uuid.String())
	//upstream sessionid
	if ses.upstream != nil {
		sb.WriteByte('|')
		sb.WriteString(ses.upstream.uuid.String())
	}

	ses.debugStr = sb.String()
}

func (ses *Session) GetPrivilegeCache() *privilegeCache {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.cache
}

func (ses *Session) InvalidatePrivilegeCache() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.cache.invalidate()

	// Clear rule cache with proper locking
	ses.ruleCacheMu.Lock()
	ses.ruleCache = nil
	ses.ruleCacheMu.Unlock()
}

// GetBackgroundExec generates a background executor
func (ses *Session) GetBackgroundExec(ctx context.Context, opts ...*BackgroundExecOption) BackgroundExec {
	ses.EnterFPrint(FPGetBackgroundExec)
	defer ses.ExitFPrint(FPGetBackgroundExec)
	return NewBackgroundExec(ctx, ses, opts...)
}

// GetShareTxnBackgroundExec returns a background executor running the sql in a shared transaction.
// newRawBatch denotes we need the raw batch instead of mysql result set.
func (ses *Session) GetShareTxnBackgroundExec(ctx context.Context, newRawBatch bool) BackgroundExec {
	ses.EnterFPrint(FPGetShareTxnBackgroundExec)
	defer ses.ExitFPrint(FPGetShareTxnBackgroundExec)
	var txnOp TxnOperator
	txnHandle := ses.GetTxnHandler()
	if txnHandle != nil {
		txnOp = txnHandle.GetTxn()
	}

	var callback outputCallBackFunc
	if newRawBatch {
		callback = batchFetcher2
	} else {
		callback = fakeDataSetFetcher2
	}

	be := ses.InitBackExec(txnOp, ses.respr.GetStr(DBNAME), callback)
	//the derived statement execute in a shared transaction in background session
	be.(*backExec).backSes.ReplaceDerivedStmt(true)
	return be
}

func (ses *Session) InitBackExec(txnOp TxnOperator, db string, callBack outputCallBackFunc, opts ...*BackgroundExecOption) BackgroundExec {
	be := &backExec{}
	be.init(ses, txnOp, db, callBack)
	be.backSes.upstream = ses
	if len(opts) > 0 && opts[0] != nil {
		be.backSes.fromRealUser = opts[0].fromRealUser
		be.backSes.forcePessimisticRC = opts[0].forcePessimisticRC
		be.backSes.cloneSnapshotUsesBackgroundTxn = opts[0].cloneSnapshotUsesBackgroundTxn
		be.backSes.cancelTxnCreateWithRequest = opts[0].cancelTxnCreateWithRequest
	}
	return be
}

func (ses *Session) GetRawBatchBackgroundExec(ctx context.Context) BackgroundExec {
	ses.EnterFPrint(FPGetRawBatchBackgroundExec)
	defer ses.ExitFPrint(FPGetRawBatchBackgroundExec)
	return ses.InitBackExec(nil, "", batchFetcher2)
}

func (ses *Session) GetIsInternal() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.isInternal
}

func (ses *Session) GetData() [][]interface{} {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.data
}

func (ses *Session) SetData(data [][]interface{}) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.data = data
}

func (ses *Session) AppendData(row []interface{}) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.data = append(ses.data, row)
}

func (ses *Session) InitExportConfig(ep *tree.ExportParam) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.ep = &ExportConfig{userConfig: ep, service: ses.service}
}

func (ses *Session) GetExportConfig() *ExportConfig {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.ep
}

func (ses *Session) ClearExportParam() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.ep = nil
}

func (ses *Session) SetShowStmtType(sst ShowStatementType) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.showStmtType = sst
}

func (ses *Session) GetShowStmtType() ShowStatementType {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.showStmtType
}

func (ses *Session) GetOutputCallback(execCtx *ExecCtx) func(*batch.Batch, *perfcounter.CounterSet) error {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return func(bat *batch.Batch, crs *perfcounter.CounterSet) error {
		if execCtx != nil && execCtx.input != nil && execCtx.input.isCursorExecute {
			if err := capturePreparedCursorBatch(ses, execCtx, bat); err != nil {
				return err
			}
			return stagePreparedCursorQueryResult(execCtx, crs, bat)
		}
		return ses.outputCallback(ses, execCtx, bat, crs)
	}
}

func (ses *Session) resetDiagnostics() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.errInfo != nil {
		ses.errInfo.reset()
	}
}

func (ses *Session) appendErrorDiagnostic(code uint16, msg string) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.errInfo != nil {
		ses.errInfo.push(code, msg)
	}
}

func (ses *Session) appendWarningDiagnostic(code uint16, msg string) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.errInfo != nil {
		ses.errInfo.pushWithLevel(code, msg, "Warning")
	}
}

// AppendWarningDiagnostic exposes the diagnostic sink to expression
// evaluation without coupling the process.Session interface to frontend
// warning storage.
func (ses *Session) AppendWarningDiagnostic(code uint16, msg string) {
	ses.appendWarningDiagnostic(code, msg)
}

// AppendWarningBatch merges the total warning count from a remote fragment
// while retaining only the bounded records needed by SHOW WARNINGS.
func (ses *Session) AppendWarningBatch(total uint64, codes []uint16, messages []string) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.errInfo != nil {
		ses.errInfo.appendWarningBatch(total, codes, messages)
	}
}

func (ses *Session) diagnosticsSnapshot() errInfo {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.errInfo == nil {
		return errInfo{}
	}
	return ses.errInfo.snapshot()
}

func (ses *Session) GenNewStmtId() uint32 {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.lastStmtId = ses.lastStmtId + 1
	return ses.lastStmtId
}

func (ses *Session) SetLastStmtID(id uint32) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.lastStmtId = id
}

func (ses *Session) GetLastStmtId() uint32 {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.lastStmtId
}

func (ses *Session) SetLastInsertID(num uint64) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.lastInsertID = num
}

func (ses *Session) GetLastInsertID() uint64 {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.lastInsertID
}

func (ses *Session) SetLastAffectedRows(num int64) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.lastAffectedRows = num
}

func (ses *Session) GetLastAffectedRows() int64 {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.lastAffectedRows
}

func (ses *Session) SetLastFoundRows(num uint64) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.lastFoundRows = num
}

func (ses *Session) GetLastFoundRows() uint64 {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.lastFoundRows
}

func (ses *Session) SetCmd(cmd CommandType) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.cmd = cmd
}

func (ses *Session) GetCmd() CommandType {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.cmd
}

// GetTenantName return tenant name according to GetTenantInfo and stmt.
//
// With stmt = nil, should be only called in TxnHandler.NewTxn, TxnHandler.CommitTxn, TxnHandler.RollbackTxn
func (ses *Session) GetTenantNameWithStmt(stmt tree.Statement) string {
	tenant := sysAccountName
	if ses.GetTenantInfo() != nil && (stmt == nil || !IsPrepareStatement(stmt)) {
		tenant = ses.GetTenantInfo().GetTenant()
	}
	return tenant
}

func (ses *Session) GetTenantName() string {
	return ses.GetTenantNameWithStmt(nil)
}

func (ses *Session) SetPrepareStmt(ctx context.Context, name string, prepareStmt *PrepareStmt) error {
	name = strings.ToLower(name)
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if stmt, ok := ses.prepareStmts[name]; !ok {
		limit := ses.getMaxPrepareStmtCountLocked()
		if uint64(len(ses.prepareStmts)) >= limit {
			return moerr.NewMaxPreparedStmtCountReached(ctx, limit)
		}
	} else {
		stmt.Close()
	}

	if prepareStmt != nil && prepareStmt.proc == nil {
		prepareStmt.proc = ses.proc
	}
	ses.prepareStmts[name] = prepareStmt

	return nil
}

func (ses *Session) getMaxPrepareStmtCountLocked() uint64 {
	limit := uint64(MaxPrepareNumberInOneSession.Load())
	if ses.gSysVars == nil {
		return limit
	}
	if value, ok := ses.gSysVars.Get(maxPreparedStmtCount).(int64); ok && value >= 0 && uint64(value) < limit {
		return uint64(value)
	}
	return limit
}

func (ses *Session) GetPrepareStmt(ctx context.Context, name string) (*PrepareStmt, error) {
	normalizedName := strings.ToLower(name)
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if prepareStmt, ok := ses.prepareStmts[normalizedName]; ok {
		return prepareStmt, nil
	}
	var connID uint32
	if ses.respr != nil {
		connID = ses.respr.GetU32(CONNID)
	}
	ses.Errorf(ctx, "prepared statement '%s' does not exist on connection %d", name, connID)
	return nil, moerr.NewInvalidStatef(ctx, "prepared statement '%s' does not exist", name)
}

func (ses *Session) GetPrepareStmts() []*PrepareStmt {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ret := make([]*PrepareStmt, 0, len(ses.prepareStmts))
	for _, st := range ses.prepareStmts {
		ret = append(ret, st)
	}
	return ret
}

func (ses *Session) RemovePrepareStmt(name string) bool {
	name = strings.ToLower(name)
	ses.mu.Lock()
	defer ses.mu.Unlock()
	stmt, ok := ses.prepareStmts[name]
	if !ok {
		return false
	}
	stmt.Close()
	delete(ses.prepareStmts, name)
	return true
}

// RemoveAllPrepareStmts closes and drops every cached prepared statement. It is
// used when a session variable that changes how statements are rewritten (e.g.
// remap_rewrites / enable_remap_hint) is set: a prepared statement bakes in the
// rewrite state captured at PREPARE time, so it must be invalidated when that
// state changes, otherwise a later EXECUTE would run with a stale rewrite.
func (ses *Session) RemoveAllPrepareStmts() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	for _, stmt := range ses.prepareStmts {
		stmt.Close()
	}
	ses.prepareStmts = make(map[string]*PrepareStmt)
}

// GetUserDefinedVar gets value of the config
func (ses *Session) GetConfig(ctx context.Context, varName, dbName, tblName string) (any, error) {
	// if val, ok := ses.configs[dbName+"-"+varName]; ok {
	// 	return val, nil
	// }
	// if varName == "unique_check_on_autoincr" {
	// 	ret, err := GetUniqueCheckOnAutoIncr(ctx, ses, dbName)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	ses.configs[dbName+"-"+varName] = ret
	// 	return ret, nil
	// }
	return nil, moerr.NewInternalError(ctx, errorConfigDoesNotExist())
}

func (ses *Session) SetCreateVersion(version string) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.createVersion = version
}

func (ses *Session) GetCreateVersion() string {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.createVersion
}

func (ses *Session) GetTxnInfo() string {
	txnH := ses.GetTxnHandler()
	if txnH == nil {
		return ""
	}
	txnOp := txnH.GetTxn()
	if txnOp == nil {
		return ""
	}
	meta := txnOp.Txn()
	return meta.DebugString()
}

func (ses *Session) GetDatabaseName() string {
	return ses.GetResponser().GetStr(DBNAME)
}

func (ses *Session) SetDatabaseName(db string) {
	ses.GetResponser().SetStr(DBNAME, db)
	ses.GetTxnCompileCtx().SetDatabase(db)
}

func (ses *Session) DatabaseNameIsEmpty() bool {
	return len(ses.GetDatabaseName()) == 0
}

func (ses *Session) SetUserName(uname string) {
	ses.GetResponser().SetStr(USERNAME, uname)
}

func (ses *Session) GetConnectionID() uint32 {
	protocol := ses.GetResponser()
	if protocol != nil {
		return protocol.GetU32(CONNID)
	}
	return 0
}

func (ses *Session) SetConnectionID(v uint32) {
	protocol := ses.GetResponser()
	if protocol != nil {
		protocol.SetU32(CONNID, v)
	}
}

func (ses *Session) skipAuthForSpecialUser() bool {
	if ses.isInternal {
		return true
	}

	acc := ses.GetTenantInfo()
	if acc != nil {
		ok, _, _ := isSpecialUser(acc.GetUser())
		return ok
	}
	return false
}

// advanceAuthenticationSnapshot is the rolling-upgrade fallback for services
// predating the TN-ordered logtail read barrier. It is correct but may wait for
// the full clock uncertainty interval, so new clusters use the generic engine
// barrier in prepareAuthenticationSnapshot instead.
func (ses *Session) advanceAuthenticationSnapshot(ctx context.Context) error {
	minimum, err := ses.legacyLogtailReadFence(ctx)
	if err != nil {
		return err
	}
	ses.updateLastCommitTS(minimum)
	return nil
}

// legacyLogtailReadFence returns a timestamp strictly beyond the local HLC
// uncertainty window. It is the rolling-upgrade fallback for catalog reads
// that require cross-CN freshness before the TN-ordered barrier is available.
func (ses *Session) legacyLogtailReadFence(
	ctx context.Context,
) (timestamp.Timestamp, error) {
	rt := moruntime.ServiceRuntime(ses.GetService())
	if rt == nil {
		return timestamp.Timestamp{}, moerr.NewInternalError(
			ctx, "missing service runtime for catalog read fence")
	}
	txnClock := rt.Clock()
	if txnClock == nil {
		return timestamp.Timestamp{}, moerr.NewInternalError(
			ctx, "missing transaction clock for catalog read fence")
	}
	if txnClock.MaxOffset() < 0 {
		return timestamp.Timestamp{}, moerr.NewInternalError(
			ctx, "negative transaction clock offset for catalog read fence")
	}

	_, upperBound := txnClock.Now()
	if upperBound.PhysicalTime < 0 || upperBound.PhysicalTime == math.MaxInt64 {
		return timestamp.Timestamp{}, moerr.NewInternalError(
			ctx, "catalog read fence timestamp overflow")
	}

	// HLC ordering compares the logical component when physical times are equal.
	// Moving to the next physical tick dominates every logical timestamp at the
	// uncertainty upper bound, including a remote commit at that exact tick.
	return timestamp.Timestamp{
		PhysicalTime: upperBound.PhysicalTime + 1,
	}, nil
}

// prepareAuthenticationSnapshot installs a session snapshot minimum only after
// a generic TN publication barrier has reached this CN's normal apply pipeline.
// The protocol gate preserves correctness during rolling upgrades by falling
// back to the legacy HLC uncertainty fence until every service supports the
// barrier wire contract.
func (ses *Session) prepareAuthenticationSnapshot(ctx context.Context) error {
	pu := getPuIfPresent(ses.GetService())
	if pu == nil || pu.TxnClient == nil {
		return moerr.NewInternalError(ctx, "missing transaction client for authentication snapshot")
	}

	if logtailReadBarrierSupported(ses) {
		frontier, err := ses.acquireLogtailReadBarrier(ctx)
		if err != nil {
			return err
		}
		ses.updateLastCommitTS(frontier)
	} else if err := ses.advanceAuthenticationSnapshot(ctx); err != nil {
		return err
	}

	minimum := ses.getLastCommitTS()
	applied, err := pu.TxnClient.WaitLogTailAppliedAt(ctx, minimum)
	if err != nil {
		return err
	}
	if applied.Less(minimum) {
		return moerr.NewInternalError(ctx, "authentication snapshot did not reach the required timestamp")
	}
	return nil
}

type authenticationRejectedError struct {
	cause error
}

func (e *authenticationRejectedError) Error() string {
	return e.cause.Error()
}

func (e *authenticationRejectedError) Unwrap() error {
	return e.cause
}

func markAuthenticationRejected(err error) error {
	if err == nil {
		return nil
	}
	var rejected *authenticationRejectedError
	if errors.As(err, &rejected) {
		return err
	}
	return &authenticationRejectedError{cause: err}
}

func isAuthenticationRejected(err error) bool {
	var rejected *authenticationRejectedError
	return errors.As(err, &rejected)
}

// isAuthenticationRequestRejected identifies a deterministic login-request
// validation failure. Unlike credential rejection, the same client request
// cannot succeed by trying another cached backend generation.
func isAuthenticationRequestRejected(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrBadDB)
}

// AuthenticateUser Verify the user's password, and if the login information contains the database name, verify if the database exists
func (ses *Session) AuthenticateUser(ctx context.Context, userInput string, dbName string, authResponse []byte, salt []byte, checkPassword func(pwd []byte, salt []byte, auth []byte) bool) ([]byte, error) {
	var (
		defaultRoleID        int64
		defaultRole          string
		sqlForCheckTenant    string
		sqlForPasswordOfUser string
		tenant               *TenantInfo
		err                  error
		rsset                []ExecResult
		userRsset            []ExecResult
		tenantID             int64
		userID               int64
		pwd, accountStatus   string
		psw                  []byte
		accountVersion       uint64
		createVersion        string
		lastChangedTime      string
		defPwdLife           int
		userStatus           string
		loginAttempts        uint64
		lockTime             string
		lockTimeExpired      bool
		needCheckLock        bool
		maxLoginAttempts     int64
		needCheckHost        bool
	)

	//Get tenant info
	tenant, err = GetTenantInfo(ctx, userInput)
	if err != nil {
		return nil, err
	}

	ses.SetTenantInfo(tenant)
	ses.UpdateDebugString()

	ses.Debugf(ctx, "check special user")
	isSpecial, pwdBytes, specialAccount := isSpecialUser(tenant.GetUser())
	isBootstrapSpecial := isSpecial && specialAccount.IsMoAdminRole()
	// Internal special users bootstrap the service before catalog access is
	// available. External special users are normal client connections and must
	// observe the same fresh catalog boundary as every other public session.
	if !isBootstrapSpecial || !ses.isInternal {
		if err = ses.prepareAuthenticationSnapshot(ctx); err != nil {
			return nil, err
		}
	}
	if isBootstrapSpecial {
		ses.SetTenantInfo(specialAccount)
		if len(ses.requestLabel) == 0 {
			ses.requestLabel = db_holder.GetLabelSelector()
		}
		return GetPassWord(HashPassWordWithByte(pwdBytes))
	}

	bh := ses.GetBackgroundExec(ctx, &BackgroundExecOption{
		fromRealUser:               true,
		cancelTxnCreateWithRequest: true,
	})
	defer bh.Close()

	//step1 : check tenant exists or not in SYS tenant context
	ses.timestampMap[TSCheckTenantStart] = time.Now()
	sysTenantCtx := defines.AttachAccount(ctx, uint32(sysAccountID), uint32(rootID), uint32(moAdminRoleID))

	err = bh.Exec(sysTenantCtx, "begin;")
	defer func() {
		err = finishTxn(sysTenantCtx, bh, err)
	}()
	if err != nil {
		return nil, err
	}

	sqlForCheckTenant, err = getSqlForCheckTenant(sysTenantCtx, tenant.GetTenant())
	if err != nil {
		return nil, err
	}
	ses.Debugf(ctx, "check tenant %s exists", tenant)
	rsset, err = executeSQLInBackgroundSession(sysTenantCtx, bh, sqlForCheckTenant)
	if err != nil {
		return nil, err
	}
	if !execResultArrayHasData(rsset) {
		return nil, markAuthenticationRejected(
			moerr.NewInternalErrorf(sysTenantCtx, "there is no tenant %s", tenant.GetTenant()))
	}

	//account id
	tenantID, err = rsset[0].GetInt64(sysTenantCtx, 0, 0)
	if err != nil {
		return nil, err
	}

	//account status
	accountStatus, err = rsset[0].GetString(sysTenantCtx, 0, 2)
	if err != nil {
		return nil, err
	}

	// account version
	accountVersion, err = rsset[0].GetUint64(sysTenantCtx, 0, 3)
	if err != nil {
		return nil, err
	}

	// create version
	createVersion, err = rsset[0].GetString(sysTenantCtx, 0, 5)
	if err != nil {
		return nil, err
	}

	if strings.ToLower(accountStatus) == tree.AccountStatusSuspend.String() {
		return nil, markAuthenticationRejected(
			moerr.NewInternalErrorf(sysTenantCtx, "Account %s is suspended", tenant.GetTenant()))
	}

	if strings.ToLower(accountStatus) == tree.AccountStatusRestricted.String() {
		logutil.Infof("[set restricted] init session, init account id %d, connection id %d restricted", tenantID, ses.GetConnectionID())
		ses.getRoutine().setResricted(true)
	} else {
		ses.getRoutine().setResricted(false)
	}

	tenant.SetTenantID(uint32(tenantID))
	ses.timestampMap[TSCheckTenantEnd] = time.Now()
	v2.CheckTenantDurationHistogram.Observe(ses.timestampMap[TSCheckTenantEnd].Sub(ses.timestampMap[TSCheckTenantStart]).Seconds())

	//step2 : check user exists or not in general tenant.
	//step3 : get the password of the user

	ses.timestampMap[TSCheckUserStart] = time.Now()
	tenantCtx := defines.AttachAccountId(ctx, uint32(tenantID))

	ses.Debugf(tenantCtx, "check user of %s exists", tenant)
	//Get the password of the user in an independent session
	sqlForPasswordOfUser, err = getSqlForPasswordOfUser(tenantCtx, tenant.GetUser())
	if err != nil {
		return nil, err
	}
	userRsset, err = executeSQLInBackgroundSession(tenantCtx, bh, sqlForPasswordOfUser)
	if err != nil {
		return nil, err
	}
	if !execResultArrayHasData(userRsset) {
		return nil, markAuthenticationRejected(
			moerr.NewInternalErrorf(tenantCtx, "there is no user %s", tenant.GetUser()))
	}

	userID, err = userRsset[0].GetInt64(tenantCtx, 0, 0)
	if err != nil {
		return nil, err
	}

	pwd, err = userRsset[0].GetString(tenantCtx, 0, 1)
	if err != nil {
		return nil, err
	}

	// The catalog value may be NULL or stale after a prior REVOKE. Do not use
	// it as an active role until the implicit-login path validates the grant.
	defaultRoleID, defaultRoleIDValid, err := readStoredDefaultRoleID(tenantCtx, userRsset[0])
	if err != nil {
		return nil, err
	}

	tenant.SetUserID(uint32(userID))
	ses.timestampMap[TSCheckUserEnd] = time.Now()
	v2.CheckUserDurationHistogram.Observe(ses.timestampMap[TSCheckUserEnd].Sub(ses.timestampMap[TSCheckUserStart]).Seconds())

	/*
		login case 1: tenant:user
		1.get the default_role of the user in mo_user
		2.validate that the role is still granted, otherwise use the public grant

		login case 2: tenant:user:role
		1.check the role has been granted to the user
			-yes: go on
			-no: error

	*/
	//it denotes that there is no default role in the input
	if tenant.HasDefaultRole() {
		ses.Debugf(tenantCtx, "check default role of user %s.", tenant)
		//step4 : check role exists or not
		ses.timestampMap[TSCheckRoleStart] = time.Now()
		sqlForCheckRoleExists, err := getSqlForRoleIdOfRole(tenantCtx, tenant.GetDefaultRole())
		if err != nil {
			return nil, err
		}
		rsset, err = executeSQLInBackgroundSession(tenantCtx, bh, sqlForCheckRoleExists)
		if err != nil {
			return nil, err
		}

		if !execResultArrayHasData(rsset) {
			return nil, markAuthenticationRejected(
				moerr.NewInternalErrorf(tenantCtx, "there is no role %s", tenant.GetDefaultRole()))
		}

		ses.Debugf(tenantCtx, "check granted role of user %s.", tenant)
		//step4.2 : check the role has been granted to the user or not
		sqlForRoleOfUser, err := getSqlForRoleOfUser(tenantCtx, userID, tenant.GetDefaultRole())
		if err != nil {
			return nil, err
		}
		rsset, err = executeSQLInBackgroundSession(tenantCtx, bh, sqlForRoleOfUser)
		if err != nil {
			return nil, err
		}
		if !execResultArrayHasData(rsset) {
			return nil, markAuthenticationRejected(moerr.NewInternalErrorf(tenantCtx,
				"the role %s has not been granted to the user %s", tenant.GetDefaultRole(), tenant.GetUser()))
		}

		defaultRoleID, err = rsset[0].GetInt64(tenantCtx, 0, 0)
		if err != nil {
			return nil, err
		}
		tenant.SetDefaultRoleID(uint32(defaultRoleID))
		ses.timestampMap[TSCheckRoleEnd] = time.Now()
		v2.CheckRoleDurationHistogram.Observe(ses.timestampMap[TSCheckRoleEnd].Sub(ses.timestampMap[TSCheckRoleStart]).Seconds())
	} else {
		ses.timestampMap[TSCheckRoleStart] = time.Now()
		ses.Debugf(tenantCtx, "validate implicit default role of user %s.", tenant)
		defaultRoleID, defaultRole, err = resolveImplicitDefaultRole(
			tenantCtx, bh, userID, defaultRoleID, defaultRoleIDValid)
		if err != nil {
			return nil, err
		}
		tenant.SetDefaultRoleID(uint32(defaultRoleID))
		tenant.SetDefaultRole(defaultRole)
		ses.timestampMap[TSCheckRoleEnd] = time.Now()
		v2.CheckRoleDurationHistogram.Observe(ses.timestampMap[TSCheckRoleEnd].Sub(ses.timestampMap[TSCheckRoleStart]).Seconds())
	}
	//------------------------------------------------------------------------------------------------------------------
	psw, err = GetPassWord(pwd)
	if err != nil {
		return nil, err
	}

	// TO Check password
	if err = ses.InitSystemVariables(tenantCtx, bh); err != nil {
		return nil, err
	}

	// check if the host is allowed to connect
	needCheckHost, err = whetherNeedToCheckIp(ses)
	if err != nil {
		return nil, err
	}

	if needCheckHost {
		ses.Debugf(tenantCtx, "check client address %s", ses.clientAddr)
		err = whetherValidIpInInvitedNodes(tenantCtx, ses, ses.clientAddr)
		if err != nil {
			return nil, err
		}
	}

	needCheckLock, err = whetherNeedCheckLoginAttempts(tenantCtx, ses)
	if err != nil {
		return nil, err
	}

	userLockInfoSql := getLockInfoOfUserSql(tenant.GetUser())
	statusColIdx := uint64(0)
	loginAttemptsColIdx := uint64(1)
	lockTimeColIdx := uint64(2)
	userRsset, err = executeSQLInBackgroundSession(tenantCtx, bh, userLockInfoSql)
	if err != nil {
		return nil, err
	}
	userStatus, err = userRsset[0].GetString(tenantCtx, 0, statusColIdx)
	if err != nil {
		return nil, err
	}

	loginAttempts, err = userRsset[0].GetUint64(tenantCtx, 0, loginAttemptsColIdx)
	if err != nil {
		return nil, err
	}

	lockTime, err = userRsset[0].GetString(tenantCtx, 0, lockTimeColIdx)
	if err != nil {
		return nil, err
	}

	if userStatus == userStatusLockForever {
		return nil, markAuthenticationRejected(
			moerr.NewInternalError(tenantCtx, "user is locked, please ask the administrator to unlock"))
	} else if userStatus == userStatusLock {
		/*
			if user lock status is locked
			check if the lock_time is not expired
		*/
		if lockTimeExpired, err = checkLockTimeExpired(tenantCtx, ses, lockTime); err != nil {
			return nil, err
		}

		if !lockTimeExpired {
			return nil, markAuthenticationRejected(
				moerr.NewInternalError(tenantCtx, "user is locked, please try again later"))
		}
	}

	// make update user login info in one transaction

	if checkPassword(psw, salt, authResponse) {
		ses.Debug(tenantCtx, "check password succeeded")
		if !isSuperUser(tenant.GetUser()) {
			// check password expired
			var expired bool

			defPwdLife, err = whetherNeedCheckExpired(tenantCtx, ses)
			if err != nil {
				return nil, err
			}

			if defPwdLife > 0 {
				userExpiredSql := getExpiredTimeOfUserSql(tenant.GetUser())
				userRsset, err = executeSQLInBackgroundSession(tenantCtx, bh, userExpiredSql)
				if err != nil {
					return nil, err
				}
				lastChangedTime, err = userRsset[0].GetString(tenantCtx, 0, 0)
				if err != nil {
					return nil, err
				}
				expired, err = checkPasswordExpired(defPwdLife, lastChangedTime)
				if err != nil {
					return nil, err
				}
				if expired {
					ses.getRoutine().setExpired(true)
				}
			}

			if needCheckLock && userStatus == userStatusLock {
				// if user lock status is locked, update status to unlock
				if err = setUserUnlock(tenantCtx, tenant.GetUser(), bh); err != nil {
					return nil, err
				}
			}
		}

	} else {
		if !isSuperUser(tenant.GetUser()) && needCheckLock {
			if userStatus != userStatusLock {
				loginAttempts++
				if maxLoginAttempts, err = getLoginAttempts(tenantCtx, ses); err != nil {
					return nil, err
				}
				if int64(loginAttempts) >= maxLoginAttempts {
					// if login attempts is greater than max login attempts, update user status to lock
					if err = setUserLock(tenantCtx, tenant.GetUser(), bh); err != nil {
						return nil, err
					}
				} else {
					// if login attempts is less than max login attempts, update login_attempts
					if err = increaseLoginAttempts(tenantCtx, tenant.GetUser(), bh); err != nil {
						return nil, err
					}
				}

			} else {
				// if user lock status is locked, update lock_time to now
				if err = updateLockTime(tenantCtx, tenant.GetUser(), bh); err != nil {
					return nil, err
				}
			}
		}

		return nil, markAuthenticationRejected(moerr.NewInternalError(tenantCtx, "check password failed"))
	}

	// If the login information contains the database name, verify if the database exists
	if dbName != "" {
		ses.timestampMap[TSCheckDbNameStart] = time.Now()
		_, err = executeSQLInBackgroundSession(tenantCtx, bh, "use `"+dbName+"`")
		if err != nil {
			return nil, err
		}
		ses.Debug(tenantCtx, "check database name succeeded")
		ses.timestampMap[TSCheckDbNameEnd] = time.Now()
		v2.CheckDbNameDurationHistogram.Observe(ses.timestampMap[TSCheckDbNameEnd].Sub(ses.timestampMap[TSCheckDbNameStart]).Seconds())
	}
	//------------------------------------------------------------------------------------------------------------------
	// record the id :routine pair in RoutineManager
	ses.getRoutineManager().accountRoutine.recordRoutine(tenantID, ses.getRoutine(), accountVersion)
	ses.Debug(ctx, tenant.String())
	ses.SetCreateVersion(createVersion)

	return GetPassWord(pwd)
}

func readStoredDefaultRoleID(ctx context.Context, userResult ExecResult) (int64, bool, error) {
	isNull, err := userResult.ColumnIsNull(ctx, 0, 2)
	if err != nil {
		return 0, false, err
	}
	if isNull {
		return 0, false, nil
	}

	roleID, err := userResult.GetInt64(ctx, 0, 2)
	if err != nil {
		return 0, false, err
	}
	if roleID < 0 || roleID > int64(^uint32(0)) {
		return 0, false, nil
	}
	return roleID, true, nil
}

// resolveImplicitDefaultRole returns a role that is currently granted to the
// user. A stale, NULL, invalid, or missing catalog default falls back to the
// user's public grant; it is never activated directly from mo_user metadata.
func resolveImplicitDefaultRole(
	ctx context.Context,
	bh BackgroundExec,
	userID int64,
	storedRoleID int64,
	storedRoleIDValid bool,
) (int64, string, error) {
	roleID := storedRoleID
	if !storedRoleIDValid {
		roleID = publicRoleID
	}

	for {
		sql := getSqlForRoleNameOfUserRole(userID, roleID)
		rsset, err := executeSQLInBackgroundSession(ctx, bh, sql)
		if err != nil {
			return 0, "", err
		}
		if execResultArrayHasData(rsset) {
			roleNameIsNull, err := rsset[0].ColumnIsNull(ctx, 0, 0)
			if err != nil {
				return 0, "", err
			}
			roleName, err := rsset[0].GetString(ctx, 0, 0)
			if err != nil {
				return 0, "", err
			}
			roleNameValid := !roleNameIsNull && roleName != ""
			if roleID == publicRoleID {
				roleNameValid = roleNameValid && isPublicRole(roleName)
			}
			if roleNameValid {
				return roleID, roleName, nil
			}
		}

		if roleID == publicRoleID {
			return 0, "", markAuthenticationRejected(moerr.NewInternalErrorf(ctx,
				"get a valid default role of the user %d failed", userID))
		}
		roleID = publicRoleID
	}
}

func (ses *Session) MaybeUpgradeTenant(ctx context.Context, curVersion string, tenantID int64) error {
	// Get mo final version, which is based on the current code version
	finalVersion := ses.rm.baseService.GetFinalVersion()
	if versions.Compare(curVersion, finalVersion) <= 0 {
		return ses.rm.baseService.CheckTenantUpgrade(ctx, tenantID)
	}
	return nil
}

func (ses *Session) UpgradeTenant(ctx context.Context, tenantName string, retryCount uint32, isALLAccount bool) error {
	// Get mo final version, which is based on the current code version
	return ses.rm.baseService.UpgradeTenant(ctx, tenantName, retryCount, isALLAccount)
}

func (ses *Session) getGlobalSysVars(ctx context.Context, bh BackgroundExec) (gSysVars map[string]interface{}, err error) {
	var execResults []ExecResult

	tenantInfo := ses.GetTenantInfo()
	tenantCtx := defines.AttachAccount(ctx, tenantInfo.TenantID, tenantInfo.UserID, tenantInfo.DefaultRoleID)
	// get system variable from mo_mysql_compatibility mode
	sqlForGetVariables := getSqlForGetSystemVariablesWithAccount(uint64(tenantInfo.GetTenantID()))

	if execResults, err = ExeSqlInBgSes(tenantCtx, bh, sqlForGetVariables); err != nil {
		return
	}

	// init with default value from gSysVarsDefs
	gSysVars = make(map[string]interface{})
	for name, sysVar := range gSysVarsDefs {
		gSysVars[name] = sysVar.Default
	}
	// The SQL compatibility defaults must describe the isolation that the txn
	// client will actually use when no SET GLOBAL override has been persisted.
	// Pessimistic deployments default to RC while optimistic deployments use SI.
	// Catalog values below still win, so SET GLOBAL remains account scoped and
	// is inherited by subsequently initialized sessions.
	if value, ok := serviceTxnIsolationSystemValue(ses.service); ok {
		gSysVars[transactionIsolationSystemVariable] = value
		gSysVars[transactionIsolationSystemVariableAlias] = value
	}
	var canonicalIsolationValue interface{}
	var aliasIsolationValue interface{}
	var hasCanonicalIsolation bool
	var hasAliasIsolation bool

	for _, execResult := range execResults {
		for i := uint64(0); i < execResult.GetRowCount(); i++ {
			var varName, varValue string
			if varName, err = execResult.GetString(tenantCtx, i, 0); err != nil {
				return
			}
			if varValue, err = execResult.GetString(tenantCtx, i, 1); err != nil {
				return
			}
			varName = strings.ToLower(varName)

			// overwrite with the values from table `mo_mysql_compatibility`
			if sv, ok := gSysVarsDefs[varName]; ok {
				var val interface{}
				if val, err = sv.GetType().ConvertFromString(varValue); err != nil {
					return
				}
				if isTransactionIsolationSystemVariable(varName) {
					if varName == transactionIsolationSystemVariable {
						canonicalIsolationValue = val
						hasCanonicalIsolation = true
					} else {
						aliasIsolationValue = val
						hasAliasIsolation = true
					}
					continue
				}
				gSysVars[varName] = val
			}
		}
	}

	// New writes are canonical. Preserve compatibility with old catalogs that
	// contain only tx_isolation, while making a canonical row authoritative if
	// both forms happen to exist.
	var catalogIsolationValue interface{}
	if hasCanonicalIsolation {
		catalogIsolationValue = canonicalIsolationValue
	} else if hasAliasIsolation {
		catalogIsolationValue = aliasIsolationValue
	}
	if catalogIsolationValue != nil {
		normalized, _, normalizeErr := normalizeTxnIsolationSystemValue(
			tenantCtx, ses.service, catalogIsolationValue)
		if normalizeErr != nil {
			return nil, normalizeErr
		}
		gSysVars[transactionIsolationSystemVariable] = normalized
		gSysVars[transactionIsolationSystemVariableAlias] = normalized
	}

	return
}

func (ses *Session) refreshGlobalSysVars(ctx context.Context, bh BackgroundExec) (err error) {
	var sv *SystemVariables
	if sv, err = GSysVarsMgr.Get(ses.GetTenantInfo().TenantID, ses, ctx, bh); err != nil {
		return
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.gSysVars = sv
	return
}

func (ses *Session) GetPrivilege() *privilege {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.priv
}

func (ses *Session) SetPrivilege(priv *privilege) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.priv = priv
}

func (ses *Session) SetDDLOwnerRoleID(roleID uint32) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.ddlOwnerRoleID = roleID
}

func (ses *Session) GetDDLOwnerRoleID() uint32 {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.ddlOwnerRoleID
}

func (ses *Session) ClearDDLOwnerRoleID() {
	ses.SetDDLOwnerRoleID(0)
}

func (ses *Session) SetFromRealUser(b bool) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.fromRealUser = b
}

func (ses *Session) GetFromRealUser() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.fromRealUser
}

// getCNLabels returns requested CN labels.
func (ses *Session) getCNLabels() map[string]string {
	return ses.requestLabel
}

func (ses *Session) SetNewResponse(category int, affectedRows uint64, cmd int, d interface{}, isLastStmt bool) *Response {
	// If the stmt has next stmt, should add SERVER_MORE_RESULTS_EXISTS to the server status.
	var resp *Response
	serverStatus := ses.GetTxnHandler().GetServerStatus()
	warnings := ses.diagnosticsSnapshot().warningCount()
	if !isLastStmt {
		resp = NewResponse(category, affectedRows, 0, warnings,
			serverStatus|SERVER_MORE_RESULTS_EXISTS, cmd, d)
	} else {
		resp = NewResponse(category, affectedRows, 0, warnings, serverStatus, cmd, d)
	}
	return resp
}

// StatusSession implements the queryservice.Session interface.
func (ses *Session) StatusSession() *status.Session {
	var (
		accountName string
		userName    string
		roleName    string
	)

	accountName, userName, roleName = getUserProfile(ses.GetTenantInfo())
	//if the query is processing, the end time is invalid.
	//we can not clear the session info under this condition.
	if !ses.GetQueryInProgress() {
		endAt := ses.GetQueryEnd()
		//if the current time is more than 3 second after the query end time, the session is timeout.
		//we clear the session statement info
		//for issue 11976
		if time.Since(endAt) > 3*time.Second {
			return &status.Session{
				NodeID:        ses.getRoutineManager().baseService.ID(),
				ConnID:        ses.GetConnectionID(),
				SessionID:     ses.GetUUIDString(),
				Account:       accountName,
				User:          userName,
				Host:          ses.getRoutineManager().baseService.SQLAddress(),
				DB:            ses.GetDatabaseName(),
				SessionStart:  ses.GetSessionStart(),
				Command:       "",
				Info:          "",
				TxnID:         uuid2Str(ses.GetTxnId()),
				StatementID:   "",
				StatementType: "",
				QueryType:     "",
				SQLSourceType: "",
				QueryStart:    time.Time{},
				ClientHost:    ses.clientAddr,
				Role:          roleName,
				FromProxy:     ses.fromProxy,
				ProxyHost:     ses.proxyAddr,
			}
		}
	}
	return &status.Session{
		NodeID:        ses.getRoutineManager().baseService.ID(),
		ConnID:        ses.GetConnectionID(),
		SessionID:     ses.GetUUIDString(),
		Account:       accountName,
		User:          userName,
		Host:          ses.getRoutineManager().baseService.SQLAddress(),
		DB:            ses.GetDatabaseName(),
		SessionStart:  ses.GetSessionStart(),
		Command:       ses.GetCmd().String(),
		Info:          ses.GetSqlOfStmt(),
		TxnID:         uuid2Str(ses.GetTxnId()),
		StatementID:   ses.GetStmtId().String(),
		StatementType: ses.GetStmtType(),
		QueryType:     ses.GetQueryType(),
		SQLSourceType: ses.GetSqlSourceType(),
		QueryStart:    ses.GetQueryStart(),
		ClientHost:    ses.clientAddr,
		Role:          roleName,
		FromProxy:     ses.fromProxy,
		ProxyHost:     ses.proxyAddr,
	}
}

// getStatusAfterTxnIsEnded
// !!! only used after the txn is ended.
// it may be called in the active txn. so, we
func (ses *Session) getStatusAfterTxnIsEnded() uint16 {
	return extendStatus(ses.GetTxnHandler().GetServerStatus())
}

func uuid2Str(uid uuid.UUID) string {
	if bytes.Equal(uid[:], dumpUUID[:]) {
		return ""
	}
	return strings.ReplaceAll(uid.String(), "-", "")
}

func (ses *Session) SetSessionRoutineStatus(status string) error {
	var err error
	if status == tree.AccountStatusRestricted.String() {
		ses.getRoutine().setResricted(true)
	} else if status == tree.AccountStatusSuspend.String() {
		ses.getRoutine().setResricted(false)
	} else if status == tree.AccountStatusOpen.String() {
		ses.getRoutine().setResricted(false)
	} else {
		err = moerr.NewInternalErrorNoCtxf("SetSessionRoutineStatus have invalid status : %s", status)
	}
	return err
}

func (ses *Session) getCleanupContext() context.Context {
	if txnHandler := ses.GetTxnHandler(); txnHandler != nil {
		if ctx := txnHandler.GetTxnCtx(); ctx != nil {
			return ctx
		}
	}
	return context.Background()
}

// inheritPhysicalConnection copies only metadata owned by the authenticated
// transport. Identity and session-scoped SQL state are intentionally excluded.
func (ses *Session) inheritPhysicalConnection(prev *Session) {
	ses.uuid = prev.uuid
	ses.fromRealUser = prev.fromRealUser
	ses.rm = prev.rm
	ses.rt = prev.rt
	ses.requestLabel = make(map[string]string, len(prev.requestLabel))
	for key, value := range prev.requestLabel {
		ses.requestLabel[key] = value
	}
	ses.connType = prev.connType
	ses.timestampMap = make(map[TS]time.Time, len(prev.timestampMap))
	for key, value := range prev.timestampMap {
		ses.timestampMap[key] = value
	}
	ses.fromProxy = prev.fromProxy
	ses.clientAddr = prev.clientAddr
	ses.proxyAddr = prev.proxyAddr
}

// reset resets the ses instance and copy some fields of prev, then
// close the prev.
func (ses *Session) reset(ctx context.Context, prev *Session) error {
	if ses == nil || prev == nil {
		return nil
	}
	// update information in the new session.
	ses.tenant = prev.tenant.Copy()
	ses.accountId = prev.accountId
	ses.label = make(map[string]string, len(prev.label))
	for k, v := range prev.label {
		ses.label[k] = v
	}
	ses.inheritPhysicalConnection(prev)

	// Initialize the unpublished generation from the account's current global
	// defaults. This deliberately does not copy the old session variables or
	// their derived runtime state (for example time_zone).
	initCtx := ctx
	if initCtx == nil {
		initCtx = context.Background()
	}
	if tenant := ses.GetTenantInfo(); tenant != nil {
		initCtx = defines.AttachAccount(
			initCtx,
			tenant.GetTenantID(),
			tenant.GetUserID(),
			tenant.GetDefaultRoleID(),
		)
	}
	prev.mu.Lock()
	globalVars := prev.gSysVars
	prev.mu.Unlock()
	var err error
	if globalVars != nil {
		err = ses.initSystemVariablesFromGlobal(initCtx, globalVars)
	} else {
		// This fallback is for internal or partially initialized sessions. Normal
		// authenticated sessions already retain the account-global snapshot.
		bh := ses.GetBackgroundExec(initCtx)
		err = ses.InitSystemVariables(initCtx, bh)
		bh.Close()
	}
	if err != nil {
		return err
	}

	return prev.closeForReset(ctx)
}

// errSessionResetConnectionMustClose marks an error after the old session
// generation has changed state. The MySQL connection must not be reused: an
// ERR response alone cannot restore physical temporary-table state.
var errSessionResetConnectionMustClose = moerr.NewInternalErrorNoCtx("session reset must close connection")

// closeForReset retires a session generation while preserving the physical
// protocol connection. All reusable server-side state must be gone before
// the replacement generation can be published.
func (ses *Session) closeForReset(ctx context.Context) error {
	// rollback the transactions in the old session.
	rollbackCtx := ses.getCleanupContext()
	if ctx != nil {
		cancelCtx, cancel := context.WithCancelCause(rollbackCtx)
		stopCancel := context.AfterFunc(ctx, func() {
			cancel(context.Cause(ctx))
		})
		defer func() {
			stopCancel()
			cancel(nil)
		}()
		rollbackCtx = cancelCtx
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
	}
	tempExecCtx := ExecCtx{
		reqCtx: rollbackCtx,
		ses:    ses,
		txnOpt: FeTxnOption{byRollback: true},
	}
	err := ses.GetTxnHandler().rollbackWithContext(rollbackCtx, &tempExecCtx)
	tempExecCtx.Close()
	if err != nil {
		ses.Error(rollbackCtx, "failed to rollback txn",
			zap.Error(err))
		// rollbackUnsafe invalidates the transaction handle even when the
		// storage rollback fails or its context expires. The old generation is
		// therefore no longer an untouched, reusable session: fail closed just
		// as we do after a partially completed temporary-table cleanup.
		return errors.Join(err, errSessionResetConnectionMustClose)
	}
	if ctx != nil {
		if cause := context.Cause(ctx); cause != nil {
			return errors.Join(cause, errSessionResetConnectionMustClose)
		}
	}
	// Internal SQL execution requires a bounded context. The transaction cleanup
	// context intentionally outlives request contexts and therefore has no
	// deadline of its own, so carry the reset deadline across when one exists and
	// otherwise use the same safety bound as asynchronous disconnect cleanup.
	tempCleanupCtx := rollbackCtx
	var tempCleanupCancel context.CancelFunc
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			tempCleanupCtx, tempCleanupCancel = context.WithDeadline(rollbackCtx, deadline)
		}
	}
	if tempCleanupCancel == nil {
		tempCleanupCtx, tempCleanupCancel = context.WithTimeout(rollbackCtx, time.Minute)
	}
	defer tempCleanupCancel()
	if err = ses.resetTempTables(tempCleanupCtx); err != nil {
		ses.Error(tempCleanupCtx, "failed to drop temporary tables during session reset",
			zap.Error(err))
		return errors.Join(err, errSessionResetConnectionMustClose)
	}
	// close the previous session.
	ses.ReserveConnAndClose()
	return nil
}

func commitAfterMigrate(ses *Session, err error) error {
	//if ses == nil {
	//	logutil.Error("session is nil")
	//	return moerr.NewInternalErrorNoCtx("session is nil")
	//}
	//txnHandler := ses.GetTxnHandler()
	//if txnHandler == nil {
	//	logutil.Error("txn handler is nil")
	//	return moerr.NewInternalErrorNoCtx("txn handler is nil")
	//}
	//if txnHandler.GetSession() == nil {
	//	logutil.Error("ses in txn handler is nil")
	//	return moerr.NewInternalErrorNoCtx("ses in txn handler is nil")
	//}
	//defer func() {
	//	txnHandler.ClearServerStatus(SERVER_STATUS_IN_TRANS)
	//	txnHandler.ClearOptionBits(OPTION_BEGIN)
	//}()
	//if err != nil {
	//	if rErr := txnHandler.RollbackTxn(); rErr != nil {
	//		logutil.Errorf("failed to rollback txn: %v", rErr)
	//	}
	//	return err
	//} else {
	//	if cErr := txnHandler.CommitTxn(); cErr != nil {
	//		logutil.Errorf("failed to commit txn: %v", cErr)
	//		return cErr
	//	}
	//}
	return nil
}

type dbMigration struct {
	db       string
	commitFn func(*Session, error) error
}

func newDBMigration(db string) *dbMigration {
	return &dbMigration{
		db:       db,
		commitFn: commitAfterMigrate,
	}
}

func (d *dbMigration) Migrate(ctx context.Context, ses *Session) error {
	ses.EnterFPrint(FPMigrateDB)
	defer ses.ExitFPrint(FPMigrateDB)
	if d.db == "" {
		return nil
	}
	tempExecCtx := &ExecCtx{
		reqCtx:      ctx,
		inMigration: true,
		ses:         ses,
	}
	defer tempExecCtx.Close()
	return doComQuery(ses, tempExecCtx, &UserInput{sql: "use `" + d.db + "`"})
}

type prepareStmtMigration struct {
	name       string
	sql        string
	paramTypes []byte
	commitFn   func(*Session, error) error
}

func quotePrepareStmtName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func newPrepareStmtMigration(name string, sql string, paramTypes []byte) *prepareStmtMigration {
	return &prepareStmtMigration{
		name:       name,
		sql:        sql,
		paramTypes: paramTypes,
		commitFn:   commitAfterMigrate,
	}
}

func (p *prepareStmtMigration) Migrate(ctx context.Context, ses *Session) error {
	ses.EnterFPrint(FPMigratePrepareStmt)
	defer ses.ExitFPrint(FPMigratePrepareStmt)
	if !strings.HasPrefix(strings.ToLower(p.sql), "prepare") {
		p.sql = fmt.Sprintf("prepare %s from %s", quotePrepareStmtName(p.name), p.sql)
	}

	tempExecCtx := &ExecCtx{
		reqCtx:            ctx,
		inMigration:       true,
		ses:               ses,
		executeParamTypes: p.paramTypes,
	}
	defer tempExecCtx.Close()
	return doComQuery(ses, tempExecCtx, &UserInput{sql: p.sql})
}

type migrateTempTableExec func(sql string) error

// isStaleTempTableMigrationError identifies only catalog errors that prove a
// migration entry cannot be cloned: the database was dropped, or its source
// physical relation was dropped and the database was subsequently recreated.
// Other clone errors remain fatal so a target problem is never mistaken for
// stale source state.
func isStaleTempTableMigrationError(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrBadDB) ||
		moerr.IsMoErrCode(err, moerr.ErrNoSuchTable)
}

func migrateTempTables(
	ctx context.Context,
	ses *Session,
	tables []*query.MigrateTempTable,
	exec migrateTempTableExec,
) error {
	seen := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if table == nil || table.Database == "" || table.Alias == "" ||
			table.PhysicalName == "" || !defines.IsTempTableName(table.PhysicalName) {
			return moerr.NewInternalError(ctx, "invalid temporary-table migration snapshot")
		}
		key := table.Database + "\x00" + table.Alias
		if _, ok := seen[key]; ok {
			return moerr.NewInternalErrorf(ctx,
				"duplicate temporary-table migration entry for %s.%s",
				table.Database, table.Alias)
		}
		seen[key] = struct{}{}
	}

	for i, table := range tables {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		// Resolve the source physical relation through a short, internal alias.
		// The destination uses its original logical alias and therefore receives
		// a physical name owned by the target session. The temporary source alias
		// is always removed before this function returns, so a failed migration
		// can close the target without dropping the source session's table.
		sourceAlias := fmt.Sprintf("__mo_migrate_source_%d", i)
		for suffix := 0; ; suffix++ {
			_, exists := ses.GetTempTable(table.Database, sourceAlias)
			if !exists && sourceAlias != table.Alias {
				break
			}
			sourceAlias = fmt.Sprintf("__mo_migrate_source_%d_%d", i, suffix+1)
		}
		ses.addTempTableWithIdentity(
			table.Database, sourceAlias, table.PhysicalName, true, "", "")
		sql := "CREATE TEMPORARY TABLE " +
			sqlquote.QualifiedIdent(table.Database, table.Alias) + " CLONE " +
			sqlquote.QualifiedIdent(table.Database, sourceAlias)
		err := func() error {
			defer ses.removeTempTable(table.Database, sourceAlias, "", "")
			return exec(sql)
		}()
		if err != nil {
			if isStaleTempTableMigrationError(err) {
				// DROP DATABASE can originate from a different session, leaving
				// this session's local alias map stale. The catalog error proves
				// that this entry has no source relation to preserve; discard just
				// this entry and continue migrating the usable session state.
				continue
			}
			return moerr.AttachCause(ctx, err)
		}
		targetName, ok := ses.GetTempTable(table.Database, table.Alias)
		if !ok || targetName == table.PhysicalName {
			if ok {
				// Never let target-session cleanup claim the source physical
				// relation, even if a faulty clone path registered it directly.
				ses.removeTempTable(table.Database, table.Alias, "", "")
			}
			return moerr.NewInternalErrorf(ctx,
				"temporary table %s.%s was not cloned into the target session",
				table.Database, table.Alias)
		}
	}
	if len(tables) > 0 {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		// Raw compatibility replay may already have restored autocommit=0 on
		// the target. Migration is admitted only at a client transaction
		// boundary, so commit the internal clone batch and leave the restored
		// autocommit mode with no target-only transaction in progress.
		if err := exec("COMMIT"); err != nil {
			return moerr.AttachCause(ctx, err)
		}
	}
	return nil
}

func Migrate(ctx context.Context, ses *Session, req *query.MigrateConnToRequest) error {
	ses.EnterFPrint(FPMigrate)
	defer ses.ExitFPrint(FPMigrate)
	parameters := getPu(ses.GetService()).SV

	if ctx == nil {
		ctx = ses.GetTxnHandler().GetTxnCtx()
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	// USE and PREPARE are replayed as internal statements and update ROW_COUNT().
	// Restore the source session values after all replay work has finished.
	defer restoreRowCount(ses, ses.GetProc(), req.LastAffectedRows)
	defer func() {
		ses.SetLastFoundRows(req.FoundRows)
		if proc := ses.GetProc(); proc != nil {
			proc.SetFoundRows(req.FoundRows)
		}
	}()
	// Migration work is bounded by both its caller/lifecycle context and the
	// configured session timeout.
	cancelRequestCtx, cancelRequestFunc := context.WithTimeoutCause(ctx, parameters.SessionTimeout.Duration, moerr.CauseMigrate)
	defer cancelRequestFunc()
	ses.UpdateDebugString()
	tenant := ses.GetTenantInfo()
	if ses.proc != nil {
		ses.proc.Base.SessionInfo.ConnectionID = uint64(req.ConnID)
		if tenant != nil {
			ses.proc.Base.SessionInfo.Account = tenant.GetTenant()
		}
	}
	nodeCtx := cancelRequestCtx
	rm := ses.getRoutineManager()
	if rm != nil && rm.baseService != nil {
		nodeCtx = context.WithValue(cancelRequestCtx, defines.NodeIDKey{}, rm.baseService.ID())
	}
	migrationCtx := defines.AttachAccount(nodeCtx, tenant.GetTenantID(), tenant.GetUserID(), tenant.GetDefaultRoleID())

	accountID, err := defines.GetAccountId(migrationCtx)

	if err != nil {
		ses.Errorf(migrationCtx, "failed to get account ID: %v", err)
		return err
	}
	userID := defines.GetUserId(migrationCtx)
	ses.Infof(migrationCtx, "do migration on connection %d, db: %s, account id: %d, user id: %d",
		req.ConnID, req.DB, accountID, userID)
	if len(req.UserLevelLocks) > 0 {
		return moerr.NewInternalError(ctx, "cannot migrate connection while user-level locks are held")
	}

	dbm := newDBMigration(req.DB)
	if err := dbm.Migrate(migrationCtx, ses); err != nil {
		if cause := context.Cause(migrationCtx); cause != nil {
			return cause
		}
		ses.Warnf(migrationCtx, "the database %s may have been deleted, "+
			"so continue to mirgrate session, conn ID: %d, err: %v",
			req.DB, req.ConnID, err)
	}
	if len(req.UserDefinedVars) > 0 && !req.UserDefinedVarsExported {
		return moerr.NewInternalError(ctx, "user variables were provided without a typed migration snapshot")
	}
	var userVars map[string]*UserDefinedVar
	if req.UserDefinedVarsExported {
		if currentProtocolVersion(ses.proc) < defines.MORPCVersion22 {
			return moerr.NewInternalError(ctx, "typed user-variable migration requires protocol version 22")
		}
		var err error
		userVars, err = decodeUserDefinedVars(
			migrationCtx, req.UserDefinedVars, req.UserDefinedVarsReplayable)
		if err != nil {
			return err
		}
	}
	var systemVars []migratedSystemVariable
	if req.SystemVariablesExported {
		if currentProtocolVersion(ses.proc) < defines.MORPCVersion22 {
			return moerr.NewInternalError(ctx, "typed system-variable migration requires protocol version 22")
		}
		var err error
		systemVars, err = decodeSessionSystemVars(migrationCtx, req.SystemVariables)
		if err != nil {
			return err
		}
	}
	if len(req.TempTables) > 0 {
		if currentProtocolVersion(ses.proc) < defines.MORPCVersion38 {
			return moerr.NewInternalError(ctx,
				"temporary-table migration requires protocol version 38")
		}
		// Clone before typed system-variable restoration. migrateTempTables also
		// commits explicitly because Proxy compatibility replay may already have
		// restored autocommit=0 on the target.
		if err := migrateTempTables(
			migrationCtx,
			ses,
			req.TempTables,
			func(sql string) error {
				tempExecCtx := &ExecCtx{
					reqCtx: migrationCtx, inMigration: true, ses: ses,
				}
				defer tempExecCtx.Close()
				return doComQuery(ses, tempExecCtx, &UserInput{sql: sql})
			},
		); err != nil {
			return err
		}
	}
	if req.UserDefinedVarsExported {
		ses.installUserDefinedVars(userVars)
	}
	if req.SystemVariablesExported {
		migrationExecCtx := &ExecCtx{
			reqCtx:      migrationCtx,
			inMigration: true,
			ses:         ses,
		}
		defer migrationExecCtx.Close()
		for _, variable := range systemVars {
			if variable.nextTransaction {
				isolation, err := txnIsolationFromSystemValue(migrationCtx, variable.value)
				if err != nil {
					return err
				}
				txnHandler := ses.GetTxnHandler()
				if txnHandler == nil {
					return moerr.NewInternalError(migrationCtx, "transaction handler is not initialized")
				}
				if err := txnHandler.setNextTxnIsolation(migrationCtx, isolation, false); err != nil {
					return moerr.AttachCause(migrationCtx, err)
				}
				ses.markMigrationSystemVarReplayable(
					migrationNextTxnIsolationKey, req.SystemVariablesReplayable)
				continue
			}
			var oldAutocommit interface{}
			if variable.name == "autocommit" {
				oldAutocommit, err = ses.GetSessionSysVar(variable.name)
				if err != nil {
					return moerr.AttachCause(migrationCtx, err)
				}
			}
			if err := ses.SetSessionSysVar(migrationCtx, variable.name, variable.value); err != nil {
				return moerr.AttachCause(migrationCtx, err)
			}
			ses.markMigrationSystemVarReplayable(variable.name, req.SystemVariablesReplayable)
			if variable.name == "autocommit" {
				oldValue, err := valueIsBoolTrue(oldAutocommit)
				if err != nil {
					return moerr.AttachCause(migrationCtx, err)
				}
				newValue, err := valueIsBoolTrue(variable.value)
				if err != nil {
					return moerr.AttachCause(migrationCtx, err)
				}
				txnHandler := ses.GetTxnHandler()
				if txnHandler == nil {
					return moerr.NewInternalError(migrationCtx, "transaction handler is not initialized")
				}
				if err := txnHandler.SetAutocommit(migrationExecCtx, oldValue, newValue); err != nil {
					return moerr.AttachCause(migrationCtx, err)
				}
			}
			if variable.runtimeValuePresent {
				ses.applySessionSysVarSideEffects(variable.name, variable.runtimeValue)
			} else {
				ses.applySessionSysVarSideEffects(variable.name, variable.value)
			}
			// The typed snapshot carries only the final value. Invalidate for
			// both cache-control variables so a source toggle (0->1 or 1->0)
			// cannot leave stale target entries after migration.
			if variable.name == "clear_privilege_cache" || variable.name == "enable_privilege_cache" {
				ses.InvalidatePrivilegeCache()
			}
		}
	} else {
		for _, stmt := range req.SetVarStmts {
			tempExecCtx := &ExecCtx{reqCtx: migrationCtx, inMigration: true, ses: ses}
			err := doComQuery(ses, tempExecCtx, &UserInput{sql: stmt})
			tempExecCtx.Close()
			if err != nil {
				return moerr.AttachCause(migrationCtx, err)
			}
		}
	}

	var maxStmtID uint32
	for _, p := range req.PrepareStmts {
		if p == nil {
			continue
		}
		pm := newPrepareStmtMigration(p.Name, p.SQL, p.ParamTypes)
		if err := pm.Migrate(migrationCtx, ses); err != nil {
			return moerr.AttachCause(migrationCtx, err)
		}
		id := parsePrepareStmtID(p.Name)
		if id > maxStmtID {
			maxStmtID = id
		}
	}
	if maxStmtID > 0 {
		ses.SetLastStmtID(maxStmtID)
	}
	if cause := context.Cause(migrationCtx); cause != nil {
		return cause
	}
	return nil
}

func (ses *Session) applySessionSysVarSideEffects(name string, value interface{}) {
	switch strings.ToLower(name) {
	case "optimizer_hints", "runtime_filter_limit_in", "runtime_filter_limit_bloom_filter":
		moruntime.ServiceRuntime(ses.service).SetGlobalVariables(strings.ToLower(name), value)
	case "disable_agg_statement":
		boolVal := InitSystemVariableBoolType("_")
		ses.disableAgg = boolVal.IsTrue(value)
	case "clear_privilege_cache":
		boolVal := InitSystemVariableBoolType("_")
		if boolVal.IsTrue(value) {
			if cache := ses.GetPrivilegeCache(); cache != nil {
				cache.invalidate()
			}
		}
	case "enable_privilege_cache":
		boolVal := InitSystemVariableBoolType("_")
		if !boolVal.IsTrue(value) {
			if cache := ses.GetPrivilegeCache(); cache != nil {
				cache.invalidate()
			}
		}
	}
}

func (ses *Session) GetLogger() SessionLogger {
	return ses
}

func (ses *Session) GetSessId() uuid.UUID {
	if ses == nil {
		return uuid.UUID{}
	}
	return uuid.UUID(ses.GetUUID())
}

func (ses *Session) GetLogLevel() zapcore.Level {
	if ses == nil {
		return zap.InfoLevel
	}
	return ses.logLevel
}

func (ses *Session) initLogger() {
	ses.loggerOnce.Do(func() {
		if ses.logger == nil {
			ses.logger = getLogger(ses.service)
		}
		config := logutil.GetDefaultConfig()
		ses.logLevel = config.GetLevel().Level()
	})
}

// log do logging.
// Please keep it called by Session.Info/Error/Debug/Warn/Fatal/Panic.
// PS: This func must be lock free. DO NOT use Session.mu.
func (ses *Session) log(ctx context.Context, level zapcore.Level, msg string, fields ...zap.Field) {
	if ses == nil {
		return
	}
	ses.initLogger()
	if ses.logLevel.Enabled(level) {
		fields = append(fields, zap.String("session_info", ses.debugStr)) // not use ses.GetDebugStr() because this func may be locked.
		if ses.tenant != nil {
			fields = append(fields, zap.String("role", ses.tenant.GetDefaultRole()))
		}
		fields = appendSessionField(fields, ses)
		fields = appendTraceField(fields, ctx)
		ses.logger.Log(msg, log.DefaultLogOptions().WithLevel(level).AddCallerSkip(2), fields...)
	}
}

func (ses *Session) logf(ctx context.Context, level zapcore.Level, format string, args ...any) {
	if ses == nil {
		return
	}
	ses.initLogger()
	if ses.logLevel.Enabled(level) {
		fields := make([]zap.Field, 0, 5)
		fields = append(fields, zap.String("session_info", ses.debugStr))
		if ses.tenant != nil {
			fields = append(fields, zap.String("role", ses.tenant.GetDefaultRole()))
		}
		fields = appendSessionField(fields, ses)
		fields = appendTraceField(fields, ctx)
		ses.logger.Log(fmt.Sprintf(format, args...), log.DefaultLogOptions().WithLevel(level).AddCallerSkip(2), fields...)
	}
}

func (ses *Session) Info(ctx context.Context, msg string, fields ...zap.Field) {
	ses.log(ctx, zap.InfoLevel, msg, fields...)
}

func (ses *Session) Error(ctx context.Context, msg string, fields ...zap.Field) {
	ses.log(ctx, zap.ErrorLevel, msg, fields...)
}

func (ses *Session) Warn(ctx context.Context, msg string, fields ...zap.Field) {
	ses.log(ctx, zap.WarnLevel, msg, fields...)
}

func (ses *Session) Fatal(ctx context.Context, msg string, fields ...zap.Field) {
	ses.log(ctx, zap.FatalLevel, msg, fields...)
}

func (ses *Session) Debug(ctx context.Context, msg string, fields ...zap.Field) {
	ses.log(ctx, zap.DebugLevel, msg, fields...)
}

func (ses *Session) LogDebug() bool {
	if ses == nil {
		return false
	}
	ses.initLogger()
	return ses.logLevel.Enabled(zap.DebugLevel)
}

func (ses *Session) Infof(ctx context.Context, format string, args ...any) {
	ses.logf(ctx, zap.InfoLevel, format, args...)
}
func (ses *Session) Errorf(ctx context.Context, format string, args ...any) {
	ses.logf(ctx, zap.ErrorLevel, format, args...)
}

func (ses *Session) Warnf(ctx context.Context, format string, args ...any) {
	ses.logf(ctx, zap.WarnLevel, format, args...)
}

func (ses *Session) Fatalf(ctx context.Context, format string, args ...any) {
	ses.logf(ctx, zap.FatalLevel, format, args...)
}

func (ses *Session) Debugf(ctx context.Context, format string, args ...any) {
	ses.logf(ctx, zap.DebugLevel, format, args...)
}

func appendTraceField(fields []zap.Field, ctx context.Context) []zap.Field {
	if sc := trace.SpanFromContext(ctx).SpanContext(); !sc.IsEmpty() {
		fields = append(fields, trace.ContextField(ctx))
	}
	return fields
}

func whetherNeedCheckExpired(ctx context.Context, ses *Session) (int, error) {
	var (
		defaultPasswordLifetime int
		err                     error
	)
	defaultPasswordLifetime, err = getPasswordLifetime(ctx, ses)
	if err != nil {
		return 0, err
	}
	return defaultPasswordLifetime, nil
}

func checkPasswordExpired(defPwdLifeTime int, lastChangedTime string) (bool, error) {
	var (
		err         error
		lastChanged time.Time
	)

	if defPwdLifeTime <= 0 {
		return false, nil
	}

	// get the last password change time as utc time
	lastChanged, err = time.ParseInLocation("2006-01-02 15:04:05", lastChangedTime, time.UTC)
	if err != nil {
		return false, err
	}

	// get the current time as utc time
	now := time.Now().UTC()
	if lastChanged.AddDate(0, 0, defPwdLifeTime).Before(now) {
		return true, nil
	}

	return false, nil
}

func getPasswordLifetime(ctx context.Context, ses *Session) (int, error) {
	value, err := ses.GetGlobalSysVar(DefaultPasswordLifetime)
	if err != nil {
		return 0, err
	}

	lifetime, ok := value.(int64)
	if !ok {
		return 0, moerr.NewInternalErrorf(ctx, "invalid value for %s", DefaultPasswordLifetime)
	}

	return int(lifetime), nil
}

func checkLockTimeExpired(ctx context.Context, ses *Session, lockTime string) (bool, error) {
	var (
		maxDelay int64
		err      error
		lt       time.Time
	)

	// get the lock time as utc time
	lt, err = time.ParseInLocation("2006-01-02 15:04:05", lockTime, time.UTC)
	if err != nil {
		return false, err
	}

	// get the max connection delay
	maxDelay, err = getLoginMaxDelay(ctx, ses)
	if err != nil {
		return false, err
	}

	// get the current time as utc time
	now := time.Now().UTC()
	if lt.Add(time.Duration(maxDelay) * time.Millisecond).After(now) {
		return false, nil
	}

	return true, nil
}

func getLoginAttempts(ctx context.Context, ses *Session) (int64, error) {
	value, err := ses.GetGlobalSysVar(ConnectionControlFailedConnectionsThreshold)
	if err != nil {
		return 0, err
	}

	attempts, ok := value.(int64)
	if !ok {
		return 0, moerr.NewInternalErrorf(ctx, "invalid value for %s", ConnectionControlFailedConnectionsThreshold)
	}

	return attempts, nil
}

func getLoginMaxDelay(ctx context.Context, ses *Session) (int64, error) {
	value, err := ses.GetGlobalSysVar(ConnectionControlMaxConnectionDelay)
	if err != nil {
		return 0, err
	}

	delay, ok := value.(int64)
	if !ok {
		return 0, moerr.NewInternalErrorf(ctx, "invalid value for %s", ConnectionControlMaxConnectionDelay)
	}

	return delay, nil
}

func whetherNeedCheckLoginAttempts(ctx context.Context, ses *Session) (bool, error) {
	var (
		loginMaxTimes int64
		err           error
		loginMaxDelay int64
	)
	loginMaxTimes, err = getLoginAttempts(ctx, ses)
	if err != nil {
		return false, err
	}

	loginMaxDelay, err = getLoginMaxDelay(ctx, ses)
	if err != nil {
		return false, err
	}
	return loginMaxTimes > 0 && loginMaxDelay > 0, nil
}

func setUserUnlock(ctx context.Context, userName string, bh BackgroundExec) error {
	var (
		sql string
		err error
	)
	sql = getSqlForUpdateUnlcokStatusOfUser(userStatusUnlock, userName)
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}
	return nil
}

func increaseLoginAttempts(ctx context.Context, userName string, bh BackgroundExec) error {
	var (
		sql string
		err error
	)
	sql = getSqlForUpdateLoginAttemptsOfUser(userName)
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}
	return nil
}

func updateLockTime(ctx context.Context, userName string, bh BackgroundExec) error {
	var (
		sql string
		err error
	)
	sql = getSqlForUpdateLockTimeOfUser(userName)
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}
	return nil
}

func setUserLock(ctx context.Context, userName string, bh BackgroundExec) error {
	var (
		sql string
		err error
	)
	sql = getSqlForUpdateStatusLockOfUser(userStatusLock, userName)
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}
	return nil
}

func whetherNeedToCheckIp(ses *Session) (bool, error) {
	var (
		ValidnodeVal interface{}
		err          error
	)
	ValidnodeVal, err = ses.GetGlobalSysVar(ValidnodeChecking)
	if err != nil {
		return false, err
	}

	validatePasswordConfig, ok := ValidnodeVal.(int8)
	if !ok || validatePasswordConfig != 1 {
		return false, nil
	}

	return true, nil
}

func whetherValidIpInInvitedNodes(ctx context.Context, ses *Session, clientAddr string) error {
	var (
		invitedNodesVal interface{}
		err             error
		ip              string
	)

	invitedNodesVal, err = ses.GetGlobalSysVar(InvitedNodes)
	if err != nil {
		return err
	}
	invitedNodes, ok := invitedNodesVal.(string)
	if !ok {
		return moerr.NewInternalErrorf(ctx, "invalid value for %s", InvitedNodes)
	}

	// get the ip address of the client
	ip, _, err = net.SplitHostPort(clientAddr)
	if err != nil {
		return err
	}

	return checkValidIpInInvitedNodes(ctx, invitedNodes, ip)
}
