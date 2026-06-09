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

// Package compile defines the compile-layer (DDL) hooks every vector index
// plugin must implement: create / reindex / drop / alter.
//
// These replace the per-algorithm Scope.handleVector<X>Index methods and
// gen{Build,Delete}<X>Index helpers in pkg/sql/compile.
package compile

import (
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// CompileContext is the narrowed view of *compile.Scope / *compile.Compile
// that plugin compile hooks operate against. Provided by the SQL layer; the
// plugin code never touches *compile.Compile directly, which keeps the
// plugin package out of an import cycle with pkg/sql/compile.
type CompileContext interface {
	// Ctx returns the request context (for cancellation, txn, etc.).
	Ctx() Context

	// Database is the engine.Database for the indexed table's database.
	Database() engine.Database

	// QryDatabase is the database name from the parsed query.
	QryDatabase() string

	// OriginalTableDef is the table-def the index is being created on.
	OriginalTableDef() *plan.TableDef

	// IndexInfo is the CreateTable carrying the hidden index-table DDL.
	// May be nil for the ALTER REINDEX path.
	IndexInfo() *plan.CreateTable

	// MainTableID is the parent table's ID.
	MainTableID() uint64

	// MainExtra is the parent table's SchemaExtra, mutated to record the
	// new index-table IDs.
	MainExtra() *api.SchemaExtra

	// RunSql executes a SQL statement in the current transactional context.
	RunSql(sql string) error

	// BuildIndexTable creates one hidden table for the index. Wraps the
	// existing indexTableBuild helper in pkg/sql/compile/ddl.go.
	BuildIndexTable(def *plan.TableDef) error

	// ResolveVariable forwards to process.GetResolveVariableFunc().
	ResolveVariable(name string, isSystemVar, isGlobalVar bool) (any, error)

	// IsFrontend reports whether the Compile is running attached to a
	// frontend client session (mysql client query or in-frontend
	// backSession). Returns false for every other caller (internal
	// SQL executor invocations from idxcron ALTER REINDEX,
	// ProcessInitSQL, bootstrap, cron jobs, …). Plugins that need to
	// distinguish "have a session" from "don't" — for example,
	// IdxcronMetadata's "defer capture until frontend re-entry"
	// pattern — should branch on this rather than on whether
	// ResolveVariable errors, since background paths set resolvers
	// too (idxcron's task.Metadata, ProcessInitSQL's
	// executor.DefaultResolveVariable).
	IsFrontend() bool

	// IsTableClone reports whether this compile runs inside a table-clone scope
	// (`create table … clone`) — which is how snapshot/restore replays a table.
	// Lets an index plugin restore a prebuilt model verbatim instead of
	// rebuilding it. IsFrontend cannot distinguish this (it is true for the
	// restore backExec too).
	IsTableClone() bool

	// IsExperimentalEnabled checks whether an experimental-feature flag is
	// set in the current session/system variables. Used by HNSW today
	// (flag "experimental_hnsw_index"). Plugins gating on a flag should
	// fail HandleCreateIndex when this returns false.
	IsExperimentalEnabled(flag string) (bool, error)

	// IsCCPRTaskTransaction reports whether this Compile is running on
	// behalf of a CCPR (cross-cluster physical replication) task. HNSW
	// skips index data population when (ccpr && tableFromPublication) —
	// the index data is synced via the CCPR pipeline instead.
	IsCCPRTaskTransaction() bool

	// IsTableFromPublication reports whether the given table is sourced
	// from a publication (Subscription Account). Used together with
	// IsCCPRTaskTransaction by the HNSW skip-during-ccpr check.
	IsTableFromPublication(tableDef *plan.TableDef) bool

	// SinkerTypeFromAlgo returns the ISCP sinker-type tag for an
	// algorithm string (e.g. "hnsw" → kSinkerTypeHnsw). Used when
	// registering CDC tasks.
	SinkerTypeFromAlgo(algo string) int8

	// CreateIndexCdcTask registers an ISCP CDC task to maintain the
	// hidden index tables asynchronously. startFromNow=true means the
	// task only sees mutations from now forward (used after an immediate
	// initial build); false means it consumes the full log from the
	// table's creation timestamp.
	CreateIndexCdcTask(dbName, tableName string, tableID uint64, indexName string,
		sinkerType int8, startFromNow bool, sql string, tableDef *plan.TableDef) error

	// DropIndexCdcTask removes any ISCP CDC task previously registered
	// for this (table, index). Safe to call when no task exists.
	DropIndexCdcTask(tableDef *plan.TableDef, dbName, tableName, indexName string) error

	// RunSqlWithResult executes a SQL statement and returns the result
	// set so callers can read rows/scalars. Counterpart to RunSql,
	// which discards results. The adapter passes the IVF-FLAT-legacy
	// NoAccountId scope; callers must Close() the returned Result.
	RunSqlWithResult(sql string) (executor.Result, error)

	// RegisterIdxcronUpdate registers a scheduled-maintenance task
	// with idxcron. Wraps idxcron.RegisterUpdate so plugin packages
	// don't have to import that package directly. action is one of
	// the idxcron.Action_* constants.
	RegisterIdxcronUpdate(tableID uint64, dbName, tableName, indexName,
		action string, metadata []byte) error
}

// Context is the algorithm-agnostic subset of context.Context the plugin needs.
// Defined locally to avoid importing context.Context into the interface
// surface; CompileContext implementations return the real context.Context via
// type assertion if needed.
type Context interface {
	// Deadline / Done / Err / Value — same shape as context.Context, but we
	// only declare what we use today. Implementations are *expected* to be
	// real context.Context values.
	Done() <-chan struct{}
	Err() error
	Value(key any) any
}

// Hooks bundles every compile-layer callback for one algorithm.
type Hooks interface {
	// HandleCreateIndex is the CREATE INDEX path. indexDefs is keyed by
	// IndexAlgoTableType (matching catalog.HiddenTableTypes()).
	// Replaces Scope.handleVector<X>Index.
	HandleCreateIndex(ctx CompileContext, indexDefs map[string]*plan.IndexDef) error

	// HandleReindex is the ALTER … REINDEX path. forceSync mirrors the
	// existing IVF-FLAT semantics (run synchronously inside the txn) and is
	// ignored by algorithms that do not support it.
	HandleReindex(ctx CompileContext, indexDefs map[string]*plan.IndexDef, forceSync bool) error

	// RestoreInitSQL returns (startFromNow, initSQL) for the restored index's
	// CDC. initSQL rebuilds the index from the cloned rows — run post-commit by
	// the CDC's first iteration (ProcessInitSQL), so it sees the committed clone
	// and re-arms the CDC at the post-clone watermark (no replay); startFromNow
	// is then true. initSQL=="" means no rebuild (the clone + CDC catch-up
	// suffice, e.g. fulltext) and startFromNow is false (the CDC catches the
	// cloned tables up from their watermark). See Scope.RestoreTable.
	RestoreInitSQL(ctx CompileContext, indexDefs map[string]*plan.IndexDef) (startFromNow bool, initSQL string, err error)

	// ValidateReindexParams checks a parameter update against the algorithm's
	// schema and returns the merged params map. Replaces the inner switch
	// at ddl.go:929. alter is the planner's AlterTable_Action_AlterIndex
	// payload; the plugin should pull the fields it cares about (e.g.
	// IndexAlgoParamList for IVF-FLAT) and ignore the rest.
	ValidateReindexParams(old map[string]string, alter ReindexParamUpdate) (map[string]string, error)

	// HandleDropIndex runs algorithm-specific cleanup when an index is
	// dropped (in addition to the generic hidden-table deletion the SQL
	// layer already performs). Examples: unregister CDC tasks, unregister
	// idxcron schedules. May be a no-op.
	HandleDropIndex(ctx CompileContext, indexDefs map[string]*plan.IndexDef) error

	// IdxcronMetadata builds the metadata blob registered with idxcron
	// alongside the action key (catalog.Hooks.CDC().IdxcronAction).
	// Called by pkg/sql/compile/iscp_util.go:CreateAllIndexUpdateTasks
	// only when IdxcronAction != "". May resolve session/system
	// variables via ctx.ResolveVariable. Return (nil, nil) when the
	// action takes no metadata.
	IdxcronMetadata(ctx CompileContext) ([]byte, error)
}

// ReindexParamUpdate carries the alter-reindex inputs the plugin may consume.
// Defined here (rather than passing the planner's AlterTable_Action_AlterIndex
// type) so this package stays free of plan-package internals.
type ReindexParamUpdate struct {
	// IndexAlgoParamList — IVF-FLAT's `lists` setting. Zero means unset.
	IndexAlgoParamList int64
}
