// Copyright 2021 - 2022 Matrix Origin
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

package plan

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	JoinSideNone       int8 = 0
	JoinSideLeft            = 1 << 1
	JoinSideRight           = 1 << 2
	JoinSideBoth            = JoinSideLeft | JoinSideRight
	JoinSideMark            = 1 << 3
	JoinSideCorrelated      = 1 << 4
	JoinSideOuter           = 1 << 5
)

type ExpandAliasMode int8

const (
	NoAlias ExpandAliasMode = iota
	AliasBeforeColumn
	AliasAfterColumn
)

type TableDefType = plan.TableDef_DefType
type TableDef = plan.TableDef
type ColDef = plan.ColDef
type ObjectRef = plan.ObjectRef
type ColRef = plan.ColRef
type Stats = plan.Stats
type Const = plan.Literal
type MaxValue = plan.MaxValue
type Expr = plan.Expr
type Node = plan.Node
type RowsetData = plan.RowsetData
type Query = plan.Query
type Plan = plan.Plan
type Type = plan.Type
type Plan_Query = plan.Plan_Query
type Property = plan.Property
type TableDef_DefType_Properties = plan.TableDef_DefType_Properties
type PropertiesDef = plan.PropertiesDef
type ViewDef = plan.ViewDef
type ClusterByDef = plan.ClusterByDef
type OrderBySpec = plan.OrderBySpec
type FkColName = plan.FkColName
type ForeignKeyDef = plan.ForeignKeyDef
type ClusterTable = plan.ClusterTable
type PrimaryKeyDef = plan.PrimaryKeyDef
type IndexDef = plan.IndexDef
type SubscriptionMeta = plan.SubscriptionMeta
type Snapshot = plan.Snapshot
type SnapshotTenant = plan.SnapshotTenant
type ExternAttr = plan.ExternAttr
type DataStreamScan = plan.DataStreamScan
type ForeignScan = plan.ForeignScan
type KafkaScan = plan.KafkaScan

const ViewSnapshotKeySuffix = "@ts="
const viewDependencyKeyPrefix = "\x00mo_view_dependency\x00"

// FormatViewKeyWithSnapshot appends snapshot information to a view key for privilege checks.
func FormatViewKeyWithSnapshot(viewKey string, snapshot *Snapshot) string {
	if !IsSnapshotValid(snapshot) || snapshot.TS == nil {
		return viewKey
	}
	return fmt.Sprintf("%s%s%d", viewKey, ViewSnapshotKeySuffix, snapshot.TS.PhysicalTime)
}

// FormatViewDependencyKey preserves database and view identifiers separately,
// plus the complete optional table-level snapshot used to resolve the view.
func FormatViewDependencyKey(databaseName, viewName string, snapshot *Snapshot) (string, error) {
	var snapshotData []byte
	if IsSnapshotValid(snapshot) {
		var err error
		snapshotData, err = snapshot.Marshal()
		if err != nil {
			return "", err
		}
	}
	return viewDependencyKeyPrefix +
		base64.RawURLEncoding.EncodeToString([]byte(databaseName)) + "." +
		base64.RawURLEncoding.EncodeToString([]byte(viewName)) + "." +
		base64.RawURLEncoding.EncodeToString(snapshotData), nil
}

// ParseViewDependencyKey returns the database, view, and optional table-level
// snapshot recorded while binding a view. Plain database#view keys remain
// readable for callers that have not recorded the structured dependency form.
func ParseViewDependencyKey(viewKey string) (string, string, *Snapshot, error) {
	if !strings.HasPrefix(viewKey, viewDependencyKeyPrefix) {
		databaseName, viewName, ok := strings.Cut(viewKey, "#")
		if !ok || databaseName == "" || viewName == "" {
			return "", "", nil, moerr.NewInternalErrorNoCtx("invalid view dependency")
		}
		return databaseName, viewName, nil, nil
	}
	parts := strings.Split(viewKey[len(viewDependencyKeyPrefix):], ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
		return "", "", nil, moerr.NewInternalErrorNoCtx("invalid encoded view dependency")
	}
	databaseName, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", "", nil, err
	}
	viewName, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", nil, err
	}
	if parts[2] == "" {
		return string(databaseName), string(viewName), nil, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", "", nil, err
	}
	snapshot := &Snapshot{}
	if err = snapshot.Unmarshal(data); err != nil {
		return "", "", nil, err
	}
	return string(databaseName), string(viewName), snapshot, nil
}

// ValidateSnapshotScope verifies that a relation belongs to the object covered
// by a named snapshot. Timestamp-only snapshots have no object restriction.
func ValidateSnapshotScope(
	snapshot *Snapshot,
	databaseName string,
	tableName string,
	databaseID uint64,
	tableID uint64,
) error {
	if snapshot == nil || snapshot.ExtraInfo == nil {
		return nil
	}

	switch snapshot.ExtraInfo.Level {
	case tree.SNAPSHOTLEVELCLUSTER.String(), tree.SNAPSHOTLEVELACCOUNT.String():
		return nil
	case tree.SNAPSHOTLEVELDATABASE.String():
		if snapshot.ExtraInfo.ObjId != databaseID {
			return moerr.NewInternalErrorNoCtxf(
				"database-level snapshot(%s) does not belong to the database(%s)",
				snapshot.ExtraInfo.Name,
				databaseName,
			)
		}
	case tree.SNAPSHOTLEVELTABLE.String():
		if snapshot.ExtraInfo.ObjId != tableID {
			return moerr.NewInternalErrorNoCtxf(
				"table-level snapshot(%s) does not belong to the table(%s-%s)",
				snapshot.ExtraInfo.Name,
				databaseName,
				tableName,
			)
		}
	default:
		return moerr.NewInternalErrorNoCtxf("unsupported snapshot level %q", snapshot.ExtraInfo.Level)
	}

	return nil
}

// SnapshotTableID returns the stable identity used by table snapshots. A
// copy-table ALTER replaces the physical table while preserving LogicalId.
func SnapshotTableID(tableDef *TableDef) uint64 {
	if tableDef == nil {
		return 0
	}
	if tableDef.LogicalId != 0 {
		return tableDef.LogicalId
	}
	return tableDef.TblId
}

// ValidateSnapshotDatabaseScope verifies that an operation scoped to a
// database is compatible with a named snapshot. A table snapshot cannot read
// database-wide metadata because it represents a single relation.
func ValidateSnapshotDatabaseScope(
	snapshot *Snapshot,
	databaseName string,
	databaseID uint64,
) error {
	if snapshot == nil || snapshot.ExtraInfo == nil {
		return nil
	}

	switch snapshot.ExtraInfo.Level {
	case tree.SNAPSHOTLEVELCLUSTER.String(), tree.SNAPSHOTLEVELACCOUNT.String():
		return nil
	case tree.SNAPSHOTLEVELDATABASE.String():
		if snapshot.ExtraInfo.ObjId != databaseID {
			return moerr.NewInternalErrorNoCtxf(
				"database-level snapshot(%s) does not belong to the database(%s)",
				snapshot.ExtraInfo.Name,
				databaseName,
			)
		}
	case tree.SNAPSHOTLEVELTABLE.String():
		return moerr.NewInternalErrorNoCtxf(
			"table-level snapshot(%s) cannot read database-wide metadata for database(%s)",
			snapshot.ExtraInfo.Name,
			databaseName,
		)
	default:
		return moerr.NewInternalErrorNoCtxf("unsupported snapshot level %q", snapshot.ExtraInfo.Level)
	}

	return nil
}

type CompilerContext interface {
	// Default database/schema in context
	DefaultDatabase() string
	// check if database exist
	DatabaseExists(name string, snapshot *Snapshot) bool
	// get table definition by database/schema
	Resolve(schemaName string, tableName string, snapshot *Snapshot) (*ObjectRef, *TableDef, error)
	// get index table definition by an ObjectRef, will skip unnecessary subscription check
	ResolveIndexTableByRef(ref *ObjectRef, tblName string, snapshot *Snapshot) (*ObjectRef, *TableDef, error)
	// get table definition by table id
	ResolveById(tableId uint64, snapshot *Snapshot) (*ObjectRef, *TableDef, error)
	// get the value of variable
	ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (interface{}, error)
	// get the list of the account id
	ResolveAccountIds(accountNames []string) ([]uint32, error)
	// get the relevant information of udf
	ResolveUdf(name string, args []*Expr) (*function.Udf, error)
	// get needed info for stats by table, NOTE: Stats May indirectly access the file service
	Stats(obj *ObjectRef, snapshot *Snapshot) (*pb.StatsInfo, error)
	// get origin sql string of the root
	GetRootSql() string
	// get username of current session
	GetUserName() string
	GetAccountId() (uint32, error)
	GetAccountName() string
	// GetContext get raw context.Context
	GetContext() context.Context

	// SetContext set raw context.Context
	SetContext(ctx context.Context)
	// GetDatabaseId Get database id
	GetDatabaseId(dbName string, snapshot *Snapshot) (uint64, error)

	GetProcess() *process.Process

	GetQueryResultMeta(uuid string) ([]*ColDef, string, error)
	SetBuildingAlterView(yesOrNo bool, dbName, viewName string)
	// is building the alter view or not
	// return: yes or no, dbName, viewName
	GetBuildingAlterView() (bool, string, string)
	GetStatsCache() *StatsCache
	GetSubscriptionMeta(dbName string, snapshot *Snapshot) (*SubscriptionMeta, error)
	CheckSubscriptionValid(subName, accName string, pubName string) error
	SetQueryingSubscription(meta *SubscriptionMeta)
	GetQueryingSubscription() *SubscriptionMeta
	IsPublishing(dbName string) (bool, error)
	BuildTableDefByMoColumns(dbName, table string) (*TableDef, error)
	ResolveSubscriptionTableById(tableId uint64, pubmeta *SubscriptionMeta) (*ObjectRef, *TableDef, error)

	ResolveSnapshotWithSnapshotName(snapshotName string) (*Snapshot, error)
	CheckTimeStampValid(ts int64) (bool, error)

	//InitExecuteStmtParam replaces the plan of the EXECUTE by the plan generated by the PREPARE.
	//return
	//	the plan generated by the PREPARE
	//	the statement generated by the PREPARE
	InitExecuteStmtParam(execPlan *plan.Execute) (*plan.Plan, tree.Statement, error)

	GetSnapshot() *Snapshot
	SetSnapshot(snapshot *Snapshot)
	GetViews() []string
	SetViews(views []string)

	GetLowerCaseTableNames() int64
}

// TableDefStatsCompilerContext is an optional extension for compiler contexts
// that can bind a statistics read to the table definition used by the plan.
// Implementations should reject schema-bound statistics from another table
// definition version. Keeping this separate from CompilerContext preserves
// compatibility with lightweight and external planner contexts.
type TableDefStatsCompilerContext interface {
	StatsWithTableDef(
		obj *ObjectRef,
		tableDef *TableDef,
		snapshot *Snapshot,
	) (*pb.StatsInfo, error)
}

// UserVariableTypeResolver is an optional extension implemented by session
// compiler contexts. User variables are stored as text on the frontend wire
// path, but their assignment type is part of the statement contract used by
// numeric binding. Keeping this optional avoids widening CompilerContext for
// callers that do not have session user variables (for example metadata
// builders and lightweight test contexts).
type UserVariableTypeResolver interface {
	ResolveVariableType(varName string, isSystemVar, isGlobalVar bool) (Type, error)
}

type Optimizer interface {
	Optimize(stmt tree.Statement) (*Query, error)
	CurrentContext() CompilerContext
}

type Rule interface {
	Match(*Node) bool                      // rule match?
	Apply(*Node, *Query, *process.Process) // apply the rule
}

// BaseOptimizer is base optimizer, capable of handling only a few simple rules
type BaseOptimizer struct {
	qry   *Query
	rules []Rule
	ctx   CompilerContext
}

type ViewData struct {
	Stmt                string
	DefaultDatabase     string
	SQLMode             *string          `json:"sql_mode,omitempty"`
	SecurityType        string           `json:"security_type,omitempty"`
	LowerCaseTableNames *int64           `json:"lower_case_table_names,omitempty"`
	Dependencies        []ViewDependency `json:"dependencies,omitempty"`
}

type QueryBuilder struct {
	qry     *plan.Query
	compCtx CompilerContext
	// persistedViewTarget is set structurally by CREATE/ALTER/regeneration
	// while one persisted view definition is bound. It is statement-local so
	// detached CTE contexts cannot lose the private system-function owner.
	persistedViewTarget string

	ctxByNode             []*BindContext
	windowValidationScans []*plan.Node
	nameByColRef          map[[2]int32]string
	protectedScans        map[int32]int
	updateTargetScans     map[int32]struct{}
	projectSpecialGuards  map[int32]*specialIndexGuard
	// projectAnchoredSorts holds Top-K SORT node ids that a PROJECT directly above them
	// will anchor the vector rewrite on. applyIndices walks children first, so without
	// this the SORT-anchored entry point would claim the classic
	// PROJECT -> SORT -> SCAN shape before the project ever ran, losing the project's
	// column information and with it the index-only scan.
	projectAnchoredSorts        map[int32]struct{}
	setBitmapByDisplayNode      map[[2]int32]int32
	indexHintsByScan            map[int32]*indexHintSet
	indexHintOwnerByNode        map[int32]int32
	preserveSinkProjection      map[int32]struct{}
	preserveLockProjection      map[int32]struct{}
	preserveFilterProjection    map[int32]struct{}
	preservePreInsertProjection map[int32]struct{}
	preserveInsertProjection    map[int32]struct{}
	preserveScanProjection      map[int32]struct{}
	positionalSinkScans         map[int32]struct{}
	// fullTableUpdateLockTargets contains only the exclusive targets admitted for
	// an unrestricted, single-target UPDATE after complete-keyspace and lock-order
	// checks. Planner-local metadata lets the final cardinality pass choose table
	// locks without weakening bounded UPDATE predicates into table-wide locks.
	fullTableUpdateLockTargets map[*plan.LockTarget]struct{}
	// userWindowNodes contains only WINDOW nodes produced from user
	// SELECT window expressions. Internal ROW_NUMBER windows used by correlated
	// LIMIT and DML deduplication must stay on their dedicated paths.
	userWindowNodes          map[int32]struct{}
	internalTopNWindows      map[int32]struct{}
	partitionTopNWindowNodes map[int32]struct{}
	// distinctKeyLocalPreAggs marks the first (group keys, DISTINCT key)
	// Group in Path B. It must retain local ownership so duplicate rows are
	// removed before any exchange.
	distinctKeyLocalPreAggs map[*plan.Node]struct{}
	// distinctKeyShuffleCols marks the second pair Group and the DISTINCT-key
	// column that owns its exchange. Both maps are planner-local; HashMapStats
	// carries the final physical decision after shuffle planning.
	distinctKeyShuffleCols map[*plan.Node]int32

	// ftJoinServed records the MATCHes rewritten while applyIndices walked a JOIN's children,
	// paired with the fulltext node producing each score. applyIndices recurses children
	// first, so those scans already exist when the PROJECT above the join is visited -- but
	// that PROJECT is a different call frame and gets no return value from them. A MATCH in
	// its select list is resolved against this.
	//
	// Never reset, and it does not need to be: a QueryBuilder is built per statement, and
	// within one build every binding tag is unique, so an entry can only ever be matched by a
	// MATCH on the very table instance it came from -- steps and subqueries cannot collide.
	// If a builder is ever reused across statements, this must be cleared with it.
	ftJoinServed []fulltextServedMatch

	tag2Table  map[int32]*TableDef
	tag2NodeID map[int32]int32

	nextBindTag      int32
	nextMsgTag       int32
	nextSQLUdfCallID uint64

	isPrepareStatement     bool
	mysqlCompatible        bool
	mysqlFullGroupByCompat bool
	// boolSumAvgCompat is the ENABLE_BOOL_SUMAVG sql_mode, resolved once per
	// builder like the two flags above so every bind path (direct, HAVING,
	// window, PREPARE) reads the same decision.
	boolSumAvgCompat      bool
	isForUpdate           bool // if it's a query plan for update
	isRestore             bool
	isRestoreByTs         bool
	isSkipResolveTableDef bool
	skipStats             bool
	isInsertIgnore        bool             // INSERT IGNORE: over-length CHAR/VARCHAR writes are truncated instead of rejected
	deleteNode            map[uint64]int32 //delete node in this query. key is tableId, value is the nodeId of sinkScan node in the delete plan

	// spill memory for aggregate function
	// jsonProbeFtNodes marks the fulltext index-scan nodes built for a json
	// PROBE — a prefilter the optimizer injected, not a user MATCH. Their score
	// is a constant, so the passes that rank by relevance must skip them, and
	// they sit under a GROUP BY that does not re-expose the scan's columns.
	jsonProbeFtNodes map[int32]bool

	aggSpillMem int64

	// spill memory for join
	joinSpillMem int64

	// spill memory for sort / merge order
	sortSpillMem int64

	optimizerHints *OptimizerHints
	// sqlCalcFoundRows disables limit pushdown that would otherwise stop a
	// source before the complete result count can be observed.
	sqlCalcFoundRows bool
	// sessionSelectLimitMayStopEarly records a finite ordinary
	// sql_select_limit or a dynamic prepared one. Such a top-level cap is
	// materialized only after optimization and therefore cannot appear in the
	// logical drain-witness walk.
	sessionSelectLimitMayStopEarly bool

	// optimizationHistory records key optimization steps for debugging remap errors
	// Only records when optimizations actually change the plan structure
	optimizationHistory []string

	// groupingSetCandidates are internally generated UNION ALL branches whose
	// common input can be shared after CTE reuse has established any nested
	// producer boundaries.
	groupingSetCandidates []groupingSetCandidate
	// sharedMaterializationMemoryBytes and sharedMaterializationSpillBytes are
	// the conservative cumulative reservations made by planner-introduced CTE
	// and grouping-set sources. They prevent individually valid rewrites from
	// jointly exceeding explicit statement caps.
	sharedMaterializationMemoryBytes float64
	sharedMaterializationSpillBytes  float64

	// Irregular index (IVF/fulltext) synchronous maintenance for the modern DML
	// path. The modern dedup+MULTI_UPDATE handles the base table and regular
	// indexes (1:1 row mapping); irregular indexes need computed 1:N maintenance
	// (tokenize / nearest-centroid) that cannot fit the UpdateCtx model. So the
	// new-row image is materialized into irregularMaintSourceStep, and the
	// maintenance sub-plans are appended after createQuery() (post-optimizer
	// form), mirroring how regular insert maintenance is built.
	//
	// For ON DUPLICATE KEY UPDATE the conflicting rows must also drop their old
	// index entries: irregularMaintDeleteStep holds the old-row image (keyed by
	// the immutable PK) from which delete sub-plans are built. It is -1 (unset)
	// for plain INSERT/LOAD where no old rows exist.
	irregularMaintSourceStep int32
	irregularMaintDeleteStep int32
	// irregularMaintDeletePkPos / Typ identify, inside the materialized maintenance
	// step, the base-table PK column the stale index entries are keyed by. For ODKU
	// this is the (immutable) final PK; for REPLACE it is the matched old row's PK,
	// which can differ from the new PK when the conflict is on a non-PK unique key.
	irregularMaintDeletePkPos int32
	irregularMaintDeletePkTyp plan.Type
	irregularMaintIndexes     []*plan.IndexDef
	// irregularMaintInsertOnlyIndexes are logical irregular indexes whose parts
	// cannot change in an ODKU conflict. Their insert maintenance reads only
	// non-conflicting rows from irregularMaintInsertOnlySourceStep; delete
	// maintenance is intentionally absent.
	irregularMaintInsertOnlySourceStep int32
	irregularMaintInsertOnlyIndexes    []*plan.IndexDef
	irregularMaintTableDef             *plan.TableDef
	irregularMaintObjRef               *plan.ObjectRef
	irregularMaintSkipInsert           bool
	irregularUpdateMaints              []irregularUpdateMaintenance

	// DML RETURNING consumes an attempt-local row image from a dedicated sink.
	// The mutation plan and the returning projection use independent SINK_SCAN
	// readers, so index/FK side-effect branches cannot multiply returned rows.
	returningSourceStep int32
	// returningFilterPos identifies an optional semantic eligibility selector in
	// the materialized row image. It filters only the RETURNING reader; mutation
	// readers continue to consume implicit FK action rows.
	returningFilterPos int32
	returningRequested bool
	returningTableDef  *plan.TableDef
	returningObjRef    *plan.ObjectRef
	returningTableName string
	returningAlias     string
	returningColPos    map[string]int32
	// updateParentActionStack bounds recursive ON UPDATE actions by the active
	// physical-table path. Acyclic multi-layer cascades recurse normally; a
	// cycle is rejected before any mutation step is appended.
	updateParentActionStack map[uint64]int
	// updateAffectedRowsCols records selector columns added while self-referencing
	// FK action rows are folded into a root UPDATE stream. The physical writer
	// consumes every row, but SQL affected-row accounting includes only rows
	// selected by the original statement.
	updateAffectedRowsCols map[uint64]updateAffectedRowsColumn
	// insertInputKeysUnique is set while binding a plain INSERT ... SELECT when
	// the source primary key proves uniqueness of the target primary-key key.
	// It is consumed only by the target-PK DEDUP node; secondary unique-index
	// DEDUP nodes retain their existing duplicate-detection semantics.
	insertInputKeysUnique bool
	// sinkColRef records, per materialized step, the post-pruning column remap
	// produced by createQuery's final remapAllColRefs pass: {step, originalColPos}
	// -> newColPos. The irregular-index maintenance sub-plans are appended after
	// createQuery and read the (already column-pruned) materialized sink directly,
	// so positions recorded pre-prune (e.g. the REPLACE old-PK key) must be remapped
	// through this map before use.
	sinkColRef map[[2]int32]int

	// cteRefs contains only non-recursive CTEs that were actually bound. It is
	// populated lazily so unused CTE bodies retain their existing lazy-binding
	// semantics.
	cteRefs []*CTERef
}

type irregularUpdateMaintenance struct {
	sourceStep           int32
	deleteStep           int32
	deletePkPos          int32
	deletePkTyp          plan.Type
	indexes              []*plan.IndexDef
	insertOnlySourceStep int32
	insertOnlyIndexes    []*plan.IndexDef
	tableDef             *plan.TableDef
	objRef               *plan.ObjectRef
}

type OptimizerHints struct {
	pushDownLimitToScan        int
	pushDownTopThroughLeftJoin int
	pushDownSemiAntiJoins      int
	aggPushDown                int
	aggPullUp                  int
	removeEffectLessLeftJoins  int
	removeRedundantJoinCond    int
	optimizeLikeExpr           int
	optimizeDateFormatExpr     int
	determineHashOnPK          int
	sendMessageFromTopToScan   int
	determineShuffle           int
	blockFilter                int
	applyIndices               int
	runtimeFilter              int
	joinOrdering               int
	forceOneCN                 int
	execType                   int
	disableRightJoin           int
	disableRightSingleRF       int
	sharedComputation          int
	subqueryPredicatePlanning  int
	printShuffle               int
	skipDedup                  int
	outerAntiPlanning          int
}

type CTERef struct {
	isRecursive    bool
	ast            *tree.CTE
	maskedCTEs     map[string]bool
	snapshot       *Snapshot
	declarationCtx *BindContext
	occurrences    []cteOccurrence
	hasNestedRef   bool
	hasNestedUse   bool
}

type cteOccurrence struct {
	rootID       int32
	rootTag      int32
	ctx          *BindContext
	headings     []string
	types        []plan.Type
	isCorrelated bool
}

type CteBindState struct {
	cte                    *CTERef
	cteBindType            int
	recScanNodeId          int32
	recursiveRefQueryBlock *BindContext
}

func (state CteBindState) masked(name string) bool {
	if state.cte == nil {
		return false
	} else {
		_, ok := state.cte.maskedCTEs[name]
		return ok
	}
}

const (
	// does not bind cte currently
	CteBindTypeNone = 0
	// bind initial select stmt of recursive cte currently
	CteBindTypeInitStmt = 1
	// bind recursive parts of recursive cte currently
	CteBindTypeRecurStmt = 2
	// bind non recursive cte currently
	CteBindTypeNonRecur = 3
)

type aliasItem struct {
	idx     int32
	astExpr tree.Expr
}

type orderResolutionMetadata struct {
	bindAsts          []tree.Expr
	semanticKeysByTag map[int32][]string
}

type BindContext struct {
	binder Binder

	// outputColumnProvenance records planner-local source or pure-NULL identity
	// by output position. An explicit None prevents later transparent-boundary
	// code from rediscovering metadata after a semantic boundary has cleared it.
	outputColumnProvenance map[int32]OutputColumnProvenance

	// mysqlSpecialOrderTypes records the storage type behind a visible ENUM/SET
	// display value.  It is planner-local semantic provenance: only a pure
	// display projection (or a pure column passthrough of one) may populate it.
	// A present key with a nil value explicitly suppresses provenance when a
	// multi-input construct proves the originating display contract unsafe.
	// The generated plan consumes the provenance by materializing an ordinary
	// numeric sort expression, so this metadata never crosses the plan wire.
	mysqlSpecialOrderTypes map[int32]*plan.Type
	// mysqlSpecialCanonicalTypes records outputs whose SQL-visible value has
	// already passed through GROUP BY or DISTINCT and must be canonically
	// re-encoded when a persisted View exposes an ENUM/SET catalog type.
	mysqlSpecialCanonicalTypes map[int32]*plan.Type
	// restoreViewMySQLSpecialTypes is inherited only while rebinding a persisted
	// View. It lets transparent derived/CTE query boundaries expose their raw
	// ENUM/SET values without changing ordinary query-boundary behavior.
	restoreViewMySQLSpecialTypes bool
	// mysqlSpecialRawProjectPositions maps a visible output position to a hidden
	// raw ENUM/SET sidecar in the query block's PROJECT. It is populated only
	// for row-preserving View ORDER BY boundaries.
	mysqlSpecialRawProjectPositions map[int32]int32

	//cteByName saves all cte definitions in the current stmt
	cteByName map[string]*CTERef
	//cteState records state of binding cte
	cteState                     CteBindState
	sliding                      bool
	explicitSliding              bool
	isDistinct                   bool
	normalizeGroupingSetDistinct bool
	// groupingSetOrderHiddenCount marks the generated ORDER BY projections at
	// the tail of a grouping-set branch select list. They are qualified after
	// FROM binding with source-column-first ORDER BY semantics.
	groupingSetOrderHiddenCount int
	// groupingSetOrderAliases carries the original select-list expressions for
	// generated hidden ORDER BY projections. Unlike normal branch projections,
	// those expressions use source-column-first alias fallback semantics.
	groupingSetOrderAliases map[string][]tree.Expr
	// groupingSetOrderSourceProbes resolves names whose presence cannot be known
	// until the generated branch has bound its FROM scope.
	groupingSetOrderSourceProbes map[string]*tree.GroupingSetOrderSourceProbe
	// preserveOrderSemanticKeys retains source-scope projection identities for
	// a grouping-set branch whose UNION output otherwise loses that identity.
	preserveOrderSemanticKeys bool
	isCorrelated              bool
	hasSingleRow              bool
	isGroupingSet             bool
	groupingFuncAllowed       bool

	//cteName denotes the alias of this BindContext.
	//it may be from view name, cte name or subquery name
	cteName string
	//cte in binding or bound already
	boundCtes map[string]*CTERef
	headings  []string

	// captureViewStarExpansion is enabled only while binding a CREATE/ALTER
	// VIEW definition. Ordinary SELECT planning must not clone its select list
	// just to support view metadata persistence.
	captureViewStarExpansion bool
	// expandedSelectLists records the expanded output for each SELECT clause
	// participating in a view definition, including UNION branches.
	expandedSelectLists map[*tree.SelectClause]tree.SelectExprs

	groupTag     int32
	aggregateTag int32
	projectTag   int32
	resultTag    int32
	sinkTag      int32
	windowTag    int32
	timeTag      int32
	sampleTag    int32

	groups     []*plan.Expr
	aggregates []*plan.Expr
	projects   []*plan.Expr
	results    []*plan.Expr
	windows    []*plan.Expr
	times      []*plan.Expr

	// pendingAggregateQuery is set after the pre-aggregate FROM/JOIN/WHERE/GROUP
	// BY clauses are bound and before HAVING, projection, and ORDER BY. At that
	// point SELECT/HAVING/ORDER BY aggregates may not have been appended to
	// aggregates yet, but the query block is already an implicit aggregate query
	// for ONLY_FULL_GROUP_BY correlation checks.
	pendingAggregateQuery bool

	// timeBoundaryType is the public type for _wstart/_wend. It is filled once
	// the time-window grouping key is bound, before the SELECT projection binds
	// boundary column references.
	timeBoundaryType *plan.Type

	groupByAst          map[string]int32
	groupByCanonicalAst map[string]int32
	groupByParamAst     map[string]int32
	// sampleGroupByAst retains the logical identity of stable GROUP BY
	// literals removed from the physical key. SAMPLE must still reject those
	// expressions even though ordinary projection binding should see literals.
	sampleGroupByAst       map[string]struct{}
	aggregateByAst         map[string]int32
	sampleByAst            map[string]int32
	windowByAst            map[string]int32
	projectByExpr          map[string]int32
	timeByAst              map[string]int32
	whereFilters           []*plan.Expr
	volatileExprMemoID     int32
	flattenedVolatileExprs map[int32]*plan.Expr
	// gapFillWhereFilters preserves the complete bound WHERE tree before
	// subqueries are flattened into joins. Bounded GAPFILL inference must see
	// every timestamp predicate, including IN/ANY/ALL subquery operands.
	gapFillWhereFilters []*plan.Expr

	projectColByAst map[string]int32

	projectByAst []SelectField
	// projectSemanticKeys is populated only when preserveOrderSemanticKeys is
	// set, keeping the ordinary-query projection path allocation-free.
	projectSemanticKeys []string
	// orderResolution is allocated only for generated ROLLUP/CUBE window
	// boundaries that must preserve output AST categories and bound identity.
	orderResolution *orderResolutionMetadata

	numericProjectionTypes          []Type
	numericTableProjectionTypes     map[string][]Type
	numericTableProjectionAmbiguous map[string][]bool
	numericCteByName                map[string]*tree.CTE

	timeAsts []tree.Expr

	aliasMap       map[string]*aliasItem
	aliasFrequency map[string]int

	bindings       []*Binding
	bindingByTag   map[int32]*Binding //rel_pos
	bindingByTable map[string]*Binding
	bindingByCol   map[string]*Binding
	// outerUsingCols maps an unqualified column name to the ordered list of
	// leaf tables whose values must be COALESCEd to produce the merged value.
	// Only populated when the column has been merged through at least one
	// FULL OUTER JOIN ... USING. Length is always >= 2 when present.
	outerUsingCols map[string][]string
	// sqlUdfArgs holds the already-bound arguments of the SQL UDF currently
	// being expanded in this query block. The UDF body uses body-unique marker
	// names for its $n parameters; resolving those markers from a child query
	// block turns the argument's column references into correlated references.
	sqlUdfArgs map[string]*plan.Expr

	// for join tables
	bindingTree *BindingTreeNode

	parent *BindContext
	// queryBlockOwner identifies the SELECT that owns this context. Structural
	// contexts created while binding one FROM clause inherit the owner, while a
	// nested SELECT replaces it when bindSelect starts.
	queryBlockOwner *BindContext
	// aggregateInputParent is set on a subquery context when that subquery is
	// bound as an aggregate argument of its parent query. Correlations back to
	// this parent are per-row aggregate inputs, not bare aggregate-query output
	// columns for ONLY_FULL_GROUP_BY validation.
	aggregateInputParent *BindContext

	defaultDatabase string

	// sample function related.
	sampleFunc SampleFuncCtx

	snapshot *Snapshot
	// all view keys(dbName#viewName)
	views []string
	//view in binding or already bound
	boundViews map[[2]string]*tree.CreateView
	// viewChain tracks view lineage for the current bind context.
	viewChain []string
	// directView tracks the outermost view referenced by the user.
	directView string

	// lower is sys var lower_case_table_names
	lower int64

	groupingFlag []bool

	remapOption *tree.RewriteOption
}

// groupOutputType describes a group key after aggregation. A grouping-set
// branch emits a synthetic NULL for every inactive key, independent of the
// source expression's nullability.
func (bc *BindContext) groupOutputType(groupPos int32) Type {
	typ := bc.groups[groupPos].Typ
	if groupPos >= 0 && int(groupPos) < len(bc.groupingFlag) && !bc.groupingFlag[groupPos] {
		typ.NotNullable = false
	}
	return typ
}

func groupingFlagOutputType(typ Type, groupingFlag []bool, groupPos int32) Type {
	if groupPos >= 0 && int(groupPos) < len(groupingFlag) && !groupingFlag[groupPos] {
		typ.NotNullable = false
	}
	return typ
}

type SelectField struct {
	ast tree.Expr
	// AsName is alias name for Expr
	aliasName string
	pos       int32
}

type NameTuple struct {
	table string
	col   string
	// coalesceArms is non-empty (len >= 2) only for FOJ-USING merged columns:
	// the ordered list of contributing leaf-table names so star-expansion at
	// this join node emits COALESCE(arm1.col, ..., armN.col) without consulting
	// the bind-context-wide outerUsingCols map (which is shared across sibling
	// subtrees and so cannot disambiguate two FOJ-USING(c) trees joined at the
	// same level).
	coalesceArms []string
}

type BindingTreeNode struct {
	using []NameTuple

	binding *Binding

	left  *BindingTreeNode
	right *BindingTreeNode
}

type Binder interface {
	BindExpr(tree.Expr, int32, bool) (*plan.Expr, error)
	BindColRef(*tree.UnresolvedName, int32, bool) (*plan.Expr, error)
	BindAggFunc(string, *tree.FuncExpr, int32, bool) (*plan.Expr, error)
	BindWinFunc(string, *tree.FuncExpr, int32, bool) (*plan.Expr, error)
	BindSubquery(*tree.Subquery, bool) (*plan.Expr, error)
	BindTimeWindowFunc(string, *tree.FuncExpr, int32, bool) (*plan.Expr, error)
	GetContext() context.Context
}

type baseBinder struct {
	sysCtx                           context.Context
	builder                          *QueryBuilder
	ctx                              *BindContext
	impl                             Binder
	boundCols                        []boundColumn
	numericParamType                 *Type
	numericSubqueryTarget            *Type
	numericFunctionTarget            bool
	mysqlSpecialTargetType           *Type
	allowCanonicalNameConstValueCast bool
	bindRawMySQLSpecialType          bool
	subqueryInAggregateInput         bool
	aggregateInputCorrelation        bool
}

type boundColumn struct {
	name      string
	relation  int32
	columnPos int32
}

type DefaultBinder struct {
	baseBinder
	typ  Type
	cols []string
}

// ReplaceValueBinder binds the RHS value expressions of a `REPLACE ... SET`
// statement. MySQL evaluates an RHS reference to a target-table column as
// DEFAULT(col), so this binder resolves every column reference to that
// column's default expression instead of an actual row value.
//
// The typ field carries the destination column type so that literal values
// (especially DECIMAL / scientific-notation) bind with the same precision as
// DefaultBinder. BindExpr delegates to baseBindExpr which uses this typ to
// drive type-aware numeric binding.
type ReplaceValueBinder struct {
	baseBinder
	typ      plan.Type
	tableDef *plan.TableDef
}

type UpdateBinder struct {
	baseBinder
	cols []*ColDef
}

type OndupUpdateBinder struct {
	baseBinder
	scanTag             int32
	selectTag           int32
	tableDef            *plan.TableDef
	targetDBName        string
	targetTableName     string
	lowerCaseTableNames int64
}

type TableBinder struct {
	baseBinder
	allowSubquery bool
}

type WhereBinder struct {
	baseBinder
}

type GroupBinder struct {
	baseBinder
	selectList        tree.SelectExprs
	projectionExprPos int32
}

type HavingBinder struct {
	baseBinder
	insideAgg             bool
	bindingProjectedAlias bool
	rollupHaving          bool
	bindingHaving         bool
}

type ProjectionBinder struct {
	baseBinder
	havingBinder      *HavingBinder
	numericTargetType *Type
}

type OrderBinder struct {
	*ProjectionBinder
	selectList     tree.SelectExprs
	distinctBinder *distinctOrderBinder
}

type LimitBinder struct {
	baseBinder
	isOffset bool // true when binding OFFSET value, false when binding LIMIT count
}

type PartitionBinder struct {
	baseBinder
}

// SetBinder for 'set @var = expr'
type SetBinder struct {
	baseBinder
}

var _ Binder = (*TableBinder)(nil)
var _ Binder = (*WhereBinder)(nil)
var _ Binder = (*GroupBinder)(nil)
var _ Binder = (*HavingBinder)(nil)
var _ Binder = (*ProjectionBinder)(nil)
var _ Binder = (*LimitBinder)(nil)
var _ Binder = (*UpdateBinder)(nil)
var _ Binder = (*OndupUpdateBinder)(nil)
var _ Binder = (*ReplaceValueBinder)(nil)

var Sequence_cols_name = []string{"last_seq_num", "min_value", "max_value", "start_value", "increment_value", "cycle", "is_called"}

const (
	NotFound      int32 = math.MaxInt32
	AmbiguousName int32 = math.MinInt32
)

type Binding struct {
	tag     int32
	nodeId  int32
	db      string
	table   string
	tableID uint64
	// lower case: used for binding/lookup
	cols []string
	// original case: only for SELECT * display, must be same length as cols (or empty)
	originCols  []string
	colIsHidden []bool
	types       []*plan.Type
	// mysqlSpecialOrderTypes is aligned with cols. A non-nil entry means that
	// the string column is a pure display of the recorded ENUM/SET storage
	// type, and may therefore use definition-order semantics when ordered.
	mysqlSpecialOrderTypes []*plan.Type
	// mysqlSpecialCanonicalTypes is aligned with cols and propagates the
	// post-semantic canonical-value contract through transparent bindings.
	mysqlSpecialCanonicalTypes []*plan.Type
	// outputColumnProvenance is aligned with cols and carries planner-local
	// source or pure-NULL output identity. It is never serialized into the plan.
	outputColumnProvenance []OutputColumnProvenance
	refCnts                []uint
	// lower case
	colIdByName    map[string]int32
	isClusterTable bool
	defaults       []string
}

const (
	maxLengthOfTableComment  int = 2048
	maxLengthOfColumnComment int = 1024
)

// fuzzy filter need to get partial unique key attrs name and its origin table name
// for Decimal type, we need colDef to get the scale
type OriginTableMessageForFuzzy struct {
	ParentTableName  string
	ParentUniqueCols []*ColDef
}

type MultiTableIndex struct {
	IndexAlgo       string
	IndexAlgoParams string
	IndexDefs       map[string]*plan.IndexDef
}

type RemapInfo struct {
	step           int32
	node           *plan.Node
	tip            string
	colRefCnt      map[[2]int32]int
	colRefBool     map[[2]int32]bool
	sinkColRef     map[[2]int32]int
	remapping      *ColRefRemapping
	interRemapping *ColRefRemapping
	srcExprIdx     int
}

func (info *RemapInfo) String() string {
	if info == nil {
		return "empty RemapInfo"
	}

	sb := strings.Builder{}
	sb.WriteString("colRefCnt:")
	for k, v := range info.colRefCnt {
		sb.WriteString(fmt.Sprintf("[%v : %v]", k, v))
	}
	sb.WriteString("colRefBool:")
	for k, v := range info.colRefBool {
		sb.WriteString(fmt.Sprintf("[%v : %v]", k, v))
	}
	sb.WriteString("sinkColRef:")
	for k, v := range info.sinkColRef {
		sb.WriteString(fmt.Sprintf("[%v : %v]", k, v))
	}
	sb.WriteString(info.remapping.String())
	sb.WriteString(info.interRemapping.String())

	return fmt.Sprintf(
		"step %d nodeId %d nodeType %s tip %s "+
			"%s "+
			"srcExprIdx %d ",
		info.step,
		info.node.NodeId,
		info.node.NodeType,
		info.tip,
		sb.String(),
		info.srcExprIdx,
	)
}
