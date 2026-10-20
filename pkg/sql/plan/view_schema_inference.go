// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// viewSchemaSubscriptionContext keeps publisher identity separate from the
// View's creation environment. Resolve supplies the publisher schema; a saved
// no-USE (or different) default database cannot identify that publication.
func viewSchemaSubscriptionContext(ctx context.Context, obj *ObjectRef, previous *SubscriptionMeta, defaultDatabase string) (*SubscriptionMeta, string, error) {
	if obj.SchemaName == "" || obj.SubscriptionName == "" {
		return nil, "", moerr.NewInvalidInput(ctx, "subscription View has no resolved publisher database or subscription name")
	}
	subscription := previous
	if subscription == nil || subscription.AccountId != obj.PubInfo.TenantId ||
		subscription.DbName != obj.SchemaName || subscription.SubName != obj.SubscriptionName {
		subscription = &SubscriptionMeta{
			AccountId: obj.PubInfo.TenantId, DbName: obj.SchemaName,
			SubName: obj.SubscriptionName, Tables: pubsub.TableAll,
		}
	}
	return subscription, defaultDatabase, nil
}

type viewSchemaContextKey struct{}
type viewSchemaDerivation struct {
	rootAuthorizationErr                                error
	requiredProtocol                                    int64
	memoFrames                                          []*viewSchemaMemoFrame
	inputBytes                                          int
	lower                                               int64
	leases                                              []*process.ExecutionTransientMemoryReservation
	memoEligible                                        bool
	planning                                            bool
	planDependencies                                    []*ObjectRef
	dependencyLog                                       []ViewDependency
	memoScans                                           map[*QueryBuilder]map[int32]bool
	request                                             *ViewSchemaRequest
	compiler                                            *viewSchemaCompiler
	capture                                             *viewDependencyCaptureContext
	stack                                               map[[32]byte]bool
	work, slots, depth, maxDepth, operations, recursion int
}

// Internal catalog statements keep the caller's cancellation and values, but
// must not inherit the outer statement's private binding/authorization scope.
type viewSchemaCatalogContext struct{ context.Context }

func (c viewSchemaCatalogContext) Value(key any) any {
	if _, ok := key.(viewSchemaContextKey); ok {
		return nil
	}
	return c.Context.Value(key)
}

// ViewSchemaCatalogContext isolates trusted compiler catalog SQL from a user
// statement's private binding scope. It preserves cancellation, account and
// snapshot values. It must not be used to bypass authorization for user SQL.
func ViewSchemaCatalogContext(ctx context.Context) context.Context {
	if ctx.Value(viewSchemaContextKey{}) == nil {
		return ctx
	}
	return viewSchemaCatalogContext{ctx}
}

// Optional metadata inference may discard lookup errors. A denied user root
// must remain denied for this derivation, even if inference attempts it again.
// Successful checks are not cached; each user catalog action is authorized.
func (s *viewSchemaDerivation) authorizeRoot(database, name string, snapshot *Snapshot) error {
	if s.rootAuthorizationErr == nil {
		// Authorization can execute catalog SQL; do not expose our private
		// planning marker to that independent trusted query.
		s.rootAuthorizationErr = s.request.binding.Authorize(s.request.workCtx, database, name, snapshot)
	}
	return s.rootAuthorizationErr
}

type viewSchemaCompiler struct {
	CompilerContext
	ctx, previousContext context.Context
	state                *viewSchemaDerivation
}

func newViewSchemaDerivation(request *ViewSchemaRequest) *viewSchemaDerivation {
	state := &viewSchemaDerivation{lower: request.binding.Compiler.GetLowerCaseTableNames(), request: request, stack: make(map[[32]byte]bool)}
	ctx := mysql.WithParseLimits(request.workCtx, mysql.ParseLimits{Input: viewSchemaInputLimit, Tokens: 32768, Work: 64 << 20})
	compiler := &viewSchemaCompiler{CompilerContext: request.binding.Compiler, state: state, ctx: context.WithValue(ctx, viewSchemaContextKey{}, state)}
	compiler.previousContext = compiler.CompilerContext.GetContext()
	compiler.CompilerContext.SetContext(compiler.ctx)
	state.compiler = compiler
	state.capture = newViewDependencyCaptureContext(compiler.CompilerContext)
	return state
}
func (c *viewSchemaCompiler) getInternalSQLExecutor(proc *process.Process) (executor.SQLExecutor, bool) {
	if provider, ok := c.CompilerContext.(interface {
		getInternalSQLExecutor(*process.Process) (executor.SQLExecutor, bool)
	}); ok {
		return provider.getInternalSQLExecutor(proc)
	}
	return nil, false
}

func (c *viewSchemaCompiler) GetLowerCaseTableNames() int64 { return c.state.lower }
func (c *viewSchemaCompiler) GetContext() context.Context   { return c.ctx }
func (c *viewSchemaCompiler) SetContext(ctx context.Context) {
	c.ctx = ctx
	c.CompilerContext.SetContext(ctx)
}

// Database existence must use the same publisher/snapshot domain as source
// resolution. The bool-only legacy probe can both lose storage errors and
// switch back to the subscriber before binding a qualified nested View.
func (c *viewSchemaCompiler) CheckViewDatabase(name string, snapshot *Snapshot) (bool, error) {
	_, err := c.CompilerContext.GetDatabaseId(name, snapshot)
	if moerr.IsMoErrCode(err, moerr.ErrBadDB) || moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
		return false, nil
	}
	return err == nil, err
}

func (c *viewSchemaCompiler) Resolve(database, name string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	if err := c.state.request.check(); err != nil {
		return nil, nil, err
	}
	if c.state.planning && c.state.depth == 0 {
		return c.resolveRoot(database, name, snapshot)
	}
	obj, def, err := c.CompilerContext.Resolve(database, name, snapshot)
	if err != nil {
		return nil, nil, err
	}
	return c.record(obj, def, snapshot, database, name)
}

// resolveRoot distinguishes the subscriber entry-point name from names inside
// persisted publisher SQL. Both paths retain the same admission and capture.
func (c *viewSchemaCompiler) resolveRoot(database, name string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	root, ok := c.CompilerContext.(interface {
		ResolveViewSchemaRoot(string, string, *Snapshot) (*ObjectRef, *TableDef, error)
	})
	if !ok {
		obj, def, err := c.CompilerContext.Resolve(database, name, snapshot)
		if err != nil {
			return nil, nil, err
		}
		return c.record(obj, def, snapshot, database, name)
	}
	if err := c.state.request.check(); err != nil {
		return nil, nil, err
	}
	obj, def, err := root.ResolveViewSchemaRoot(database, name, snapshot)
	if err != nil {
		return nil, nil, err
	}
	return c.record(obj, def, snapshot, database, name)
}
func (c *viewSchemaCompiler) ResolveById(id uint64, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	if err := c.state.request.check(); err != nil {
		return nil, nil, err
	}
	obj, def, err := c.CompilerContext.ResolveById(id, snapshot)
	if err != nil {
		return nil, nil, err
	}
	if obj == nil {
		return obj, def, nil
	}
	return c.record(obj, def, snapshot, obj.SchemaName, obj.ObjName)
}
func (c *viewSchemaCompiler) record(obj *ObjectRef, def *TableDef, snapshot *Snapshot, database, name string) (*ObjectRef, *TableDef, error) {
	if err := c.state.request.check(); err != nil {
		return nil, nil, err
	}
	if obj == nil || def == nil {
		return obj, def, nil
	}
	if def.TableType == catalog.SystemViewRel && (def.ViewSql == nil || def.ViewSql.View == "") {
		return nil, nil, moerr.NewInvalidInput(c.ctx, "view has no persisted definition")
	}
	if len(c.state.capture.deps) >= viewSchemaWorkLimit {
		return nil, nil, ErrViewSchemaLimit
	}
	// A View's physical column snapshot is compatibility data, not an input
	// to inference or its admission. Do not inspect/copy stale columns or let
	// their count reject an otherwise small, valid semantic definition.
	if def.TableType == catalog.SystemViewRel {
		def = CloneTableDefForPlan(def, false)
		def.Name2ColIndex = nil
	}
	if len(def.Cols) > MaxViewMetadataColumns {
		return nil, nil, ErrViewSchemaLimit
	}
	if def.ViewSql != nil && len(def.ViewSql.View) > viewSchemaInputLimit {
		return nil, nil, ErrViewSchemaLimit
	}
	if def.TableType == catalog.SystemViewRel {
		if err := requireViewSchemaCreationContext(c.ctx, def.ViewSql.View); err != nil {
			return nil, nil, err
		}
	}
	size := def.ProtoSize() + obj.ProtoSize()
	if snapshot != nil {
		size += snapshot.ProtoSize()
	}
	if size > 64<<20-c.state.inputBytes || len(c.state.dependencyLog) >= viewSchemaWorkLimit {
		return nil, nil, ErrViewSchemaLimit
	}
	c.state.inputBytes += size
	lease, err := c.state.request.reserve(size)
	if err != nil {
		return nil, nil, err
	}
	c.state.leases = append(c.state.leases, lease)
	// Resolve returns catalog-owned identities. The execution subtree and its
	// dependency copies must share the effective account without mutating them.
	obj = DeepCopyObjectRef(obj)
	single := newViewDependencyCaptureContext(c)
	single.snapshotNames = c.state.capture.snapshotNames
	if err := single.record(obj, def, snapshot, database, name); err != nil {
		return nil, nil, err
	}
	for _, dep := range single.deps {
		c.state.capture.deps[viewSchemaDependencyKey(dep)] = dep
		c.state.dependencyLog = append(c.state.dependencyLog, dep)
		if c.state.planning {
			account, err := c.state.request.binding.Compiler.GetAccountId()
			if err != nil {
				return nil, nil, err
			}
			if obj.PubInfo == nil && dep.AccountID != account && def.TableType != catalog.SystemViewRel {
				// Physical scans need their execution account. For Views, PubInfo
				// identifies a resolved subscription entry, not merely ownership;
				// ordinary publisher Views inherit the active binding domain.
				obj.PubInfo = &planpb.PubInfo{TenantId: int32(dep.AccountID)}
			}
			ref := prepareSchemaRefWithSnapshot(obj, def, snapshot)
			if ref.PubInfo == nil && dep.AccountID != account {
				// Dependency validation still needs every View's actual account,
				// without changing how bindView classifies its executable input.
				ref.PubInfo = &planpb.PubInfo{TenantId: int32(dep.AccountID)}
			}
			c.state.planDependencies = appendPrepareSchemas(c.state.planDependencies, ref)
		}
	}
	if def.TableType != catalog.SystemViewRel && (def.TableType != "" && def.TableType != catalog.SystemOrdinaryRel || def.IsTemporary) {
		c.state.memoEligible = false
	}
	// Ordinary binding mutates name maps, projections and external-table columns.
	// It may only mutate this request's copy, never the catalog resolver's graph.
	return obj, DeepCopyTableDef(def, true), nil
}

// SELECT/CTAS use the same logical column budget as description inference.
// inferViewColumns may wrap this compiler in its own capture context; that
// wrapper charges directly, so each bound select list is still charged once.
func (c *viewSchemaCompiler) enterNestedView() error { return c.state.request.check() }
func (c *viewSchemaCompiler) leaveNestedView()       {}
func (c *viewSchemaCompiler) chargeViewColumns(count int) error {
	return c.state.charge(0, count)
}

func (c *viewSchemaCompiler) ResolveSnapshotWithSnapshotName(name string) (*Snapshot, error) {
	snapshot, err := c.CompilerContext.ResolveSnapshotWithSnapshotName(name)
	if err == nil && snapshot != nil {
		c.state.capture.snapshotNames[snapshot.String()] = name
	}
	return snapshot, err
}
func (s *viewSchemaDerivation) charge(work, slots int) error {
	if work > viewSchemaRootWorkLimit-s.work || work > viewSchemaWorkLimit-s.request.work || slots > MaxViewMetadataColumns-s.slots {
		return ErrViewSchemaLimit
	}
	s.work += work
	s.request.work += work
	s.slots += slots
	return s.request.check()
}
func (s *viewSchemaDerivation) enter(obj *ObjectRef, def *TableDef, snapshot *Snapshot) (func(), error) {
	key := viewSchemaKey(obj, def, snapshot)
	if s.stack[key] {
		return nil, moerr.NewParseError(s.compiler.ctx, "cyclic view definition")
	}
	if s.depth >= viewSchemaDepthLimit {
		return nil, ErrViewSchemaLimit
	}
	if err := s.charge(1, 0); err != nil {
		return nil, err
	}
	s.stack[key] = true
	s.depth++
	s.maxDepth = max(s.maxDepth, s.depth)
	return func() { delete(s.stack, key); s.depth-- }, nil
}
func (s *viewSchemaDerivation) describe(database, name string, snapshot *Snapshot) (*viewSchemaMemoEntry, error) {
	// Each root starts in the caller's entry domain, including dependency
	// capture. Only a resolved published root may establish a publisher domain
	// for its stored SQL; inherited subscription state is not root identity.
	previousSub := s.compiler.GetQueryingSubscription()
	s.compiler.SetQueryingSubscription(nil)
	defer s.compiler.SetQueryingSubscription(previousSub)
	if snapshot == nil {
		snapshot = s.compiler.GetSnapshot()
	}
	obj, def, err := s.compiler.resolveRoot(database, name, snapshot)
	if err != nil {
		return nil, err
	}
	if obj == nil || def == nil {
		return nil, moerr.NewNoSuchTable(s.compiler.ctx, database, name)
	}
	if def.TableType != catalog.SystemViewRel || def.ViewSql == nil {
		return nil, moerr.NewInvalidInput(s.compiler.ctx, "object is not a view")
	}
	key := viewSchemaKey(obj, def, snapshot)
	if cached := s.request.memo[key]; !s.request.memoDisabled && cached != nil && len(cached.columns) > 0 {
		if cached.depth > viewSchemaDepthLimit {
			return nil, ErrViewSchemaLimit
		}
		if err := s.charge(cached.work, cached.slots); err != nil {
			return nil, err
		}
		s.request.hits++
		return cached, nil
	}
	leave, err := s.enter(obj, def, snapshot)
	if err != nil {
		return nil, err
	}
	defer leave()
	previous := s.compiler.GetSnapshot()
	s.compiler.SetSnapshot(DeepCopySnapshot(snapshot))
	defer s.compiler.SetSnapshot(previous)
	parsed, err := parsePersistedViewDefinition(s.compiler, def.ViewSql.View)
	if err != nil {
		return nil, err
	}
	defer parsed.free()
	if err := rejectViewSchemaUnstableStar(s.request, parsed.selectStmt); err != nil {
		return nil, err
	}
	if parsed.data.RequiredProtocolVersion != nil {
		s.observeProtocol(*parsed.data.RequiredProtocolVersion)
	}
	if obj.PubInfo != nil {
		subscription, defaultDatabase, err := viewSchemaSubscriptionContext(s.compiler.ctx, obj, previousSub, parsed.data.DefaultDatabase)
		if err != nil {
			return nil, err
		}
		s.compiler.SetQueryingSubscription(subscription)
		parsed.ctx.defaultDatabase = defaultDatabase
	}
	s.lower = parsed.ctx.lowerCaseTableNames
	s.memoEligible = transparentViewProjection(parsed.selectStmt) && obj.PubInfo == nil && s.compiler.GetQueryingSubscription() == nil
	inferred, err := inferViewColumns(parsed.ctx, parsed.selectStmt, parsed.columnNames, parsed.database, parsed.name, false)
	if err != nil {
		return nil, err
	}
	s.request.binds++
	table := &TableDef{Cols: inferred.columns}
	if table.ProtoSize() > viewSchemaResultLimit {
		return nil, ErrViewSchemaLimit
	}
	columns, err := table.Marshal()
	if err != nil {
		return nil, err
	}
	deps := s.capture.dependencies()
	sort.Slice(deps, func(i, j int) bool { return viewSchemaDependencyKey(deps[i]) < viewSchemaDependencyKey(deps[j]) })
	dependencies, err := json.Marshal(deps)
	if err != nil {
		return nil, err
	}
	provenance, err := encodeViewSchemaProvenance(inferred.provenance, inferred.columns)
	if err != nil {
		return nil, err
	}
	if len(columns)+len(dependencies)+len(provenance) > viewSchemaResultLimit {
		return nil, ErrViewSchemaLimit
	}
	value := &viewSchemaMemoEntry{columns: columns, dependencies: dependencies, provenance: provenance, requiredProtocol: max(inferred.requiredProtocol, s.requiredProtocol), work: s.work, slots: s.slots, depth: s.maxDepth}
	if err := s.request.check(); err != nil {
		return nil, err
	}
	s.request.remember(key, value)
	return value, nil
}

// Check the actual recursive binder entries. This covers parentheses, unions,
// expressions and derived tables rather than trusting AST depth estimates.
func enterViewSchemaBinding(ctx context.Context) (func(), error) {
	state, _ := ctx.Value(viewSchemaContextKey{}).(*viewSchemaDerivation)
	if state == nil {
		return func() {}, nil
	}
	if err := state.request.check(); err != nil {
		return nil, err
	}
	if state.recursion >= 256 || state.operations >= 65536 {
		return nil, ErrViewSchemaLimit
	}
	state.recursion++
	state.operations++
	return func() { state.recursion-- }, nil
}

func (s *viewSchemaDerivation) close() {
	s.compiler.CompilerContext.SetContext(s.compiler.previousContext)
	for _, lease := range s.leases {
		lease.Release()
	}
	s.leases = nil
}
func (c *viewSchemaCompiler) ResolveViewDependencyAccount(obj *ObjectRef, def *TableDef, snapshot *Snapshot) (uint32, error) {
	if resolver, ok := c.CompilerContext.(ViewDependencyIdentityResolver); ok {
		return resolver.ResolveViewDependencyAccount(obj, def, snapshot)
	}
	account, err := c.GetAccountId()
	if err != nil {
		return 0, err
	}
	if obj.PubInfo != nil {
		return uint32(obj.PubInfo.TenantId), nil
	}
	if snapshot != nil && snapshot.Tenant != nil {
		return snapshot.Tenant.TenantID, nil
	}
	return account, nil
}

func (c *viewSchemaCompiler) ResolveViewUdf(name string, args []*Expr, database string) (*function.Udf, error) {
	if resolver, ok := c.CompilerContext.(ViewUdfResolver); ok {
		return resolver.ResolveViewUdf(name, args, database)
	}
	return c.CompilerContext.ResolveUdf(name, args)
}
func (c *viewSchemaCompiler) ResolveIndexTableByRef(ref *ObjectRef, name string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	if err := c.state.request.check(); err != nil {
		return nil, nil, err
	}
	obj, def, err := c.CompilerContext.ResolveIndexTableByRef(ref, name, snapshot)
	if err != nil {
		return nil, nil, err
	}
	if obj == nil {
		return obj, def, nil
	}
	return c.record(obj, def, snapshot, obj.SchemaName, name)
}
func (c *viewSchemaCompiler) ResolveSubscriptionTableById(id uint64, sub *SubscriptionMeta) (*ObjectRef, *TableDef, error) {
	if err := c.state.request.check(); err != nil {
		return nil, nil, err
	}
	obj, def, err := c.CompilerContext.ResolveSubscriptionTableById(id, sub)
	if err != nil {
		return nil, nil, err
	}
	if obj == nil {
		return obj, def, nil
	}
	return c.record(obj, def, c.GetSnapshot(), obj.SchemaName, obj.ObjName)
}

func viewSchemaDependencyKey(dep ViewDependency) string {
	return viewDependencyKey(dep) + "\x00" + strconv.FormatInt(dep.LowerCaseTableNames, 10)
}

func (s *viewSchemaDerivation) observeProtocol(version int64) {
	s.requiredProtocol = max(s.requiredProtocol, version)
	for _, frame := range s.memoFrames {
		frame.requiredProtocol = max(frame.requiredProtocol, version)
	}
}
