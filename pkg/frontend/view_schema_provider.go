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

package frontend

import (
	"context"
	"reflect"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	plan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// ViewSchemaAuthorizer is supplied by the metadata entry point. SHOW visibility
// and query execution privileges are different contracts. There is no implicit
// allow policy, and this callback is invoked before every root catalog read.
type ViewSchemaAuthorizer func(context.Context, string, string, *plan.Snapshot) error

// Shared catalog SQL belongs to the same statement. The existing compiler's
// internal-read flag prevents it from advancing the caller's workspace boundary.
type viewSchemaCatalogReadKey struct{}

func withViewSchemaCatalogRead(ctx context.Context) context.Context {
	if active, _ := ctx.Value(viewSchemaCatalogReadKey{}).(bool); active {
		return ctx
	}
	return context.WithValue(ctx, viewSchemaCatalogReadKey{}, true)
}

type viewSchemaProvider struct {
	parent    *TxnCompilerContext
	authorize ViewSchemaAuthorizer
}

// NewViewSchemaRequest is opt-in and must be used by the statement owner, after
// its transaction/snapshot has been established. It never starts a transaction.
// Parent session mutation must be serialized by that owner; the request's gate
// is not a substitute for the session's statement execution protocol.
func (tcc *TxnCompilerContext) NewViewSchemaRequest(ctx context.Context, authorize ViewSchemaAuthorizer) *plan.ViewSchemaRequest {
	return plan.NewViewSchemaRequest(ctx, &viewSchemaProvider{parent: tcc, authorize: authorize})
}

type catalogStampReader interface {
	CatalogReadStamp() (client.CatalogReadStamp, error)
}
type catalogVisibilityReader interface{ CatalogVisibility() (uint64, bool) }

func (p *viewSchemaProvider) OpenViewSchemaBinding(ctx context.Context) (result *plan.ViewSchemaBinding, err error) {
	if p == nil || p.parent == nil || p.authorize == nil {
		return nil, moerr.NewInternalError(ctx, "missing view schema owner or authorizer")
	}
	parent := p.parent
	parent.mu.Lock()
	exec := parent.execCtx
	parent.mu.Unlock()
	if exec == nil || exec.proc == nil {
		return nil, moerr.NewInternalError(ctx, "view schema requires an active statement")
	}
	ses, ok := exec.ses.(*Session)
	if !ok || ses.GetTxnHandler() == nil {
		return nil, moerr.NewInternalError(ctx, "view schema requires a user statement session")
	}
	proc := exec.proc
	op := ses.GetTxnHandler().GetTxn()
	stampReader, ok := op.(catalogStampReader)
	if !ok {
		return nil, moerr.NewInternalError(ctx, "transaction does not expose a catalog read stamp")
	}
	stamp, err := stampReader.CatalogReadStamp()
	if err != nil {
		return nil, err
	}
	workspace, ok := stamp.Workspace.(catalogVisibilityReader)
	if !ok {
		return nil, moerr.NewInternalError(ctx, "workspace does not expose catalog visibility")
	}
	visibility, stable := workspace.CatalogVisibility()
	if !stable {
		return nil, plan.ErrViewSchemaChanged
	}
	generation, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return nil, err
	}
	identity := ses.GetTenantInfo()
	if identity == nil {
		return nil, moerr.NewInternalError(ctx, "missing view schema identity")
	}
	identity = identity.Copy()
	statementID := proc.GetStmtProfile().GetStmtId()
	protocol := currentProtocolVersion(proc)
	floor, floorPresent := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	accountID := ses.GetAccountId()
	tempVersion, ddlVersion := ses.GetTempTableVersion(), ses.getDDLVersion()
	roleGeneration := ses.GetPrivilegeCache().getActiveRoleGrantGeneration()
	defaultDatabase := parent.DefaultDatabase()
	compiler, cleanup, err := parent.NewViewDescriptionCompilerContext(withViewSchemaCatalogRead(withResolveUdfInCallerTxn(ctx)))
	if err != nil {
		return nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			cleanup()
		}
	}()
	child := compiler.(*TxnCompilerContext)
	child.viewSchemaRead = true
	if err = child.GetProcess().BorrowViewSchemaResources(proc, generation); err != nil {
		return nil, err
	}
	variables, err := freezeViewSchemaVariables(ses, child.execCtx)
	if err != nil {
		return nil, err
	}
	environmentLease, err := generation.ReserveTransientMemory(uint64(variables.bytes))
	if err != nil {
		return nil, err
	}
	closeChild := cleanup
	cleanup = func() { environmentLease.Release(); closeChild() }
	frozen := &viewSchemaCompilerContext{TxnCompilerContext: child, variables: variables, database: defaultDatabase, stats: plan.NewStatsCache()}
	childProc := child.GetProcess()
	childProc.GetSessionInfo().DefaultWeekFormat = proc.GetSessionInfo().DefaultWeekFormat
	childProc.GetSessionInfo().DefaultWeekFormatSet = proc.GetSessionInfo().DefaultWeekFormatSet
	childProc.GetSessionInfo().MaxErrorCount = proc.GetSessionInfo().MaxErrorCount
	childProc.GetSessionInfo().MaxErrorCountSet = proc.GetSessionInfo().MaxErrorCountSet
	childProc.GetSessionInfo().CompilerContext = frozen
	childProc.GetSessionInfo().SqlHelper = &viewSchemaSQLHelper{compiler: frozen}
	childProc.SetResolveVariableFunc(frozen.ResolveVariable)
	childProc.SetResolveVariableTypeFunc(frozen.ResolveVariableType)
	childProc.SetResolveVariableIsBinFunc(frozen.ResolveVariableIsBin)
	childProc.SetResolveVariableStringDomainFunc(frozen.ResolveVariableStringDomain)
	childProc.SetResolveVariablePrepareParamKindFunc(frozen.ResolveVariablePrepareParamKind)
	check := func() error {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		parent.mu.Lock()
		same := parent.execCtx == exec && parent.execCtx.proc == proc
		parent.mu.Unlock()
		currentFloor, currentFloorPresent := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
		if ses.GetAccountId() != accountID || floorPresent != currentFloorPresent || !reflect.DeepEqual(floor, currentFloor) || !proc.UsesExecutionResourceGeneration(generation) || !childProc.UsesExecutionResourceGeneration(generation) {
			return plan.ErrViewSchemaChanged
		}
		if !same || generation.Closed() || ses.GetTxnHandler().GetTxn() != op || proc.GetStmtProfile().GetStmtId() != statementID || currentProtocolVersion(proc) != protocol || ses.GetTempTableVersion() != tempVersion || ses.getDDLVersion() != ddlVersion || ses.GetPrivilegeCache().getActiveRoleGrantGeneration() != roleGeneration {
			return plan.ErrViewSchemaChanged
		}
		currentIdentity := ses.GetTenantInfo()
		if currentIdentity == nil || !reflect.DeepEqual(identity, currentIdentity.Copy()) {
			return plan.ErrViewSchemaChanged
		}
		current, stampErr := stampReader.CatalogReadStamp()
		if stampErr != nil {
			return stampErr
		}
		if current.TransactionID != stamp.TransactionID || current.Revision != stamp.Revision || current.Workspace != stamp.Workspace || !reflect.DeepEqual(current.Snapshot, stamp.Snapshot) {
			return plan.ErrViewSchemaChanged
		}
		currentVisibility, currentStable := workspace.CatalogVisibility()
		if !currentStable || currentVisibility != visibility {
			return plan.ErrViewSchemaChanged
		}
		return nil
	}
	if err = check(); err != nil {
		return nil, err
	}
	result = &plan.ViewSchemaBinding{Compiler: frozen, Generation: generation, Authorize: func(ctx context.Context, database, name string, snapshot *plan.Snapshot) error {
		return p.authorize(withViewSchemaCatalogRead(ctx), database, name, snapshot)
	}, Check: check, Close: cleanup}
	transferred = true
	return result, nil
}

// This small private session is used only by the existing variable resolvers.
// Catalog, transaction, privileges and I/O continue to belong to the compiler
// child; no second session transaction or SQL parser is created.
const viewSchemaEnvironmentLimit = 16 << 20

type viewSchemaVariables struct {
	*TxnCompilerContext
	bytes int
}

func freezeViewSchemaVariables(ses *Session, source *ExecCtx) (*viewSchemaVariables, error) {
	remaining := viewSchemaEnvironmentLimit
	frozen := &Session{feSessionImpl: feSessionImpl{service: ses.service}, userDefinedVars: make(map[string]*UserDefinedVar)}
	cloneSystem := func(source *SystemVariables) (*SystemVariables, error) {
		if source == nil {
			return nil, nil
		}
		source.mu.Lock()
		defer source.mu.Unlock()
		for name, value := range source.mp {
			remaining -= len(name) + 128 + viewSchemaVariableBytes(value)
			if remaining < 0 {
				return nil, plan.ErrViewSchemaLimit
			}
		}
		copy := &SystemVariables{mp: make(map[string]interface{}, len(source.mp))}
		for name, value := range source.mp {
			if bytes, ok := value.([]byte); ok {
				value = slices.Clone(bytes)
			}
			copy.mp[name] = value
		}
		return copy, nil
	}
	var err error
	if frozen.sesSysVars, err = cloneSystem(ses.GetSessionSysVars()); err != nil {
		return nil, err
	}
	if frozen.gSysVars, err = cloneSystem(ses.GetGlobalSysVars()); err != nil {
		return nil, err
	}
	ses.mu.Lock()
	for name, value := range ses.userDefinedVars {
		remaining -= len(name) + 128
		if value != nil {
			remaining -= len(value.Sql) + len(value.Type.Enumvalues) + viewSchemaVariableBytes(value.Value)
		}
		if remaining < 0 {
			ses.mu.Unlock()
			return nil, plan.ErrViewSchemaLimit
		}
	}
	for name, value := range ses.userDefinedVars {
		if value == nil {
			continue
		}
		copy := *value
		if bytes, ok := copy.Value.([]byte); ok {
			copy.Value = slices.Clone(bytes)
		}
		frozen.userDefinedVars[name] = &copy
	}
	ses.mu.Unlock()
	copy := *source
	copy.ses = frozen
	if !copy.diagnosticCountsSnapshotSet {
		copy.captureDiagnosticCountsSnapshot(ses)
	}
	return &viewSchemaVariables{TxnCompilerContext: &TxnCompilerContext{execCtx: &copy}, bytes: viewSchemaEnvironmentLimit - remaining}, nil
}
func viewSchemaVariableBytes(value any) int {
	switch value := value.(type) {
	case string:
		return len(value)
	case []byte:
		return len(value)
	default:
		return 64
	}
}

type viewSchemaCompilerContext struct {
	*TxnCompilerContext
	variables *viewSchemaVariables
	database  string
	stats     *plan.StatsCache
}

func (c *viewSchemaCompilerContext) DefaultDatabase() string { return c.database }
func (c *viewSchemaCompilerContext) SetContext(ctx context.Context) {
	if !resolvesUdfInCallerTxn(ctx) {
		ctx = withResolveUdfInCallerTxn(ctx)
	}
	c.TxnCompilerContext.SetContext(withViewSchemaCatalogRead(ctx))
}
func (c *viewSchemaCompilerContext) GetLowerCaseTableNames() int64 {
	return c.variables.GetLowerCaseTableNames()
}
func (c *viewSchemaCompilerContext) ResolveVariable(name string, system, global bool) (any, error) {
	return c.variables.ResolveVariable(strings.ToLower(name), system, global)
}

func (c *viewSchemaCompilerContext) ResolveVariableType(name string, system, global bool) (pb.Type, error) {
	return c.variables.ResolveVariableType(name, system, global)
}
func (c *viewSchemaCompilerContext) ResolveVariableIsBin(name string, system, global bool) (bool, error) {
	return c.variables.ResolveVariableIsBin(name, system, global)
}
func (c *viewSchemaCompilerContext) ResolveVariableStringDomain(name string, system, global bool) (types.RuntimeStringDomain, error) {
	return c.variables.ResolveVariableStringDomain(name, system, global)
}
func (c *viewSchemaCompilerContext) ResolveVariablePrepareParamKind(name string, system, global bool) (vector.PrepareParamKind, error) {
	return c.variables.ResolveVariablePrepareParamKind(name, system, global)
}
func (c *viewSchemaCompilerContext) GetStatsCache() *plan.StatsCache { return c.stats }
func (c *viewSchemaCompilerContext) Stats(*plan.ObjectRef, *plan.Snapshot) (*statsinfo.StatsInfo, error) {
	return nil, nil
}
func (c *viewSchemaCompilerContext) StatsWithTableDef(*plan.ObjectRef, *plan.TableDef, *plan.Snapshot) (*statsinfo.StatsInfo, error) {
	return nil, nil
}

type viewSchemaSQLHelper struct{ compiler *viewSchemaCompilerContext }

func (h *viewSchemaSQLHelper) GetCompilerContext() any { return h.compiler }
func (h *viewSchemaSQLHelper) GetSubscriptionMeta(name string) (*pb.SubscriptionMeta, error) {
	return h.compiler.GetSubscriptionMeta(name, h.compiler.GetSnapshot())
}
func (h *viewSchemaSQLHelper) ExecSql(string) ([][]interface{}, error) {
	return nil, moerr.NewNotSupported(h.compiler.GetContext(), "executing SQL while deriving view schema")
}
func (h *viewSchemaSQLHelper) ExecSqlWithCtx(context.Context, string) ([][]interface{}, error) {
	return h.ExecSql("")
}

func (c *viewSchemaCompilerContext) CheckTimeStampValid(ts int64) (bool, error) {
	ctx := c.GetContext()
	bh := c.getOrCreateBackExec(ctx)
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, getSqlForCheckSnapshotTs(ts)); err != nil {
		return false, err
	}
	results, err := getResultSet(ctx, bh)
	return execResultArrayHasData(results), err
}

// ResolveViewSchemaRoot uses the caller's normal subscription lookup. Database
// names in this entry-point namespace are not names in persisted publisher SQL.
func (c *viewSchemaCompilerContext) ResolveViewSchemaRoot(database, table string, snapshot *plan.Snapshot) (*pb.ObjectRef, *pb.TableDef, error) {
	return c.TxnCompilerContext.Resolve(database, table, snapshot)
}

// Resolve keeps every View source lookup in the publisher's catalog domain,
// including databases other than the publication database. Ordinary Resolve
// only switches tenants when the database itself is a subscription.
func (c *viewSchemaCompilerContext) Resolve(database, table string, snapshot *plan.Snapshot) (*pb.ObjectRef, *pb.TableDef, error) {
	sub := c.GetQueryingSubscription()
	if sub == nil {
		return c.TxnCompilerContext.Resolve(database, table, snapshot)
	}
	previous := c.GetContext()
	c.SetContext(defines.AttachAccountId(previous, uint32(sub.AccountId)))
	defer c.SetContext(previous)
	return c.TxnCompilerContext.Resolve(database, table, viewSchemaPublisherSnapshot(snapshot, uint32(sub.AccountId)))
}

func (c *viewSchemaCompilerContext) ResolveById(id uint64, snapshot *plan.Snapshot) (*pb.ObjectRef, *pb.TableDef, error) {
	sub := c.GetQueryingSubscription()
	if sub == nil {
		return c.TxnCompilerContext.ResolveById(id, snapshot)
	}
	previous := c.GetContext()
	c.SetContext(defines.AttachAccountId(previous, uint32(sub.AccountId)))
	defer c.SetContext(previous)
	return c.TxnCompilerContext.ResolveById(id, viewSchemaPublisherSnapshot(snapshot, uint32(sub.AccountId)))
}

func (c *viewSchemaCompilerContext) ResolveIndexTableByRef(ref *pb.ObjectRef, table string, snapshot *plan.Snapshot) (*pb.ObjectRef, *pb.TableDef, error) {
	sub := c.GetQueryingSubscription()
	if sub == nil {
		return c.TxnCompilerContext.ResolveIndexTableByRef(ref, table, snapshot)
	}
	previous := c.GetContext()
	c.SetContext(defines.AttachAccountId(previous, uint32(sub.AccountId)))
	defer c.SetContext(previous)
	return c.TxnCompilerContext.ResolveIndexTableByRef(ref, table, viewSchemaPublisherSnapshot(snapshot, uint32(sub.AccountId)))
}

func (c *viewSchemaCompilerContext) ResolveViewDependencyAccount(obj *pb.ObjectRef, def *pb.TableDef, snapshot *plan.Snapshot) (uint32, error) {
	sub := c.GetQueryingSubscription()
	if sub == nil {
		return c.TxnCompilerContext.ResolveViewDependencyAccount(obj, def, snapshot)
	}
	// Resolve has restored the caller context before dependency capture. Retain
	// its effective publisher domain without changing the caller's snapshot.
	previous := c.GetContext()
	c.SetContext(defines.AttachAccountId(previous, uint32(sub.AccountId)))
	defer c.SetContext(previous)
	return c.TxnCompilerContext.ResolveViewDependencyAccount(obj, def, viewSchemaPublisherSnapshot(snapshot, uint32(sub.AccountId)))
}

func viewSchemaPublisherSnapshot(snapshot *plan.Snapshot, publisher uint32) *plan.Snapshot {
	if snapshot == nil {
		return nil
	}
	owned := plan.DeepCopySnapshot(snapshot)
	owned.Tenant = &pb.SnapshotTenant{TenantID: publisher}
	return owned
}

// A View source database belongs to its publisher, even when the entry point
// uses the subscriber's local alias. Only this isolated child changes context;
// root authorization and the parent session keep the subscriber identity.
func (c *viewSchemaCompilerContext) GetDatabaseId(name string, snapshot *plan.Snapshot) (uint64, error) {
	sub := c.GetQueryingSubscription()
	if sub == nil {
		return c.TxnCompilerContext.GetDatabaseId(name, snapshot)
	}
	previous := c.GetContext()
	c.SetContext(defines.AttachAccountId(previous, uint32(sub.AccountId)))
	defer c.SetContext(previous)
	return c.TxnCompilerContext.GetDatabaseId(name, viewSchemaPublisherSnapshot(snapshot, uint32(sub.AccountId)))
}

func (c *viewSchemaCompilerContext) ResolveViewUdf(name string, args []*pb.Expr, database string) (*function.Udf, error) {
	sub := c.GetQueryingSubscription()
	if sub == nil {
		return c.TxnCompilerContext.ResolveViewUdf(name, args, database)
	}
	previous := c.GetContext()
	c.SetContext(defines.AttachAccountId(previous, uint32(sub.AccountId)))
	defer c.SetContext(previous)
	return c.TxnCompilerContext.ResolveViewUdf(name, args, database)
}
