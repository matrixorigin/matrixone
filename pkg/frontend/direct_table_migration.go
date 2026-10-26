// Copyright 2026 Matrix Origin
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

package frontend

import (
	"context"
	"math"
	"reflect"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
)

const (
	directMigrationSQLLimit    = 4096
	directMigrationItemLimit   = 16
	directMigrationColumnLimit = 64
	directMigrationProofLimit  = 8 << 10
	directMigrationBatchLimit  = 64 << 10
)

// This is intentionally not a general SQL/certificate transport. See the
// byte-preserved D2-DTS-1 design and its independent preimplementation review.
type directSelectShape struct {
	database, table string
	projection      []string
	order           []*query.DirectSelectOrderV1
}

func directMigrationRefusal() error { return moerr.GetOkExpectedNotSafeToStartTransfer() }

func directIdentifier(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for i, c := range []byte(s) {
		if c != '_' && (c < 'a' || c > 'z') && (i == 0 || c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// Future exported AST fields are rejected unless explicitly admitted here.
func directASTFields(v any, allowed ...string) bool {
	r := reflect.Indirect(reflect.ValueOf(v))
	for i := 0; i < r.NumField(); i++ {
		f := r.Type().Field(i)
		if f.IsExported() && !slices.Contains(allowed, f.Name) && !r.Field(i).IsZero() {
			return false
		}
	}
	return true
}

func directName(expr tree.Expr) (string, bool) {
	n, ok := expr.(*tree.UnresolvedName)
	if !ok || n == nil || n.NumParts != 1 || n.Star || !directASTFields(n, "NumParts", "CStrParts") {
		return "", false
	}
	if n.CStrParts[0] == nil || n.CStrParts[1] != nil || n.CStrParts[2] != nil || n.CStrParts[3] != nil {
		return "", false
	}
	name := n.ColNameOrigin()
	return name, directIdentifier(name)
}

// The real binder qualifies saved columns in-place. D2-DTS-1.1 permits
// normalization of only that exact same-table qualifier, never qualified raw
// input, a foreign qualifier, or any additional name component.
func directSavedName(expr tree.Expr, table string) (string, bool) {
	n, ok := expr.(*tree.UnresolvedName)
	if !ok || n == nil || !directIdentifier(table) || n.Star ||
		(n.NumParts != 1 && n.NumParts != 2) || !directASTFields(n, "NumParts", "CStrParts") {
		return "", false
	}
	for i, part := range n.CStrParts {
		if (i < n.NumParts) != (part != nil) {
			return "", false
		}
	}
	if n.NumParts == 2 && n.CStrParts[1].Origin() != table {
		return "", false
	}
	name := n.ColNameOrigin()
	return name, directIdentifier(name)
}

func directShapeFromAST(stmt tree.Statement) (directSelectShape, bool) {
	return directShapeWithNames(stmt, directName)
}

func directSavedShapeFromAST(stmt tree.Statement, table string) (directSelectShape, bool) {
	return directShapeWithNames(stmt, func(expr tree.Expr) (string, bool) {
		return directSavedName(expr, table)
	})
}

func directShapeWithNames(stmt tree.Statement, nameOf func(tree.Expr) (string, bool)) (directSelectShape, bool) {
	var shape directSelectShape
	s, ok := stmt.(*tree.Select)
	if !ok || s == nil || !directASTFields(s, "Select", "OrderBy") {
		return shape, false
	}
	c, ok := s.Select.(*tree.SelectClause)
	if !ok || c == nil || !directASTFields(c, "Exprs", "From") ||
		len(c.Exprs) == 0 || len(c.Exprs) > 16 || c.From == nil || len(c.From.Tables) != 1 ||
		!directASTFields(c.From, "Tables") {
		return shape, false
	}
	expr := c.From.Tables[0]
	if j, ok := expr.(*tree.JoinTableExpr); ok {
		if j == nil || j.JoinType != tree.JOIN_TYPE_CROSS || !directASTFields(j, "JoinType", "Left") {
			return shape, false
		}
		expr = j.Left
	}
	a, ok := expr.(*tree.AliasedTableExpr)
	if !ok || a == nil || !directASTFields(a, "Expr") {
		return shape, false
	}
	n, ok := a.Expr.(*tree.TableName)
	if !ok || n == nil || !n.ExplicitSchema || n.ExplicitCatalog || n.CatalogName != "" ||
		n.AtTsExpr != nil || n.IcebergRef != nil {
		return shape, false
	}
	shape.database, shape.table = string(n.SchemaName), string(n.ObjectName)
	if !directIdentifier(shape.database) || !directIdentifier(shape.table) {
		return shape, false
	}
	for _, e := range c.Exprs {
		// The parser represents no alias by an allocated empty CStr.
		if e.As != nil && !e.As.Empty() || !directASTFields(e, "Expr", "As") {
			return shape, false
		}
		name, ok := nameOf(e.Expr)
		if !ok || slices.Contains(shape.projection, name) {
			return shape, false
		}
		shape.projection = append(shape.projection, name)
	}
	if len(s.OrderBy) > 16 {
		return shape, false
	}
	for _, o := range s.OrderBy {
		if o == nil || o.NullOrder || o.NullsPosition != tree.DefaultNullsPosition ||
			(o.Direction != tree.DefaultDirection && o.Direction != tree.Ascending && o.Direction != tree.Descending) ||
			!directASTFields(o, "Expr", "Direction") {
			return shape, false
		}
		name, ok := nameOf(o.Expr)
		if !ok || !slices.Contains(shape.projection, name) {
			return shape, false
		}
		shape.order = append(shape.order, &query.DirectSelectOrderV1{Column: name, Desc: o.Direction == tree.Descending})
	}
	return shape, true
}

func parseDirectTableSelectV1(ctx context.Context, sql, mode string) (directSelectShape, bool) {
	var zero directSelectShape
	if len(sql) == 0 || len(sql) > directMigrationSQLLimit || context.Cause(ctx) != nil {
		return zero, false
	}
	// Lexical closure excludes comments, quotes, literals, operators, variables,
	// params and executable hints before invoking the mode-sensitive parser.
	for _, c := range []byte(sql) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '_' || c == ',' || c == '.' || c == ' ' || c == '\t' || c == '\r' || c == '\n') {
			return zero, false
		}
	}
	statements, err := mysql.ParseWithSQLMode(ctx, sql, 1, mode)
	if err != nil {
		return zero, false
	}
	defer func() {
		for _, s := range statements {
			s.Free()
		}
	}()
	if len(statements) != 1 {
		return zero, false
	}
	return directShapeFromAST(statements[0])
}

func directShapeMatches(s directSelectShape, p *query.PreparedDirectTableSelectV1) bool {
	if p == nil || s.database != p.DatabaseName || s.table != p.TableName ||
		!slices.Equal(s.projection, p.Projection) || len(s.order) != len(p.OrderBy) {
		return false
	}
	for i, o := range s.order {
		if p.OrderBy[i] == nil || o.Column != p.OrderBy[i].Column || o.Desc != p.OrderBy[i].Desc {
			return false
		}
	}
	return true
}

func directColumns(def *plan.TableDef) ([]*query.DirectSelectColumnV1, bool) {
	if def == nil || len(def.Cols) > directMigrationColumnLimit+2 ||
		def.TblId == 0 || def.TableType != catalog.SystemOrdinaryRel || def.Hidden ||
		def.TblFunc != nil || def.ViewSql != nil || def.IsTemporary || def.IsDynamic ||
		def.Partition != nil || def.FeatureFlag != 0 || len(def.Indexes) != 0 ||
		len(def.Fkeys) != 0 || len(def.RefChildTbls) != 0 || len(def.Checks) != 0 ||
		def.ClusterBy != nil || def.Pkey == nil || def.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
		return nil, false
	}
	var cols []*query.DirectSelectColumnV1
	names, seqnums := map[string]bool{}, map[uint32]bool{}
	fakePK := false
	for _, c := range def.Cols {
		if c == nil || names[c.Name] || seqnums[c.Seqnum] || c.GeneratedCol != nil || c.OnUpdate != nil ||
			c.ClusterBy || c.Headers || c.Header != "" ||
			c.Typ.Enumvalues != "" || c.Typ.Charset != 0 || c.Typ.PadSpace ||
			c.Typ.CollationCoercibilitySet || c.Typ.CollationMergeConflict || c.Typ.CollationVersion != 0 {
			return nil, false
		}
		names[c.Name], seqnums[c.Seqnum] = true, true
		if c.Hidden {
			if c.Typ.Scale != 0 {
				return nil, false
			}
			switch c.Name {
			case catalog.Row_ID:
				if c.Typ.Id != int32(types.T_Rowid) || c.Typ.AutoIncr || c.Primary {
					return nil, false
				}
			case catalog.FakePrimaryKeyColName:
				if c.Typ.Id != int32(types.T_uint64) || !c.Typ.AutoIncr || !c.Primary {
					return nil, false
				}
				fakePK = true
			default:
				return nil, false
			}
			if c.Default != nil && c.Default.Expr != nil {
				return nil, false
			}
			continue
		}
		// GetTypeFromAst's ordinary signed INT uses Scale=-1 (build_util.go).
		// Both 0 and that exact sentinel mean no decimal scale for INT32.
		// Do not admit another scale or apply this normalization to hidden types.
		if !directIdentifier(c.Name) || c.Typ.Id != int32(types.T_int32) ||
			(c.Typ.Scale != 0 && c.Typ.Scale != -1) || c.Typ.AutoIncr || c.Typ.Width < 0 ||
			c.Primary || c.Unique {
			return nil, false
		}
		cols = append(cols, &query.DirectSelectColumnV1{Name: c.Name, Seqnum: c.Seqnum,
			TypeID: c.Typ.Id, Width: c.Typ.Width, NotNullable: c.Typ.NotNullable})
	}
	if !fakePK || len(cols) == 0 || len(cols) > directMigrationColumnLimit {
		return nil, false
	}
	slices.SortFunc(cols, func(a, b *query.DirectSelectColumnV1) int {
		if a.Seqnum < b.Seqnum {
			return -1
		}
		if a.Seqnum > b.Seqnum {
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})
	return cols, true
}

func directColumnsMatch(def *plan.TableDef, p *query.PreparedDirectTableSelectV1) bool {
	cols, ok := directColumns(def)
	if !ok || len(cols) != len(p.Columns) {
		return false
	}
	for i, c := range cols {
		w := p.Columns[i]
		if w == nil || c.Name != w.Name || c.Seqnum != w.Seqnum || c.TypeID != w.TypeID ||
			c.Width != w.Width || c.NotNullable != w.NotNullable {
			return false
		}
	}
	for _, name := range p.Projection {
		if !slices.ContainsFunc(cols, func(c *query.DirectSelectColumnV1) bool { return c.Name == name }) {
			return false
		}
	}
	return true
}

func directObjectMatches(ref *plan.ObjectRef, p *query.PreparedDirectTableSelectV1, version bool) bool {
	return ref != nil && ref.Db > 0 && uint64(ref.Db) == p.DatabaseID && ref.Obj > 0 &&
		uint64(ref.Obj) == p.TableID && ref.SchemaName == p.DatabaseName &&
		(ref.DbName == "" || ref.DbName == p.DatabaseName) && ref.ObjName == p.TableName &&
		ref.SubscriptionName == "" && ref.PubInfo == nil && !ref.NotLockMeta && ref.Snapshot == nil &&
		(!version || ref.Server >= 0 && uint64(ref.Server) == uint64(p.TableVersion))
}

func directPlanMatches(ctx context.Context, pp *plan.Plan, p *query.PreparedDirectTableSelectV1) bool {
	prepared := pp.GetDcl().GetPrepare()
	if prepared == nil || len(prepared.ParamTypes) != 0 || len(prepared.Schemas) != 1 ||
		!directObjectMatches(prepared.Schemas[0], p, true) {
		return false
	}
	q := prepared.Plan.GetQuery()
	if q == nil || len(q.ViewReferences) != 0 || len(q.CatalogDependencies) == 0 {
		return false
	}
	for _, dep := range q.CatalogDependencies {
		if !directObjectMatches(dep, p, false) {
			return false
		}
	}
	err := plan.VisitExpressionsInOwner(pp, func(root *plan.Expr) error {
		return plan.VisitExprTree(root, func(e *plan.Expr) error {
			if err := context.Cause(ctx); err != nil {
				return err
			}
			if f := e.GetF(); f != nil && (f.Func == nil || int32(f.Func.Obj>>32) == function.CRC32) {
				return directMigrationRefusal()
			}
			return nil
		})
	})
	return err == nil
}

// Only immutable, complete catalog-cache metadata is used. No session txn,
// SQL, variable setter, authorization cache or source prepared handle changes.
func directCatalogTable(ctx context.Context, ses *Session, p *query.PreparedDirectTableSelectV1) (*plan.TableDef, error) {
	pu := getPuIfPresent(ses.GetService())
	if pu == nil {
		return nil, directMigrationRefusal()
	}
	storage := pu.StorageEngine
	if wrapped, ok := storage.(*engine.EntireEngine); ok {
		storage = wrapped.Engine
	}
	eng, ok := storage.(*disttae.Engine)
	if !ok || ses.GetTenantInfo() == nil || ses.GetTenantInfo().GetTenantID() != p.AccountID {
		return nil, directMigrationRefusal()
	}
	minimum := ses.getLastCommitTS()
	admission, supported, err := eng.AcquireCatalogSnapshot(ctx, minimum)
	if err != nil {
		return nil, err
	}
	if !supported || admission == nil {
		return nil, directMigrationRefusal()
	}
	defer admission.End()
	ts, err := eng.ValidateCatalogSnapshot(ctx, minimum, admission)
	if err != nil {
		return nil, err
	}
	cc := eng.GetLatestCatalogCache()
	if cc == nil {
		return nil, directMigrationRefusal()
	}
	db := &cache.DatabaseItem{AccountId: p.AccountID, Name: p.DatabaseName, Ts: ts}
	if !cc.GetDatabase(db) || db.Id == 0 || (p.DatabaseID != 0 && db.Id != p.DatabaseID) || db.Typ != "" {
		return nil, directMigrationRefusal()
	}
	item := &cache.TableItem{AccountId: p.AccountID, DatabaseId: db.Id, Name: p.TableName, Ts: ts}
	if !cc.GetTable(item) || item.IsDeleted() || item.Kind != catalog.SystemOrdinaryRel || item.ViewDef != "" ||
		item.Partitioned != 0 || item.TableDef == nil || item.Id == 0 ||
		(p.TableID != 0 && (item.Id != p.TableID || item.Version != p.TableVersion)) {
		return nil, directMigrationRefusal()
	}
	if _, err = eng.ValidateCatalogSnapshot(ctx, minimum, admission); err != nil {
		return nil, err
	}
	return item.TableDef, nil
}

func sourceDirectTableSelectV1(ctx context.Context, ses *Session, st *PrepareStmt) (*query.PreparedDirectTableSelectV1, error) {
	if st == nil || ses.GetTenantInfo() == nil || !st.rewritePolicyCaptured || st.rewritePolicyEnabled || st.rewritePolicyInvalidated.Load() ||
		len(st.remapDb) != 0 || st.needsRebuild || len(st.ParamTypes) != 0 || !st.sqlModeFlagsSet ||
		st.ddlVersion != ses.getDDLVersion() || st.tempTableVersion != ses.GetTempTableVersion() ||
		st.protocolVersion != currentProtocolVersion(ses.proc) || st.protocolVersion < defines.MORPCVersion133 ||
		ses.GetTxnHandler() == nil || ses.GetTxnHandler().InActiveTxn() ||
		len(ses.snapshotTempTables()) != 0 || ses.hasForeignConns() {
		return nil, directMigrationRefusal()
	}
	shape, ok := parseDirectTableSelectV1(ctx, st.Sql, st.schedulingSQLMode)
	saved, savedOK := directSavedShapeFromAST(st.PrepareStmt, shape.table)
	if !ok || !savedOK || !reflect.DeepEqual(shape, saved) || shape.database != ses.GetDatabaseName() {
		return nil, directMigrationRefusal()
	}
	prepared := st.PreparePlan.GetDcl().GetPrepare()
	if prepared == nil || len(prepared.Schemas) != 1 {
		return nil, directMigrationRefusal()
	}
	ref := prepared.Schemas[0]
	if ref == nil || ref.Db <= 0 || ref.Obj <= 0 || ref.Server < 0 || ref.Server > math.MaxUint32 {
		return nil, directMigrationRefusal()
	}
	p := &query.PreparedDirectTableSelectV1{Version: 1, AccountID: ses.GetTenantInfo().GetTenantID(),
		DatabaseID: uint64(ref.Db), TableID: uint64(ref.Obj), TableVersion: uint32(ref.Server),
		DatabaseName: shape.database, TableName: shape.table, Projection: shape.projection, OrderBy: shape.order}
	if !directPlanMatches(ctx, st.PreparePlan, p) {
		return nil, directMigrationRefusal()
	}
	def, err := directCatalogTable(ctx, ses, p)
	if err != nil {
		return nil, err
	}
	p.Columns, ok = directColumns(def)
	if !ok || !directColumnsMatch(def, p) || p.ProtoSize() > directMigrationProofLimit {
		return nil, directMigrationRefusal()
	}
	return p, nil
}

func hasDirectTableMigration(stmts []*query.PrepareStmt) bool {
	return slices.ContainsFunc(stmts, func(s *query.PrepareStmt) bool { return s != nil && s.DirectTableSelect != nil })
}

func directRuntimeMatches(ses *Session, vars []migratedSystemVariable) bool {
	count := 0
	for _, v := range vars {
		if !hasMigrationRuntimeSideEffect(v.name) {
			continue
		}
		count++
		value, present := moruntime.ServiceRuntime(ses.GetService()).GetGlobalVariables(v.name)
		if present != v.runtimeValuePresent || present && !reflect.DeepEqual(value, v.runtimeValue) {
			return false
		}
	}
	return count == 3
}

func directTypedMode(ctx context.Context, vars []*query.MigrateSystemVariable) (string, []migratedSystemVariable, error) {
	decoded, err := decodeSessionSystemVars(ctx, vars)
	if err != nil {
		return "", nil, err
	}
	mode, found := "", false
	seen := map[string]bool{}
	for _, v := range decoded {
		def, ok := gSysVarsDefs[v.name]
		if !ok || def.Scope == ScopeGlobal || !def.Dynamic {
			return "", nil, directMigrationRefusal()
		}
		if _, err = def.Type.Convert(v.value); err != nil {
			return "", nil, err
		}
		if !v.nextTransaction {
			seen[v.name] = true
		}
		if v.name == "sql_mode" && !v.nextTransaction {
			value, ok := v.value.(string)
			if !ok || found {
				return "", nil, directMigrationRefusal()
			}
			mode, found = mysql.SessionSQLModeForParser(value), true
		}
	}
	for name, def := range gSysVarsDefs {
		if def.Scope != ScopeGlobal && def.Dynamic && !seen[canonicalSystemVariableName(name)] {
			return "", nil, directMigrationRefusal()
		}
	}
	if !found {
		return "", nil, directMigrationRefusal()
	}
	return mode, decoded, nil
}

func preflightDirectTableMigrationV1(ctx context.Context, ses *Session, req *query.MigrateConnToRequest) error {
	if !hasDirectTableMigration(req.PrepareStmts) {
		return nil
	}
	if !req.CloseTargetOnReplayFailure || !req.LastInsertIDExported || !req.UserDefinedVarsExported ||
		!req.SystemVariablesExported || len(req.SetVarStmts) != 0 || len(req.TempTables) != 0 ||
		len(req.UserLevelLocks) != 0 || len(req.PrepareStmts) > directMigrationItemLimit ||
		ses.GetTxnHandler() == nil || ses.GetTxnHandler().InActiveTxn() ||
		len(ses.snapshotTempTables()) != 0 || ses.hasForeignConns() {
		return directMigrationRefusal()
	}
	handles, invalidated := ses.getPrepareStmtsForMigration()
	if invalidated || len(handles) != 0 || len(function.UserLevelLocksForMigration(ses.proc)) != 0 {
		return directMigrationRefusal()
	}
	if _, err := decodeUserDefinedVars(ctx, req.UserDefinedVars, req.UserDefinedVarsReplayable); err != nil {
		return err
	}
	mode, vars, err := directTypedMode(ctx, req.SystemVariables)
	if err != nil {
		return err
	}
	if !directRuntimeMatches(ses, vars) {
		return directMigrationRefusal()
	}
	total := 0
	names := map[string]bool{}
	for _, st := range req.PrepareStmts {
		if st == nil || names[strings.ToLower(st.Name)] {
			return directMigrationRefusal()
		}
		names[strings.ToLower(st.Name)] = true
		p := st.DirectTableSelect
		if p == nil {
			if !crc32ReplaySQLSafe(ctx, st.SQL, mode, true) {
				return directMigrationRefusal()
			}
			continue
		}
		if len(p.Columns) == 0 || len(p.Columns) > directMigrationColumnLimit ||
			len(p.Projection) == 0 || len(p.Projection) > 16 || len(p.OrderBy) > 16 {
			return directMigrationRefusal()
		}
		total += p.ProtoSize()
		if p.Version != 1 || p.DatabaseID == 0 || p.TableID == 0 || p.ProtoSize() > directMigrationProofLimit ||
			total > directMigrationBatchLimit || len(st.ParamTypes) != 0 ||
			st.RequiredProtocolVersion < defines.MORPCVersion133 || st.RequiredProtocolVersion > currentProtocolVersion(ses.proc) ||
			req.DB != p.DatabaseName || ses.GetTenantInfo() == nil || ses.GetTenantInfo().GetTenantID() != p.AccountID {
			return directMigrationRefusal()
		}
		shape, ok := parseDirectTableSelectV1(ctx, st.SQL, mode)
		if !ok || !directShapeMatches(shape, p) {
			return directMigrationRefusal()
		}
		def, err := directCatalogTable(ctx, ses, p)
		if err != nil {
			return err
		}
		if !directColumnsMatch(def, p) {
			return directMigrationRefusal()
		}
	}
	return context.Cause(ctx)
}

// The preflight snapshot is advisory. The real PREPARE uses its own statement
// transaction. Refuse another object BEFORE handing metadata to the binder.
type directTableMigrationCompiler struct {
	plan2.CompilerContext
	proof    *query.PreparedDirectTableSelectV1
	resolved bool
}

func (c *directTableMigrationCompiler) Resolve(db, name string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	p := c.proof
	account, err := c.GetAccountId()
	if err != nil {
		return nil, nil, err
	}
	if snapshot != nil || c.GetSnapshot() != nil || account != p.AccountID || db != p.DatabaseName || name != p.TableName {
		return nil, nil, directMigrationRefusal()
	}
	ref, def, err := c.CompilerContext.Resolve(db, name, snapshot)
	if err != nil {
		return nil, nil, err
	}
	if ref == nil || ref.SubscriptionName != "" || ref.PubInfo != nil || ref.Snapshot != nil || ref.NotLockMeta ||
		def == nil || def.DbId != p.DatabaseID || def.TblId != p.TableID || def.Version != p.TableVersion ||
		def.DbName != p.DatabaseName || def.Name != p.TableName || !directColumnsMatch(def, p) {
		return nil, nil, directMigrationRefusal()
	}
	c.resolved = true
	return ref, def, nil
}

func (c *directTableMigrationCompiler) ResolveById(uint64, *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	return nil, nil, directMigrationRefusal()
}
func (c *directTableMigrationCompiler) ResolveIndexTableByRef(*plan.ObjectRef, string, *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	return nil, nil, directMigrationRefusal()
}

// Capture only after the ordinary statement boundary. This check detects
// snapshot/workspace/identity drift; it is not a global DDL lock.
func directPublicationGuard(ctx context.Context, ses *Session, exec *ExecCtx, parent *TxnCompilerContext) (func() error, error) {
	proc := exec.proc
	if proc == nil || ses.GetTxnHandler() == nil {
		return nil, directMigrationRefusal()
	}
	op := ses.GetTxnHandler().GetTxn()
	reader, ok := op.(catalogStampReader)
	if !ok {
		return nil, directMigrationRefusal()
	}
	stamp, err := reader.CatalogReadStamp()
	if err != nil {
		return nil, err
	}
	workspace, ok := stamp.Workspace.(catalogVisibilityReader)
	if !ok {
		return nil, directMigrationRefusal()
	}
	visibility, stable := workspace.CatalogVisibility()
	if !stable {
		return nil, directMigrationRefusal()
	}
	generation, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return nil, err
	}
	identity := ses.GetTenantInfo()
	if identity == nil {
		return nil, directMigrationRefusal()
	}
	identity = identity.Copy()
	account, statement := ses.GetAccountId(), proc.GetStmtProfile().GetStmtId()
	protocol, ddl, temp := currentProtocolVersion(proc), ses.getDDLVersion(), ses.GetTempTableVersion()
	role := ses.GetPrivilegeCache().getActiveRoleGrantGeneration()
	floor, floorPresent := moruntime.ServiceRuntime(ses.GetService()).GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	check := func() error {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		parent.mu.Lock()
		sameCompilerOwner := parent.execCtx == exec && parent.execCtx.proc == proc
		parent.mu.Unlock()
		if !sameCompilerOwner {
			return directMigrationRefusal()
		}
		if exec.proc != proc || ses.GetTxnHandler().GetTxn() != op || generation.Closed() ||
			!proc.UsesExecutionResourceGeneration(generation) || ses.GetAccountId() != account ||
			proc.GetStmtProfile().GetStmtId() != statement || currentProtocolVersion(proc) != protocol ||
			ses.getDDLVersion() != ddl || ses.GetTempTableVersion() != temp ||
			ses.GetPrivilegeCache().getActiveRoleGrantGeneration() != role ||
			ses.GetTenantInfo() == nil || !reflect.DeepEqual(identity, ses.GetTenantInfo().Copy()) {
			return directMigrationRefusal()
		}
		nowFloor, nowPresent := moruntime.ServiceRuntime(ses.GetService()).GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
		if nowPresent != floorPresent || !reflect.DeepEqual(floor, nowFloor) {
			return directMigrationRefusal()
		}
		now, err := reader.CatalogReadStamp()
		if err != nil {
			return err
		}
		if now.TransactionID != stamp.TransactionID || now.Revision != stamp.Revision ||
			now.Workspace != stamp.Workspace || !reflect.DeepEqual(now.Snapshot, stamp.Snapshot) {
			return directMigrationRefusal()
		}
		current, stable := workspace.CatalogVisibility()
		if !stable || current != visibility {
			return directMigrationRefusal()
		}
		return nil
	}
	return check, check()
}

func checkDirectPublication(exec *ExecCtx, prepared *PrepareStmt) error {
	if exec.directTableMigration == nil {
		return nil
	}
	if exec.directPublicationCheck == nil || !directPlanMatches(exec.reqCtx, prepared.PreparePlan, exec.directTableMigration) {
		return directMigrationRefusal()
	}
	return exec.directPublicationCheck()
}
