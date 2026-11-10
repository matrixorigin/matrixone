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

package compile

import (
	"context"
	"slices"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func scalarAlterCopyDefinition(def *plan.TableDef) bool {
	if def == nil || def.TableType != catalog.SystemOrdinaryRel || def.IsTemporary || def.Hidden ||
		def.FeatureFlag != 0 || def.Partition != nil || def.ClusterBy != nil || def.ViewSql != nil || def.TblFunc != nil ||
		len(def.Indexes) != 0 || len(def.Fkeys) != 0 || len(def.RefChildTbls) != 0 || len(def.Checks) != 0 || len(def.Cols) == 0 {
		return false
	}
	for _, col := range def.Cols {
		if col == nil || col.Name == "" || col.GeneratedCol != nil {
			return false
		}
	}
	for _, entry := range def.Defs {
		if entry == nil {
			return false
		}
		properties, ok := entry.Def.(*plan.TableDef_DefType_Properties)
		if !ok || properties == nil || properties.Properties == nil {
			return false
		}
		for _, property := range properties.Properties.Properties {
			if property == nil {
				return false
			}
		}
	}
	return !isTableFromPublication(def)
}

// Physical column coordinates and table-origin hints are reallocated by COPY.
// Everything else, including defaults and unknown semantic fields, must match.
func sameAlterCopyColumn(a, b *plan.ColDef) bool {
	if a == nil || b == nil {
		return false
	}
	x, y := *a, *b
	normalizeExpr := func(expr *plan.Expr) *plan.Expr {
		copy := proto.Clone(expr).(*plan.Expr)
		// Type.Table is catalog lineage, including on bound column arguments;
		// rebinding CREATE can omit it without changing expression semantics.
		_ = plan.VisitExprTree(copy, func(node *plan.Expr) error {
			node.Typ.Table = ""
			return nil
		})
		return copy
	}
	for _, col := range []*plan.ColDef{&x, &y} {
		col.ColId, col.Seqnum, col.Pkidx, col.Alg = 0, 0, 0, 0
		col.TblName, col.OriginTblName, col.DbName = "", "", ""
		col.Typ.Table = ""
		// CREATE carries nullability in Default.NullAbility; stored definitions
		// also carry it in Type.NotNullable. Keep the full default comparison.
		col.Typ.NotNullable = col.Typ.NotNullable || col.NotNull || col.Primary ||
			col.Default != nil && !col.Default.NullAbility
		col.NotNull = false
		if d := col.Default; d != nil && d.Expr != nil {
			canonical := *d
			canonical.Expr = normalizeExpr(d.Expr)
			col.Default = &canonical
		}
		if u := col.OnUpdate; u != nil && u.Expr != nil {
			canonical := *u
			canonical.Expr = normalizeExpr(u.Expr)
			col.OnUpdate = &canonical
		}
		if d := col.Default; d != nil && (d.Expr == nil && d.OriginString == "" ||
			strings.EqualFold(strings.TrimSpace(d.OriginString), "null") &&
				(d.Expr == nil || d.Expr.GetLit() != nil && d.Expr.GetLit().Isnull)) {
			// Regenerated DDL writes DEFAULT NULL for an implicit NULL default.
			canonical := *d
			canonical.Expr, canonical.OriginString = nil, ""
			col.Default = &canonical
		}
	}
	return proto.Equal(&x, &y)
}

// The planner certifies the complete original statement. COPY markers describe
// the final Cols layout (including hidden columns), not engine ADD instructions.
func scalarAlterCopyPlan(q *plan.AlterTable, source *plan.TableDef) bool {
	if q == nil || q.AlgorithmType != plan.AlterTable_COPY || q.IsClusterTable ||
		q.AlterPartition != nil || len(q.UpdateFkSqls) != 0 || len(q.DetectSqls) != 0 || len(q.Actions) == 0 ||
		!scalarAlterCopyDefinition(source) || !scalarAlterCopyDefinition(q.CopyTableDef) {
		return false
	}
	target := q.CopyTableDef
	if source.Pkey.GetPkeyColName() != target.Pkey.GetPkeyColName() ||
		!slices.Equal(source.Pkey.GetNames(), target.Pkey.GetNames()) ||
		source.DefaultCharset != target.DefaultCharset || source.CollationVersion != target.CollationVersion ||
		source.KeyFormat != target.KeyFormat || source.AutoIdCache != target.AutoIdCache ||
		len(target.Cols) != len(source.Cols)+len(q.Actions) {
		return false
	}
	old := make(map[string]*plan.ColDef, len(source.Cols))
	for _, col := range source.Cols {
		if _, duplicate := old[col.Name]; duplicate {
			return false
		}
		old[col.Name] = col
	}
	nextOld, nextAdd := 0, 0
	seen := make(map[string]bool, len(target.Cols))
	for pos, col := range target.Cols {
		if seen[col.Name] {
			return false
		}
		seen[col.Name] = true
		if previous := old[col.Name]; previous != nil {
			if source.Cols[nextOld].Name != col.Name || previous.ColId != col.ColId || !sameAlterCopyColumn(previous, col) {
				return false
			}
			nextOld++
			continue
		}
		if nextAdd >= len(q.Actions) || q.Actions[nextAdd] == nil || col.Hidden || col.Primary || col.Unique ||
			col.Typ.AutoIncr || col.ClusterBy || col.GeneratedCol != nil {
			return false
		}
		action, ok := q.Actions[nextAdd].Action.(*plan.AlterTable_Action_AddColumn)
		if !ok || action == nil || action.AddColumn == nil {
			return false
		}
		add := action.AddColumn
		previous := ""
		if pos > 0 {
			previous = target.Cols[pos-1].Name
		}
		if add.Name != col.Name || add.Pos != int32(pos) || add.PreName != previous || !proto.Equal(&add.Type, &col.Typ) {
			return false
		}
		nextAdd++
	}
	return nextOld == len(source.Cols) && nextAdd == len(q.Actions)
}

func (c *Compile) scalarAlterCopyEligible(q *plan.AlterTable, rel engine.Relation) (bool, error) {
	if q == nil || q.Database == "" || !c.isLifecycleRC() || plan2.IsFkBannedDatabase(q.Database) ||
		!scalarAlterCopyPlan(q, rel.CopyTableDef(c.proc.Ctx)) {
		return false, nil
	}
	shape, err := scalarReplacementShape(c.proc.Ctx, rel)
	if err != nil || !shape {
		return false, err
	}
	protected, err := c.scalarReplacementProtected(q.Database, q.TableDef.Name,
		rel.GetTableID(c.proc.Ctx), plan2.SnapshotTableID(rel.GetTableDef(c.proc.Ctx)))
	return !protected && err == nil, err
}

func scalarAlterCopyReceipt(database, name string, rel engine.Relation, ctx context.Context) dropLifecycleAdmission {
	return dropLifecycleAdmission{rootID: rel.GetTableID(ctx), root: dropLifecycleIdentity{
		database: database, table: name, databaseID: rel.GetDBID(ctx),
		logicalID: plan2.SnapshotTableID(rel.GetTableDef(ctx)),
	}}
}

func (c *Compile) validateScalarAlterCopyReplica(rel engine.Relation, sourceID, databaseID, logicalID uint64) error {
	id := rel.GetTableID(c.proc.Ctx)
	created, ok := c.proc.GetTxnOperator().GetWorkspace().(client.TableCreationView)
	if !ok || id == 0 || id == sourceID || !created.IsTableCreatedInTxn(id) ||
		rel.GetDBID(c.proc.Ctx) != databaseID || plan2.SnapshotTableID(rel.GetTableDef(c.proc.Ctx)) != logicalID {
		return c.alterCopyPostPublicationConflict("private scalar replacement identity changed")
	}
	return nil
}

// Unlike TRUNCATE's transaction-local shortcut, COPY always takes full C/G/D
// admission. Only locks acquired by this preparation may be allowlisted.
func (c *Compile) admitScalarAlterCopyRC(database, table string, id uint64, private []uint64) (engine.Database, engine.Relation, bool, lock.WaitPolicy, error) {
	retained, err := c.proc.GetTxnOperator().Snapshot()
	if err != nil {
		return nil, nil, false, lock.WaitPolicy_FastFail, err
	}
	// Capture before C/G/D are added. Compatible G S cannot by itself exclude
	// a reverse edge from a caller's prior DML through K or metadata names.
	policy := lifecycleAdmissionWaitPolicy(retained.LockTables, private...)
	db, rel, err := c.admitTableLifecycleRC(database, table, id, false, private...)
	if err != nil {
		return nil, nil, false, policy, err
	}
	dag, err := c.loadBranchReclaimComponentsRC([]uint64{id}, policy)
	return db, rel, len(dag.Info) == 0, policy, err
}

type scalarAlterCopyCreateKey struct{}

// This lexical receipt validates the actual nested CREATE before effects, even
// when its private preparation intentionally disables all locking.
type scalarAlterCopyCreate struct {
	owner    client.TxnOperator
	actor    uint32
	database string
	target   *plan.TableDef
	closed   bool
}

func (c *Compile) withScalarAlterCopyCreate(database string, target *plan.TableDef, create func() error) error {
	actor, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	receipt := &scalarAlterCopyCreate{owner: c.proc.GetTxnOperator(), actor: actor, database: database, target: target}
	old := c.proc.Ctx
	c.proc.Ctx = context.WithValue(old, scalarAlterCopyCreateKey{}, receipt)
	defer func() { receipt.closed = true; c.proc.Ctx = old }()
	return create()
}

func (c *Compile) validateScalarAlterCopyCreate(q *plan.CreateTable) error {
	r, ok := c.proc.Ctx.Value(scalarAlterCopyCreateKey{}).(*scalarAlterCopyCreate)
	if !ok {
		return nil
	}
	actor, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	database := q.Database
	if database == "" {
		database = c.db
	}
	def := q.GetTableDef()
	// CREATE planning leaves ordinary relkind implicit; storage adds rowid
	// later and regenerates PK helper columns. Compare the user layout and
	// validate those internal members by their exact primary-key ownership.
	if def != nil && def.TableType == "" {
		ordinary := *def
		ordinary.TableType = catalog.SystemOrdinaryRel
		def = &ordinary
	}
	if r.closed || r.owner != c.proc.GetTxnOperator() || actor != r.actor || database != r.database ||
		q.Temporary || q.Replace || q.IfNotExists || q.CreateAsSelectSql != "" || len(q.IndexTables) != 0 ||
		len(q.UnresolvedFkParents) != 0 || createTablePublishesForeignKeys(q) || !scalarAlterCopyDefinition(def) ||
		def.Name != r.target.Name || len(createNameDomain(q, database)) != 1 ||
		def.DefaultCharset != r.target.DefaultCharset || def.CollationVersion != r.target.CollationVersion ||
		def.KeyFormat != r.target.KeyFormat || def.AutoIdCache != r.target.AutoIdCache ||
		def.Pkey.GetPkeyColName() != r.target.Pkey.GetPkeyColName() ||
		!slices.Equal(def.Pkey.GetNames(), r.target.Pkey.GetNames()) {
		return moerr.NewInternalError(c.proc.Ctx, "CREATE exceeds scalar ALTER COPY preparation")
	}
	for _, prop := range def.Props {
		if prop == nil || prop.Key == catalog.SystemRelAttr_Kind && prop.Value != catalog.SystemOrdinaryRel {
			return moerr.NewInternalError(c.proc.Ctx, "CREATE changes scalar ALTER COPY relation kind")
		}
	}
	userColumns := func(def *plan.TableDef) ([]*plan.ColDef, bool) {
		var columns []*plan.ColDef
		seen := make(map[string]bool, len(def.Cols))
		for _, col := range def.Cols {
			if seen[col.Name] {
				return nil, false
			}
			seen[col.Name] = true
			if col.Hidden {
				expected := plan2.FindColumn(r.target.Cols, col.Name)
				if expected == nil || !expected.Hidden || col.Typ.Id != expected.Typ.Id ||
					col.Name != catalog.Row_ID && col.Name != def.Pkey.GetPkeyColName() {
					return nil, false
				}
			} else {
				columns = append(columns, col)
			}
		}
		if pk := def.Pkey.GetPkeyColName(); (pk == catalog.FakePrimaryKeyColName || pk == catalog.CPrimaryKeyColName) && !seen[pk] {
			return nil, false
		}
		return columns, true
	}
	actual, valid := userColumns(def)
	expected, expectedValid := userColumns(r.target)
	if !valid || !expectedValid || len(actual) != len(expected) {
		return moerr.NewInternalError(c.proc.Ctx, "CREATE changes scalar ALTER COPY column domain")
	}
	for i, col := range actual {
		if !sameAlterCopyColumn(col, expected[i]) {
			return moerr.NewInternalErrorf(c.proc.Ctx, "CREATE changes scalar ALTER COPY column %s", col.Name)
		}
	}
	return nil
}
