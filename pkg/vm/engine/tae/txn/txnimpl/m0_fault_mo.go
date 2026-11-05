//go:build mo_m0

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

package txnimpl

import (
	"bytes"
	"context"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"reflect"
	"strings"

	"github.com/gogo/protobuf/proto"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/internal/m0binding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/internal/m0catalog"
)

// This validates the incoming transition only. It does not authorize an old
// object's missing columns or prove the history of the current schema.
func m0BeforeApplyAddColumn(ctx context.Context, tbl *txnTable, req *api.AlterTableReq) error {
	if tbl == nil || tbl.dataTable == nil {
		return moerr.NewInternalError(ctx, "M0: invalid TN ADD COLUMN relation")
	}
	return m0ValidateTNAddRequest(ctx, tbl.GetLocalSchema(false), req)
}

func m0ValidateTNAddRequest(ctx context.Context, schema *catalog.Schema, req *api.AlterTableReq) error {
	if schema == nil || len(schema.ColDefs) == 0 || schema.ColDefs[len(schema.ColDefs)-1] == nil || !schema.ColDefs[len(schema.ColDefs)-1].IsPhyAddr() || req == nil || req.GetAddColumn() == nil || req.GetAddColumn().Column == nil {
		return moerr.NewInternalError(ctx, "M0: invalid TN ADD COLUMN request")
	}
	add := req.GetAddColumn()
	col := add.Column
	if add.InsertPosition != -1 && add.InsertPosition != int32(len(schema.ColDefs)-1) {
		return moerr.NewInternalError(ctx, "M0: TN ADD COLUMN must append before rowid")
	}
	if strings.TrimSpace(col.Name) == "" || !strings.EqualFold(col.Name, col.GetOriginCaseName()) || col.Hidden || col.Primary || col.Unique || col.ClusterBy || col.NotNull ||
		col.Typ.NotNullable || col.Typ.AutoIncr || col.Typ.Id == int32(types.T_enum) || len(col.Typ.Enumvalues) != 0 ||
		col.GeneratedCol != nil || col.OnUpdate != nil || col.Comment != "" || col.Default == nil || !col.Default.NullAbility || col.Default.OriginString != "" {
		return moerr.NewInternalError(ctx, "M0: unsupported TN appended column semantics")
	}
	// The schema mutation uses MustTypeFromPlan; validate first so malformed
	// wire metadata cannot panic after a transactional schema node is made.
	if err := m0ValidateTNColumnType(col.Typ); err != nil {
		return errors.Join(moerr.NewInternalError(ctx, "M0: invalid TN appended column type"), err)
	}
	if col.Default.Expr != nil {
		lit := col.Default.Expr.GetLit()
		if lit == nil || !lit.GetIsnull() {
			return moerr.NewInternalError(ctx, "M0: TN ADD COLUMN requires no backfill")
		}
	}
	for _, existing := range schema.ColDefs {
		if existing == nil || strings.EqualFold(existing.Name, col.Name) {
			return moerr.NewInternalError(ctx, "M0: duplicate or invalid TN column identity")
		}
	}
	return nil
}

func m0BindAddColumnContext(ctx context.Context, tbl *txnTable, req *api.AlterTableReq) (context.Context, error) {
	txn := tbl.store.txn
	binding := &m0TNAddBinding{ctx: ctx, dbID: tbl.entry.GetDB().ID, tableID: tbl.entry.ID,
		txnID: txn.GetID(), start: txn.GetStartTS(), snapshot: txn.GetSnapshotTS()}
	// Copy the transaction-visible schema under the catalog read lock. Catalog
	// I/O happens only after releasing that lock and is rechecked under the
	// write lock before publication.
	expected, err := func() (*catalog.Schema, error) {
		tbl.entry.RLock()
		defer tbl.entry.RUnlock()
		visible := tbl.entry.GetVisibleNodeLocked(txn)
		if visible == nil || visible.BaseNode == nil || visible.BaseNode.Schema == nil {
			return nil, moerr.NewInternalErrorNoCtx("M0: TN bound schema is not visible")
		}
		return visible.BaseNode.Schema.Clone(), nil
	}()
	if err != nil {
		return nil, err
	}
	if err := m0ValidateTNAddRequest(ctx, expected, req); err != nil {
		return nil, err
	}
	binding.expected = expected
	// Re-establish the current column semantics from this TN transaction's
	// catalog snapshot. This check does not authorize old-object history.
	bound, err := m0catalog.BindSchema(ctx, txn, tbl.entry, expected, func(ctx context.Context, rel handle.Relation, id *common.ID, row uint32) (bool, error) {
		return m0WorkspaceCatalogRow(ctx, tbl.store, rel, id, row)
	})
	if err != nil {
		return nil, err
	}
	layout, err := catalog.M0AppendLayoutFromSchema(bound)
	if err != nil {
		return nil, err
	}
	if bound.Extra == nil || bound.Extra.M0Evolution == nil {
		return nil, moerr.NewInternalErrorNoCtx("M0: TN ADD has no evolution contract")
	}
	if err = m0read.ValidateM0AppendLayout(layout); err != nil {
		return nil, err
	}
	binding.bound = bound
	if err = binding.validateTxn(txn); err != nil {
		return nil, err
	}
	return m0binding.WithAdd(ctx, binding), nil
}

type m0TNAddBinding struct {
	ctx      context.Context
	dbID     uint64
	tableID  uint64
	txnID    string
	start    types.TS
	snapshot types.TS
	expected *catalog.Schema
	bound    *catalog.Schema
}

// Validate runs under the table lock after the ordinary conflict check. The
// snapshot captured before catalog I/O must still match the visible schema.
func (binding *m0TNAddBinding) validateTxn(txn txnif.TxnReader) error {
	if txn == nil || binding.txnID == "" || txn.GetID() != binding.txnID ||
		txn.GetStartTS() != binding.start || txn.GetSnapshotTS() != binding.snapshot {
		return moerr.NewInternalErrorNoCtx("M0: TN transaction changed during catalog binding")
	}
	return nil
}

func (binding *m0TNAddBinding) Validate(dbID, tableID uint64, txn txnif.TxnReader, value any) error {
	if binding.ctx != nil {
		if err := binding.ctx.Err(); err != nil {
			return err
		}
	}
	if err := binding.validateTxn(txn); err != nil {
		return err
	}
	schema, ok := value.(*catalog.Schema)
	expected := binding.expected
	if dbID != binding.dbID || tableID != binding.tableID || !ok || schema == nil || expected == nil || schema.Extra == nil || expected.Extra == nil ||
		schema.Name != expected.Name || schema.Version != expected.Version || schema.CatalogVersion != expected.CatalogVersion ||
		schema.AcInfo != expected.AcInfo {
		return moerr.NewInternalErrorNoCtx("M0: TN schema identity changed during catalog binding")
	}
	if schema.Comment != expected.Comment || schema.Partitioned != expected.Partitioned ||
		schema.Partition != expected.Partition || schema.Relkind != expected.Relkind || schema.Createsql != expected.Createsql || schema.View != expected.View ||
		schema.Extra.NextColSeqnum != expected.Extra.NextColSeqnum ||
		schema.Extra.DefaultCharset != expected.Extra.DefaultCharset || schema.Extra.CollationVersion != expected.Extra.CollationVersion ||
		schema.Extra.KeyFormat != expected.Extra.KeyFormat ||
		!proto.Equal(&api.SchemaExtra{Checks: schema.Extra.Checks}, &api.SchemaExtra{Checks: expected.Extra.Checks}) ||
		!bytes.Equal(schema.Constraint, expected.Constraint) || len(schema.ColDefs) != len(expected.ColDefs) {
		return moerr.NewInternalErrorNoCtx("M0: TN schema changed during catalog binding")
	}
	for i, current := range schema.ColDefs {
		want := expected.ColDefs[i]
		if current == nil || want == nil || !bytes.Equal(current.Default, want.Default) ||
			!bytes.Equal(current.OnUpdate, want.OnUpdate) || !bytes.Equal(current.GeneratedCol, want.GeneratedCol) {
			return moerr.NewInternalErrorNoCtx("M0: TN column changed during catalog binding")
		}
		a, b := *current, *want
		a.Default, b.Default, a.OnUpdate, b.OnUpdate, a.GeneratedCol, b.GeneratedCol = nil, nil, nil, nil, nil, nil
		if !reflect.DeepEqual(a, b) {
			return moerr.NewInternalErrorNoCtx("M0: TN column changed during catalog binding")
		}
	}
	return nil
}

// Hydrate modifies only the new transaction-owned MVCC schema under the same
// table lock; the prior committed schema remains untouched.
func (binding *m0TNAddBinding) Hydrate(dbID, tableID uint64, txn txnif.TxnReader, value any) error {
	if err := binding.Validate(dbID, tableID, txn, value); err != nil {
		return err
	}
	schema := value.(*catalog.Schema)
	if binding.bound == nil || len(binding.bound.ColDefs) != len(schema.ColDefs) {
		return moerr.NewInternalErrorNoCtx("M0: TN generated-column binding is incomplete")
	}
	for i, col := range schema.ColDefs {
		if binding.bound.ColDefs[i] == nil || col.SeqNum != binding.bound.ColDefs[i].SeqNum {
			return moerr.NewInternalErrorNoCtx("M0: TN generated-column identity changed")
		}
	}
	for i, col := range schema.ColDefs {
		col.GeneratedCol = bytes.Clone(binding.bound.ColDefs[i].GeneratedCol)
	}
	return nil
}

// Only the current TN transaction's own in-memory catalog row can use
// GetValue without object I/O. All other identities keep the physical scan.
func m0WorkspaceCatalogRow(ctx context.Context, owner *txnStore, rel handle.Relation, id *common.ID, row uint32) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	r, ok := rel.(*txnRelation)
	if !ok || r.table == nil || owner == nil || r.table.store != owner || r.table.dataTable == nil || id == nil {
		return false, moerr.NewInternalErrorNoCtx("M0: transaction-local catalog owner mismatch")
	}
	if r.table.dataTable.tableSpace == nil {
		return false, nil
	}
	space := r.table.dataTable.tableSpace
	if space.entry == nil {
		return false, moerr.NewInternalErrorNoCtx("M0: transaction-local catalog identity unavailable")
	}
	if id.DbID != pkgcatalog.MO_CATALOG_ID || id.TableID != pkgcatalog.MO_COLUMNS_ID || !id.ObjectID().EQ(space.entry.ID()) {
		return false, nil
	}
	if space.node == nil || space.node.data == nil || row >= space.Rows() || space.IsDeleted(row) || len(space.node.data.Vecs) <= pkgcatalog.MO_COLUMNS_MAXIDX {
		return false, moerr.NewInternalErrorNoCtx("M0: invalid transaction-local catalog row")
	}
	for _, vec := range space.node.data.Vecs[:pkgcatalog.MO_COLUMNS_MAXIDX+1] {
		if vec == nil || uint64(row) >= uint64(vec.Length()) {
			return false, moerr.NewInternalErrorNoCtx("M0: incomplete transaction-local catalog row")
		}
	}
	return true, ctx.Err()
}

// TypeFromPlan validates collation but its size lookup panics for unknown
// OIDs. Contain that conversion at the request boundary, before mutation.
func m0ValidateTNColumnType(typ plan.Type) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = moerr.NewInternalErrorNoCtxf("M0: invalid TN column type: %v", recovered)
		}
	}()
	_, err = types.TypeFromPlan(typ)
	return err
}

func m0AfterApplyAddColumn(ctx context.Context) error {
	_, message, injected := fault.TriggerFaultWithContext(ctx, "fj/m0/alter/tn-after-apply")
	if !injected {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if message != "" {
		return moerr.NewInternalError(ctx, message)
	}
	return nil
}
