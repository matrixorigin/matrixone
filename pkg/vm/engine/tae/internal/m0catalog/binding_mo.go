//go:build mo_m0

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

package m0catalog

import (
	"bytes"
	"context"
	"errors"

	"github.com/gogo/protobuf/proto"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
)

// BindMergeSchema obtains generated-column semantics from the catalog at
// the merge transaction's snapshot. The WAL schema codec does not itself
// preserve this field. An unavailable or inconsistent row is not evidence
// that a column has no generated expression.
func BindMergeSchema(ctx context.Context, txn txnif.AsyncTxn, table *catalog.TableEntry, schema *catalog.Schema) (*catalog.Schema, error) {
	if !catalog.M0SchemaHasAppendDomainShape(schema) {
		return schema, nil
	}
	return BindSchema(ctx, txn, table, schema, nil)
}

// WorkspaceRow verifies that the selected physical identity is owned by the
// current transaction's in-memory catalog workspace. It must reject deleted
// or incomplete rows; false delegates to the persisted-object path.
type WorkspaceRow func(context.Context, handle.Relation, *common.ID, uint32) (bool, error)

// BindSchema validates current column semantics against the relation's
// transaction-visible catalog, including for a version-zero ADD root. It is
// not an authorization for the evolution history of any data object.
func BindSchema(ctx context.Context, txn txnif.AsyncTxn, table *catalog.TableEntry, schema *catalog.Schema, workspaceRow WorkspaceRow) (*catalog.Schema, error) {
	if ctx == nil || txn == nil || table == nil || table.GetDB() == nil || schema == nil {
		return nil, moerr.NewInternalErrorNoCtx("M0: merge catalog binding has no relation")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, err := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	if err != nil {
		return nil, errors.Join(moerr.NewInternalErrorNoCtx("M0: catalog binding database unavailable"), err)
	}
	columns, err := db.GetRelationByID(pkgcatalog.MO_COLUMNS_ID)
	if err != nil {
		return nil, errors.Join(moerr.NewInternalErrorNoCtx("M0: catalog binding relation unavailable"), err)
	}
	bound := schema.Clone()
	packer := types.NewPacker()
	defer packer.Close()
	for _, col := range bound.ColDefs {
		if col.IsPhyAddr() {
			continue
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		packer.Reset()
		packer.EncodeUint32(bound.AcInfo.TenantID)
		packer.EncodeStringType([]byte(table.GetDB().GetName()))
		packer.EncodeStringType([]byte(bound.Name))
		packer.EncodeStringType([]byte(col.Name))
		id, row, lookupErr := columns.GetByFilter(ctx, handle.NewEQFilter(packer.Bytes()))
		if lookupErr != nil {
			return nil, errors.Join(moerr.NewInternalErrorNoCtx("M0: merge column catalog row unavailable"), lookupErr)
		}
		if id == nil {
			return nil, moerr.NewInternalErrorNoCtx("M0: merge column catalog row has no identity")
		}
		if workspaceRow != nil {
			local, localErr := workspaceRow(ctx, columns, id, row)
			if localErr != nil {
				return nil, localErr
			}
			if local {
				if err = checkM0MergeColumn(ctx, columns, id, row, table, bound, col); err != nil {
					return nil, err
				}
				continue
			}
		}
		if err = checkM0MergeCatalogRow(ctx, columns, id, row, table, bound, col); err != nil {
			return nil, errors.Join(moerr.NewInternalErrorNoCtx("M0: catalog binding row validation failed"), err)
		}
	}
	return bound, nil
}

// The row ID returned by GetByFilter is a physical offset. Keep physical
// rows, check both committed and transaction-local deletes, and release the
// scan before moving to the next column. Unlike Relation.GetValue, HybridScan
// carries the caller's context to the underlying object read.
func checkM0MergeCatalogRow(ctx context.Context, rel handle.Relation, id *common.ID, row uint32, table *catalog.TableEntry, schema *catalog.Schema, col *catalog.ColDef) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}
	if rel == nil || id == nil {
		return moerr.NewInternalErrorNoCtx("M0: merge catalog row has no identity")
	}
	obj, err := rel.GetObject(id.ObjectID(), false)
	if obj != nil {
		defer func() { err = errors.Join(err, obj.Close()) }()
	}
	if err != nil {
		return errors.Join(moerr.NewInternalErrorNoCtx("M0: catalog row object unavailable"), err)
	}
	if obj == nil {
		return moerr.NewInternalErrorNoCtx("M0: merge catalog object unavailable")
	}
	idxs := make([]int, pkgcatalog.MO_COLUMNS_MAXIDX+1)
	for i := range idxs {
		idxs[i] = i
	}
	_, block := id.BlockID.Offsets()
	var bat *containers.Batch
	err = obj.HybridScan(tables.WithPreservePhysicalRows(ctx), &bat, block, idxs, common.MergeAllocator)
	if bat != nil {
		defer bat.Close()
	}
	if err != nil {
		return errors.Join(moerr.NewInternalErrorNoCtx("M0: catalog row scan failed"), err)
	}
	if bat == nil || len(bat.Vecs) != len(idxs) {
		return moerr.NewInternalErrorNoCtx("M0: merge catalog row outside scanned block")
	}
	for _, vec := range bat.Vecs {
		if vec == nil || uint64(row) >= uint64(vec.Length()) {
			return moerr.NewInternalErrorNoCtx("M0: merge catalog column unavailable")
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = rel.FillInWorkspaceDeletes(id.BlockID, &bat.Deletes, 0); err != nil {
		return err
	}
	if bat.Deletes != nil && bat.Deletes.Contains(uint64(row)) {
		return moerr.NewInternalErrorNoCtx("M0: merge catalog row is deleted")
	}
	return checkM0MergeColumn(ctx, &m0ScannedCatalogRow{batch: bat, row: int(row)}, id, row, table, schema, col)
}

type m0ScannedCatalogRow struct {
	handle.Relation
	batch *containers.Batch
	row   int
}

func (r *m0ScannedCatalogRow) GetValue(_ *common.ID, _ uint32, column uint16, _ bool) (any, bool, error) {
	if int(column) >= len(r.batch.Vecs) || r.batch.Vecs[column] == nil {
		return nil, false, moerr.NewInternalErrorNoCtx("M0: merge catalog column unavailable")
	}
	vec := r.batch.Vecs[column]
	if vec.IsNull(r.row) {
		return nil, true, nil
	}
	return vec.Get(r.row), false, nil
}

func m0MergeValue[T any](ctx context.Context, rel handle.Relation, id *common.ID, row uint32, column uint16) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	value, null, err := rel.GetValue(id, row, column, false)
	if err != nil {
		return zero, err
	}
	typed, ok := value.(T)
	if null || !ok {
		return zero, moerr.NewInternalErrorNoCtx("M0: merge column catalog value missing or invalid")
	}
	return typed, nil
}

func checkM0MergeColumn(ctx context.Context, rel handle.Relation, id *common.ID, row uint32, table *catalog.TableEntry, schema *catalog.Schema, col *catalog.ColDef) error {
	account, e1 := m0MergeValue[uint32](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ACCOUNT_ID_IDX)
	dbID, e2 := m0MergeValue[uint64](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_DATABASE_ID_IDX)
	tableID, e3 := m0MergeValue[uint64](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_RELNAME_ID_IDX)
	name, e4 := m0MergeValue[[]byte](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATTNAME_IDX)
	seq, e5 := m0MergeValue[uint16](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_SEQNUM_IDX)
	ordinal, e6 := m0MergeValue[int32](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATTNUM_IDX)
	typeBytes, e7 := m0MergeValue[[]byte](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATTTYP_IDX)
	notNull, e8 := m0MergeValue[int8](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATTNOTNULL_IDX)
	hidden, e9 := m0MergeValue[int8](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_IS_HIDDEN_IDX)
	cluster, e10 := m0MergeValue[int8](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_IS_CLUSTERBY)
	auto, e11 := m0MergeValue[int8](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX)
	constraint, e12 := m0MergeValue[[]byte](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX)
	def, e13 := m0MergeValue[[]byte](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_DEFAULT_IDX)
	onUpdate, e14 := m0MergeValue[[]byte](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_UPDATE_IDX)
	enum, e15 := m0MergeValue[[]byte](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_ENUM_IDX)
	marker, e16 := m0MergeValue[int8](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_HAS_GENERATED_IDX)
	generated, e17 := m0MergeValue[[]byte](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_GENERATED_IDX)
	dbName, e18 := m0MergeValue[[]byte](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_DATABASE_IDX)
	tableName, e19 := m0MergeValue[[]byte](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_RELNAME_IDX)
	hasDefault, e20 := m0MergeValue[int8](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATTHASDEF_IDX)
	hasUpdate, e21 := m0MergeValue[int8](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATT_HAS_UPDATE_IDX)
	dropped, e22 := m0MergeValue[int8](ctx, rel, id, row, pkgcatalog.MO_COLUMNS_ATTISDROPPED_IDX)
	if err := errors.Join(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22); err != nil {
		return errors.Join(moerr.NewInternalErrorNoCtx("M0: merge column catalog binding failed"), err)
	}
	var typ types.Type
	if types.Decode(typeBytes, &typ) != nil || account != schema.AcInfo.TenantID || dbID != table.GetDB().ID ||
		string(dbName) != table.GetDB().GetName() || tableID != table.ID || string(tableName) != schema.Name ||
		string(name) != col.Name || seq != col.SeqNum || ordinal != int32(col.Idx+1) || typ != col.Type ||
		(notNull != 0 && notNull != 1) || (hidden != 0 && hidden != 1) ||
		(cluster != 0 && cluster != 1) || (auto != 0 && auto != 1) ||
		(hasDefault != 0 && hasDefault != 1) || (hasUpdate != 0 && hasUpdate != 1) || dropped != 0 ||
		(hasDefault == 1) != (len(def) != 0) || (hasUpdate == 1) != (len(onUpdate) != 0) ||
		(string(constraint) != pkgcatalog.SystemColPKConstraint && string(constraint) != pkgcatalog.SystemColNoConstraint) ||
		(notNull == 0) != col.NullAbility || (hidden == 1) != col.Hidden || (cluster == 1) != col.ClusterBy ||
		(auto == 1) != col.AutoIncrement || (string(constraint) == pkgcatalog.SystemColPKConstraint) != col.Primary ||
		!bytes.Equal(def, col.Default) || !bytes.Equal(onUpdate, col.OnUpdate) || string(enum) != col.EnumValues {
		return moerr.NewInternalErrorNoCtx("M0: merge column catalog semantics differ")
	}
	switch marker {
	case 0:
		if len(generated) != 0 || len(col.GeneratedCol) != 0 {
			return moerr.NewInternalErrorNoCtx("M0: inconsistent generated expression in catalog")
		}
		col.GeneratedCol = nil
	case 1:
		var parsed plan.GeneratedCol
		if len(generated) == 0 || types.Decode(generated, &parsed) != nil {
			return moerr.NewInternalErrorNoCtx("M0: invalid generated expression in catalog")
		}
		if len(col.GeneratedCol) != 0 {
			var current plan.GeneratedCol
			if types.Decode(col.GeneratedCol, &current) != nil || current.IsStored != parsed.IsStored || current.OriginString != parsed.OriginString ||
				!proto.Equal(current.Expr, parsed.Expr) {
				return moerr.NewInternalErrorNoCtx("M0: generated expression differs from catalog")
			}
		}
		col.GeneratedCol = bytes.Clone(generated)
	default:
		return moerr.NewInternalErrorNoCtx("M0: invalid generated marker in catalog")
	}
	return nil
}
