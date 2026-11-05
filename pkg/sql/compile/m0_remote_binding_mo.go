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

package compile

import (
	"context"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// The waiter returns the exclusive successor of the applied timestamp. A
// successful wait with a smaller (or zero) result is not snapshot admission.
func waitM0RemoteReadSnapshot(ctx context.Context, cli client.TxnClient, snapshot timestamp.Timestamp) error {
	if ctx == nil || cli == nil || snapshot.IsEmpty() {
		return moerr.NewInternalErrorNoCtx("M0: remote snapshot watermark is unavailable")
	}
	applied, err := cli.WaitLogTailAppliedAt(ctx, snapshot.Prev())
	if err != nil {
		return err
	}
	if applied.Less(snapshot) {
		return moerr.NewInternalErrorNoCtx("M0: remote logtail watermark precedes scan snapshot")
	}
	return nil
}

// The existing stamp includes a revision that changes even if a transaction
// returns to the same snapshot. It detects stale generations but does not make
// concurrent workspace mutation safe.
func captureM0RemoteReadGeneration(op client.TxnOperator) (func() error, error) {
	stamper, ok := op.(interface {
		CatalogReadStamp() (client.CatalogReadStamp, error)
	})
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("M0: remote transaction has no catalog read generation")
	}
	start, err := stamper.CatalogReadStamp()
	if err != nil {
		return nil, err
	}
	return func() error {
		current, err := stamper.CatalogReadStamp()
		if err != nil {
			return err
		}
		if start.TransactionID != current.TransactionID || start.Revision != current.Revision || !start.Snapshot.Equal(current.Snapshot) {
			return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		}
		return nil
	}, nil
}

// A remote scope reopens a relation for ranges but builds the remote reader
// with the transmitted definition. Bind both to the same relation/schema and
// transaction snapshot before either component can consume selected objects.
// The state is checked per execution; nothing is cached across retries.
func validateM0RemoteReadBinding(ctx context.Context, source *Source, local *plan.TableDef, snapshot timestamp.Timestamp, resolve plan2.LegacyTinyTextTableResolver) error {
	if source == nil || source.node == nil || source.node.ObjRef == nil || source.node.TableDef == nil || source.TableDef == nil || local == nil {
		return moerr.NewInternalErrorNoCtx("M0: remote reader missing relation binding")
	}
	ref := source.node.ObjRef
	if source.SchemaName == "" || source.RelationName == "" ||
		!strings.EqualFold(source.SchemaName, ref.SchemaName) ||
		!strings.EqualFold(source.RelationName, ref.ObjName) ||
		!strings.EqualFold(source.RelationName, local.Name) ||
		source.TableDef.TblId == 0 || local.TblId == 0 {
		return moerr.NewInternalErrorNoCtx("M0: remote reader invalid relation identity")
	}
	if source.TableDef.TblId != local.TblId {
		return moerr.NewInternalErrorNoCtx("M0: remote reader relation identity mismatch")
	}
	if ref.Obj != 0 && uint64(ref.Obj) != local.TblId {
		return moerr.NewInternalErrorNoCtx("M0: remote scan object reference mismatch")
	}
	planDef := source.node.TableDef
	if planDef.TblId != local.TblId || !strings.EqualFold(planDef.Name, local.Name) {
		return moerr.NewInternalErrorNoCtx("M0: remote scan plan relation mismatch")
	}
	if !source.Timestamp.Equal(snapshot) || source.TableDef.Version != local.Version {
		return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	if planDef.Version != local.Version {
		return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	if !proto.Equal(source.TableDef, local) {
		return moerr.NewInternalErrorNoCtx("M0: remote reader schema mismatch")
	}
	physicalColumns := make(map[uint32]*plan.ColDef, len(local.Cols))
	var rowID *plan.ColDef
	for _, col := range local.Cols {
		if col == nil {
			return moerr.NewInternalErrorNoCtx("M0: remote reader nil schema column")
		}
		// GetTableDef may synthesize rowid with the default seqnum zero. Its
		// physical identity is its reserved name, not a user column sequence.
		if objectio.IsPhysicalAddr(col.Name) {
			if rowID != nil {
				return moerr.NewInternalErrorNoCtx("M0: remote reader duplicate rowid")
			}
			rowID = col
			continue
		}
		if physicalColumns[col.Seqnum] != nil {
			return moerr.NewInternalErrorNoCtx("M0: remote reader ambiguous schema columns")
		}
		physicalColumns[col.Seqnum] = col
	}
	planNames := make(map[string]struct{}, len(planDef.Cols)*2)
	var recovered *plan.TableDef
	for _, col := range planDef.Cols {
		if col == nil {
			return moerr.NewInternalErrorNoCtx("M0: remote scan plan has nil column")
		}
		actual := physicalColumns[col.Seqnum]
		if objectio.IsPhysicalAddr(col.Name) {
			actual = rowID
		}
		if actual == nil || col.ColId != actual.ColId || !strings.EqualFold(col.Name, actual.Name) ||
			col.NotNull != actual.NotNull || col.Default.GetNullAbility() != actual.Default.GetNullAbility() {
			return moerr.NewInternalErrorNoCtx("M0: remote scan plan column mismatch")
		}
		if !proto.Equal(&col.Typ, &actual.Typ) {
			// The planner can recover a legacy TEXT subtype from its own CREATE
			// statement while the physical catalog still has width zero. Reapply
			// that existing, identity-checked recovery instead of accepting an
			// arbitrary width difference as a schema match.
			physicalType := actual.Typ
			physicalType.Width = col.Typ.Width
			if types.T(actual.Typ.Id) != types.T_text || actual.Typ.Width != 0 || !proto.Equal(&col.Typ, &physicalType) {
				return moerr.NewInternalErrorNoCtx("M0: remote scan plan column mismatch")
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if recovered == nil {
				recovered = proto.Clone(local).(*plan.TableDef)
				if recovered.DbName == "" {
					recovered.DbName = source.SchemaName
				}
				if err := plan2.RecoverLegacyTinyText(ctx, recovered, resolve); err != nil {
					return err
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			valid := false
			for _, candidate := range recovered.Cols {
				if candidate != nil && !objectio.IsPhysicalAddr(candidate.Name) && candidate.Seqnum == col.Seqnum {
					valid = proto.Equal(&col.Typ, &candidate.Typ)
					break
				}
			}
			if !valid {
				return moerr.NewInternalErrorNoCtx("M0: remote scan plan column mismatch")
			}
		}
		planNames[strings.ToLower(col.Name)] = struct{}{}
		if col.OriginName != "" {
			planNames[strings.ToLower(col.OriginName)] = struct{}{}
		}
	}
	for _, attr := range source.Attributes {
		if _, ok := planNames[strings.ToLower(attr)]; !ok {
			return moerr.NewInternalErrorNoCtx("M0: remote reader attribute outside scan plan")
		}
	}
	return nil
}
