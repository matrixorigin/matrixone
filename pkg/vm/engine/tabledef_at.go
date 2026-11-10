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

package engine

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// TableDefAt returns the definition of databaseName.tableName as of at, read
// through a snapshot clone of txnOp. It returns nil when the database or the
// table does not exist at that time.
func TableDefAt(
	ctx context.Context,
	e Engine,
	txnOp client.TxnOperator,
	databaseName, tableName string,
	at types.TS,
) (*plan.TableDef, error) {
	snapshotOp := txnOp.CloneSnapshotOp(at.ToTimestamp())
	database, err := e.Database(ctx, databaseName, snapshotOp)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrBadDB) {
			return nil, nil
		}
		return nil, err
	}
	relation, err := database.Relation(ctx, tableName, nil)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return nil, nil
		}
		return nil, err
	}
	return relation.CopyTableDef(ctx), nil
}

// SameTableSchema reports whether left and right are the same table at the
// same schema version.
func SameTableSchema(left, right *plan.TableDef) bool {
	return left != nil && right != nil &&
		left.TblId == right.TblId && left.Version == right.Version
}
