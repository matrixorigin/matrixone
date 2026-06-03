// Copyright 2024 Matrix Origin
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

package casgc

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// sqlTxnDuration is the per-query transaction budget for sweep reference reads.
const sqlTxnDuration = 5 * time.Minute

// sqlEnv implements sweepEnv over the real CN SQL executor.
type sqlEnv struct {
	uuid      string
	engine    engine.Engine
	txnClient client.TxnClient
}

// listAccountIDs reads all live account ids from the sys-account mo_account view.
func (e *sqlEnv) listAccountIDs(ctx context.Context) ([]uint32, error) {
	var ids []uint32
	err := sqlexec.RunTxnWithSqlContext(ctx, e.engine, e.txnClient, e.uuid,
		catalog.System_Account, sqlTxnDuration, nil, nil,
		func(sqlproc *sqlexec.SqlProcess, _ any) error {
			res, err := sqlexec.RunSql(sqlproc, "SELECT account_id FROM mo_catalog.mo_account")
			if err != nil {
				return err
			}
			defer res.Close()

			for _, bat := range res.Batches {
				vec := bat.Vecs[0]
				for i := 0; i < bat.RowCount(); i++ {
					if vec.IsNull(uint64(i)) {
						continue
					}
					// mo_account.account_id is INT SIGNED (T_int32).
					ids = append(ids, uint32(vector.GetFixedAtWithTypeCheck[int32](vec, i)))
				}
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// refsForAccount returns the per-account reference source (no I/O).
func (e *sqlEnv) refsForAccount(_ context.Context, accountID uint32) (accountRefs, error) {
	return &acctRefs{env: e, accountID: accountID}, nil
}

// acctRefs implements accountRefs scoped to one account tenant context.
type acctRefs struct {
	env       *sqlEnv
	accountID uint32
}

// datalinkColumns lists the account's datalink-typed, non-hidden, user-table columns.
func (r *acctRefs) datalinkColumns(ctx context.Context) ([]columnRef, error) {
	var cols []columnRef
	err := sqlexec.RunTxnWithSqlContext(ctx, r.env.engine, r.env.txnClient, r.env.uuid,
		r.accountID, sqlTxnDuration, nil, nil,
		func(sqlproc *sqlexec.SqlProcess, _ any) error {
			const sql = "SELECT att_database, att_relname, attname, atttyp FROM mo_catalog.mo_columns " +
				"WHERE att_database NOT IN ('mo_catalog','information_schema','mysql','system','system_metrics') " +
				"AND att_is_hidden = 0"
			res, err := sqlexec.RunSql(sqlproc, sql)
			if err != nil {
				return err
			}
			defer res.Close()

			for _, bat := range res.Batches {
				dbVec := bat.Vecs[0]
				tblVec := bat.Vecs[1]
				colVec := bat.Vecs[2]
				typVec := bat.Vecs[3]
				for i := 0; i < bat.RowCount(); i++ {
					if typVec.IsNull(uint64(i)) {
						continue
					}
					atttyp := typVec.CloneBytesAt(i)
					if !isDatalinkType(atttyp) {
						continue
					}
					cols = append(cols, columnRef{
						DBName:    dbVec.GetStringAt(i),
						TableName: tblVec.GetStringAt(i),
						ColName:   colVec.GetStringAt(i),
					})
				}
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return cols, nil
}

// scanColumn reads the non-null datalink URL strings of one column, optionally
// at a snapshot. snapshotHint is "" (live) or "{snapshot = 'name'}".
func (r *acctRefs) scanColumn(ctx context.Context, ref columnRef, snapshotHint string) ([]string, error) {
	sql := "SELECT `" + ref.ColName + "` FROM `" + ref.DBName + "`.`" + ref.TableName + "`"
	if snapshotHint != "" {
		sql += " " + snapshotHint
	}

	var values []string
	err := sqlexec.RunTxnWithSqlContext(ctx, r.env.engine, r.env.txnClient, r.env.uuid,
		r.accountID, sqlTxnDuration, nil, nil,
		func(sqlproc *sqlexec.SqlProcess, _ any) error {
			res, err := sqlexec.RunSql(sqlproc, sql)
			if err != nil {
				return err
			}
			defer res.Close()

			for _, bat := range res.Batches {
				vec := bat.Vecs[0]
				for i := 0; i < bat.RowCount(); i++ {
					if vec.IsNull(uint64(i)) {
						continue
					}
					values = append(values, vec.GetStringAt(i))
				}
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return values, nil
}

// liveSnapshots lists the account's currently-live snapshots.
func (r *acctRefs) liveSnapshots(ctx context.Context) ([]snapshotRef, error) {
	var snaps []snapshotRef
	err := sqlexec.RunTxnWithSqlContext(ctx, r.env.engine, r.env.txnClient, r.env.uuid,
		r.accountID, sqlTxnDuration, nil, nil,
		func(sqlproc *sqlexec.SqlProcess, _ any) error {
			res, err := sqlexec.RunSql(sqlproc, "SELECT sname, ts FROM mo_catalog.mo_snapshots")
			if err != nil {
				return err
			}
			defer res.Close()

			for _, bat := range res.Batches {
				nameVec := bat.Vecs[0]
				tsVec := bat.Vecs[1]
				for i := 0; i < bat.RowCount(); i++ {
					if nameVec.IsNull(uint64(i)) {
						continue
					}
					// mo_snapshots.ts is BIGINT (T_int64).
					snaps = append(snaps, snapshotRef{
						Name: nameVec.GetStringAt(i),
						TS:   vector.GetFixedAtWithTypeCheck[int64](tsVec, i),
					})
				}
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return snaps, nil
}
