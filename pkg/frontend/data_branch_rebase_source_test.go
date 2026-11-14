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
	"errors"
	"math"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type rebaseSourceTestEngine struct {
	engine.Engine
	t                      *testing.T
	op                     client.TxnOperator
	db                     *rebaseSourceTestDatabase
	events                 []string
	resolveErr, reserveErr error
	onReserve              func()
}

func (e *rebaseSourceTestEngine) Database(ctx context.Context, name string, op client.TxnOperator) (engine.Database, error) {
	require.Equal(e.t, "feature", name)
	require.Same(e.t, e.op, op)
	account, err := defines.GetAccountId(ctx)
	require.NoError(e.t, err)
	require.Equal(e.t, uint32(7), account)
	e.events = append(e.events, "database")
	return e.db, e.resolveErr
}

func (e *rebaseSourceTestEngine) ReserveDatabaseInventory(_ context.Context, name string, id, epoch uint64, op client.TxnOperator) error {
	require.Equal(e.t, "feature", name)
	require.Equal(e.t, uint64(11), id)
	require.Equal(e.t, uint64(3), epoch)
	require.Same(e.t, e.op, op)
	e.events = append(e.events, "reserve")
	if e.onReserve != nil {
		e.onReserve()
	}
	return e.reserveErr
}

type rebaseSourceTestDatabase struct {
	engine.Database
	owner      *rebaseSourceTestEngine
	id         string
	relations  map[string]engine.Relation
	resolveErr error
}

func (d *rebaseSourceTestDatabase) GetDatabaseId(context.Context) string { return d.id }

func (d *rebaseSourceTestDatabase) Relation(_ context.Context, name string, opts any) (engine.Relation, error) {
	require.Nil(d.owner.t, opts)
	d.owner.events = append(d.owner.events, "relation:"+name)
	return d.relations[name], d.resolveErr
}

type rebaseSourceTestRelation struct {
	engine.Relation
	id uint64
}

func (r *rebaseSourceTestRelation) GetTableID(context.Context) uint64 { return r.id }

type rebaseSourceTestDependency struct {
	rebaseSourceTestRelation
	owner      *rebaseSourceTestEngine
	name       string
	err        error
	onRegister func()
}

func (d *rebaseSourceTestDependency) RegisterRebaseSnapshotDependency(_ context.Context, ts int64) error {
	require.Equal(d.owner.t, int64(123), ts)
	d.owner.events = append(d.owner.events, "guard:"+d.name)
	if d.onRegister != nil {
		d.onRegister()
	}
	return d.err
}

type rebaseSourceTestFixture struct {
	ctx     context.Context
	cancel  context.CancelFunc
	eng     *rebaseSourceTestEngine
	op      client.TxnOperator
	feature rebaseBoundDatabase
	token   databaseInventoryToken
}

func newRebaseSourceTestFixture(t *testing.T, mode pbtxn.TxnMode, isolation pbtxn.TxnIsolation) *rebaseSourceTestFixture {
	t.Helper()
	op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	op.EXPECT().IsSnapOp().Return(false).AnyTimes()
	op.EXPECT().Txn().Return(pbtxn.TxnMeta{Mode: mode, Isolation: isolation}).AnyTimes()
	ctx, cancel := context.WithCancel(defines.AttachAccountId(t.Context(), 7))
	t.Cleanup(cancel)
	e := &rebaseSourceTestEngine{t: t, op: op}
	e.db = &rebaseSourceTestDatabase{owner: e, id: "11", relations: map[string]engine.Relation{}}
	feature := rebaseBoundDatabase{
		source: cloneDatabaseSource{opAccountId: 7, toAccountId: 7, srcResolveDBName: "feature",
			snapshot: &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 123}}},
		defs: map[string]*plan.TableDef{},
	}
	// Cover a replayed table and an omitted ledger: both need dependencies.
	for i, name := range []string{"a", "z_ledger"} {
		id := uint64(21 + i)
		feature.defs[name] = &plan.TableDef{Name: name, DbId: 11, TblId: id}
		e.db.relations[name] = &rebaseSourceTestDependency{
			rebaseSourceTestRelation: rebaseSourceTestRelation{id: id}, owner: e, name: name,
		}
	}
	return &rebaseSourceTestFixture{ctx: ctx, cancel: cancel, eng: e, op: op, feature: feature,
		token: databaseInventoryToken{accountID: 7, databaseID: 11, epoch: 3, tracked: true}}
}

func (f *rebaseSourceTestFixture) reserve() error {
	return reserveRebaseFeatureSource(f.ctx, f.eng, f.op, f.feature, f.token)
}

func TestRebaseSourceReservationOwnerAndInventory(t *testing.T) {
	f := newRebaseSourceTestFixture(t, pbtxn.TxnMode_Pessimistic, pbtxn.TxnIsolation_RC)
	require.NoError(t, f.reserve())
	require.Equal(t, []string{"database", "relation:a", "relation:z_ledger", "reserve", "guard:a", "guard:z_ledger"}, f.eng.events)
	// Any attempted Commit/Rollback/new owner would be an unexpected mock call.
}

func TestRebaseExpectedSourceSQLContract(t *testing.T) {
	const base = "data branch rebase database dst from f{snapshot='fh'} against u{snapshot='uh'} between f{snapshot='bf'} and u{snapshot='bu'}"
	for _, suffix := range []string{
		" for update expect (11, 3)",
		" tables (t) for update expect (18446744073709551615, 3) when conflict accept",
	} {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, base+suffix, 1)
		require.NoError(t, err)
		func() {
			defer stmt.Free()
			r := stmt.(*tree.DataBranchRebase)
			require.NotNil(t, r.SourceExpectation)
			parsed, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tree.String(stmt, dialect.MYSQL), 1)
			require.NoError(t, err)
			defer parsed.Free()
			require.Equal(t, r.SourceExpectation, parsed.(*tree.DataBranchRebase).SourceExpectation)
			restore := SetRebaseSourceExpectationForTest(false)
			defer restore()
			// No session/engine: default-disabled rejection precedes all work.
			err = handleBranchRebase(&ExecCtx{reqCtx: t.Context()}, nil, r)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
			require.ErrorContains(t, err, "publication is disabled")
		}()
	}
	for _, suffix := range []string{
		" for update", " for update expect (0, 3)", " for update expect (11, 0)",
		" for update expect (-1, 3)", " for update expect (11, 1+2)",
		" for update expect (18446744073709551616, 3)", " for update expect ('11', 3)",
	} {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, base+suffix, 1)
		if stmt != nil {
			stmt.Free()
		}
		require.Error(t, err, suffix)
	}
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select expect from t", 1)
	require.NoError(t, err, "EXPECT must remain an unreserved identifier")
	stmt.Free()
}

func TestRebaseSourceReservationPrewriteRejection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*rebaseSourceTestFixture)
	}{
		{"untracked", func(f *rebaseSourceTestFixture) { f.token.tracked = false }},
		{"wrong_account", func(f *rebaseSourceTestFixture) { f.token.accountID++ }},
		{"zero_id", func(f *rebaseSourceTestFixture) { f.token.databaseID = 0 }},
		{"zero_epoch", func(f *rebaseSourceTestFixture) { f.token.epoch = 0 }},
		{"exhausted_epoch", func(f *rebaseSourceTestFixture) { f.token.epoch = math.MaxUint64 }},
		{"missing_owner", func(f *rebaseSourceTestFixture) { f.op = nil }},
		{"snapshot_owner", func(f *rebaseSourceTestFixture) {
			op := mock_frontend.NewMockTxnOperator(gomock.NewController(f.eng.t))
			op.EXPECT().IsSnapOp().Return(true)
			f.op = op
		}},
		{"cross_account_source", func(f *rebaseSourceTestFixture) { f.feature.source.opAccountId++ }},
		{"cross_account_destination", func(f *rebaseSourceTestFixture) { f.feature.source.toAccountId++ }},
		{"missing_snapshot", func(f *rebaseSourceTestFixture) { f.feature.source.snapshot = nil }},
		{"missing_timestamp", func(f *rebaseSourceTestFixture) { f.feature.source.snapshot.TS = nil }},
		{"invalid_timestamp", func(f *rebaseSourceTestFixture) { f.feature.source.snapshot.TS.PhysicalTime = 0 }},
		{"empty_inventory", func(f *rebaseSourceTestFixture) { f.feature.defs = nil }},
		{"missing_account", func(f *rebaseSourceTestFixture) { f.ctx = context.Background() }},
		{"missing_database", func(f *rebaseSourceTestFixture) { f.feature.source.srcResolveDBName = "" }},
		{"database_recreated", func(f *rebaseSourceTestFixture) { f.eng.db.id = "12" }},
		{"invalid_database_id", func(f *rebaseSourceTestFixture) { f.eng.db.id = "invalid" }},
		{"missing_definition", func(f *rebaseSourceTestFixture) { f.feature.defs["z_ledger"] = nil }},
		{"wrong_database", func(f *rebaseSourceTestFixture) { f.feature.defs["z_ledger"].DbId++ }},
		{"table_recreated", func(f *rebaseSourceTestFixture) { f.feature.defs["z_ledger"].TblId++ }},
		{"table_renamed", func(f *rebaseSourceTestFixture) { f.feature.defs["z_ledger"].Name = "renamed" }},
		{"storage_capability", func(f *rebaseSourceTestFixture) {
			f.eng.db.relations["z_ledger"] = &rebaseSourceTestRelation{id: 22}
		}},
		{"cancelled", func(f *rebaseSourceTestFixture) { f.cancel() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRebaseSourceTestFixture(t, pbtxn.TxnMode_Pessimistic, pbtxn.TxnIsolation_RC)
			tc.mutate(f)
			require.Error(t, f.reserve())
			require.NotContains(t, f.eng.events, "reserve")
			require.NotContains(t, f.eng.events, "guard:a")
			require.NotContains(t, f.eng.events, "guard:z_ledger")
		})
	}
	for _, tc := range []struct {
		name      string
		mode      pbtxn.TxnMode
		isolation pbtxn.TxnIsolation
	}{
		{"optimistic", pbtxn.TxnMode_Optimistic, pbtxn.TxnIsolation_RC},
		{"snapshot_isolation", pbtxn.TxnMode_Pessimistic, pbtxn.TxnIsolation_SI},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRebaseSourceTestFixture(t, tc.mode, tc.isolation)
			require.Error(t, f.reserve())
			require.Empty(t, f.eng.events)
		})
	}
}

func TestRebaseSourceReservationErrorsStopBeforeCandidate(t *testing.T) {
	sentinel := errors.New("source operation failed")
	for _, tc := range []struct {
		name     string
		mutate   func(*rebaseSourceTestFixture)
		expected []string
		want     error
	}{
		{"database", func(f *rebaseSourceTestFixture) { f.eng.resolveErr = sentinel }, []string{"database"}, sentinel},
		{"relation", func(f *rebaseSourceTestFixture) { f.eng.db.resolveErr = sentinel }, []string{"database", "relation:a"}, sentinel},
		{"reservation", func(f *rebaseSourceTestFixture) { f.eng.reserveErr = sentinel }, []string{"database", "relation:a", "relation:z_ledger", "reserve"}, sentinel},
		{"dependency", func(f *rebaseSourceTestFixture) { f.eng.db.relations["a"].(*rebaseSourceTestDependency).err = sentinel }, []string{"database", "relation:a", "relation:z_ledger", "reserve", "guard:a"}, sentinel},
		{"cancel_after_reservation", func(f *rebaseSourceTestFixture) { f.eng.onReserve = f.cancel }, []string{"database", "relation:a", "relation:z_ledger", "reserve"}, context.Canceled},
		{"cancel_between_dependencies", func(f *rebaseSourceTestFixture) {
			f.eng.db.relations["a"].(*rebaseSourceTestDependency).onRegister = f.cancel
		}, []string{"database", "relation:a", "relation:z_ledger", "reserve", "guard:a"}, context.Canceled},
		{"cancel_after_last_dependency", func(f *rebaseSourceTestFixture) {
			f.eng.db.relations["z_ledger"].(*rebaseSourceTestDependency).onRegister = f.cancel
		}, []string{"database", "relation:a", "relation:z_ledger", "reserve", "guard:a", "guard:z_ledger"}, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRebaseSourceTestFixture(t, pbtxn.TxnMode_Pessimistic, pbtxn.TxnIsolation_RC)
			tc.mutate(f)
			require.ErrorIs(t, f.reserve(), tc.want)
			require.Equal(t, tc.expected, f.eng.events)
		})
	}
}
