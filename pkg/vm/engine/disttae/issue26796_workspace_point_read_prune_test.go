// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"

	"github.com/stretchr/testify/require"
)

// Issue 26796: in a long transaction, single-row primary-key-point UPDATE
// latency grew linearly with the number of rows already modified by the
// transaction. The in-memory read re-encoded every workspace insert batch's
// primary keys on every statement, so one statement's read cost scaled with the
// whole workspace. The fix records each insert entry's encoded primary-key
// envelope once at write time, skips whole batches that cannot contain the
// filter's keys, and answers exact single-key probes from a per-batch index
// that is rebuilt only when a statement-boundary merge or shrink invalidates
// the batch layout.
//
// The work oracles below count real work, not time: EncodePKRows counts every
// row EncodePrimaryKeyVector encoded (write-time envelopes, legacy per-read
// filter passes, and index constructions all share that encoder), so a scaling
// claim covers the maintenance replacement cost as well; FilterVectorRows and
// IndexProbes separate data-preservation. Every assertion is deterministic.

const (
	issue26796DBID   = 7
	issue26796TblID  = 42
	issue26796DB     = "db"
	issue26796Table  = "tbl"
	issue26796PKName = "pk"
)

// newShuffleForTest returns a deterministic PRNG for workload-order shuffles so
// growth and semantics tests replay identically.
func newShuffleForTest(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}

// newIssue26796Txn returns a transaction whose workspace resolves the
// single-column int64 primary key of one table at write time, with a working
// engine packer pool for canonical key encoding.
func newIssue26796Txn(t testing.TB) (*Transaction, *fileservice.Pool[*types.Packer]) {
	t.Helper()
	txn, pool := newIssue26796TxnWithTable(t, issue26796Table, issue26796TblID, issue26796TableDef())
	return txn, pool
}

// newIssue26796TxnWithTable registers one table under the given id and
// definition; both the primary workload fixture and the secondary-index
// membership fixture build on it.
func newIssue26796TxnWithTable(
	t testing.TB,
	tableName string,
	tableID uint64,
	tableDef *pbplan.TableDef,
) (*Transaction, *fileservice.Pool[*types.Packer]) {
	t.Helper()
	packerPool := fileservice.NewPool(
		4,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) { packer.Close() },
	)
	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc:            proc,
		engine:          &Engine{packerPool: packerPool},
		tableOps:        newTableOps(),
		databaseOps:     newDbOps(),
		deletedBlocks:   &deletedBlocks{},
		batchSelectList: make(map[*batch.Batch][]int64),
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),
	}
	op := newTxnOperatorForTestWithWorkspace(t, txn)
	txn.op = op

	db := &txnDatabase{
		op:           op,
		databaseId:   issue26796DBID,
		databaseName: issue26796DB,
	}
	txn.databaseOps.addCreateDatabase(genDatabaseKey(1, issue26796DB), 0, db)
	txn.tableOps.addCreateTable(
		genTableKey(1, tableName, issue26796DBID, issue26796DB),
		0,
		&txnTable{
			accountId: 1,
			tableId:   tableID,
			tableName: tableName,
			db:        db,
			tableDef:  tableDef,
		},
	)
	return txn, packerPool
}

// issue26796InsertOne appends one single-row insert entry in the shape of the
// issue's workload: one primary-key-point update contributes one insert row
// per statement.
func issue26796InsertOne(t *testing.T, txn *Transaction, pk, value int64) {
	t.Helper()
	bat := batch.NewWithSize(2)
	bat.Attrs = []string{issue26796PKName, "value"}
	bat.SetVector(0, vector.NewVec(types.T_int64.ToType()))
	bat.SetVector(1, vector.NewVec(types.T_int64.ToType()))
	require.NoError(t, vector.AppendFixed(bat.GetVector(0), pk, false, txn.proc.Mp()))
	require.NoError(t, vector.AppendFixed(bat.GetVector(1), value, false, txn.proc.Mp()))
	bat.SetRowCount(1)
	_, err := txn.WriteBatch(
		INSERT, "", 1, issue26796DBID, issue26796TblID,
		issue26796DB, issue26796Table, bat, DNStore{},
	)
	require.NoError(t, err)
}

// issue26796InsertBatch appends one multi-row insert entry with the given
// primary keys; values mirror the primary keys.
func issue26796InsertBatch(t *testing.T, txn *Transaction, pks ...int64) {
	t.Helper()
	bat := batch.NewWithSize(2)
	bat.Attrs = []string{issue26796PKName, "value"}
	bat.SetVector(0, vector.NewVec(types.T_int64.ToType()))
	bat.SetVector(1, vector.NewVec(types.T_int64.ToType()))
	for _, pk := range pks {
		require.NoError(t, vector.AppendFixed(bat.GetVector(0), pk, false, txn.proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.GetVector(1), pk, false, txn.proc.Mp()))
	}
	bat.SetRowCount(len(pks))
	_, err := txn.WriteBatch(
		INSERT, "", 1, issue26796DBID, issue26796TblID,
		issue26796DB, issue26796Table, bat, DNStore{},
	)
	require.NoError(t, err)
}

// issue26796Filter builds the in-memory primary-key filter for this table.
func issue26796Filter(
	t *testing.T,
	pool *fileservice.Pool[*types.Packer],
	base readutil.BasePKFilter,
) *readutil.MemPKFilter {
	t.Helper()
	filter, err := readutil.NewMemPKFilter(
		issue26796TableDef(),
		types.MaxTs().ToTimestamp(),
		pool,
		base,
		engine.FilterHint{},
	)
	require.NoError(t, err)
	return &filter
}

func issue26796TableDef() *pbplan.TableDef {
	return &pbplan.TableDef{
		Cols: []*pbplan.ColDef{
			{Name: issue26796PKName, Typ: pbplan.Type{Id: int32(types.T_int64)}},
			{Name: "value", Typ: pbplan.Type{Id: int32(types.T_int64)}},
		},
		Pkey: &pbplan.PrimaryKeyDef{PkeyColName: issue26796PKName},
	}
}

// issue26796Counters snapshots the readutil test counters.
type issue26796Counters struct {
	FilterVectorRows uint64
	EncodePKRows     uint64
	IndexProbes      uint64
	IndexBuilds      uint64
}

func issue26796SnapshotCounters() issue26796Counters {
	return issue26796Counters{
		FilterVectorRows: readutil.FilterVectorRowsForTest(),
		EncodePKRows:     readutil.EncodePKRowsForTest(),
		IndexProbes:      readutil.IndexProbesForTest(),
		IndexBuilds:      readutil.IndexBuildsForTest(),
	}
}

func (s issue26796Counters) minus(other issue26796Counters) issue26796Counters {
	return issue26796Counters{
		FilterVectorRows: s.FilterVectorRows - other.FilterVectorRows,
		EncodePKRows:     s.EncodePKRows - other.EncodePKRows,
		IndexProbes:      s.IndexProbes - other.IndexProbes,
		IndexBuilds:      s.IndexBuilds - other.IndexBuilds,
	}
}

// issue26796PointReadTable runs the in-memory uncommitted-insert read
// against one table, mirroring one point-read statement of the workload. It
// returns the values read plus the work counters of this statement's read:
// every EncodePrimaryKeyVector call path (envelope computation on the write
// side, the legacy filter pass, and index construction) shares the
// EncodePKRows counter, so no hidden job escapes the scaling oracles.
func issue26796PointReadTable(
	t *testing.T,
	txn *Transaction,
	filter *readutil.MemPKFilter,
	dbID uint64,
	tableID uint64,
	tableName string,
) (values []int64, delta issue26796Counters) {
	t.Helper()
	before := issue26796SnapshotCounters()
	ls := &LocalDisttaeDataSource{
		ctx: context.Background(),
		table: &txnTable{
			db:        &txnDatabase{op: txn.op, databaseId: dbID, databaseName: issue26796DB},
			tableId:   tableID,
			tableName: tableName,
		},
		txnOffset:   len(txn.writes),
		memPKFilter: filter,
	}
	outBatch := batch.NewWithSize(1)
	outBatch.Attrs = []string{"value"}
	outBatch.SetVector(0, vector.NewVec(types.T_int64.ToType()))
	err := ls.filterInMemUnCommittedInserts(context.Background(), []uint16{1}, 0, txn.proc.Mp(), outBatch)
	require.NoError(t, err)

	vals := vector.MustFixedColNoTypeCheck[int64](outBatch.GetVector(0))
	out := make([]int64, len(vals))
	copy(out, vals)
	return out, issue26796SnapshotCounters().minus(before)
}

// issue26796PointRead runs the point read for the default workload table.
func issue26796PointRead(
	t *testing.T,
	txn *Transaction,
	filter *readutil.MemPKFilter,
) ([]int64, issue26796Counters) {
	t.Helper()
	return issue26796PointReadTable(t, txn, filter, issue26796DBID, issue26796TblID, issue26796Table)
}

// TestIssue26796PointReadWorkStaysBounded proves the defect and its fix
// deterministically: one primary-key-point read over a workspace holding K
// inserted rows must not re-encode the whole workspace's primary keys on the
// read path. The test counts encoded rows, so the assertion measures work,
// not time. It covers both workspace shapes the issue records:
//
//   - singleton entries: one single-row insert per statement (ORM loop);
//   - merged fat batches: statement-boundary merging coalesces the same
//     entries into multi-row batches with dispersed keys, whose wide
//     envelopes cannot be pruned by range checks alone.
//
// On the unfixed base both shapes fail: every read encodes every row of
// every entry through FilterVector. With the entry-envelope skip plus the
// per-batch exact-key index, a point read encodes nothing once the index is
// warm; the cold first probe pays at most one construction.
func TestIssue26796PointReadWorkStaysBounded(t *testing.T) {
	bound := uint64(8)

	t.Run("singleton_entries", func(t *testing.T) {
		for _, stmtCount := range []int64{64, 512} {
			t.Run("entries_"+strconv.FormatInt(stmtCount, 10), func(t *testing.T) {
				txn, pool := newIssue26796Txn(t)
				for pk := int64(1); pk <= stmtCount; pk++ {
					issue26796InsertOne(t, txn, pk, pk)
				}

				filter := issue26796Filter(t, pool, readutil.BasePKFilter{
					Valid: true,
					Op:    function.EQUAL,
					Oid:   types.T_int64,
					LB:    types.EncodeFixed(int64(17)),
				})
				values, d := issue26796PointRead(t, txn, filter)

				require.Equal(t, []int64{17}, values,
					"the point read must return exactly the pk=17 row")
				require.LessOrEqual(t, d.FilterVectorRows, bound,
					"point-read filter work must stay bounded: got %d rows encoded over %d workspace rows; "+
						"per-statement workspace work must not grow with transaction size (issue 26796)",
					d.FilterVectorRows, stmtCount)
				require.LessOrEqual(t, d.EncodePKRows, uint64(1),
					"one cold probe hydrates only the matching single-row entry; got %d rows", d.EncodePKRows)
				require.NotZero(t, d.IndexProbes,
					"the fixed reader answers exact probes through the per-batch key index")
			})
		}
	})

	t.Run("merged_dispersed_keys", func(t *testing.T) {
		// The recorded workload updates rows in shuffled primary-key order, so
		// the statement-boundary merge coalesces entries into fat batches
		// whose encoded-key envelopes always overlap the probe. A bounded
		// point read then needs an exact per-batch key index, not a range test.
		txn, pool := newIssue26796Txn(t)
		const total = 400
		order := make([]int64, 0, total)
		for pk := int64(1); pk <= total; pk++ {
			order = append(order, pk)
		}
		rnd26796 := newShuffleForTest(20260101)
		for i := len(order) - 1; i > 0; i-- {
			j := rnd26796.Intn(i + 1)
			order[i], order[j] = order[j], order[i]
		}
		for _, pk := range order {
			issue26796InsertOne(t, txn, pk, pk)
			// statement boundary, like IncrStatementID -> merge
			txn.Lock()
			require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
			txn.Unlock()
		}

		for i, probe := range []int64{1, 17, 200, 400} {
			filter := issue26796Filter(t, pool, readutil.BasePKFilter{
				Valid: true,
				Op:    function.EQUAL,
				Oid:   types.T_int64,
				LB:    types.EncodeFixed(probe),
			})
			values, d := issue26796PointRead(t, txn, filter)
			require.Equal(t, []int64{probe}, values,
				"point read of pk=%d must return its row", probe)
			require.LessOrEqual(t, d.FilterVectorRows, bound,
				"point read over merged fat batches with dispersed keys must not encode whole batches; got %d rows",
				d.FilterVectorRows)
			require.NotZero(t, d.IndexProbes,
				"probing pk=%d must answer through the merged batch's key index", probe)
			if i == 0 {
				// The first probe after the merges pays one cold build of the
				// merged batch's index; later probes must reuse it.
				require.LessOrEqual(t, d.EncodePKRows, uint64(total),
					"the cold probe may build the index once (bounded by the batch), got %d", d.EncodePKRows)
			} else {
				require.Zero(t, d.EncodePKRows,
					"warm probes over merged fat batches must not re-encode; got %d rows", d.EncodePKRows)
			}
		}
	})
}

// TestIssue26796WorkspaceMaintenanceWorkBounded measures the TOTAL key-encoding
// work of a production-cadence loop - insert one row, run the statement-boundary
// merge, then point-read the updated row - across a whole transaction of 400
// statements. Cold index construction after each merge and write-time envelope
// computation are included, because the counter observes EncodePrimaryKeyVector
// itself, not one caller. The unfixed base encodes every accumulated row on
// every read (O(K^2) over K statements); the fix keeps the total linear with a
// small slope (envelope bytes per write plus one rebuild per merge gate), so
// the bound scales with the statement count, never with its square.
func TestIssue26796WorkspaceMaintenanceWorkBounded(t *testing.T) {
	const total = 400
	// Amortized bound: envelope writes (total rows) plus at most one cold
	// rebuild of the merged batch per merge-gate event. With the >=30 entry
	// gate that is 13 merges, each rebuilding a batch below 400 rows, so
	// 400 + 13*400 = 5600 dominates the observed ~3.2k; the base's
	// sum-over-statements behavior would need ~80k encodes for 400 rows.
	const encodeBound = uint64(5600)

	txn, pool := newIssue26796Txn(t)
	order := make([]int64, 0, total)
	for pk := int64(1); pk <= total; pk++ {
		order = append(order, pk)
	}
	rnd26796 := newShuffleForTest(20260501)
	for i := len(order) - 1; i > 0; i-- {
		j := rnd26796.Intn(i + 1)
		order[i], order[j] = order[j], order[i]
	}

	readutil.ResetFilterVectorRowsForTest()
	readutil.ResetEncodePKRowsForTest()
	readutil.ResetIndexProbesForTest()
	readutil.ResetIndexBuildsForTest()
	start := issue26796SnapshotCounters()

	for _, pk := range order {
		issue26796InsertOne(t, txn, pk, pk)
		// one statement boundary per statement: the >=30-entry gate inside
		// mergeTxnWorkspaceLocked fires periodically, exactly like production.
		txn.Lock()
		require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
		txn.Unlock()

		filter := issue26796Filter(t, pool, readutil.BasePKFilter{
			Valid: true,
			Op:    function.EQUAL,
			Oid:   types.T_int64,
			LB:    types.EncodeFixed(pk),
		})
		values, d := issue26796PointRead(t, txn, filter)
		require.Equal(t, []int64{pk}, values,
			"the statement's own point read must return the updated row")
		require.Zero(t, d.FilterVectorRows,
			"point reads never run the per-row filter once pruning applies")
		require.NotZero(t, d.IndexProbes)
	}

	end := issue26796SnapshotCounters()
	d := end.minus(start)
	t.Logf("issue26796 maintenance totals over %d statements: encodePKRows=%d indexBuilds=%d indexProbes=%d filterVectorRows=%d",
		total, d.EncodePKRows, d.IndexBuilds, d.IndexProbes, d.FilterVectorRows)

	require.LessOrEqual(t, d.EncodePKRows, encodeBound,
		"total key-encoding work over %d statements must stay near-linear: got %d rows encoded "+
			"(write envelopes + one rebuild per merge gate); the unfixed base needs the whole workspace per read",
		total, d.EncodePKRows)
	require.Zero(t, d.FilterVectorRows,
		"no statement may fall back to the per-row filter pass")
	require.GreaterOrEqual(t, d.IndexProbes, uint64(total),
		"every statement's read must be answered by the exact-key index")
	require.LessOrEqual(t, d.IndexBuilds, 2*uint64(total),
		"index constructions stay near one per statement (its own target entry) plus one per merge-gate event; got %d builds for %d statements",
		d.IndexBuilds, total)

	// Warm phase: with no further batching mutations, probing any key encodes
	// nothing new - reuse, not reconstruction.
	for r := 0; r < 30; r++ {
		probe := order[(r*37)%int(total)]
		filter := issue26796Filter(t, pool, readutil.BasePKFilter{
			Valid: true,
			Op:    function.EQUAL,
			Oid:   types.T_int64,
			LB:    types.EncodeFixed(probe),
		})
		values, w := issue26796PointRead(t, txn, filter)
		require.Equal(t, []int64{probe}, values)
		require.Zero(t, w.EncodePKRows,
			"warm probes must reuse hydrated indexes without re-encoding")
		require.Zero(t, w.IndexBuilds,
			"warm probes must not rebuild any index")
		require.NotZero(t, w.IndexProbes)
	}
}

// TestIssue26796WorkspaceMergingBoundsEntryCount documents that the workspace
// merge, which runs at every statement boundary (IncrStatementID ->
// mergeTxnWorkspaceLocked), keeps the entry count bounded during long
// transactions, so the remaining per-statement growth driver on the unfixed
// base is the per-row filter work this fix removes, not the entry count.
func TestIssue26796WorkspaceMergingBoundsEntryCount(t *testing.T) {
	txn, _ := newIssue26796Txn(t)
	ctx := context.Background()

	const maxCycles = 2000
	for c := 1; c <= maxCycles; c++ {
		issue26796InsertOne(t, txn, int64(c), int64(c))
		txn.Lock()
		require.NoError(t, txn.mergeTxnWorkspaceLocked(ctx))
		txn.Unlock()
	}
	require.LessOrEqual(t, len(txn.writes), 300,
		"statement-boundary merging must keep the workspace entry count bounded in long transactions; got %d",
		len(txn.writes))
}
