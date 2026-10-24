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

package plan

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// Unique SQL isolates recovery/wrapper cases without resetting production state,
// including repeated -count executions in one process. Mechanics use local caches.
var legacyTextCacheFixtureID atomic.Uint64

func legacyCacheFixtureSQL(sql string) string {
	return fmt.Sprintf("%s /* legacy_cache_fixture_%d */", sql, legacyTextCacheFixtureID.Add(1))
}

func legacyCacheColumn(name string, sequence uint32, width int32) *planpb.ColDef {
	return &planpb.ColDef{Name: name, OriginName: name, Seqnum: sequence,
		Typ: planpb.Type{Id: int32(types.T_text), Width: width}}
}

func legacyCacheTable(sql string, columns ...*planpb.ColDef) *planpb.TableDef {
	return &planpb.TableDef{TblId: 71, LogicalId: 71, DbName: "cache_db", Name: "cache_table",
		TableType: catalog.SystemOrdinaryRel, Createsql: sql, Cols: columns}
}

func warmLegacyCacheSQL(t *testing.T, sql string) legacyTinyTextCreateEvidence {
	t.Helper()
	evidence, err := parseCachedLegacyTinyTextEvidence(t.Context(), sql)
	require.NoError(t, err)
	_, hit := legacyTextEvidenceCache.get(sql)
	require.True(t, hit, "bounded successful fixture must actually be warm")
	return evidence
}

func legacyCacheRetainedPayload(cache *legacyTextParseCache) (entries, bytes int) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	for _, entry := range cache.entries {
		if entry.createSQL == "" {
			continue
		}
		entries++
		bytes += len(entry.createSQL) + len(entry.evidence.likeDB) + len(entry.evidence.likeName)
		bytes += len(entry.evidence.columns) * int(unsafe.Sizeof(legacyTinyTextColumn{}))
		for _, column := range entry.evidence.columns {
			bytes += len(column.name)
		}
	}
	return
}

func TestLegacyTextParseCacheOwnershipAndExactKeys(t *testing.T) {
	var cache legacyTextParseCache
	backing := strings.Repeat("padding", 1024) + "CREATE TABLE X db_name source_name column_name"
	start := strings.Index(backing, "CREATE TABLE")
	key := backing[start : start+len("CREATE TABLE X")]
	db := backing[strings.Index(backing, "db_name"):][:len("db_name")]
	name := backing[strings.Index(backing, "source_name"):][:len("source_name")]
	columnName := backing[strings.Index(backing, "column_name"):][:len("column_name")]
	input := legacyTinyTextCreateEvidence{likeDB: db, likeName: name,
		columns: []legacyTinyTextColumn{{ordinal: 1, name: columnName, width: types.MaxTinyTextLen}}}
	cache.put(key, input)
	got, hit := cache.get(key)
	require.True(t, hit)
	require.Equal(t, input, got)
	require.False(t, unsafe.StringData(db) == unsafe.StringData(got.likeDB), "database string must be detached")
	require.False(t, unsafe.StringData(name) == unsafe.StringData(got.likeName), "LIKE name must be detached")
	require.False(t, unsafe.StringData(columnName) == unsafe.StringData(got.columns[0].name), "column name must be detached")
	require.NotSame(t, &input.columns[0], &got.columns[0])
	require.False(t, unsafe.StringData(key) == unsafe.StringData(cache.entries[0].createSQL), "SQL key must be detached")
	// Mutate only caller-owned input, never returned/shared cached evidence.
	input.likeDB, input.likeName = "changed_db", "changed_table"
	input.columns[0] = legacyTinyTextColumn{ordinal: 9, name: "changed_column", width: types.MaxLongTextLen}
	got, hit = cache.get(key)
	require.True(t, hit)
	require.Equal(t, "db_name", got.likeDB)
	require.Equal(t, "source_name", got.likeName)
	require.Equal(t, legacyTinyTextColumn{ordinal: 1, name: "column_name", width: types.MaxTinyTextLen}, got.columns[0])
	for _, different := range []string{strings.ToLower(key), key + " ", key + " /* x */"} {
		_, hit = cache.get(different)
		require.False(t, hit, "cache keys must be exact SQL text")
	}
	cache.put(strings.ToLower(key), legacyTinyTextCreateEvidence{likeName: "different_exact_key"})
	other, hit := cache.get(strings.ToLower(key))
	require.True(t, hit)
	require.Equal(t, "different_exact_key", other.likeName)
	got, hit = cache.get(key)
	require.True(t, hit)
	require.Equal(t, "source_name", got.likeName)
}

func TestLegacyTextParseCacheAdmissionAndEviction(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		e    legacyTinyTextCreateEvidence
		want bool
	}{
		{"empty key", "", legacyTinyTextCreateEvidence{}, false},
		{"SQL at limit", strings.Repeat("s", 16<<10), legacyTinyTextCreateEvidence{}, true},
		{"SQL above limit", strings.Repeat("s", (16<<10)+1), legacyTinyTextCreateEvidence{}, false},
		{"columns at limit", "c", legacyTinyTextCreateEvidence{columns: make([]legacyTinyTextColumn, 256)}, true},
		{"columns above limit", "c", legacyTinyTextCreateEvidence{columns: make([]legacyTinyTextColumn, 257)}, false},
		// Synthetic private-API evidence isolates the owned-byte guard from SQL
		// and column limits; a valid <=16KiB SQL need not reach this defensive cap.
		{"owned bytes at limit", "b", legacyTinyTextCreateEvidence{likeName: strings.Repeat("n", (64<<10)-1)}, true},
		{"owned bytes above limit", "b", legacyTinyTextCreateEvidence{likeName: strings.Repeat("n", 64<<10)}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var cache legacyTextParseCache
			cache.put(test.sql, test.e)
			_, hit := cache.get(test.sql)
			require.Equal(t, test.want, hit)
			count, retained := legacyCacheRetainedPayload(&cache)
			if test.want {
				require.Equal(t, 1, count)
			} else {
				require.Zero(t, count)
			}
			require.LessOrEqual(t, retained, 64<<10)
		})
	}
	var cache legacyTextParseCache
	for index := 0; index < 32; index++ {
		key := fmt.Sprintf("%02d", index)
		cache.put(key, legacyTinyTextCreateEvidence{likeName: strings.Repeat("x", (64<<10)-len(key))})
	}
	count, retained := legacyCacheRetainedPayload(&cache)
	require.Equal(t, 32, count)
	require.Equal(t, 2<<20, retained)
	held, hit := cache.get("00")
	require.True(t, hit)
	before := cache.next
	cache.put("00", held)
	require.Equal(t, before, cache.next, "duplicate publication must not evict an unrelated key")
	cache.put("new", legacyTinyTextCreateEvidence{likeName: "replacement"})
	_, hit = cache.get("00")
	require.False(t, hit)
	require.Equal(t, strings.Repeat("x", (64<<10)-2), held.likeName, "reader-held immutable evidence survives eviction")
	for index := 1; index < 32; index++ {
		_, hit = cache.get(fmt.Sprintf("%02d", index))
		require.True(t, hit)
	}
	count, retained = legacyCacheRetainedPayload(&cache)
	require.Equal(t, 32, count)
	require.LessOrEqual(t, retained, 2<<20)
}

func TestLegacyTextParseCacheConcurrentPublication(t *testing.T) {
	var cache legacyTextParseCache
	value := func(index int) legacyTinyTextCreateEvidence {
		return legacyTinyTextCreateEvidence{columns: []legacyTinyTextColumn{{ordinal: 1, name: fmt.Sprintf("p%d", index), width: types.MaxTinyTextLen}}}
	}
	for index := 0; index < 32; index++ {
		cache.put(fmt.Sprintf("key%d", index), value(index))
	}
	held, hit := cache.get("key0")
	require.True(t, hit)
	run := func(work func(int) error) {
		t.Helper()
		var wait sync.WaitGroup
		start := make(chan struct{})
		workerErrors := make(chan error, 8)
		for worker := 0; worker < 8; worker++ {
			wait.Add(1)
			go func(worker int) {
				defer wait.Done()
				<-start
				if err := work(worker); err != nil {
					workerErrors <- err
				}
			}(worker)
		}
		close(start)
		wait.Wait()
		close(workerErrors)
		for err := range workerErrors {
			require.NoError(t, err)
		}
	}
	// A read-only phase guarantees both hits and misses, independently of the
	// scheduler. An existing-key phase then races duplicate publication.
	run(func(worker int) error {
		for index := 0; index < 16; index++ {
			key := worker + index
			got, ok := cache.get(fmt.Sprintf("key%d", key))
			if !ok || len(got.columns) != 1 || got.columns[0].name != fmt.Sprintf("p%d", key) {
				return fmt.Errorf("stable concurrent hit lost/corrupted: %d", key)
			}
			if _, ok := cache.get("absent"); ok {
				return errors.New("concurrent exact-key miss became a hit")
			}
		}
		return nil
	})
	run(func(int) error { cache.put("key0", value(0)); return nil })
	require.Zero(t, cache.next)
	// Unique new keys force eviction; any concurrently observed hit must still
	// be complete evidence for that exact key. Eviction itself permits a miss.
	run(func(worker int) error {
		for step := 0; step < 16; step++ {
			index := 32 + worker*16 + step
			key := fmt.Sprintf("key%d", index)
			cache.put(key, value(index))
			if got, ok := cache.get(key); ok && (len(got.columns) != 1 || got.columns[0].name != fmt.Sprintf("p%d", index) || got.columns[0].width != types.MaxTinyTextLen) {
				return fmt.Errorf("concurrent publication corrupted key %d", index)
			}
		}
		return nil
	})
	_, hit = cache.get("key0")
	require.False(t, hit)
	require.Equal(t, "p0", held.columns[0].name)
	count, retained := legacyCacheRetainedPayload(&cache)
	require.Equal(t, 32, count)
	require.LessOrEqual(t, retained, 2<<20)
}

func TestLegacyTextParseCacheWarmDirectRecovery(t *testing.T) {
	sql := legacyCacheFixtureSQL("CREATE TABLE cache_direct (id INT, tiny TINYTEXT, medium MEDIUMTEXT, large LONGTEXT, ordinary TEXT)")
	evidence := warmLegacyCacheSQL(t, sql)
	require.Equal(t, []legacyTinyTextColumn{{ordinal: 2, name: "tiny", width: types.MaxTinyTextLen},
		{ordinal: 3, name: "medium", width: types.MaxMediumTextLen}, {ordinal: 4, name: "large", width: types.MaxLongTextLen}}, evidence.columns)
	for iteration := 0; iteration < 2; iteration++ {
		cols := []*planpb.ColDef{{Name: "id", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_int32)}},
			legacyCacheColumn("tiny", 1, 0), legacyCacheColumn("medium", 2, 0),
			legacyCacheColumn("large", 3, 0), legacyCacheColumn("ordinary", 4, 0)}
		table := legacyCacheTable(sql, append([]*planpb.ColDef(nil), cols...)...)
		require.NoError(t, RecoverLegacyTinyTextFromCreateSQL(t.Context(), table))
		for index, width := range []int32{0, types.MaxTinyTextLen, types.MaxMediumTextLen, types.MaxLongTextLen, 0} {
			require.Equal(t, width, table.Cols[index].Typ.Width)
			require.Zero(t, cols[index].Typ.Width, "cached evidence must not mutate catalog-owned columns")
			if width != 0 {
				require.NotSame(t, cols[index], table.Cols[index])
			}
		}
	}
	guardSQL := legacyCacheFixtureSQL("CREATE TABLE cache_guards (id INT, payload TINYTEXT, ordinary TEXT)")
	warmLegacyCacheSQL(t, guardSQL)
	for _, test := range []struct {
		name   string
		change func(*planpb.TableDef)
		want   int32
	}{
		{"rename keeps Seqnum", func(table *planpb.TableDef) { table.Version = 1; table.Cols[1].Name = "renamed" }, types.MaxTinyTextLen},
		{"copied ALTER identity", func(table *planpb.TableDef) { table.TblId++ }, 0},
		{"missing logical identity", func(table *planpb.TableDef) { table.LogicalId = 0 }, 0},
		{"different Seqnum", func(table *planpb.TableDef) { table.Cols[1].Seqnum = 9 }, 0},
		{"duplicate Seqnum", func(table *planpb.TableDef) { table.Cols[2].Seqnum = 1 }, 0},
		{"hidden original", func(table *planpb.TableDef) { table.Cols[1].Hidden = true }, 0},
		{"different type", func(table *planpb.TableDef) { table.Cols[1].Typ.Id = int32(types.T_varchar) }, 0},
		{"existing width", func(table *planpb.TableDef) { table.Cols[1].Typ.Width = types.MaxLongTextLen }, types.MaxLongTextLen},
		{"view", func(table *planpb.TableDef) { table.TableType = catalog.SystemViewRel }, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			table := legacyCacheTable(guardSQL, &planpb.ColDef{Name: "id", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_int32)}},
				legacyCacheColumn("payload", 1, 0), legacyCacheColumn("ordinary", 2, 0))
			test.change(table)
			require.NoError(t, RecoverLegacyTinyTextFromCreateSQL(t.Context(), table))
			require.Equal(t, test.want, table.Cols[1].Typ.Width)
			if test.name == "duplicate Seqnum" {
				require.Zero(t, table.Cols[2].Typ.Width)
			}
		})
	}
}

func TestLegacyTextParseCacheWarmLikeResolvesCurrentState(t *testing.T) {
	sourceSQL := legacyCacheFixtureSQL("CREATE TABLE cache_source (payload TINYTEXT, ordinary TEXT)")
	mediumSQL := legacyCacheFixtureSQL("CREATE TABLE cache_source (payload MEDIUMTEXT, ordinary TEXT)")
	likeSQL := legacyCacheFixtureSQL("CREATE TABLE cache_target LIKE source_db.cache_source")
	cycleSQL := legacyCacheFixtureSQL("CREATE TABLE cache_source LIKE source_db.cache_source")
	for _, sql := range []string{sourceSQL, mediumSQL, likeSQL, cycleSQL} {
		warmLegacyCacheSQL(t, sql)
	}
	makeSource := func(sql string) *planpb.TableDef {
		table := legacyCacheTable(sql, legacyCacheColumn("payload", 0, 0), legacyCacheColumn("ordinary", 1, 0))
		table.TblId, table.LogicalId = 81, 81
		table.Name = "cache_source"
		table.DbName = "" // The resolver's raw definition remains caller-owned.
		return table
	}
	tiny, medium, cyclic := makeSource(sourceSQL), makeSource(mediumSQL), makeSource(cycleSQL)
	medium.TblId, medium.LogicalId = 82, 82 // A replacement source in a newer snapshot.
	cyclic.TblId, cyclic.LogicalId = 83, 83
	incompatible := makeSource(sourceSQL)
	incompatible.Cols[0].NotNull = true
	copied := makeSource(sourceSQL)
	copied.TblId++
	type snapshotKey struct{}
	sentinel := errors.New("current snapshot resolver error")
	calls := 0
	resolver := func(ctx context.Context, db, name string) (*planpb.TableDef, error) {
		calls++
		require.Equal(t, "source_db", db)
		require.Equal(t, "cache_source", name)
		switch ctx.Value(snapshotKey{}).(string) {
		case "tiny":
			return tiny, nil
		case "medium":
			return medium, nil
		case "missing":
			return nil, nil
		case "error":
			return nil, sentinel
		case "incompatible":
			return incompatible, nil
		case "copied source":
			return copied, nil
		case "cycle":
			return cyclic, nil
		default:
			return nil, errors.New("unexpected snapshot")
		}
	}
	for _, test := range []struct {
		snapshot string
		width    int32
	}{
		{"tiny", types.MaxTinyTextLen}, {"medium", types.MaxMediumTextLen}, {"missing", 0},
		{"error", 0}, {"incompatible", 0}, {"copied source", 0}, {"cycle", 0}, {"tiny", types.MaxTinyTextLen},
	} {
		before := calls
		table := legacyCacheTable(likeSQL, legacyCacheColumn("payload", 0, 0), legacyCacheColumn("ordinary", 1, 0))
		original := table.Cols[0]
		err := RecoverLegacyTinyText(context.WithValue(t.Context(), snapshotKey{}, test.snapshot), table, resolver)
		if test.snapshot == "error" {
			require.ErrorIs(t, err, sentinel)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, before+1, calls, "cached LIKE syntax must re-resolve in every current snapshot, including cycles/errors")
		require.Equal(t, test.width, table.Cols[0].Typ.Width, test.snapshot)
		require.Zero(t, original.Typ.Width)
		for _, source := range []*planpb.TableDef{tiny, medium, cyclic, incompatible, copied} {
			require.Zero(t, source.Cols[0].Typ.Width)
			require.Empty(t, source.DbName)
		}
	}
	// Same exact unqualified SQL, different current default databases. Neither
	// the first database nor its resolved definition belongs in syntax evidence.
	unqualified := legacyCacheFixtureSQL("CREATE TABLE cache_target LIKE cache_source")
	warmLegacyCacheSQL(t, unqualified)
	for _, db := range []string{"first_db", "second_db", "first_db"} {
		table := legacyCacheTable(unqualified, legacyCacheColumn("payload", 0, 0), legacyCacheColumn("ordinary", 1, 0))
		table.DbName = db
		resolved := 0
		err := RecoverLegacyTinyText(t.Context(), table, func(_ context.Context, database, name string) (*planpb.TableDef, error) {
			resolved++
			require.Equal(t, db, database)
			require.Equal(t, "cache_source", name)
			if database == "first_db" {
				return tiny, nil
			}
			return medium, nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, resolved)
		want := int32(types.MaxTinyTextLen)
		if db == "second_db" {
			want = types.MaxMediumTextLen
		}
		require.Equal(t, want, table.Cols[0].Typ.Width)
	}
}

func TestLegacyTextParseCacheDoesNotMemoizeErrors(t *testing.T) {
	// With normal escapes the first string swallows the TINYTEXT declaration.
	// NO_BACKSLASH_ESCAPES closes it earlier and # hides the trailing quote.
	// Both parses succeed, but their declaration evidence is different.
	ambiguous := legacyCacheFixtureSQL("CREATE TABLE cache_ambiguous (ordinary TEXT COMMENT 'x\\', payload TINYTEXT # '\n)")
	for _, test := range []struct {
		mode string
		cols int
	}{{"", 1}, {"NO_BACKSLASH_ESCAPES", 2}} {
		func() {
			stmt, err := parsers.ParseOneWithSQLMode(t.Context(), dialect.MYSQL, ambiguous, 0, test.mode)
			require.NoError(t, err)
			defer stmt.Free()
			create, ok := stmt.(*tree.CreateTable)
			require.True(t, ok)
			require.Len(t, create.Defs, test.cols)
		}()
	}
	for _, sql := range []string{ambiguous,
		legacyCacheFixtureSQL("CREATE TABLE cache_malformed (payload TINYTEXT"),
		legacyCacheFixtureSQL("SELECT 'tinytext'")} {
		for repeat := 0; repeat < 2; repeat++ {
			_, err := parseCachedLegacyTinyTextEvidence(t.Context(), sql)
			require.Error(t, err)
			if sql == ambiguous {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
			}
			_, hit := legacyTextEvidenceCache.get(sql)
			require.False(t, hit, "failed/ambiguous parsing must not publish positive or negative cache state")
			table := legacyCacheTable(sql, legacyCacheColumn("ordinary", 0, 0), legacyCacheColumn("payload", 1, 0))
			require.NoError(t, RecoverLegacyTinyTextFromCreateSQL(t.Context(), table))
			require.Zero(t, table.Cols[1].Typ.Width)
		}
	}
}

func TestLegacyTextParseCacheOversizeRecoveryFallsBack(t *testing.T) {
	t.Run("SQL bytes", func(t *testing.T) {
		sql := legacyCacheFixtureSQL("CREATE TABLE cache_large_sql (payload TINYTEXT)") + " /*" + strings.Repeat("x", 16<<10) + "*/"
		evidence, err := parseCachedLegacyTinyTextEvidence(t.Context(), sql)
		require.NoError(t, err)
		require.Equal(t, []legacyTinyTextColumn{{ordinal: 1, name: "payload", width: types.MaxTinyTextLen}}, evidence.columns)
		table := legacyCacheTable(sql, legacyCacheColumn("payload", 0, 0))
		require.NoError(t, RecoverLegacyTinyTextFromCreateSQL(t.Context(), table))
		require.Equal(t, int32(types.MaxTinyTextLen), table.Cols[0].Typ.Width)
		_, hit := legacyTextEvidenceCache.get(sql)
		require.False(t, hit)
	})
	t.Run("subtype columns", func(t *testing.T) {
		declarations := make([]string, 257)
		columns := make([]*planpb.ColDef, len(declarations))
		for index := range declarations {
			name := fmt.Sprintf("p%d", index)
			declarations[index] = name + " TINYTEXT"
			columns[index] = legacyCacheColumn(name, uint32(index), 0)
		}
		sql := legacyCacheFixtureSQL("CREATE TABLE cache_large_columns (" + strings.Join(declarations, ",") + ")")
		require.Less(t, len(sql), 16<<10)
		evidence, err := parseCachedLegacyTinyTextEvidence(t.Context(), sql)
		require.NoError(t, err)
		require.Len(t, evidence.columns, 257)
		table := legacyCacheTable(sql, columns...)
		require.NoError(t, RecoverLegacyTinyTextFromCreateSQL(t.Context(), table))
		for _, column := range table.Cols {
			require.Equal(t, int32(types.MaxTinyTextLen), column.Typ.Width)
		}
		_, hit := legacyTextEvidenceCache.get(sql)
		require.False(t, hit)
	})
}

func BenchmarkLegacyTextParseCacheRecovery(b *testing.B) {
	// Ordinary TEXT keeps the precheck active; the STORED LONGTEXT marker is
	// already durable, matching the measured repeated metadata-validation path.
	base := "CREATE TABLE cache_bench (id BIGINT, raw TEXT, projected LONGTEXT GENERATED ALWAYS AS (raw) STORED)"
	fixture := func(sql string) *planpb.TableDef {
		projected := legacyCacheColumn("projected", 2, types.MaxLongTextLen)
		projected.GeneratedCol = &planpb.GeneratedCol{OriginString: "raw", IsStored: true}
		return legacyCacheTable(sql, &planpb.ColDef{Name: "id", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_int64)}},
			legacyCacheColumn("raw", 1, 0), projected)
	}
	b.Run("warm_actual_recovery", func(b *testing.B) {
		sql := legacyCacheFixtureSQL(base)
		table := fixture(sql)
		if err := RecoverLegacyTinyTextFromCreateSQL(b.Context(), table); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if err := RecoverLegacyTinyTextFromCreateSQL(b.Context(), table); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if table.Cols[1].Typ.Width != 0 || table.Cols[2].Typ.Width != types.MaxLongTextLen || !table.Cols[2].GeneratedCol.IsStored {
			b.Fatal("metadata recovery changed the durable mixed-TEXT schema")
		}
	})
	b.Run("unchanged_64_mode_parse", func(b *testing.B) {
		sql := legacyCacheFixtureSQL(base)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			evidence, err := parseLegacyTinyTextEvidence(b.Context(), sql)
			if err != nil || len(evidence.columns) != 1 || evidence.columns[0].ordinal != 3 || evidence.columns[0].width != types.MaxLongTextLen {
				b.Fatalf("uncached parser contract changed: %+v, %v", evidence, err)
			}
		}
	})
	b.Run("cold_actual_recovery", func(b *testing.B) {
		table := fixture(base)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			table.Createsql = legacyCacheFixtureSQL(base)
			if err := RecoverLegacyTinyTextFromCreateSQL(b.Context(), table); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("SQL_cap_bypass_actual_recovery", func(b *testing.B) {
		table := fixture(legacyCacheFixtureSQL(base) + " /*" + strings.Repeat("x", 16<<10) + "*/")
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if err := RecoverLegacyTinyTextFromCreateSQL(b.Context(), table); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("warm_LIKE_current_resolver", func(b *testing.B) {
		source := fixture(legacyCacheFixtureSQL(base))
		target := fixture(legacyCacheFixtureSQL("CREATE TABLE cache_clone LIKE cache_source"))
		target.TblId, target.LogicalId = 72, 72
		calls := 0
		resolve := func(context.Context, string, string) (*planpb.TableDef, error) { calls++; return source, nil }
		if err := RecoverLegacyTinyText(b.Context(), target, resolve); err != nil {
			b.Fatal(err)
		}
		before := calls
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if err := RecoverLegacyTinyText(b.Context(), target, resolve); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if calls-before != b.N || source.Cols[1].Typ.Width != 0 || target.Cols[2].Typ.Width != types.MaxLongTextLen {
			b.Fatal("warm LIKE stopped resolving current metadata or mutated source")
		}
	})
}
