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

package disttae

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

// These entries deliberately have no backing file: valid disjoint stats must
// never read one, and over-budget unknown entries must bail out before I/O.
func addPKCheckObjects(t *testing.T, tbl *txnTable, eng *Engine, fs fileservice.FileService, n int, shape, lifecycle string) {
	t.Helper()
	ctx := tbl.proc.Load().Ctx
	part := eng.GetOrCreateLatestPart(ctx, uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
	state, done := part.MutateState()
	defer done()
	for i := range n {
		stats := objectio.NewObjectStats()
		id := objectio.NewObjectid()
		require.NoError(t, objectio.SetObjectStatsObjectName(stats, objectio.BuildObjectNameWithObjectID(&id)))
		require.NoError(t, objectio.SetObjectStatsSize(stats, 1))
		zm := index.NewZM(types.T_int64, 0)
		switch shape {
		case "disjoint":
			require.NoError(t, zm.Update(int64(1000)))
		case "overlap":
			require.NoError(t, zm.Update(int64(7)))
		case "wrong_type":
			zm = index.NewZM(types.T_uint64, 0)
			require.NoError(t, zm.Update(uint64(1000)))
		case "wrong_scale":
			zm.SetScale(1)
			require.NoError(t, zm.Update(int64(1000)))
		case "uninitialized": // Nonzero type byte, but no usable bounds.
		case "missing":
			zm = make(objectio.ZoneMap, index.ZMSize)
		default:
			t.Fatalf("unknown stats shape %s", shape)
		}
		require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, zm))
		entry := objectio.ObjectEntry{ObjectStats: *stats, CreateTime: types.BuildTS(40, 0)}
		if lifecycle == "deleted" || (lifecycle == "mixed" && i%2 == 0) {
			// Old data removed after the reservation can still contain a match.
			entry.CreateTime, entry.DeleteTime = types.BuildTS(1, 0), types.BuildTS(40, 0)
		}
		require.NoError(t, state.HandleObjectEntry(ctx, fs, entry, false))
	}
}

func TestPersistedPKCheckDisjointObjectBudget(t *testing.T) {
	for _, lifecycle := range []string{"created", "deleted", "mixed"} {
		t.Run(lifecycle, func(t *testing.T) {
			tbl, eng, fs, keys := newPersistedPKCheckTable(t, types.T_int64, false)
			ctx, mp := tbl.proc.Load().Ctx, tbl.proc.Load().Mp()
			reads := 0
			eng.fs = tombstoneReadFS{FileService: fs, read: func(ctx context.Context, v *fileservice.IOVector) error {
				reads++
				return fs.Read(ctx, v)
			}}
			for _, count := range []int{64, 65} {
				if count == 64 {
					addPKCheckObjects(t, tbl, eng, fs, 63, "disjoint", lifecycle)
				} else {
					addPKCheckObjects(t, tbl, eng, fs, 1, "disjoint", lifecycle)
				}
				require.NoError(t, vector.SetFixedAtWithTypeCheck(keys.Vecs[0], 0, int64(99)))
				baseline := mp.CurrNB()
				changed, err := tbl.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(10, 0), types.BuildTS(50, 0), keys, 0)
				require.NoError(t, err)
				require.False(t, changed, "count=%d: disjoint metadata is evidence of absence", count)
				require.Zero(t, reads)
				require.Equal(t, baseline, mp.CurrNB())
			}
			// A real manual-key collision survives the same disjoint object set,
			// including an object flushed after the transaction's snapshot TS.
			require.NoError(t, vector.SetFixedAtWithTypeCheck(keys.Vecs[0], 0, int64(7)))
			changed, err := tbl.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(10, 0), types.BuildTS(20, 0), keys, 0)
			require.NoError(t, err)
			require.True(t, changed)
			require.Positive(t, reads)
			require.Empty(t, pkCheckSemaphore)
		})
	}
}

func TestPersistedPKCheckUnknownObjectBudget(t *testing.T) {
	for _, lifecycle := range []string{"created", "deleted", "mixed"} {
		for _, shape := range []string{"missing", "uninitialized", "wrong_type", "wrong_scale", "overlap", "fake_pk"} {
			t.Run(lifecycle+"/"+shape, func(t *testing.T) {
				tbl, eng, fs, keys := newPersistedPKCheckTable(t, types.T_int64, false)
				if shape == "fake_pk" {
					tbl.tableDef.Pkey.PkeyColName = catalog.FakePrimaryKeyColName
					shape = "disjoint" // A fake PK's sort-key stats cannot exclude it.
				}
				reads := 0
				readErr := errors.New("candidate metadata read")
				eng.fs = tombstoneReadFS{FileService: fs, read: func(context.Context, *fileservice.IOVector) error {
					reads++
					return readErr
				}}
				ctx := tbl.proc.Load().Ctx
				addPKCheckObjects(t, tbl, eng, fs, 63, shape, lifecycle)
				_, err := tbl.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(10, 0), types.BuildTS(50, 0), keys, 0)
				require.ErrorIs(t, err, readErr, "64 unresolved objects may enter metadata I/O")
				require.Positive(t, reads)
				reads = 0
				addPKCheckObjects(t, tbl, eng, fs, 1, shape, lifecycle)
				changed, err := tbl.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(10, 0), types.BuildTS(50, 0), keys, 0)
				require.NoError(t, err)
				require.True(t, changed)
				require.Zero(t, reads, "65 candidates must bail out before any metadata/BF/block I/O")
				require.Empty(t, pkCheckSemaphore)
			})
		}
	}
}

func TestPersistedPKCheckUnsortedKeysAndTimeWindow(t *testing.T) {
	for _, oid := range []types.T{types.T_int64, types.T_varchar} {
		t.Run(oid.String(), func(t *testing.T) {
			tbl, _, _, keys := newPersistedPKCheckTable(t, oid, false)
			ctx, mp := tbl.proc.Load().Ctx, tbl.proc.Load().Mp()
			// The matching key is second in an unsorted vector, including a
			// byte-encoded string key as used for composite primary keys.
			if oid == types.T_int64 {
				require.NoError(t, vector.SetFixedAtWithTypeCheck(keys.Vecs[0], 0, int64(99)))
				require.NoError(t, vector.AppendFixed(keys.Vecs[0], int64(7), false, mp))
			} else {
				keys.Vecs[0].Reset(oid.ToType())
				require.NoError(t, vector.AppendBytes(keys.Vecs[0], []byte("zzz"), false, mp))
				require.NoError(t, vector.AppendBytes(keys.Vecs[0], []byte("target"), false, mp))
			}
			keys.SetRowCount(2)
			original, err := keys.Vecs[0].MarshalBinary()
			require.NoError(t, err)
			for _, method := range []string{"modified", "upserted"} {
				for _, tc := range []struct {
					name     string
					from, to types.TS
					want     bool
				}{
					{"after_to_flush", types.BuildTS(10, 0), types.BuildTS(20, 0), true},
					{"at_from", types.BuildTS(20, 0), types.BuildTS(30, 0), false},
					{"from_next", types.BuildTS(20, 0).Prev(), types.BuildTS(30, 0), true},
					{"after_to_commit", types.BuildTS(10, 0), types.BuildTS(20, 0).Prev(), false},
				} {
					t.Run(method+"/"+tc.name, func(t *testing.T) {
						var changed bool
						var err error
						if method == "upserted" {
							changed, err = tbl.PrimaryKeysMayBeUpserted(ctx, tc.from, tc.to, keys, 0)
						} else {
							changed, err = tbl.PrimaryKeysMayBeModified(ctx, tc.from, tc.to, keys, 0, -1)
						}
						require.NoError(t, err)
						require.Equal(t, tc.want, changed)
						after, err := keys.Vecs[0].MarshalBinary()
						require.NoError(t, err)
						require.Equal(t, original, after, "caller values and null bitmap must stay unchanged")
					})
				}
			}
			t.Run("deleted_old_appendable", func(t *testing.T) {
				old, engine, _, oldKeys := newPersistedPKCheckTableBlocks(t, oid, false, 1, true)
				part := engine.GetOrCreateLatestPart(ctx, uint64(old.accountId), old.db.databaseId, old.tableId)
				deleted, created := part.Snapshot().GetChangedObjsBetween(types.BuildTS(10, 0), types.MaxTs())
				require.Len(t, deleted, 1)
				require.Empty(t, created)
				changed, err := old.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(10, 0), types.BuildTS(30, 0), oldKeys, 0)
				require.NoError(t, err)
				require.True(t, changed)
			})
			for _, nullCase := range []string{"mixed", "all_null", "empty"} {
				t.Run(nullCase, func(t *testing.T) {
					keys.Vecs[0].SetNull(0)
					if nullCase == "all_null" {
						keys.Vecs[0].SetNull(1)
					} else if nullCase == "empty" {
						keys.Vecs[0].Reset(oid.ToType())
						keys.SetRowCount(0)
					}
					before, err := keys.Vecs[0].MarshalBinary()
					require.NoError(t, err)
					changed, err := tbl.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(10, 0), types.BuildTS(30, 0), keys, 0)
					require.NoError(t, err)
					require.Equal(t, nullCase == "mixed", changed)
					after, err := keys.Vecs[0].MarshalBinary()
					require.NoError(t, err)
					require.Equal(t, before, after)
				})
			}
		})
	}
}

func TestPersistedPKCheckCandidateBlockBudget(t *testing.T) {
	for _, blocks := range []int{32, 33} {
		t.Run(fmt.Sprint(blocks), func(t *testing.T) {
			tbl, eng, fs, keys := newPersistedPKCheckTableBlocks(t, types.T_int64, false, blocks, false)
			ctx := tbl.proc.Load().Ctx
			blockReads := 0
			eng.fs = tombstoneReadFS{FileService: fs, read: func(ctx context.Context, v *fileservice.IOVector) error {
				if len(pkCheckSemaphore) > 0 {
					blockReads++
				}
				return fs.Read(ctx, v)
			}}
			// All matching row commits predate from. Only the over-budget
			// case may conservatively report a conflict before reading rows.
			changed, err := tbl.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(20, 0), types.BuildTS(50, 0), keys, 0)
			require.NoError(t, err)
			require.Equal(t, blocks > 32, changed)
			if blocks == 32 {
				require.Positive(t, blockReads)
			} else {
				require.Zero(t, blockReads)
			}
			require.Empty(t, pkCheckSemaphore)
		})
	}
}

func TestPersistedPKCheckUnusableZoneMaps(t *testing.T) {
	for _, shape := range []string{"missing", "uninitialized", "wrong_type", "wrong_scale"} {
		t.Run(shape, func(t *testing.T) {
			tbl, eng, fs, keys := newPersistedPKCheckTable(t, types.T_int64, false)
			ctx := tbl.proc.Load().Ctx
			part := eng.GetOrCreateLatestPart(ctx, uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
			func() {
				state, done := part.MutateState()
				defer done()
				_, created := state.GetChangedObjsBetween(types.TS{}, types.MaxTs())
				for name := range created {
					obj, ok := state.GetObject(name)
					require.True(t, ok)
					zm := index.NewZM(types.T_int64, 0)
					switch shape {
					case "missing":
						zm = make(objectio.ZoneMap, index.ZMSize)
					case "wrong_type":
						zm = index.NewZM(types.T_uint64, 0)
						require.NoError(t, zm.Update(uint64(1000)))
					case "wrong_scale":
						zm.SetScale(1)
						require.NoError(t, zm.Update(int64(1000)))
					}
					require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&obj.ObjectStats, zm))
					require.NoError(t, state.HandleObjectEntry(ctx, fs, obj, false))
					// Fault injection into this fixture's unique, exclusively owned
					// cached metadata proves the object/block fallback, including BF
					// bounds. Restore immediately on exit; no shared/global fixture.
					loc := obj.Location()
					meta, err := objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
					require.NoError(t, err)
					data := meta.MustDataMeta()
					for _, col := range []objectio.ColumnMeta{data.MustGetColumn(0), data.GetBlockMeta(0).MustGetColumn(0)} {
						original := col.ZoneMap().Clone()
						t.Cleanup(func() { col.SetZoneMap(original) })
						col.SetZoneMap(zm)
					}
				}
			}()
			changed, err := tbl.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(10, 0), types.BuildTS(30, 0), keys, 0)
			require.NoError(t, err)
			require.True(t, changed, "unknown metadata cannot hide the real committed key")
		})
	}
}

func TestPersistedPKCheckDisjointDataStillChecksTombstones(t *testing.T) {
	tbl, eng, fs, keys := newPersistedPKCheckTable(t, types.T_int64, true)
	ctx := tbl.proc.Load().Ctx
	addPKCheckObjects(t, tbl, eng, fs, 65, "disjoint", "mixed")
	changed, err := tbl.PrimaryKeysMayBeUpserted(ctx, types.BuildTS(10, 0), types.BuildTS(30, 0), keys, 0)
	require.NoError(t, err)
	require.False(t, changed, "upserted deliberately ignores deletes")
	changed, err = tbl.PrimaryKeysMayBeModified(ctx, types.BuildTS(10, 0), types.BuildTS(30, 0), keys, 0, -1)
	require.NoError(t, err)
	require.True(t, changed, "zero data candidates cannot bypass the matching tombstone")
	require.NoError(t, vector.SetFixedAtWithTypeCheck(keys.Vecs[0], 0, int64(99)))
	changed, err = tbl.PrimaryKeysMayBeModified(ctx, types.BuildTS(10, 0), types.BuildTS(30, 0), keys, 0, -1)
	require.NoError(t, err)
	require.False(t, changed)
	require.Empty(t, pkCheckSemaphore)
}

// Err calls are the deterministic cancellation boundary under test. No goroutine
// or timing assumption is needed to cancel during the snapshot-only loop.
type pkCheckCancelContext struct {
	context.Context
	remaining int
}

func (c *pkCheckCancelContext) Err() error {
	c.remaining--
	if c.remaining <= 0 {
		return context.Canceled
	}
	return nil
}

func TestCollectPKCheckObjectsUnknownAndCancellation(t *testing.T) {
	tbl, eng, fs, keys := newPersistedPKCheckTable(t, types.T_int64, false)
	ctx := tbl.proc.Load().Ctx
	addPKCheckObjects(t, tbl, eng, fs, 65, "disjoint", "created")
	part := eng.GetOrCreateLatestPart(ctx, uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
	state := part.Snapshot()
	deleted, created := state.GetChangedObjsBetween(types.BuildTS(10, 0), types.MaxTs())
	// Deliberate duplicate identity must not consume two budget slots.
	candidates, unknown, err := collectPKCheckObjects(ctx, state, created, created, keys.Vecs[0], false)
	require.NoError(t, err)
	require.False(t, unknown)
	require.Len(t, candidates, 1)
	candidates, unknown, err = collectPKCheckObjects(&pkCheckCancelContext{Context: ctx, remaining: 3}, state, created, deleted, keys.Vecs[0], false)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, unknown)
	require.Empty(t, candidates)
	id := objectio.NewObjectid()
	missing := map[objectio.ObjectNameShort]struct{}{*objectio.BuildObjectNameWithObjectID(&id).Short(): {}}
	_, unknown, err = collectPKCheckObjects(ctx, state, missing, nil, keys.Vecs[0], false)
	require.NoError(t, err)
	require.True(t, unknown, "missing snapshot entry is not evidence of absence")
}
