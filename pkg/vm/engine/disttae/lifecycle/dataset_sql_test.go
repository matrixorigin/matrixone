// Copyright 2026 Matrix Origin
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

package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestSQLDatasetReaderReturnsPublishedDataset(t *testing.T) {
	mp := mpool.MustNewZero()
	datasetID := uuid.MustParse("b38f31bd-6089-47ed-a63c-3796901dc79a")
	rootID := uuid.MustParse("e9af575c-e9fe-44c4-b9cd-b57aa5aa2424")
	attemptID := uuid.MustParse("7b23d1c1-c721-4753-8882-744a4623f786")
	leaseID := uuid.MustParse("51c9bd97-a47d-4c99-af80-54bf6038b0cb")
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "from mo_catalog.mo_lifecycle_datasets",
			accountID: 17,
			result: lifecycleDatasetResult(t, mp, lifecycleDatasetRow{
				DatasetID:       datasetID,
				RootID:          rootID,
				AttemptID:       attemptID,
				RestoreLeaseID:  leaseID,
				RestoreDeadline: "2026-08-05 12:00:00.000000",
			}),
		}},
	}

	dataset, err := (SQLDatasetReader{Executor: fake}).GetRestoreDataset(
		context.Background(), 17, datasetID.String(),
	)
	require.NoError(t, err)
	require.Equal(t, datasetID.String(), dataset.DatasetID)
	require.Equal(t, rootID.String(), dataset.RootID)
	require.Equal(t, attemptID.String(), dataset.AttemptID)
	require.Equal(t, leaseID.String(), dataset.RestoreLeaseID)
	require.Equal(t, "archive/root/manifest.json", dataset.ManifestKey)
	require.Equal(t, uint64(8), dataset.RowCount)
	require.Equal(t, uint64(4096), dataset.LogicalBytes)
	require.Equal(t, uint64(3), dataset.Version)
	require.Equal(t, uint64(9), dataset.StageID)
	require.Equal(t, uint64(42), dataset.LogicalTableID)
	require.Equal(t, uint64(7), dataset.LifecycleRange.SourceColumnID)
	require.Equal(t, int32(types.T_timestamp), dataset.LifecycleRange.TypeID)
	require.Equal(t, int64(100), dataset.LifecycleRange.Min)
	require.Equal(t, int64(300), dataset.LifecycleRange.Max)
	require.True(t, dataset.HasLifecycleRange)
	require.Equal(t, []byte(`{"stage_id":9}`), dataset.StageIdentity)
	require.Equal(t, "2026-08-05T12:00:00Z", dataset.RestoreDeadline.Format(time.RFC3339))
	require.Equal(t, 1, fake.offset)
}

func TestSQLDatasetReaderReloadsFrozenSelectionInAttemptOrder(t *testing.T) {
	mp := mpool.MustNewZero()
	firstID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	secondID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	second := lifecycleDatasetResult(t, mp, lifecycleDatasetRow{
		DatasetID: secondID,
		RootID:    uuid.New(),
		AttemptID: uuid.New(),
	})
	first := lifecycleDatasetResult(t, mp, lifecycleDatasetRow{
		DatasetID: firstID,
		RootID:    uuid.New(),
		AttemptID: uuid.New(),
	})
	second.Batches = append(second.Batches, first.Batches...)
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "where dataset_id in (",
			accountID: 17,
			result:    second,
		}},
	}
	datasets, err := (SQLDatasetReader{Executor: fake}).GetRestoreDatasets(
		context.Background(),
		17,
		[]string{firstID.String(), secondID.String()},
	)
	require.NoError(t, err)
	require.Equal(t, []string{firstID.String(), secondID.String()}, []string{
		datasets[0].DatasetID,
		datasets[1].DatasetID,
	})
}

func TestSQLDatasetReaderListsOnlyRangeOverlapsForAccount(t *testing.T) {
	mp := mpool.MustNewZero()
	datasetID := uuid.New()
	fake := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		lower := strings.ToLower(sql)
		require.Contains(t, lower, "account_id=17")
		require.Contains(t, lower, "logical_table_id=42")
		require.Contains(t, lower, "state='published'")
		require.Contains(t, lower, "lifecycle_column_type=")
		require.Contains(t, lower, "lifecycle_max>=")
		require.Contains(t, lower, "lifecycle_min<")
		require.Contains(t, lower, "limit 4097")
		return lifecycleDatasetResult(t, mp, lifecycleDatasetRow{
			DatasetID: datasetID,
			RootID:    uuid.New(),
			AttemptID: uuid.New(),
		}), nil
	})

	datasets, err := (SQLDatasetReader{Executor: fake}).ListRestoreDatasets(
		context.Background(),
		17,
		42,
		"2026-01-01 00:00:00",
		"2026-02-01 00:00:00",
	)
	require.NoError(t, err)
	require.Len(t, datasets, 1)
	require.Equal(t, datasetID.String(), datasets[0].DatasetID)
}

func TestSQLDatasetReaderFailsClosedOnInvalidInputAndRows(t *testing.T) {
	ctx := context.Background()
	_, err := (SQLDatasetReader{}).GetRestoreDataset(ctx, 17, uuid.NewString())
	require.ErrorContains(t, err, "reader is incomplete")
	_, err = (SQLDatasetReader{Executor: &scriptedLifecycleSQLExecutor{t: t}}).
		GetRestoreDataset(ctx, 0, uuid.NewString())
	require.ErrorContains(t, err, "reader is incomplete")
	_, err = (SQLDatasetReader{Executor: &scriptedLifecycleSQLExecutor{t: t}}).
		GetRestoreDataset(ctx, 17, "not-a-uuid")
	require.ErrorContains(t, err, "invalid Lifecycle Dataset ID")

	datasetID := uuid.New()
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "from mo_catalog.mo_lifecycle_datasets",
			accountID: 17,
			result:    executor.Result{Mp: mp},
		}},
	}
	_, err = (SQLDatasetReader{Executor: fake}).GetRestoreDataset(
		ctx, 17, datasetID.String(),
	)
	require.ErrorContains(t, err, "does not exist")
}

func TestSQLDatasetReaderRejectsCorruptPersistentFields(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*lifecycleDatasetRow)
		want   string
	}{
		{
			name: "digest",
			mutate: func(row *lifecycleDatasetRow) {
				row.ManifestDigest = "bad"
			},
			want: "invalid Lifecycle Dataset digest",
		},
		{
			name: "purge timestamp",
			mutate: func(row *lifecycleDatasetRow) {
				row.PurgeEligibleAt = "bad"
			},
			want: "cannot parse",
		},
		{
			name: "restore deadline",
			mutate: func(row *lifecycleDatasetRow) {
				row.RestoreDeadline = "bad"
			},
			want: "cannot parse",
		},
		{
			name: "identity",
			mutate: func(row *lifecycleDatasetRow) {
				row.CorruptRootID = true
			},
			want: "identity is corrupt",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			row := lifecycleDatasetRow{
				DatasetID:       uuid.New(),
				RootID:          uuid.New(),
				AttemptID:       uuid.New(),
				RestoreDeadline: "2026-08-05 12:00:00.000000",
			}
			test.mutate(&row)
			fake := &scriptedLifecycleSQLExecutor{
				t: t,
				steps: []lifecycleSQLStep{{
					contains:  "from mo_catalog.mo_lifecycle_datasets",
					accountID: 17,
					result:    lifecycleDatasetResult(t, mp, row),
				}},
			}
			_, err := (SQLDatasetReader{Executor: fake}).GetRestoreDataset(
				context.Background(), 17, row.DatasetID.String(),
			)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestDatasetPersistentDecoders(t *testing.T) {
	digest := sha256.Sum256([]byte("dataset"))
	decoded, err := decodeDatasetDigest(hex.EncodeToString(digest[:]))
	require.NoError(t, err)
	require.Equal(t, digest, decoded)
	require.Empty(t, parseDatasetUUID("not-hex"))
	require.Empty(t, parseDatasetUUID("00"))
	require.Equal(t, uuid.Nil.String(), parseDatasetUUID(hex.EncodeToString(uuid.Nil[:])))
}

type lifecycleDatasetRow struct {
	DatasetID       uuid.UUID
	RootID          uuid.UUID
	AttemptID       uuid.UUID
	RestoreLeaseID  uuid.UUID
	ManifestDigest  string
	PurgeEligibleAt string
	RestoreDeadline string
	CorruptRootID   bool
}

func lifecycleDatasetResult(
	t *testing.T,
	mp *mpool.MPool,
	row lifecycleDatasetRow,
) executor.Result {
	t.Helper()
	digest := sha256.Sum256([]byte("dataset"))
	digestHex := hex.EncodeToString(digest[:])
	if row.ManifestDigest == "" {
		row.ManifestDigest = digestHex
	}
	if row.PurgeEligibleAt == "" {
		row.PurgeEligibleAt = "2026-09-01 00:00:00.000000"
	}
	value := batch.NewWithSize(21)
	strings := map[int]string{
		0:  hex.EncodeToString(row.DatasetID[:]),
		1:  hex.EncodeToString(row.RootID[:]),
		2:  hex.EncodeToString(row.AttemptID[:]),
		3:  "archive/root/manifest.json",
		4:  row.ManifestDigest,
		5:  digestHex,
		6:  digestHex,
		10: "PUBLISHED",
		12: `{"stage_id":9}`,
		13: row.PurgeEligibleAt,
		14: "",
		15: row.RestoreDeadline,
	}
	if row.CorruptRootID {
		strings[1] = "00"
	}
	if row.RestoreLeaseID != uuid.Nil {
		strings[14] = hex.EncodeToString(row.RestoreLeaseID[:])
	}
	numbers := map[int]uint64{7: 8, 8: 4096, 9: 3, 11: 9, 16: 42, 17: 7}
	for column := range value.Vecs {
		if column == 18 {
			value.Vecs[column] = vector.NewVec(types.T_uint32.ToType())
			require.NoError(t, vector.AppendFixed(
				value.Vecs[column], uint32(types.T_timestamp), false, mp,
			))
			continue
		}
		if column == 19 || column == 20 {
			value.Vecs[column] = vector.NewVec(types.T_int64.ToType())
			physical := int64(100)
			if column == 20 {
				physical = 300
			}
			require.NoError(t, vector.AppendFixed(
				value.Vecs[column], physical, false, mp,
			))
			continue
		}
		if number, ok := numbers[column]; ok {
			value.Vecs[column] = vector.NewVec(types.T_uint64.ToType())
			require.NoError(t, vector.AppendFixed(
				value.Vecs[column], number, false, mp,
			))
			continue
		}
		value.Vecs[column] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(
			value.Vecs[column], []byte(strings[column]), false, mp,
		))
	}
	value.SetRowCount(1)
	return executor.Result{Batches: []*batch.Batch{value}, Mp: mp}
}
