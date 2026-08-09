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
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type SQLDatasetReader struct {
	Executor executor.SQLExecutor
}

// GetRestoreDatasets reloads the immutable Dataset selection of a resumable
// range Restore in one bounded Catalog read. The returned order is exactly the
// order frozen in the parent Restore Attempt.
func (reader SQLDatasetReader) GetRestoreDatasets(
	ctx context.Context,
	accountID uint32,
	datasetIDs []string,
) ([]RestoreDataset, error) {
	if reader.Executor == nil || accountID == 0 || len(datasetIDs) == 0 ||
		len(datasetIDs) > maxRestoreRangeDatasets {
		return nil, moerr.NewInternalErrorNoCtxf("Lifecycle Dataset selection reader is incomplete")
	}
	encodedIDs := make([]string, 0, len(datasetIDs))
	seen := make(map[string]struct{}, len(datasetIDs))
	for _, datasetID := range datasetIDs {
		parsed, err := uuid.Parse(datasetID)
		if err != nil {
			return nil, moerr.NewInternalErrorNoCtxf("invalid Lifecycle Dataset ID")
		}
		canonical := parsed.String()
		if _, exists := seen[canonical]; exists {
			return nil, moerr.NewInternalErrorNoCtxf("duplicate Lifecycle Dataset ID")
		}
		seen[canonical] = struct{}{}
		encodedIDs = append(encodedIDs, "unhex('"+hex.EncodeToString(parsed[:])+"')")
	}
	result, err := reader.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`select hex(dataset_id),hex(root_id),hex(attempt_id),manifest_key,
hex(manifest_sha256),hex(schema_descriptor_digest),hex(content_hash),
row_count,logical_bytes,version,state,stage_id,stage_identity_blob,
date_format(purge_eligible_at,'%%Y-%%m-%%d %%H:%%i:%%s.%%f'),
coalesce(hex(restore_lease_id),''),
coalesce(date_format(restore_deadline,'%%Y-%%m-%%d %%H:%%i:%%s.%%f'),''),
logical_table_id,lifecycle_column_id,lifecycle_column_type,
lifecycle_min,lifecycle_max
from mo_catalog.mo_lifecycle_datasets where dataset_id in (%s)`,
			strings.Join(encodedIDs, ","),
		),
		executor.Options{}.WithAccountID(accountID),
	)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	datasets, err := decodeRestoreDatasets(result, accountID, len(datasetIDs))
	if err != nil {
		return nil, err
	}
	if len(datasets) != len(datasetIDs) {
		return nil, moerr.NewInternalErrorNoCtxf("Lifecycle Restore Dataset selection is incomplete")
	}
	byID := make(map[string]RestoreDataset, len(datasets))
	for _, dataset := range datasets {
		byID[dataset.DatasetID] = dataset
	}
	ordered := make([]RestoreDataset, 0, len(datasetIDs))
	for _, datasetID := range datasetIDs {
		dataset, ok := byID[datasetID]
		if !ok {
			return nil, moerr.NewInternalErrorNoCtxf("Lifecycle Restore Dataset selection changed")
		}
		ordered = append(ordered, dataset)
	}
	return ordered, nil
}

const maxRestoreRangeDatasets = 4096

func (reader SQLDatasetReader) GetRestoreDataset(
	ctx context.Context,
	accountID uint32,
	datasetID string,
) (RestoreDataset, error) {
	if reader.Executor == nil || accountID == 0 {
		return RestoreDataset{}, moerr.NewInternalErrorNoCtxf("Lifecycle Dataset reader is incomplete")
	}
	parsed, err := uuid.Parse(datasetID)
	if err != nil {
		return RestoreDataset{}, moerr.NewInternalErrorNoCtxf("invalid Lifecycle Dataset ID")
	}
	result, err := reader.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`select hex(dataset_id),hex(root_id),hex(attempt_id),manifest_key,
hex(manifest_sha256),hex(schema_descriptor_digest),hex(content_hash),
row_count,logical_bytes,version,state,stage_id,stage_identity_blob,
date_format(purge_eligible_at,'%%Y-%%m-%%d %%H:%%i:%%s.%%f'),
coalesce(hex(restore_lease_id),''),
coalesce(date_format(restore_deadline,'%%Y-%%m-%%d %%H:%%i:%%s.%%f'),''),
logical_table_id,lifecycle_column_id,lifecycle_column_type,
lifecycle_min,lifecycle_max
from mo_catalog.mo_lifecycle_datasets where dataset_id=unhex('%s')`,
			hex.EncodeToString(parsed[:]),
		),
		executor.Options{}.WithAccountID(accountID),
	)
	if err != nil {
		return RestoreDataset{}, err
	}
	defer result.Close()
	datasets, decodeErr := decodeRestoreDatasets(result, accountID, 1)
	if decodeErr != nil {
		return RestoreDataset{}, decodeErr
	}
	if len(datasets) != 1 {
		return RestoreDataset{}, moerr.NewInternalErrorNoCtxf(
			"Lifecycle Dataset %s does not exist",
			datasetID,
		)
	}
	return datasets[0], nil
}

// ListRestoreDatasets returns a bounded, deterministic candidate set. Exact
// interval overlap is evaluated from the verified physical range fields by the
// caller after parsing the user boundary for the frozen Lifecycle column type.
func (reader SQLDatasetReader) ListRestoreDatasets(
	ctx context.Context,
	accountID uint32,
	logicalTableID uint64,
) ([]RestoreDataset, error) {
	if reader.Executor == nil || accountID == 0 || logicalTableID == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("Lifecycle Dataset range reader is incomplete")
	}
	result, err := reader.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`select hex(dataset_id),hex(root_id),hex(attempt_id),manifest_key,
hex(manifest_sha256),hex(schema_descriptor_digest),hex(content_hash),
row_count,logical_bytes,version,state,stage_id,stage_identity_blob,
date_format(purge_eligible_at,'%%Y-%%m-%%d %%H:%%i:%%s.%%f'),
coalesce(hex(restore_lease_id),''),
coalesce(date_format(restore_deadline,'%%Y-%%m-%%d %%H:%%i:%%s.%%f'),''),
logical_table_id,lifecycle_column_id,lifecycle_column_type,
lifecycle_min,lifecycle_max
from mo_catalog.mo_lifecycle_datasets
where logical_table_id=%d and state='PUBLISHED'
order by lifecycle_min,lifecycle_max,created_at,dataset_id
limit %d`,
			logicalTableID,
			maxRestoreRangeDatasets+1,
		),
		executor.Options{}.WithAccountID(accountID),
	)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	datasets, err := decodeRestoreDatasets(
		result,
		accountID,
		maxRestoreRangeDatasets+1,
	)
	if err != nil {
		return nil, err
	}
	if len(datasets) > maxRestoreRangeDatasets {
		return nil, moerr.NewInternalErrorNoCtxf(
			"RESOURCE_BLOCKED: Lifecycle Restore range exceeds the certified Dataset limit %d",
			maxRestoreRangeDatasets,
		)
	}
	return datasets, nil
}

func decodeRestoreDatasets(
	result executor.Result,
	accountID uint32,
	maxRows int,
) ([]RestoreDataset, error) {
	datasets := make([]RestoreDataset, 0, min(maxRows, 16))
	var decodeErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 21 || len(datasets)+rows > maxRows {
			decodeErr = moerr.NewInternalErrorNoCtxf("Lifecycle Dataset row is invalid")
			return false
		}
		for row := 0; row < rows; row++ {
			manifestDigest, err := decodeDatasetDigest(columns[4].GetStringAt(row))
			if err != nil {
				decodeErr = err
				return false
			}
			schemaDigest, err := decodeDatasetDigest(columns[5].GetStringAt(row))
			if err != nil {
				decodeErr = err
				return false
			}
			contentHash, err := decodeDatasetDigest(columns[6].GetStringAt(row))
			if err != nil {
				decodeErr = err
				return false
			}
			purgeTime, err := time.ParseInLocation(
				lifecycleSQLTimestampLayout,
				columns[13].GetStringAt(row),
				time.UTC,
			)
			if err != nil {
				decodeErr = err
				return false
			}
			dataset := RestoreDataset{
				DatasetID:       parseDatasetUUID(columns[0].GetStringAt(row)),
				AccountID:       accountID,
				RootID:          parseDatasetUUID(columns[1].GetStringAt(row)),
				AttemptID:       parseDatasetUUID(columns[2].GetStringAt(row)),
				ManifestKey:     columns[3].GetStringAt(row),
				ManifestDigest:  manifestDigest,
				SchemaDigest:    schemaDigest,
				ContentHash:     contentHash,
				RowCount:        vector.GetFixedAtNoTypeCheck[uint64](columns[7], row),
				LogicalBytes:    vector.GetFixedAtNoTypeCheck[uint64](columns[8], row),
				Version:         vector.GetFixedAtNoTypeCheck[uint64](columns[9], row),
				State:           columns[10].GetStringAt(row),
				StageID:         vector.GetFixedAtNoTypeCheck[uint64](columns[11], row),
				StageIdentity:   append([]byte(nil), columns[12].GetBytesAt(row)...),
				PurgeEligibleAt: purgeTime,
				RestoreLeaseID:  parseDatasetUUID(columns[14].GetStringAt(row)),
				LogicalTableID:  vector.GetFixedAtNoTypeCheck[uint64](columns[16], row),
				LifecycleRange: ArchiveLifecycleRange{
					SourceColumnID: vector.GetFixedAtNoTypeCheck[uint64](columns[17], row),
					TypeID:         int32(vector.GetFixedAtNoTypeCheck[uint32](columns[18], row)),
					Min:            vector.GetFixedAtNoTypeCheck[int64](columns[19], row),
					Max:            vector.GetFixedAtNoTypeCheck[int64](columns[20], row),
				},
				HasLifecycleRange: true,
			}
			if deadline := columns[15].GetStringAt(row); deadline != "" {
				dataset.RestoreDeadline, decodeErr = time.ParseInLocation(
					lifecycleSQLTimestampLayout,
					deadline,
					time.UTC,
				)
				if decodeErr != nil {
					return false
				}
			}
			if dataset.DatasetID == "" ||
				dataset.RootID == "" ||
				dataset.AttemptID == "" ||
				dataset.StageID == 0 ||
				len(dataset.StageIdentity) == 0 ||
				dataset.LogicalTableID == 0 ||
				dataset.LifecycleRange.Min > dataset.LifecycleRange.Max ||
				!isLifecycleRangeType(types.T(dataset.LifecycleRange.TypeID)) {
				decodeErr = moerr.NewInternalErrorNoCtxf("Lifecycle Dataset identity is corrupt")
				return false
			}
			datasets = append(datasets, dataset)
		}
		return true
	})
	return datasets, decodeErr
}

func decodeDatasetDigest(value string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(digest) {
		return digest, moerr.NewInternalErrorNoCtxf("invalid Lifecycle Dataset digest")
	}
	copy(digest[:], decoded)
	return digest, nil
}

func parseDatasetUUID(value string) string {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 16 {
		return ""
	}
	parsed, err := uuid.FromBytes(decoded)
	if err != nil {
		return ""
	}
	return parsed.String()
}
