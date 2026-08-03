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
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type SQLDatasetReader struct {
	Executor executor.SQLExecutor
}

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
coalesce(date_format(restore_deadline,'%%Y-%%m-%%d %%H:%%i:%%s.%%f'),'')
from mo_catalog.mo_lifecycle_datasets where dataset_id=unhex('%s')`,
			hex.EncodeToString(parsed[:]),
		),
		executor.Options{}.WithAccountID(accountID),
	)
	if err != nil {
		return RestoreDataset{}, err
	}
	defer result.Close()
	var dataset RestoreDataset
	rowsRead := 0
	var decodeErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 16 || rowsRead+rows != 1 {
			decodeErr = moerr.NewInternalErrorNoCtxf("Lifecycle Dataset row is invalid")
			return false
		}
		manifestDigest, err := decodeDatasetDigest(columns[4].GetStringAt(0))
		if err != nil {
			decodeErr = err
			return false
		}
		schemaDigest, err := decodeDatasetDigest(columns[5].GetStringAt(0))
		if err != nil {
			decodeErr = err
			return false
		}
		contentHash, err := decodeDatasetDigest(columns[6].GetStringAt(0))
		if err != nil {
			decodeErr = err
			return false
		}
		purgeTime, err := time.ParseInLocation(
			lifecycleSQLTimestampLayout,
			columns[13].GetStringAt(0),
			time.UTC,
		)
		if err != nil {
			decodeErr = err
			return false
		}
		dataset = RestoreDataset{
			DatasetID:       parseDatasetUUID(columns[0].GetStringAt(0)),
			AccountID:       accountID,
			RootID:          parseDatasetUUID(columns[1].GetStringAt(0)),
			AttemptID:       parseDatasetUUID(columns[2].GetStringAt(0)),
			ManifestKey:     columns[3].GetStringAt(0),
			ManifestDigest:  manifestDigest,
			SchemaDigest:    schemaDigest,
			ContentHash:     contentHash,
			RowCount:        vector.GetFixedAtNoTypeCheck[uint64](columns[7], 0),
			LogicalBytes:    vector.GetFixedAtNoTypeCheck[uint64](columns[8], 0),
			Version:         vector.GetFixedAtNoTypeCheck[uint64](columns[9], 0),
			State:           columns[10].GetStringAt(0),
			StageID:         vector.GetFixedAtNoTypeCheck[uint64](columns[11], 0),
			StageIdentity:   append([]byte(nil), columns[12].GetBytesAt(0)...),
			PurgeEligibleAt: purgeTime,
			RestoreLeaseID:  parseDatasetUUID(columns[14].GetStringAt(0)),
		}
		if deadline := columns[15].GetStringAt(0); deadline != "" {
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
			len(dataset.StageIdentity) == 0 {
			decodeErr = moerr.NewInternalErrorNoCtxf("Lifecycle Dataset identity is corrupt")
			return false
		}
		rowsRead += rows
		return true
	})
	if decodeErr != nil {
		return RestoreDataset{}, decodeErr
	}
	if rowsRead != 1 {
		return RestoreDataset{}, moerr.NewInternalErrorNoCtxf(
			"Lifecycle Dataset %s does not exist",
			datasetID,
		)
	}
	return dataset, nil
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
