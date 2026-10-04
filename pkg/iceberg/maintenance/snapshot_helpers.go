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

package maintenance

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func nextMaintenanceSnapshotID(now time.Time, meta *api.TableMetadata) int64 {
	candidate := now.UnixNano()
	if candidate <= 0 {
		candidate = time.Now().UnixNano()
	}
	maxSnapshotID := int64(0)
	if meta != nil {
		if meta.CurrentSnapshotID != nil && *meta.CurrentSnapshotID > maxSnapshotID {
			maxSnapshotID = *meta.CurrentSnapshotID
		}
		for _, snapshot := range meta.Snapshots {
			if snapshot.SnapshotID > maxSnapshotID {
				maxSnapshotID = snapshot.SnapshotID
			}
		}
	}
	if candidate <= maxSnapshotID {
		return maxSnapshotID + 1
	}
	return candidate
}

func nextMaintenanceSequenceNumber(meta *api.TableMetadata) int64 {
	next := int64(1)
	if meta == nil {
		return next
	}
	if meta.LastSequenceNumber >= next {
		next = meta.LastSequenceNumber + 1
	}
	for _, snapshot := range meta.Snapshots {
		if snapshot.SequenceNumber >= next {
			next = snapshot.SequenceNumber + 1
		}
	}
	return next
}

func maintenanceNow(now func() time.Time) time.Time {
	if now != nil {
		if value := now(); !value.IsZero() {
			return value.UTC()
		}
	}
	return time.Now().UTC()
}
