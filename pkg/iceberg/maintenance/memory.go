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
	"context"
	"errors"
	"math"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const defaultMaintenancePlanningMemory int64 = 256 << 20

func maintenanceMemoryLimit(configured int64) int64 {
	if configured > 0 {
		return configured
	}
	return defaultMaintenancePlanningMemory
}

func readMaintenanceMetadataObject(ctx context.Context, reader api.ObjectReader, path string, remaining int64) ([]byte, error) {
	if remaining <= 0 {
		return nil, maintenanceMemoryExceeded(0, remaining)
	}
	if bounded, ok := reader.(interface {
		ReadBounded(context.Context, string, int64) ([]byte, error)
	}); ok {
		return bounded.ReadBounded(ctx, path, remaining)
	}
	// Production object readers provide ReadBounded. The post-check preserves
	// compatibility for catalog/test adapters that only implement ObjectReader.
	data, err := reader.Read(ctx, path, 0, -1)
	if err != nil {
		return nil, err
	}
	if int64(cap(data)) > remaining {
		return nil, maintenanceMemoryExceeded(int64(cap(data)), remaining)
	}
	return data, nil
}

func readMaintenanceManifestList(ctx context.Context, facade api.MetadataFacade, data []byte, maxRecords int, maxMemory int64) ([]api.ManifestFile, error) {
	if bounded, ok := facade.(interface {
		ReadManifestListWithLimits(context.Context, []byte, int, int64) ([]api.ManifestFile, error)
	}); ok {
		return bounded.ReadManifestListWithLimits(ctx, data, maxRecords, maxMemory)
	}
	if bounded, ok := facade.(interface {
		ReadManifestListBounded(context.Context, []byte, int) ([]api.ManifestFile, error)
	}); ok {
		return bounded.ReadManifestListBounded(ctx, data, maxRecords)
	}
	return facade.ReadManifestList(ctx, data)
}

func readMaintenanceManifest(ctx context.Context, facade api.MetadataFacade, data []byte, maxRecords int, maxMemory int64) ([]api.ManifestEntry, error) {
	if bounded, ok := facade.(interface {
		ReadManifestWithLimits(context.Context, []byte, int, int64) ([]api.ManifestEntry, error)
	}); ok {
		return bounded.ReadManifestWithLimits(ctx, data, maxRecords, maxMemory)
	}
	if bounded, ok := facade.(interface {
		ReadManifestBounded(context.Context, []byte, int) ([]api.ManifestEntry, error)
	}); ok {
		return bounded.ReadManifestBounded(ctx, data, maxRecords)
	}
	return facade.ReadManifest(ctx, data)
}

func maintenanceRecordLimit(remaining, perRecord int64) int {
	if perRecord <= 0 || remaining <= 0 {
		return 1
	}
	limit := remaining / perRecord
	if limit < 1 {
		return 1
	}
	if limit > int64(^uint(0)>>1) {
		return int(^uint(0) >> 1)
	}
	return int(limit)
}

func checkMaintenanceMemory(used, addition, limit int64) error {
	if used >= 0 && addition >= 0 && used <= limit && addition <= limit-used {
		return nil
	}
	actual := used + addition
	if actual < 0 {
		actual = int64(^uint64(0) >> 1)
	}
	return maintenanceMemoryExceeded(actual, limit)
}

func reserveMaintenanceMemory(used *int64, addition, limit int64) error {
	if used == nil {
		return maintenanceMemoryExceeded(addition, limit)
	}
	if err := checkMaintenanceMemory(*used, addition, limit); err != nil {
		return err
	}
	*used += addition
	return nil
}

func saturatingMaintenanceMul(left, right int64) int64 {
	if left <= 0 || right <= 0 {
		return 0
	}
	if left > math.MaxInt64/right {
		return math.MaxInt64
	}
	return left * right
}

func saturatingMaintenanceAdd(left, right int64) int64 {
	if left < 0 || right < 0 || left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func maintenanceMemoryExceeded(actual, limit int64) error {
	return api.NewError(api.ErrPlanningLimitExceeded, "Iceberg maintenance planning memory limit exceeded", map[string]string{
		"actual_bytes": strconv.FormatInt(actual, 10),
		"limit_bytes":  strconv.FormatInt(limit, 10),
	})
}

func isMaintenancePlanningLimit(err error) bool {
	var icebergErr *api.IcebergError
	return errors.As(err, &icebergErr) && icebergErr.Code == api.ErrPlanningLimitExceeded
}

var errMaintenanceBufferLimit = api.NewError(api.ErrPlanningLimitExceeded, "Iceberg maintenance output buffer exceeded its memory limit", nil)

// boundedMaintenanceBuffer grows deterministically and never lets retained
// capacity or the old+new make/copy peak exceed its allowance. Avro encoders
// buffer a block internally, so callers must reserve encoder scratch separately
// before assigning this output allowance.
type boundedMaintenanceBuffer struct {
	data     []byte
	maxBytes int64
	exceeded bool
}

func (b *boundedMaintenanceBuffer) Write(payload []byte) (int, error) {
	if b.maxBytes <= 0 {
		b.exceeded = true
		return 0, errMaintenanceBufferLimit
	}
	if int64(len(b.data)) > b.maxBytes-int64(len(payload)) {
		b.exceeded = true
		return 0, errMaintenanceBufferLimit
	}
	required := len(b.data) + len(payload)
	if required > cap(b.data) {
		maxCapacity := math.MaxInt
		if b.maxBytes < int64(maxCapacity) {
			maxCapacity = int(b.maxBytes)
		}
		nextCapacity := cap(b.data) * 2
		if nextCapacity < 512 {
			nextCapacity = 512
		}
		if nextCapacity < required {
			nextCapacity = required
		}
		if nextCapacity > maxCapacity {
			nextCapacity = maxCapacity
		}
		if cap(b.data) > maxCapacity-nextCapacity {
			b.exceeded = true
			return 0, errMaintenanceBufferLimit
		}
		next := make([]byte, len(b.data), nextCapacity)
		copy(next, b.data)
		b.data = next
	}
	start := len(b.data)
	b.data = b.data[:required]
	copy(b.data[start:], payload)
	return len(payload), nil
}

func (b *boundedMaintenanceBuffer) Bytes() []byte {
	return b.data
}
