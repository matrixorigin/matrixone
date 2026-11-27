// Copyright 2022 Matrix Origin
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

package objectio

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const MaxObjectCacheProbeIDs = 16

// CountCachedObjectFiles uses fixed-size internal IDs and consults only the
// local whole-object index. Entries may be evicted after the probe returns.
func CountCachedObjectFiles(ctx context.Context, fs fileservice.FileService, objectIDs [][]byte) (int, error) {
	if len(objectIDs) > MaxObjectCacheProbeIDs {
		return 0, moerr.NewInvalidInput(ctx, "too many object cache probe IDs")
	}
	hits := 0
	var seen [MaxObjectCacheProbeIDs]ObjectId
	for i, encoded := range objectIDs {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if len(encoded) != ObjectIDSize {
			return 0, moerr.NewInvalidInput(ctx, "invalid object cache probe ID")
		}
		id := ObjectId(encoded)
		for _, prev := range seen[:i] {
			if id == prev {
				return 0, moerr.NewInvalidInput(ctx, "duplicate object cache probe ID")
			}
		}
		seen[i] = id
		if fileservice.IsWholeObjectCached(fs, BuildObjectNameWithObjectID(&id).UnsafeString()) {
			hits++
		}
	}
	return hits, nil
}
