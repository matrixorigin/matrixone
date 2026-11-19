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

package ioutil

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// LoadColumnsDataByExactFilter keeps conversion views inside ObjectIO and uses
// the same IOVector/lease owner as ordinary column materialization.
func LoadColumnsDataByExactFilter(ctx context.Context, columns []uint16, typs []types.Type,
	fs fileservice.FileService, location objectio.Location, destinations []*vector.Vector,
	inputRows []int64, state *objectio.ExactScanState, mp *mpool.MPool, policy fileservice.Policy) (objectio.ExactScanStats, error) {
	if len(columns) == 0 || len(columns) != len(typs) || len(columns) != len(destinations) || fs == nil || state == nil || mp == nil {
		return objectio.ExactScanStats{}, moerr.NewInvalidInputNoCtx("invalid exact column read request")
	}
	for i, dst := range destinations {
		if dst == nil || dst.GetType().Oid != typs[i].Oid {
			return objectio.ExactScanStats{}, moerr.NewInvalidInputNoCtx("invalid exact column destination")
		}
	}
	ioVector, _, err := readColumnsData(ctx, columns, typs, fs, location, nil, mp, policy)
	if err != nil {
		return objectio.ExactScanStats{}, err
	}
	defer objectio.ReleaseIOVector(&ioVector)
	return objectio.MaterializeExactScan(ctx, ioVector.Entries[:len(columns)], typs, destinations, inputRows, state, mp)
}
