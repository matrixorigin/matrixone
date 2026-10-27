//go:build mo_m0

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

package readutil

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func init() {
	activeM0ReadLocationProofEnabled = true
	activeM0ReadLocationProof = proveM0ReadLocation
}

func proveM0ReadLocation(
	ctx context.Context,
	table *plan.TableDef,
	location objectio.Location,
	fs fileservice.FileService,
) (bool, objectio.M0ObjectProof, error) {
	candidate := table != nil && (table.AppendMissingState != nil || catalog.M0PlanLooksLikeAppendDomain(table))
	next := uint32(0)
	if table != nil && len(table.Cols) > 0 {
		next = uint32(len(table.Cols) - 1)
	}
	layout, err := catalog.M0AppendLayoutFromPlan(table, next)
	if err != nil {
		if candidate {
			return true, objectio.M0ObjectProof{}, err
		}
		return false, objectio.M0ObjectProof{}, nil
	}
	if err = objectio.ValidateM0AppendLayout(layout); err != nil {
		if candidate {
			return true, objectio.M0ObjectProof{}, err
		}
		return false, objectio.M0ObjectProof{}, nil
	}
	reader, err := objectio.NewObjectReaderWithStr(location.Name().String(), fs)
	if err != nil {
		return true, objectio.M0ObjectProof{}, err
	}
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	header, err := reader.ReadHeader(ctx, mp)
	if err != nil {
		return true, objectio.M0ObjectProof{}, err
	}
	meta, err := reader.ReadAllMeta(ctx, mp)
	if err != nil {
		return true, objectio.M0ObjectProof{}, err
	}
	width, err := objectio.ValidateM0Object(layout, header, meta.MustDataMeta())
	if err != nil {
		return true, objectio.M0ObjectProof{}, err
	}
	proof, err := objectio.NewM0ObjectProof(location.Name().String(), width, layout.NextColSeqnum,
		table.TblId, table.Version, table.AppendMissingState)
	return true, proof, err
}
