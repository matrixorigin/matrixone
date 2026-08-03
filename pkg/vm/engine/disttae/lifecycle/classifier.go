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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type ExpirationClassifier struct {
	ColumnOrdinal int
	ColumnType    types.T
	Cutoff        int64
}

func (classifier ExpirationClassifier) Classify(
	_ context.Context,
	value *batch.Batch,
	snapshotDeleted *nulls.Nulls,
) (*nulls.Nulls, error) {
	if value == nil ||
		classifier.ColumnOrdinal < 0 ||
		classifier.ColumnOrdinal >= len(value.Vecs) {
		return nil, fmt.Errorf("Lifecycle expiration classifier input is incomplete")
	}
	column := value.Vecs[classifier.ColumnOrdinal]
	if column.GetType().Oid != classifier.ColumnType {
		return nil, fmt.Errorf(
			"Lifecycle column type changed from %s to %s",
			classifier.ColumnType,
			column.GetType().Oid,
		)
	}
	expired := &nulls.Nulls{}
	for row := 0; row < value.RowCount(); row++ {
		if snapshotDeleted != nil && snapshotDeleted.Contains(uint64(row)) {
			continue
		}
		if column.GetNulls().Contains(uint64(row)) {
			return nil, fmt.Errorf("Lifecycle column contains NULL")
		}
		var encoded int64
		switch classifier.ColumnType {
		case types.T_date:
			encoded = int64(vector.GetFixedAtNoTypeCheck[types.Date](column, row))
		case types.T_datetime:
			encoded = int64(vector.GetFixedAtNoTypeCheck[types.Datetime](column, row))
		case types.T_timestamp:
			encoded = int64(vector.GetFixedAtNoTypeCheck[types.Timestamp](column, row))
		default:
			return nil, fmt.Errorf(
				"unsupported Lifecycle column type %s",
				classifier.ColumnType,
			)
		}
		if encoded < classifier.Cutoff {
			expired.Add(uint64(row))
		}
	}
	return expired, nil
}
