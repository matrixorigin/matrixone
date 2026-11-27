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

package colexec

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// CheckZeroTemporalVector checks the temporal values of one column about to be
// written. With rejectZero (strict mode with NO_ZERO_DATE), a zero DATE,
// DATETIME or TIMESTAMP is an error. A TIMESTAMP below the supported lower
// bound is an error regardless of rejectZero. Other types are not checked.
// Errors name the 1-based row.
//
// The type is resolved once per vector; each row loop is typed.
func CheckZeroTemporalVector(ctx context.Context, vec *vector.Vector, rejectZero bool) error {
	switch vec.GetType().Oid {
	case types.T_date:
		if !rejectZero {
			return nil
		}
		if row, ok := firstNonNullEqual(vec, types.ZeroDate); ok {
			return moerr.NewTruncatedValueForField(ctx, "date", "0000-00-00", "value", row+1)
		}
	case types.T_datetime:
		if !rejectZero {
			return nil
		}
		if row, ok := firstNonNullEqual(vec, types.ZeroDatetime); ok {
			return moerr.NewTruncatedValueForField(ctx, "datetime", "0000-00-00 00:00:00", "value", row+1)
		}
	case types.T_timestamp:
		n := vec.Length()
		for row := 0; row < n; row++ {
			if vec.IsNull(uint64(row)) {
				continue
			}
			timestamp := vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, row)
			if !types.ValidTimestamp(timestamp) {
				return moerr.NewTruncatedValueForField(ctx, "datetime", fmt.Sprintf("%d", int64(timestamp)), "value", row+1)
			}
			if rejectZero && timestamp == types.ZeroTimestamp {
				return moerr.NewTruncatedValueForField(ctx, "datetime", "0000-00-00 00:00:00", "value", row+1)
			}
		}
	}
	return nil
}

// firstNonNullEqual returns the first non-null row of vec equal to v.
func firstNonNullEqual[T comparable](vec *vector.Vector, v T) (int, bool) {
	n := vec.Length()
	for row := 0; row < n; row++ {
		if vec.IsNull(uint64(row)) {
			continue
		}
		if vector.GetFixedAtNoTypeCheck[T](vec, row) == v {
			return row, true
		}
	}
	return 0, false
}
