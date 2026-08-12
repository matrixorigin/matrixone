// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// FormatDedupKey formats a duplicate key from the expression vector used by a
// DEDUP join. FLOAT/DOUBLE primary-key identity expressions are serial(...)
// encodings, so decode those bytes with the original column types instead of
// leaking the binary identity key into the user-facing duplicate-entry error.
func FormatDedupKey(vec *vector.Vector, row int, colTypes []plan.Type) (string, error) {
	if len(colTypes) == 1 {
		originalType := types.T(colTypes[0].Id)
		if (originalType == types.T_float32 || originalType == types.T_float64) &&
			(vec.GetType().Oid == types.T_varchar || vec.GetType().Oid == types.T_varbinary) {
			items, err := types.StringifyTuple(vec.GetBytesAt(row), colTypes)
			if err != nil {
				return "", err
			}
			return items[0], nil
		}
		return vec.RowToString(row), nil
	}

	items, err := types.StringifyTuple(vec.GetBytesAt(row), colTypes)
	if err != nil {
		return "", err
	}
	return "(" + strings.Join(items, ",") + ")", nil
}
