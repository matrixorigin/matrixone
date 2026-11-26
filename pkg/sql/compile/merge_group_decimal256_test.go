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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/stretchr/testify/require"
)

// A MIN/MAX over a DECIMAL(p > 38) column precomputed from zone maps is a
// decimal256 partial result; it must survive the trip to a remote pipeline.
// EncodeMergeGroup used to skip it, and DecodeMergeGroup to misread what
// followed.
func TestMergeGroupPartialResultDecimal256(t *testing.T) {
	d := types.Decimal256{B0_63: 1, B64_127: 2, B128_191: 3, B192_255: 4}
	merge := &group.MergeGroup{
		PartialResults:     []any{d, int64(42)},
		PartialResultTypes: []types.T{types.T_decimal256, types.T_int64},
	}
	var pipe pipeline.Group
	EncodeMergeGroup(merge, &pipe)
	require.Len(t, pipe.PartialResults, 32+8)

	var decoded group.MergeGroup
	DecodeMergeGroup(&decoded, &pipe)
	require.Equal(t, []types.T{types.T_decimal256, types.T_int64}, decoded.PartialResultTypes)
	require.Equal(t, []any{d, int64(42)}, decoded.PartialResults)
}
