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

package plan

import (
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestIndexDefIncludedColumnsRoundTripAndDeepCopy(t *testing.T) {
	indexDef := &planpb.IndexDef{
		IndexName:       "idx_embedding",
		Parts:           []string{"embedding"},
		IndexAlgo:       "ivfflat",
		IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops"}`,
		IncludedColumns: []string{"title", "category"},
	}

	data, err := indexDef.Marshal()
	require.NoError(t, err)

	var decoded planpb.IndexDef
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, indexDef.IncludedColumns, decoded.IncludedColumns)

	copied := DeepCopyIndexDef(indexDef)
	require.Equal(t, indexDef.IncludedColumns, copied.IncludedColumns)

	indexDef.IncludedColumns[0] = "headline"
	require.Equal(t, []string{"title", "category"}, copied.IncludedColumns)
}
