//go:build gpu

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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestClassifyAlwaysAsyncGPUIndexesForUpdate(t *testing.T) {
	for _, algo := range []string{
		catalog.MoIndexCagraAlgo.ToString(),
		catalog.MoIndexIvfpqAlgo.ToString(),
	} {
		t.Run(algo, func(t *testing.T) {
			tableDef := &TableDef{
				Pkey: &PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
				Indexes: []*IndexDef{{
					IndexName:      "idx",
					IndexTableName: "idx_storage",
					IndexAlgo:      algo,
					Parts:          []string{"vec"},
					TableExist:     true,
				}},
			}

			inline, unsupported, err := classifyIrregularIndexesForUpdate(
				context.Background(), tableDef, map[string]tree.Expr{"id": nil, "vec": nil})
			require.NoError(t, err)
			require.False(t, unsupported)
			require.Empty(t, inline)
		})
	}
}
