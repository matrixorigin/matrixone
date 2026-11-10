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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// CREATE TABLE accepts and ignores MySQL storage-engine options such as
// ENGINE=InnoDB; ALTER TABLE must ignore them the same way instead of failing
// with "unsupported alter option in inplace/copy mode".
func TestAlterTableIgnoresStorageOptions(t *testing.T) {
	cases := []struct {
		sql       string
		algorithm plan.AlterTable_AlgorithmType
		actions   int // non-nil plan actions
	}{
		{"alter table nation engine=InnoDB", plan.AlterTable_INPLACE, 0},
		{"alter table nation engine MyISAM", plan.AlterTable_INPLACE, 0},
		{"alter table nation row_format=dynamic, key_block_size=8, stats_persistent=0, checksum=1, pack_keys=1", plan.AlterTable_INPLACE, 0},
		{"alter table nation max_rows=10, min_rows=1, avg_row_length=100, delay_key_write=1, compression='zlib'", plan.AlterTable_INPLACE, 0},
		{"alter table nation engine=InnoDB, comment 'c'", plan.AlterTable_INPLACE, 1},
		{"alter table nation add column w int, engine=InnoDB", plan.AlterTable_COPY, 0},
		{"alter table nation engine=InnoDB, algorithm=copy", plan.AlterTable_COPY, 0},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			p, err := runOneStmt(mock, t, c.sql)
			require.NoError(t, err)
			alter := p.GetDdl().GetAlterTable()
			require.NotNil(t, alter)
			require.Equal(t, c.algorithm, alter.AlgorithmType)
			if c.algorithm == plan.AlterTable_INPLACE {
				actions := 0
				for _, action := range alter.Actions {
					if action != nil {
						actions++
					}
				}
				require.Equal(t, c.actions, actions)
			}
		})
	}
}
