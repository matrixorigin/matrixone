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

package mysql

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAutoIDCacheRejectsPartitionOwnership(t *testing.T) {
	for _, size := range []int{0, 1, 8} {
		for _, prefix := range []string{"", "comment='keep' "} {
			for _, definition := range []string{
				"partition by range(id) (partition p0 values less than (10) %s, partition p1 values less than maxvalue)",
				"partition by range(id) subpartition by hash(id) (partition p0 values less than maxvalue (subpartition s0 %s))",
			} {
				sql := "create table t(id bigint auto_increment primary key) " + fmt.Sprintf(definition, fmt.Sprintf("%sauto_id_cache=%d", prefix, size))
				stmt, err := ParseOne(t.Context(), sql, 1)
				if stmt != nil {
					stmt.Free()
				}
				require.ErrorContains(t, err, "AUTO_ID_CACHE", sql)
			}
		}
	}
	stmt, err := ParseOne(t.Context(), "create table t(id bigint auto_increment primary key) auto_id_cache=1 partition by range(id) (partition p0 values less than maxvalue comment='keep')", 1)
	require.NoError(t, err)
	defer stmt.Free()
}
