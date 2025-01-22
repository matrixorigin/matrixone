// Copyright 2021 - 2024 Matrix Origin
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

package partition

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestCreateAndDeleteRangeBased(t *testing.T) {
	runPartitionTableCreateAndDeleteTests(
		t,
		"create table %s (c int comment 'abc') partition by range (c) (partition p1 values less than (1) engine = innodb, partition p2 values less than (2) engine = innodb)",
		partition.PartitionMethod_Range,
		func(idx int, p partition.Partition) {
			require.Equal(t, fmt.Sprintf("values less than (%d)", idx+1), p.Comment)
		},
	)
}

func TestCreateAndDeleteRangeColumnsBased(t *testing.T) {
	runPartitionTableCreateAndDeleteTests(
		t,
		"create table %s (c DATETIME) partition by range columns (c) (partition p1 values less than ('2024-01-21 00:00:00'), partition p2 values less than ('2024-01-22 00:00:00'))",
		partition.PartitionMethod_Range,
		func(idx int, p partition.Partition) {
			require.Equal(t, fmt.Sprintf("values less than ('2024-01-2%d 00:00:00')", idx+1), p.Comment)
		},
	)
}

func TestParse(t *testing.T) {
	sql := `
	create table t (
		c DATE comment 'abc'
	) partition by range (YEAR(c)) (
	 	partition p1 values less than (10), 
		partition p2 values less than (100)
	)`
	_, err := parsers.Parse(context.TODO(), dialect.MYSQL, sql, 0)
	require.NoError(t, err)
}
