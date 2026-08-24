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

package tree

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestSampleExprFormat(t *testing.T) {
	column := NewUnresolvedColName("c")

	percentStar, err := NewSamplePercentFuncExpression1(50, true, nil)
	require.NoError(t, err)
	require.Equal(t, "sample(*, 50.0 percent)", String(percentStar, dialect.MYSQL))

	percentColumn, err := NewSamplePercentFuncExpression2(12.5, false, Exprs{column})
	require.NoError(t, err)
	require.Equal(t, "sample(c, 12.5 percent)", String(percentColumn, dialect.MYSQL))

	rowsBlock, err := NewSampleRowsFuncExpression(10, true, nil, "block")
	require.NoError(t, err)
	require.Equal(t, "sample(*, 10 rows)", String(rowsBlock, dialect.MYSQL))

	rowsColumn, err := NewSampleRowsFuncExpression(3, false, Exprs{column}, "row")
	require.NoError(t, err)
	require.Equal(t, "sample(c, 3 rows, 'row')", String(rowsColumn, dialect.MYSQL))
}
