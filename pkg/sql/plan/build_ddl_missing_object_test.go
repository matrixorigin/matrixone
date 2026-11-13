// Copyright 2021 - 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

// TestDropTableMissingErrorContract pins MySQL's ER_BAD_TABLE_ERROR contract
// (1051, "Unknown table") for DROP TABLE on an absent table (#31451), while
// DROP TABLE IF EXISTS keeps its no-op behavior. Query/DML lookups on missing
// tables keep their own 1146 classification on their own paths.
func TestDropTableMissingErrorContract(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))

	_, err := runOneStmt(mock, t, "drop table if exists drop_missing_ghost")
	require.NoError(t, err)

	_, err = runOneStmt(mock, t, "drop table drop_missing_ghost")
	require.Error(t, err)
	var moErr *moerr.Error
	require.True(t, errors.As(err, &moErr))
	require.Equal(t, uint16(1051), moErr.MySQLCode())
	require.Contains(t, err.Error(), "Unknown table")
	require.Contains(t, err.Error(), "drop_missing_ghost")
}

// TestDropIndexMissingErrorContract pins MySQL's ER_CANT_DROP_FIELD_OR_KEY
// contract (1091, "Can't DROP ...; check that it exists") for a missing index
// on an existing table (#31735), instead of leaking the internal lookup
// failure. DROP INDEX IF EXISTS keeps its no-op behavior.
func TestDropIndexMissingErrorContract(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))

	_, err := runOneStmt(mock, t, "drop index if exists nope_idx on test_idx")
	require.NoError(t, err)

	_, err = runOneStmt(mock, t, "drop index nope_idx on test_idx")
	require.Error(t, err)
	var moErr *moerr.Error
	require.True(t, errors.As(err, &moErr))
	require.Equal(t, uint16(1091), moErr.MySQLCode())
	require.Contains(t, err.Error(), "Can't DROP 'nope_idx'")
}
