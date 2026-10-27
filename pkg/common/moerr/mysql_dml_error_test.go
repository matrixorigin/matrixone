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

package moerr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMySQLDMLCompatibilityErrors(t *testing.T) {
	wrongUsage := NewWrongUsage(context.Background(), "UPDATE", "ORDER BY")
	require.Equal(t, uint16(ER_WRONG_USAGE), wrongUsage.MySQLCode())
	require.Equal(t, "Incorrect usage of UPDATE and ORDER BY", wrongUsage.Error())

	targetUsed := NewUpdateTableUsed(context.Background(), "items")
	require.Equal(t, uint16(ER_UPDATE_TABLE_USED), targetUsed.MySQLCode())
	require.Equal(t, "You can't specify target table 'items' for update in FROM clause", targetUsed.Error())

	txCharacteristics := NewCantChangeTxCharacteristics(context.Background())
	require.Equal(t, uint16(ER_CANT_CHANGE_TX_CHARACTERISTICS), txCharacteristics.MySQLCode())
	require.Equal(t, "25001", txCharacteristics.SqlState())
	require.Equal(t, "Transaction characteristics can't be changed while a transaction is in progress", txCharacteristics.Error())

	fieldSpecifiedTwice := NewFieldSpecifiedTwice(context.Background(), "ItemID")
	require.Equal(t, ErrFieldSpecifiedTwice, fieldSpecifiedTwice.ErrorCode())
	require.Equal(t, uint16(ER_FIELD_SPECIFIED_TWICE), fieldSpecifiedTwice.MySQLCode())
	require.Equal(t, "42000", fieldSpecifiedTwice.SqlState())
	require.Equal(t, "Column 'ItemID' specified twice", fieldSpecifiedTwice.Error())
}

func TestMySQLDefaultAndDigestErrors(t *testing.T) {
	for _, tc := range []struct {
		err     *Error
		code    uint16
		message string
	}{
		{NewDigestParse(t.Context()), ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN, "Could not parse argument to digest function."},
		{NewNoDefaultForColumn(t.Context(), "a"), ER_NO_DEFAULT_FOR_FIELD, "Field 'a' doesn't have a default value"},
		{NewDefaultExpression(t.Context()), ER_DEFAULT_AS_VAL_GENERATED, "DEFAULT function cannot be used with default value expressions"},
	} {
		require.Equal(t, tc.code, tc.err.MySQLCode())
		require.Equal(t, "HY000", tc.err.SqlState())
		require.Equal(t, tc.message, tc.err.Error())
		data, err := tc.err.MarshalBinary()
		require.NoError(t, err)
		var decoded Error
		require.NoError(t, decoded.UnmarshalBinary(data))
		require.Equal(t, tc.err.ErrorCode(), decoded.ErrorCode())
		require.Equal(t, tc.code, decoded.MySQLCode())
		require.Equal(t, tc.message, decoded.Error())
	}
}
