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

package siriusbridge

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestNativeNumericStatusesPreservePublicErrorIdentity(t *testing.T) {
	for _, tc := range []struct {
		status uint32
		code   uint16
		mysql  uint16
		state  string
	}{{12, moerr.ErrOutOfRange, 1690, "22003"}, {13, moerr.ErrInvalidInput, 1105, "HY000"}} {
		err := nativeNumericError(tc.status, "bounded native numeric failure")
		require.True(t, moerr.IsMoErrCode(err, tc.code))
		joined := errors.Join(err, context.Canceled)
		require.ErrorIs(t, joined, context.Canceled)
		var typed *moerr.Error
		require.ErrorAs(t, joined, &typed)
		require.Equal(t, tc.code, typed.ErrorCode())
		require.Equal(t, tc.mysql, typed.MySQLCode())
		require.Equal(t, tc.state, typed.SqlState())
	}
	for _, code := range []uint32{0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 14} {
		require.Nil(t, nativeNumericError(code, "not a numeric status"))
	}
}
