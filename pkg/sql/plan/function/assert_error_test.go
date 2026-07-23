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

package function

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func TestAssertCheckConstraintFailureUsesMySQL3819(t *testing.T) {
	err := newAssertFailure(context.Background(), "Check constraint 't_chk_1' is violated")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
	require.Equal(t, uint16(moerr.ER_CHECK_CONSTRAINT_VIOLATED), err.(*moerr.Error).MySQLCode())

	err = newAssertFailure(context.Background(), "ordinary assert failure")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
}
