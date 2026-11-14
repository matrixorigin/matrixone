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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/udf/udferr"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/stretchr/testify/require"
)

func TestRoutineDiagnosticSQLContract(t *testing.T) {
	cause := udferr.New("TYPE_CONTRACT: result length mismatch")
	for _, err := range []error{cause, errutil.Wrapf(cause, "python udf: worker error")} {
		code, state, message := RewriteError(err, "")
		require.Equal(t, uint16(moerr.ER_INTERNAL_ERROR), code)
		require.Equal(t, "HY000", state)
		require.Equal(t, err.Error(), message)
	}

	// Existing MO-classified errors retain their own SQL mapping.
	classified := moerr.NewInternalErrorNoCtx("classified")
	code, state, message := RewriteError(classified, "")
	require.Equal(t, classified.ErrorCode(), code)
	require.Equal(t, classified.SqlState(), state)
	require.Equal(t, "internal error: classified", message)
}
