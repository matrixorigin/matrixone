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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func runCurrentTimeWithFSP(t *testing.T, fsp int64) (int32, error) {
	proc := testutil.NewProcess(t)
	arg, err := vector.NewConstFixed(types.T_int64.ToType(), fsp, 1, proc.Mp())
	require.NoError(t, err)
	defer arg.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_time.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))
	err = builtInCurrentTime([]*vector.Vector{arg}, result, proc, 1, nil)
	return result.GetResultVector().GetType().Scale, err
}

// CURTIME()/CURRENT_TIME() must reject an FSP above 6 with MySQL error 1426,
// like NOW(), SYSDATE() and UTC_TIME(), instead of silently clamping it.
func TestBuiltInCurrentTimeRejectsTooBigPrecision(t *testing.T) {
	for _, fsp := range []int64{7, 10, -1} {
		_, err := runCurrentTimeWithFSP(t, fsp)
		require.Error(t, err, "fsp=%d", fsp)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTooBigPrecision), "fsp=%d: %v", fsp, err)
		require.Contains(t, err.Error(), "specified for 'curtime'. Maximum is 6.")
	}
	for _, fsp := range []int64{0, 3, 6} {
		scale, err := runCurrentTimeWithFSP(t, fsp)
		require.NoError(t, err, "fsp=%d", fsp)
		require.Equal(t, int32(fsp), scale)
	}
}
