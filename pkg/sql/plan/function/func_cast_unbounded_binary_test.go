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
package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestCastUnboundedBinaryEnvelope(t *testing.T) {
	for _, from := range []types.T{types.T_varchar, types.T_binary, types.T_varbinary} {
		for _, to := range []types.T{types.T_binary, types.T_varbinary} {
			t.Run(from.String()+"/"+to.String(), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				source := vector.NewVec(from.ToType())
				defer source.Free(proc.Mp())
				value := strings.Repeat("\xff\x00A", types.MaxStringSize/3+1)
				require.NoError(t, vector.AppendBytes(source, []byte(value), false, proc.Mp()))
				require.NoError(t, vector.AppendBytes(source, nil, true, proc.Mp()))
				typ := types.New(to, -1, 0)
				destination := vector.NewVec(typ)
				defer destination.Free(proc.Mp())
				result := vector.NewFunctionResultWrapper(typ, proc.Mp())
				defer result.Free()
				require.NoError(t, result.PreExtendAndReset(2))
				require.NoError(t, NewCast([]*vector.Vector{source, destination}, result, proc, 2, nil))
				got := result.GetResultVector()
				require.Equal(t, value, string(got.GetBytesAt(0)))
				require.True(t, got.IsNull(1))
			})
		}
	}
}
