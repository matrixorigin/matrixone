// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativePADCTASWireFieldsRemainIndependent(t *testing.T) {
	for _, tc := range []struct {
		name              string
		wire              []byte
		execution, native int64
		revision          uint32
	}{
		{"absent", nil, 0, 0, 0},
		{"upstream_execution_19", []byte{0x98, 0x01, 122}, 122, 0, 0},
		{"binding_20", []byte{0xa0, 0x01, 1}, 0, 0, 1},
		{"native_requirement_21", []byte{0xa8, 0x01, 0x81, 0x01}, 0, 129, 0},
		{"combined", []byte{0x98, 0x01, 122, 0xa0, 0x01, 1, 0xa8, 0x01, 0x81, 0x01}, 122, 129, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var create CreateTable
			require.NoError(t, create.Unmarshal(tc.wire))
			require.Equal(t, tc.execution, create.CtasExecutionProtocolVersion)
			require.Equal(t, tc.native, create.RequiredProtocolVersion)
			require.Equal(t, tc.revision, create.CollationBindingRevision)
			wire, err := create.Marshal()
			require.NoError(t, err)
			var decoded CreateTable
			require.NoError(t, decoded.Unmarshal(wire))
			require.Equal(t, create, decoded)
		})
	}
}
