//go:build !mo_m0

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

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestM0DefaultWriterPreservesDeclaredVersion(t *testing.T) {
	require.Zero(t, activeM0WriterSchemaVersion(nil, []uint16{0, 1, 2}))
	for _, seqnums := range [][]uint16{nil, {0, 1}, {7, 2, 0}} {
		require.Equal(t, uint32(9), activeM0WriterSchemaVersion(&plan.TableDef{Version: 9}, seqnums))
	}
}
