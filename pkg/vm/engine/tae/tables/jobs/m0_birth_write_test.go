// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jobs

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestM0BirthCapturedWritePreservesOldPrefix(t *testing.T) {
	s := catalog.NewEmptySchema("captured")
	s.Version = 1
	s.Extra.AppendMissingState = &plan.AppendMissingState{FormatVersion: 1, LayoutId: []byte("0123456789abcdef"), BaseWidth: 3, PkSeqnum: 2,
		Columns: []*plan.ColumnBirthFill{{Seqnum: 3, BirthAppendOrdinal: 1, Type: plan.Type{Id: plan.AppendMissingBoolOID}, Kind: plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1}}}
	require.NoError(t, validateM0CapturedWrite(s, []uint16{0, 1, 2}, 0))
	require.NoError(t, validateM0CapturedWrite(s, []uint16{0, 1, 3, 2}, 1))
	require.Error(t, validateM0CapturedWrite(s, []uint16{0, 1, 2}, 1))
	require.Error(t, validateM0CapturedWrite(s, []uint16{0, 1, 3}, 0))
	require.Error(t, validateM0CapturedWrite(s, []uint16{0, 1, 2, 3}, 0))
}
