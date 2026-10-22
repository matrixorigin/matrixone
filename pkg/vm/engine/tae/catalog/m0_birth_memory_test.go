//go:build mo_m0

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestM0BirthMemoryCapturedSchema(t *testing.T) {
	read := appendMissingSchema(t)
	write := read.Clone()
	write.Extra.AppendMissingState = nil
	write.Version = 0
	write.ColDefs = append(write.ColDefs[:3], write.ColDefs[4])
	write.ColDefs[3].Idx = 3
	write.Extra.NextColSeqnum = 3
	write.NameMap = make(map[string]int)
	write.SeqnumMap = make(map[uint16]int)
	require.NoError(t, write.Finalize(true))
	kind, err := M0MemorySuffixKind(read, write, 3)
	require.NoError(t, err)
	require.Equal(t, plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1, kind)
	_, err = M0MemorySuffixKind(read, write, 2)
	require.Error(t, err)
	write.Version = 1
	_, err = M0MemorySuffixKind(read, write, 3)
	require.Error(t, err)
	write.Version = 0
	write.ColDefs[1].SeqNum = 0
	_, err = M0MemorySuffixKind(read, write, 3)
	require.Error(t, err)
}
