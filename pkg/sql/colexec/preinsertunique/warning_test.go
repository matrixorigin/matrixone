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

package preinsertunique

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type preInsertWarning struct {
	code uint16
	msg  string
}

type preInsertWarningSession struct {
	total    uint64
	warnings []preInsertWarning
}

func (*preInsertWarningSession) GetTempTable(string, string) (string, bool) { return "", false }
func (*preInsertWarningSession) AddTempTable(string, string, string)        {}
func (*preInsertWarningSession) RemoveTempTable(string, string)             {}
func (*preInsertWarningSession) RemoveTempTableByRealName(string)           {}
func (*preInsertWarningSession) GetSqlModeNoAutoValueOnZero() (bool, bool)  { return false, false }
func (s *preInsertWarningSession) AppendWarningBatch(total uint64, codes []uint16, messages []string) {
	s.total += total
	for i := 0; i < len(codes) && i < len(messages); i++ {
		s.warnings = append(s.warnings, preInsertWarning{code: codes[i], msg: messages[i]})
	}
}

func TestInsertIgnoreMultiDedupReportsWarningForAcceptedSetConflict(t *testing.T) {
	session := &preInsertWarningSession{}
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Session = session
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)

	input := makeInsertIgnoreMultiDedupBatch(t, proc,
		[]int32{1, 2, 2}, []int32{10, 10, 20},
		nil, []bool{false, false, false}, []bool{false, false, false})
	arg := newInsertIgnoreMultiDedupArgument(input)
	require.NoError(t, arg.Prepare(proc))
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{1, 2},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()])
	require.Equal(t, uint64(1), session.total)
	require.Equal(t, []preInsertWarning{{
		code: moerr.ER_DUP_ENTRY,
		msg:  "Duplicate entry '10' for key 'v'",
	}}, session.warnings)

	arg.Free(proc, false, nil)
	input.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestBuildInsertIgnoreKeyMetadataRejectsMalformedCounts(t *testing.T) {
	typ := &plan.Type{Id: int32(types.T_int32)}
	for _, tc := range []struct {
		name string
		ctx  *plan.PreInsertUkCtx
	}{
		{
			name: "count exceeds type payload",
			ctx: &plan.PreInsertUkCtx{
				KeyColumns:    []int32{0},
				KeyNames:      []string{"id"},
				KeyTypes:      []*plan.Type{typ},
				KeyTypeCounts: []int32{2},
			},
		},
		{
			name: "negative count",
			ctx: &plan.PreInsertUkCtx{
				KeyColumns:    []int32{0},
				KeyNames:      []string{"id"},
				KeyTypes:      []*plan.Type{typ},
				KeyTypeCounts: []int32{-1},
			},
		},
		{
			name: "nil type",
			ctx: &plan.PreInsertUkCtx{
				KeyColumns:    []int32{0},
				KeyNames:      []string{"id"},
				KeyTypes:      []*plan.Type{nil},
				KeyTypeCounts: []int32{1},
			},
		},
		{
			name: "unconsumed type payload",
			ctx: &plan.PreInsertUkCtx{
				KeyColumns:    []int32{0},
				KeyNames:      []string{"id"},
				KeyTypes:      []*plan.Type{typ, typ},
				KeyTypeCounts: []int32{1},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Nil(t, buildInsertIgnoreKeyMetadata(tc.ctx))
		})
	}

	valid := &plan.PreInsertUkCtx{
		KeyColumns:    []int32{0, 1},
		KeyNames:      []string{"id", "(a,b)"},
		KeyTypes:      []*plan.Type{typ, typ, typ},
		KeyTypeCounts: []int32{1, 2},
	}
	metadata := buildInsertIgnoreKeyMetadata(valid)
	require.Len(t, metadata, 2)
	require.Equal(t, "id", metadata[0].name)
	require.Equal(t, "(a,b)", metadata[1].name)
	require.Len(t, metadata[0].types, 1)
	require.Len(t, metadata[1].types, 2)
}
