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

package dedupjoin

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type dedupJoinWarning struct {
	code uint16
	msg  string
}

type dedupJoinWarningSession struct {
	total    uint64
	warnings []dedupJoinWarning
}

func (*dedupJoinWarningSession) GetTempTable(string, string) (string, bool) { return "", false }
func (*dedupJoinWarningSession) AddTempTable(string, string, string)        {}
func (*dedupJoinWarningSession) RemoveTempTable(string, string)             {}
func (*dedupJoinWarningSession) RemoveTempTableByRealName(string)           {}
func (*dedupJoinWarningSession) GetSqlModeNoAutoValueOnZero() (bool, bool)  { return false, false }
func (s *dedupJoinWarningSession) AppendWarningBatch(total uint64, codes []uint16, messages []string) {
	s.total += total
	for i := 0; i < len(codes) && i < len(messages); i++ {
		s.warnings = append(s.warnings, dedupJoinWarning{code: codes[i], msg: messages[i]})
	}
}

func TestDedupJoinIgnoreReportsExistingKeyConflict(t *testing.T) {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()

	session := &dedupJoinWarningSession{}
	proc.Session = session
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)

	int32Typ := types.T_int32.ToType()
	tag++
	curTag := tag
	conditions := [][]*plan.Expr{
		{newExpr(0, int32Typ)},
		{newExpr(0, int32Typ)},
	}
	dedupArg := &DedupJoin{
		LeftTypes:         []types.Type{int32Typ},
		RightTypes:        []types.Type{int32Typ},
		Conditions:        conditions,
		Result:            []colexec.ResultPos{colexec.NewResultPos(1, 0)},
		OnDuplicateAction: plan.Node_IGNORE,
		DedupColName:      "PRIMARY",
		DedupColTypes:     []plan.Type{newExpr(0, int32Typ).Typ},
		JoinMapTag:        curTag,
		OperatorBase:      vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	buildArg := &hashbuild.HashBuild{
		NeedHashMap:       true,
		NeedBatches:       true,
		Conditions:        conditions[1],
		IsDedup:           true,
		DelColIdx:         -1,
		OnDuplicateAction: plan.Node_IGNORE,
		DedupColName:      "PRIMARY",
		DedupColTypes:     []plan.Type{newExpr(0, int32Typ).Typ},
		JoinMapTag:        curTag,
		JoinMapRefCnt:     1,
		OperatorBase:      vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}

	buildBat := makeInt32Batch(proc.Mp(), [][]int32{{1, 2}}, nil)
	probeBat := makeInt32Batch(proc.Mp(), [][]int32{{1}}, nil)
	out := runFinalizeFixture(t, dedupArg, buildArg, proc, buildBat, probeBat)
	require.Len(t, out, 1)
	require.Equal(t, 1, out[0].RowCount())
	require.Equal(t, uint64(1), session.total)
	require.Equal(t, []dedupJoinWarning{{
		code: moerr.ER_DUP_ENTRY,
		msg:  "Duplicate entry '1' for key 'PRIMARY'",
	}}, session.warnings)

	dedupArg.Reset(proc, false, nil)
	buildArg.Reset(proc, false, nil)
	dedupArg.Free(proc, false, nil)
	buildArg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}
