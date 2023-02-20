// Copyright 2021 Matrix Origin
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

package compile

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopleft"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopmark"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsingle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mark"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/single"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/update"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestInstructionSerializationCover(t *testing.T) {
	// ensure that encodeScope and decodeScope can reach every instruction types.
	testCases := []struct {
		instruction vm.Instruction
		ignore      bool
	}{
		{instruction: vm.Instruction{Op: vm.Top, Arg: &top.Argument{}}},
		{instruction: vm.Instruction{Op: vm.Join, Arg: &join.Argument{Conditions: make([][]*plan.Expr, 2)}}},
		{instruction: vm.Instruction{Op: vm.Semi, Arg: &semi.Argument{Conditions: make([][]*plan.Expr, 2)}}},
		{instruction: vm.Instruction{Op: vm.Left, Arg: &left.Argument{Conditions: make([][]*plan.Expr, 2)}}},
		{instruction: vm.Instruction{Op: vm.Limit, Arg: &limit.Argument{}}},
		{instruction: vm.Instruction{Op: vm.Merge, Arg: &merge.Argument{}}},
		{instruction: vm.Instruction{Op: vm.Order, Arg: &order.Argument{}}},
		{instruction: vm.Instruction{Op: vm.Group, Arg: &group.Argument{}}},
		{instruction: vm.Instruction{Op: vm.Output, Arg: &output.Argument{}}, ignore: true},
		{instruction: vm.Instruction{Op: vm.Offset, Arg: &offset.Argument{}}},
		{instruction: vm.Instruction{Op: vm.Product, Arg: &product.Argument{}}},
		{instruction: vm.Instruction{Op: vm.Restrict, Arg: &restrict.Argument{}}},
		{instruction: vm.Instruction{Op: vm.Dispatch, Arg: &dispatch.Argument{}}},
		{instruction: vm.Instruction{Op: vm.Connector, Arg: &connector.Argument{}}, ignore: true},
		{instruction: vm.Instruction{Op: vm.Projection, Arg: &projection.Argument{}}},
		{instruction: vm.Instruction{Op: vm.Anti, Arg: &anti.Argument{Conditions: make([][]*plan.Expr, 2)}}},
		{instruction: vm.Instruction{Op: vm.Single, Arg: &single.Argument{Conditions: make([][]*plan.Expr, 2)}}},
		{instruction: vm.Instruction{Op: vm.Mark, Arg: &mark.Argument{Conditions: make([][]*plan.Expr, 2)}}},
		{instruction: vm.Instruction{Op: vm.LoopJoin, Arg: &loopjoin.Argument{}}},
		{instruction: vm.Instruction{Op: vm.LoopLeft, Arg: &loopleft.Argument{}}},
		{instruction: vm.Instruction{Op: vm.LoopSemi, Arg: &loopsemi.Argument{}}},
		{instruction: vm.Instruction{Op: vm.LoopAnti, Arg: &loopanti.Argument{}}},
		{instruction: vm.Instruction{Op: vm.LoopSingle, Arg: &loopsingle.Argument{}}},
		{instruction: vm.Instruction{Op: vm.LoopMark, Arg: &loopmark.Argument{}}},
		{instruction: vm.Instruction{Op: vm.MergeTop, Arg: &mergetop.Argument{}}},
		{instruction: vm.Instruction{Op: vm.MergeLimit, Arg: &mergelimit.Argument{}}},
		{instruction: vm.Instruction{Op: vm.MergeOrder, Arg: &mergeorder.Argument{}}},
		{instruction: vm.Instruction{Op: vm.MergeGroup, Arg: &mergegroup.Argument{}}},
		{instruction: vm.Instruction{Op: vm.MergeOffset, Arg: &mergeoffset.Argument{}}},
		{instruction: vm.Instruction{Op: vm.Deletion, Arg: &deletion.Argument{}}, ignore: true},
		{instruction: vm.Instruction{Op: vm.Insert, Arg: &insert.Argument{}}, ignore: true},
		{instruction: vm.Instruction{Op: vm.Update, Arg: &update.Argument{}}, ignore: true},
		{instruction: vm.Instruction{Op: vm.External, Arg: &external.Argument{Es: &external.ExternalParam{}}}},
		{instruction: vm.Instruction{Op: vm.Minus, Arg: &minus.Argument{}}},
		{instruction: vm.Instruction{Op: vm.Intersect, Arg: &intersect.Argument{}}},
		{instruction: vm.Instruction{Op: vm.IntersectAll, Arg: &intersectall.Argument{}}},
		{instruction: vm.Instruction{Op: vm.HashBuild, Arg: &hashbuild.Argument{}}},
		{instruction: vm.Instruction{Op: vm.TableFunction, Arg: &table_function.Argument{}}},
	}
	{
		typeReached := make([]int, vm.LastInstructionOp)
		for _, tc := range testCases {
			typeReached[tc.instruction.Op]++
		}
		for i, num := range typeReached {
			require.Greater(t, num, 0, fmt.Sprintf("lack of serialization ut for instruction (op is %d)", i))
		}
	}

	for _, tc := range testCases {
		if tc.ignore {
			continue
		}
		_, encode, err := convertToPipelineInstruction(
			&tc.instruction,
			nil,
			0,
		)
		require.NoError(t, err)
		_, err = convertToVmInstruction(encode, nil)
		require.NoError(t, err)
	}
}
