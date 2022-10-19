package compile

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/generate_series"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/unnest"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/update"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInstructionSerializationCover(t *testing.T) {
	// ensure that encodeScope and decodeScope can reach every instruction types.
	var instructions = []*vm.Instruction{
		{Op: vm.Top, Arg: &top.Argument{}},
		{Op: vm.Join, Arg: &join.Argument{}},
		{Op: vm.Semi, Arg: &semi.Argument{}},
		{Op: vm.Left, Arg: &left.Argument{}},
		{Op: vm.Limit, Arg: &limit.Argument{}},
		{Op: vm.Merge, Arg: &merge.Argument{}},
		{Op: vm.Order, Arg: &order.Argument{}},
		{Op: vm.Group, Arg: &group.Argument{}},
		{Op: vm.Output, Arg: &output.Argument{}},
		{Op: vm.Offset, Arg: &offset.Argument{}},
		{Op: vm.Product, Arg: &product.Argument{}},
		{Op: vm.Restrict, Arg: &restrict.Argument{}},
		{Op: vm.Dispatch, Arg: &dispatch.Argument{}},
		{Op: vm.Connector, Arg: &connector.Argument{}},
		{Op: vm.Projection, Arg: &projection.Argument{}},
		{Op: vm.Anti, Arg: &anti.Argument{}},
		{Op: vm.Single, Arg: &single.Argument{}},
		{Op: vm.Mark, Arg: &mark.Argument{}},
		{Op: vm.LoopJoin, Arg: &loopjoin.Argument{}},
		{Op: vm.LoopLeft, Arg: &loopleft.Argument{}},
		{Op: vm.LoopSemi, Arg: &loopsemi.Argument{}},
		{Op: vm.LoopAnti, Arg: &loopanti.Argument{}},
		{Op: vm.LoopSingle, Arg: &loopsingle.Argument{}},
		{Op: vm.MergeTop, Arg: &mergetop.Argument{}},
		{Op: vm.MergeLimit, Arg: &mergelimit.Argument{}},
		{Op: vm.MergeOrder, Arg: &mergeorder.Argument{}},
		{Op: vm.MergeGroup, Arg: &mergegroup.Argument{}},
		{Op: vm.MergeOffset, Arg: &mergeoffset.Argument{}},
		{Op: vm.Deletion, Arg: &deletion.Argument{}},
		{Op: vm.Insert, Arg: &insert.Argument{}},
		{Op: vm.Update, Arg: &update.Argument{}},
		{Op: vm.External, Arg: &external.Argument{}},
		{Op: vm.Minus, Arg: &minus.Argument{}},
		{Op: vm.Intersect, Arg: &intersect.Argument{}},
		{Op: vm.IntersectAll, Arg: &intersectall.Argument{}},
		{Op: vm.HashBuild, Arg: &hashbuild.Argument{}},
		{Op: vm.Unnest, Arg: &unnest.Argument{}},
		{Op: vm.GenerateSeries, Arg: &generate_series.Argument{}},
	}
	{
		typeReached := make([]int, vm.LastInstructionOp)
		for _, instruction := range instructions {
			typeReached[instruction.Op]++
		}
		for i, num := range typeReached {
			require.Greater(t, num, 0, fmt.Sprintf("lack of serialization ut for instruction (op is %d)", i))
		}
	}

	for _, instruction := range instructions {
		_, encode, err := convertToPipelineInstruction(instruction, nil, 0)
		require.NoError(t, err)
		_, err = convertToVmInstruction(encode, nil)
		require.NoError(t, err)
	}
}
