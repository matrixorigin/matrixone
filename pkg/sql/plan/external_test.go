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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func plannerFilePruningNode(physicalCols ...string) *pbplan.Node {
	cols := make([]*pbplan.ColDef, 0, len(physicalCols)+1)
	physicalPositions := make(map[string]int32, len(physicalCols))
	for i, name := range physicalCols {
		cols = append(cols, &pbplan.ColDef{Name: name, Typ: pbplan.Type{Id: int32(types.T_varchar)}})
		physicalPositions[name] = int32(i)
	}
	cols = append(cols, &pbplan.ColDef{Name: catalog.ExternalFilePath, Typ: pbplan.Type{Id: int32(types.T_varchar)}})
	return &pbplan.Node{
		NodeType: pbplan.Node_EXTERNAL_SCAN,
		TableDef: &pbplan.TableDef{Cols: cols},
		ExternScan: &pbplan.ExternScan{
			Type:           int32(pbplan.ExternType_EXTERNAL_TB),
			TbColToDataCol: physicalPositions,
		},
	}
}

func plannerFilePruningColumn(pos int32, name string) *pbplan.Expr {
	return &pbplan.Expr{
		Typ:  pbplan.Type{Id: int32(types.T_varchar)},
		Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{ColPos: pos, Name: name}},
	}
}

func plannerFilePruningStringLiteral(value string) *pbplan.Expr {
	return &pbplan.Expr{
		Typ: pbplan.Type{Id: int32(types.T_varchar)},
		Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{
			Value: &pbplan.Literal_Sval{Sval: value},
		}},
	}
}

func plannerFilePruningBoolLiteral(value bool) *pbplan.Expr {
	return &pbplan.Expr{
		Typ: pbplan.Type{Id: int32(types.T_bool)},
		Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{
			Value: &pbplan.Literal_Bval{Bval: value},
		}},
	}
}

func plannerFilePruningFunction(name string, id int64, typ types.T, args ...*pbplan.Expr) *pbplan.Expr {
	return &pbplan.Expr{
		Typ: pbplan.Type{Id: int32(typ)},
		Expr: &pbplan.Expr_F{F: &pbplan.Function{
			Func: &pbplan.ObjectRef{Obj: id, ObjName: name},
			Args: args,
		}},
	}
}

func plannerFilePruningEqual(left, right *pbplan.Expr) *pbplan.Expr {
	return plannerFilePruningFunction("=", function.EqualFunctionEncodedID, types.T_bool, left, right)
}

func TestExternalFileLevelColumnUsesSchemaPosition(t *testing.T) {
	node := plannerFilePruningNode("customer.account", "customer.__mo_filepath", "account", catalog.ExternalFilePath)
	virtualPos := int32(len(node.TableDef.Cols) - 1)
	physicalFilepathNode := &pbplan.Node{
		TableDef: &pbplan.TableDef{Cols: []*pbplan.ColDef{{Name: catalog.ExternalFilePath}}},
		ExternScan: &pbplan.ExternScan{TbColToDataCol: map[string]int32{
			catalog.ExternalFilePath: 0,
		}},
	}
	wrongLastColumnNode := &pbplan.Node{
		TableDef:   &pbplan.TableDef{Cols: []*pbplan.ColDef{{Name: "account"}}},
		ExternScan: &pbplan.ExternScan{},
	}

	for _, test := range []struct {
		name string
		node *pbplan.Node
		col  *pbplan.ColRef
		want bool
	}{
		{name: "virtual filepath", node: node, col: &pbplan.ColRef{ColPos: virtualPos, Name: "ext.__mo_filepath"}, want: true},
		{name: "dotted physical account", node: node, col: &pbplan.ColRef{ColPos: 0, Name: "customer.account"}},
		{name: "dotted physical filepath", node: node, col: &pbplan.ColRef{ColPos: 1, Name: "customer.__mo_filepath"}},
		{name: "physical account", node: node, col: &pbplan.ColRef{ColPos: 2, Name: "account"}},
		{name: "physical exact filepath", node: node, col: &pbplan.ColRef{ColPos: 3, Name: catalog.ExternalFilePath}},
		{name: "physical filepath in metadata", node: physicalFilepathNode, col: &pbplan.ColRef{ColPos: 0, Name: catalog.ExternalFilePath}},
		{name: "last column has different name", node: wrongLastColumnNode, col: &pbplan.ColRef{ColPos: 0, Name: catalog.ExternalFilePath}},
		{name: "out of range", node: node, col: &pbplan.ColRef{ColPos: 99, Name: catalog.ExternalFilePath}},
		{name: "missing node", col: &pbplan.ColRef{ColPos: virtualPos, Name: catalog.ExternalFilePath}},
		{name: "missing column", node: node},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, isFileLevelColumn(test.node, test.col))
		})
	}
}

func TestExternalFileLevelFilterClassifiesSafeExpressions(t *testing.T) {
	require.False(t, isSafeFileLevelFunction(nil))
	node := plannerFilePruningNode("account_id")
	filepathColumn := plannerFilePruningColumn(1, catalog.ExternalFilePath)
	physicalColumn := plannerFilePruningColumn(0, "account_id")
	filepathFilter := plannerFilePruningEqual(filepathColumn, plannerFilePruningStringLiteral("/x"))
	physicalFilter := plannerFilePruningEqual(physicalColumn, plannerFilePruningStringLiteral("x"))
	literal := plannerFilePruningBoolLiteral(true)
	list := func(items ...*pbplan.Expr) *pbplan.Expr {
		return &pbplan.Expr{Expr: &pbplan.Expr_List{List: &pbplan.ExprList{List: items}}}
	}

	for _, test := range []struct {
		name           string
		expr           *pbplan.Expr
		hasFileLevel   bool
		hasUnsupported bool
	}{
		{name: "nil expression"},
		{name: "empty expression", expr: &pbplan.Expr{}, hasUnsupported: true},
		{name: "nil column", expr: &pbplan.Expr{Expr: &pbplan.Expr_Col{}}, hasUnsupported: true},
		{name: "nil function", expr: &pbplan.Expr{Expr: &pbplan.Expr_F{}}, hasUnsupported: true},
		{name: "nil list", expr: &pbplan.Expr{Expr: &pbplan.Expr_List{}}},
		{name: "literal", expr: literal},
		{name: "virtual list", expr: list(filepathColumn, literal), hasFileLevel: true},
		{name: "mixed function", expr: plannerFilePruningEqual(filepathColumn, physicalColumn), hasFileLevel: true, hasUnsupported: true},
		{name: "mixed list", expr: list(filepathColumn, physicalColumn), hasFileLevel: true, hasUnsupported: true},
		{name: "unsupported expression", expr: &pbplan.Expr{Expr: &pbplan.Expr_Raw{Raw: &pbplan.RawColRef{}}}, hasUnsupported: true},
	} {
		t.Run("columns/"+test.name, func(t *testing.T) {
			hasFileLevel, hasUnsupported := classifyFileLevelColumns(node, test.expr)
			require.Equal(t, test.hasFileLevel, hasFileLevel)
			require.Equal(t, test.hasUnsupported, hasUnsupported)
		})
	}

	for _, test := range []struct {
		name string
		expr *pbplan.Expr
		want bool
	}{
		{name: "nil expression"},
		{name: "non function", expr: literal},
		{name: "nil function", expr: &pbplan.Expr{Expr: &pbplan.Expr_F{}}},
		{name: "missing metadata", expr: &pbplan.Expr{Expr: &pbplan.Expr_F{F: &pbplan.Function{}}}},
		{name: "unknown function", expr: plannerFilePruningFunction("unknown", -1, types.T_bool, filepathFilter)},
		{name: "empty or", expr: plannerFilePruningFunction("or", int64(function.OR)<<32, types.T_bool)},
		{name: "filepath", expr: filepathFilter, want: true},
		{name: "physical", expr: physicalFilter},
		{name: "or filepath branches", expr: plannerFilePruningFunction("or", int64(function.OR)<<32, types.T_bool, filepathFilter, filepathFilter), want: true},
		{name: "or physical branch", expr: plannerFilePruningFunction("or", int64(function.OR)<<32, types.T_bool, filepathFilter, physicalFilter)},
		{name: "or literal branch", expr: plannerFilePruningFunction("or", int64(function.OR)<<32, types.T_bool, filepathFilter, literal)},
	} {
		t.Run("filter/"+test.name, func(t *testing.T) {
			require.Equal(t, test.want, isFileLevelFilter(node, test.expr))
		})
	}

	randFn, err := function.GetFunctionByName(context.Background(), "rand", nil)
	require.NoError(t, err)
	lessFn, err := function.GetFunctionByName(context.Background(), "<", []types.Type{types.T_float64.ToType(), types.T_float64.ToType()})
	require.NoError(t, err)
	randExpr := plannerFilePruningFunction("rand", randFn.GetEncodedOverloadID(), types.T_float64)
	randFilter := plannerFilePruningFunction("<", lessFn.GetEncodedOverloadID(), types.T_bool, randExpr, &pbplan.Expr{
		Typ:  pbplan.Type{Id: int32(types.T_float64)},
		Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{Value: &pbplan.Literal_Dval{Dval: 0.5}}},
	})
	volatileFilter := plannerFilePruningFunction("and", function.AndFunctionEncodedID, types.T_bool, randFilter, filepathFilter)
	require.False(t, isFileLevelFilter(node, volatileFilter), "volatile predicates must stay at row level")

	currentDateFn, err := function.GetFunctionByName(context.Background(), "current_date", nil)
	require.NoError(t, err)
	realtimeFilter := plannerFilePruningFunction("and", function.AndFunctionEncodedID, types.T_bool,
		plannerFilePruningFunction("current_date", currentDateFn.GetEncodedOverloadID(), types.T_date), filepathFilter)
	require.False(t, isFileLevelFilter(node, realtimeFilter), "time-dependent predicates must stay at row level")

	moLogDateFn, err := function.GetFunctionByName(context.Background(), "mo_log_date", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	require.True(t, isSafeFileLevelFunction(&pbplan.ObjectRef{
		Obj: moLogDateFn.GetEncodedOverloadID(), ObjName: "mo_log_date",
	}), "the deterministic filepath date extractor remains eligible for pruning")
}

func TestExternalFileFilteringKeepsPhysicalColumnsAtRowLevel(t *testing.T) {
	for _, columnName := range []string{"account", "customer.account", "customer.__mo_filepath"} {
		t.Run(columnName, func(t *testing.T) {
			filter := plannerFilePruningEqual(
				plannerFilePruningColumn(0, columnName), plannerFilePruningStringLiteral("row-account"))
			node := plannerFilePruningNode(columnName)
			node.FilterList = []*pbplan.Expr{filter}
			fileList := []string{"etl:/path-account/file.csv"}
			fileSize := []int64{1}

			gotFiles, gotSizes, leftover, err := filterByAccountAndFilename(
				context.Background(), node, testutil.NewProc(t), fileList, fileSize)
			require.NoError(t, err)
			require.Equal(t, fileList, gotFiles)
			require.Equal(t, fileSize, gotSizes)
			require.Equal(t, []*pbplan.Expr{filter}, leftover)
			require.Equal(t, []*pbplan.Expr{filter}, node.FilterList)
		})
	}
}

func TestExternalFileFilteringReusesPreparedFilter(t *testing.T) {
	proc := testutil.NewProc(t)
	node := plannerFilePruningNode()
	parameter := &pbplan.Expr{
		Typ:  pbplan.Type{Id: int32(types.T_varchar)},
		Expr: &pbplan.Expr_P{P: &pbplan.ParamRef{Pos: 0}},
	}
	filter := plannerFilePruningEqual(plannerFilePruningColumn(0, catalog.ExternalFilePath), parameter)
	node.FilterList = []*pbplan.Expr{filter}

	params, err := proc.AllocVectorOfRows(types.T_varchar.ToType(), 1, nil)
	require.NoError(t, err)
	proc.SetPrepareParams(params)
	t.Cleanup(func() {
		proc.SetPrepareParams(nil)
		params.Free(proc.Mp())
	})

	fileList := []string{"etl:/a.csv", "etl:/b.csv"}
	fileSize := []int64{10, 20}
	require.NoError(t, vector.SetStringAt(params, 0, fileList[0], proc.Mp()))
	gotFiles, gotSizes, leftover, err := filterByAccountAndFilename(
		context.Background(), node, proc, fileList, fileSize)
	require.NoError(t, err)
	require.Equal(t, fileList[:1], gotFiles)
	require.Equal(t, fileSize[:1], gotSizes)
	require.Empty(t, leftover)
	require.Equal(t, []*pbplan.Expr{filter}, node.FilterList)

	require.NoError(t, vector.SetStringAt(params, 0, fileList[1], proc.Mp()))
	gotFiles, gotSizes, leftover, err = filterByAccountAndFilename(
		context.Background(), node, proc, fileList, fileSize)
	require.NoError(t, err)
	require.Equal(t, fileList[1:], gotFiles)
	require.Equal(t, fileSize[1:], gotSizes)
	require.Empty(t, leftover)
	require.Equal(t, []*pbplan.Expr{filter}, node.FilterList)
}

func TestExternalFileFilteringBroadcastsConstantResults(t *testing.T) {
	ifFn, err := function.GetFunctionByName(context.Background(), "if", []types.Type{
		types.T_bool.ToType(), types.T_bool.ToType(), types.T_bool.ToType(),
	})
	require.NoError(t, err)

	fileList := []string{"etl:/a.csv", "etl:/b.csv"}
	fileSize := []int64{10, 20}
	for _, test := range []struct {
		name      string
		thenValue bool
		wantFiles []string
		wantSizes []int64
	}{
		{name: "constant true", thenValue: true, wantFiles: fileList, wantSizes: fileSize},
		{name: "constant false", thenValue: false, wantFiles: []string{}, wantSizes: []int64{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			node := plannerFilePruningNode()
			filepathFilter := plannerFilePruningEqual(
				plannerFilePruningStringLiteral("never-selected"),
				plannerFilePruningColumn(0, catalog.ExternalFilePath),
			)
			filter := plannerFilePruningFunction("if", ifFn.GetEncodedOverloadID(), types.T_bool,
				plannerFilePruningBoolLiteral(true), plannerFilePruningBoolLiteral(test.thenValue), filepathFilter)
			node.FilterList = []*pbplan.Expr{filter}

			gotFiles, gotSizes, leftover, err := filterByAccountAndFilename(
				context.Background(), node, testutil.NewProc(t), fileList, fileSize)
			require.NoError(t, err)
			require.Equal(t, test.wantFiles, gotFiles)
			require.Equal(t, test.wantSizes, gotSizes)
			require.Empty(t, leftover)
			require.Equal(t, []*pbplan.Expr{filter}, node.FilterList)
		})
	}
}
