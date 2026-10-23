// Copyright 2026 MatrixOne
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	util "github.com/matrixorigin/matrixone/pkg/sql/util"
)

// reportedClusterTableDef mirrors what DDL produces for the table in issue
// #26996: six visible columns, no primary key, and the hidden compound
// cluster-by column.
func reportedClusterTableDef() *plan.TableDef {
	compoundName := util.BuildCompositeClusterByColumnName(
		[]string{"tenant_id", "device_id", "metric_id", "event_ts"})
	mkCol := func(name string, oid types.T, seqnum uint32, hidden bool) *plan.ColDef {
		typ := makeSimplePlan2Type(oid)
		if oid == types.T_datetime {
			typ = makePlan2Type(&types.Type{Oid: oid, Width: 64, Scale: 6})
		}
		if oid == types.T_varchar {
			typ.Width = types.MaxVarcharLen
		}
		return &plan.ColDef{
			Name:      name,
			Seqnum:    seqnum,
			Hidden:    hidden,
			ClusterBy: name == compoundName,
			Typ:       typ,
		}
	}
	cols := []*plan.ColDef{
		mkCol("tenant_id", types.T_int32, 0, false),
		mkCol("device_id", types.T_int32, 1, false),
		mkCol("metric_id", types.T_int32, 2, false),
		mkCol("event_ts", types.T_datetime, 3, false),
		mkCol("value", types.T_float64, 4, false),
		mkCol("quality", types.T_int16, 5, false),
		mkCol(catalog.FakePrimaryKeyColName, types.T_uint64, 6, true),
		mkCol(compoundName, types.T_varchar, 7, true),
	}
	compoundCol := cols[len(cols)-1]
	compoundCol.Typ.Width = 65535
	name2Idx := make(map[string]int32, len(cols))
	for i, col := range cols {
		name2Idx[col.Name] = int32(i)
	}
	return &plan.TableDef{
		Name:          "metrics",
		Cols:          cols,
		Name2ColIndex: name2Idx,
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: catalog.FakePrimaryKeyColName,
			Names:       []string{catalog.FakePrimaryKeyColName},
		},
		ClusterBy: &plan.ClusterByDef{
			Name:         compoundName,
			CompCbkeyCol: compoundCol,
		},
	}
}

// serializedConstPayload extracts the payload of the folded serialized
// constant the planner publishes for a compound-key bound.
func serializedConstPayload(expr *plan.Expr) (string, bool) {
	if lit := expr.GetLit(); lit != nil && lit.IsSerialized {
		return lit.GetSval(), true
	}
	return "", false
}

// unqualified mirrors readutil's runtime matcher compPkCol: a bound scan
// column reference carries its alias qualifier, and consumers strip it.
func unqualified(name string) string {
	if idx := strings.LastIndexByte(name, '.'); idx >= 0 {
		return name[idx+1:]
	}
	return name
}

// TestReportedClusterBySQLEmitsPrefixInRangePredicate proves the public path
// from the exact SQL of issue #26996 to the pruning predicate: parsing and
// building the plan must merge the reported conjunctive predicate into a
// single closed prefix_in_range over the hidden compound cluster column,
// with the reported datetime lower bound and the three-part prefix upper
// bound as serialized serial constants. The read-side tests consume exactly
// this predicate shape, and the runtime resolves the column by the same
// unqualified name comparison.
func TestReportedClusterBySQLEmitsPrefixInRangePredicate(t *testing.T) {
	proc := newPlanTestProcess(t)
	ctx := NewMockCompilerContext(true, proc)
	tableDef := reportedClusterTableDef()
	ctx.tables[tableDef.Name] = tableDef
	ctx.objects[tableDef.Name] = &plan.ObjectRef{
		SchemaName: "tpch", ObjName: tableDef.Name,
	}

	const sql = `select count(*), sum(value) from metrics ` +
		`where tenant_id = 0 and device_id = 0 and metric_id = 0 ` +
		`and event_ts >= '2026-01-01 00:03:00'`
	stmt, err := mysql.ParseOne(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	query := p.GetQuery()
	require.NotNil(t, query)

	var scan *plan.Node
	for i := range query.Nodes {
		if query.Nodes[i].NodeType == plan.Node_TABLE_SCAN &&
			query.Nodes[i].TableDef != nil &&
			query.Nodes[i].TableDef.Name == "metrics" {
			scan = query.Nodes[i]
			break
		}
	}
	require.NotNil(t, scan, "the plan must contain a table scan of metrics")

	compoundName := tableDef.ClusterBy.Name
	require.Len(t, scan.FilterList, 1,
		"all four reported conjuncts must merge into one compound predicate")
	merged := scan.FilterList[0]
	fn := merged.GetF()
	require.NotNil(t, fn)
	require.Equal(t, "prefix_in_range", fn.Func.ObjName,
		"a >= terminated full-prefix conjunct must merge into prefix_in_range")

	args := fn.Args
	require.Len(t, args, 4)
	// The runtime matches the predicate column by stripping the binding
	// alias and comparing against the compound cluster column name, so the
	// merged predicate must reference that column by a resolvable name.
	colRef := args[0].GetCol()
	require.NotNil(t, colRef)
	require.Equal(t, compoundName, unqualified(colRef.Name),
		"the interval must range over the hidden compound cluster column")

	// The lower bound is serial(0,0,0,'2026-01-01 00:03:00').
	lb, ok := serializedConstPayload(args[1])
	require.True(t, ok, "the lower bound must be a folded serialized constant")
	bound, err := types.ParseDatetime("2026-01-01 00:03:00", 6)
	require.NoError(t, err)
	packer := types.NewPacker()
	packer.EncodeInt32(0)
	packer.EncodeInt32(0)
	packer.EncodeInt32(0)
	packer.EncodeDatetime(bound)
	require.Equal(t, string(packer.GetBuf()), lb)
	packer.Close()

	// The upper bound is the three-part prefix serial(0,0,0): the
	// unconstrained trailing part excludes itself from the interval.
	ub, ok := serializedConstPayload(args[2])
	require.True(t, ok, "the upper bound must be a folded serialized constant")
	packer = types.NewPacker()
	packer.EncodeInt32(0)
	packer.EncodeInt32(0)
	packer.EncodeInt32(0)
	require.Equal(t, string(packer.GetBuf()), ub)
	packer.Close()

	// >= keeps the lower bound closed: flag bit 0 stays unset.
	flag := args[3].GetLit()
	require.NotNil(t, flag)
	require.Equal(t, uint32(0), flag.GetU8Val(),
		"the >= predicate must keep the lower bound closed")

	// The same compound interval also reaches the object/block pruning
	// path, proving the merged predicate survives to both consumers.
	blockFilter := false
	for _, expr := range scan.BlockFilterList {
		bfn := expr.GetF()
		if bfn == nil || bfn.Func.ObjName != "prefix_in_range" {
			continue
		}
		if col := bfn.Args[0].GetCol(); col != nil &&
			unqualified(col.Name) == compoundName {
			blockFilter = true
			break
		}
	}
	require.True(t, blockFilter,
		"the compound interval must also be exposed to block filtering")
}
