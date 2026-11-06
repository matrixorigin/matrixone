// Copyright 2026 Matrix Origin
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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// singletonRuntimeFilterCandidate proves a small build without changing the
// unavailable-statistics sentinel. It is only used when ordinary costing
// rejects an INNER-join filter; all codec and delivery checks still apply.
func (builder *QueryBuilder) singletonRuntimeFilterCandidate(join, probe *plan.Node, buildID int32) bool {
	return len(join.Children) == 2 && singletonOrderedPKProbe(join, probe, false) &&
		builder.singletonPointInput(buildID, join.Children[0])
}

// singletonOrderedPKProbe restricts a new dependency to a discardable scan
// whose full ordered primary key is the only join condition.
func singletonOrderedPKProbe(join, probe *plan.Node, allowOwnFilter bool) bool {
	if join.JoinType != plan.Node_INNER || join.IsRightJoin || len(join.OnList) != 1 ||
		probe == nil || probe.NodeType != plan.Node_TABLE_SCAN || probe.TableDef == nil ||
		probe.TableDef.Pkey == nil || probe.Limit != nil || probe.Offset != nil ||
		len(probe.FilterList) != 0 || len(probe.BlockFilterList) != 0 ||
		len(probe.BindingTags) != 1 {
		return false
	}
	if len(probe.RuntimeFilterProbeList) != 0 {
		if !allowOwnFilter || len(probe.RuntimeFilterProbeList) != 1 || len(join.RuntimeFilterBuildList) != 1 ||
			probe.RuntimeFilterProbeList[0] == nil || join.RuntimeFilterBuildList[0] == nil ||
			probe.RuntimeFilterProbeList[0].Tag != join.RuntimeFilterBuildList[0].Tag {
			return false
		}
	} else if allowOwnFilter && len(join.RuntimeFilterBuildList) != 0 {
		return false
	}
	table := probe.TableDef
	pkPos, ok := table.Name2ColIndex[table.Pkey.PkeyColName]
	if !ok || pkPos < 0 || int(pkPos) >= len(table.Cols) || table.Cols[pkPos] == nil ||
		table.Cols[pkPos].Name != table.Pkey.PkeyColName || GetSortOrder(table, pkPos) != 0 {
		return false
	}
	for _, expr := range probe.ProjectList {
		if expr == nil || expr.GetCol() == nil {
			return false
		}
		col := expr.GetCol()
		if col.RelPos != probe.BindingTags[0] || col.ColPos < 0 || int(col.ColPos) >= len(table.Cols) || table.Cols[col.ColPos] == nil {
			return false
		}
	}
	if join.OnList[0] == nil {
		return false
	}
	fn := join.OnList[0].GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil {
		return false
	}
	functionID, _ := function.DecodeOverloadID(fn.Func.Obj)
	if functionID != function.EQUAL {
		return false
	}
	probeIsPK := false
	for _, arg := range fn.Args {
		if col := arg.GetCol(); col != nil && col.RelPos == probe.BindingTags[0] && col.ColPos == pkPos && arg.Typ.Id == table.Cols[pkPos].Typ.Id {
			probeIsPK = true
		}
	}
	return probeIsPK
}

// singletonPointInput proves a global bound, rejecting shared nodes so an
// outer probe cannot also be needed to produce its own runtime filter.
func (builder *QueryBuilder) singletonPointInput(nodeID, forbiddenID int32) bool {
	if builder.qry == nil {
		return false
	}
	seen := make(map[int32]bool)
	if forbiddenID >= 0 {
		seen[forbiddenID] = true
	}
	return builder.singletonPointSubtree(nodeID, seen, 0) != nil
}

func (builder *QueryBuilder) singletonPointSubtree(nodeID int32, seen map[int32]bool, depth int) *plan.Node {
	if depth >= 64 || nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) ||
		builder.qry.Nodes[nodeID] == nil || seen[nodeID] {
		return nil
	}
	seen[nodeID] = true
	node := builder.qry.Nodes[nodeID]
	if len(node.RuntimeFilterProbeList) != 0 {
		return nil
	}
	switch node.NodeType {
	case plan.Node_PROJECT, plan.Node_FILTER, plan.Node_SORT:
		if len(node.Children) == 1 {
			return builder.singletonPointSubtree(node.Children[0], seen, depth+1)
		}
	case plan.Node_TABLE_SCAN:
		if singletonIntegerPKScan(node) {
			return node
		}
	case plan.Node_JOIN:
		if len(node.Children) != 2 || node.Stats == nil || node.Stats.HashmapStats == nil || node.Stats.HashmapStats.Shuffle {
			return nil
		}
		probeID, buildID := node.Children[0], node.Children[1]
		if probeID < 0 || int(probeID) >= len(builder.qry.Nodes) || seen[probeID] {
			return nil
		}
		probe := builder.qry.Nodes[probeID]
		if !singletonOrderedPKProbe(node, probe, true) {
			return nil
		}
		seen[probeID] = true
		source := builder.singletonPointSubtree(buildID, seen, depth+1)
		if source != nil && singletonUniqueScan(probe, source) && builder.singletonJoinKey(node, probe, buildID) &&
			builder.singletonOwnedFilter(node, probe) {
			return source
		}
	}
	return nil
}

func (builder *QueryBuilder) singletonOwnedFilter(join, probe *plan.Node) bool {
	if len(probe.RuntimeFilterProbeList) == 0 {
		return true
	}
	probeSpec, buildSpec := probe.RuntimeFilterProbeList[0], join.RuntimeFilterBuildList[0]
	col := probeSpec.Expr.GetCol()
	return col != nil && col.RelPos == probe.BindingTags[0] &&
		col.ColPos == probe.TableDef.Name2ColIndex[probe.TableDef.Pkey.PkeyColName] &&
		builder.exactRuntimeFilterPairContractValid(probeSpec, buildSpec)
}

// Regular secondary-index entries are unique because their physical key
// includes the full primary key of the already-proven singleton source.
func singletonUniqueScan(scan, source *plan.Node) bool {
	table := scan.TableDef
	if scan.ObjRef == nil || len(scan.Children) != 0 || table.IsTemporary || table.IsDynamic ||
		table.Partition != nil || table.FeatureFlag&(features.Partitioned|features.Partition) != 0 || table.ClusterBy != nil ||
		(table.TableType != catalog.SystemOrdinaryRel &&
			(table.TableType != catalog.SystemIndexRel ||
				table.Pkey.PkeyColName != catalog.IndexTableIndexColName ||
				(!catalog.IsSecondaryIndexTable(table.Name) && !catalog.IsUniqueIndexTable(table.Name)))) ||
		len(table.Pkey.Names) != 1 || table.Pkey.Names[0] != table.Pkey.PkeyColName || catalog.IsFakePkName(table.Pkey.PkeyColName) {
		return false
	}
	pkPos := table.Name2ColIndex[table.Pkey.PkeyColName] // validated by singletonOrderedPKProbe
	typ := types.T(table.Cols[pkPos].Typ.Id)
	if !typ.IsInteger() && !typ.IsMySQLString() {
		return false
	}
	if table.TableType == catalog.SystemOrdinaryRel {
		return true
	}
	probeRef, sourceRef := scan.ObjRef, source.ObjRef
	if source.ScanSnapshot != nil || scan.ScanSnapshot != nil ||
		sourceRef.SchemaName == "" || probeRef.SchemaName != sourceRef.SchemaName ||
		probeRef.Db != sourceRef.Db || probeRef.Schema != sourceRef.Schema ||
		probeRef.DbName != sourceRef.DbName || probeRef.ServerName != sourceRef.ServerName ||
		probeRef.ObjName != table.Name || sourceRef.ObjName != source.TableDef.Name ||
		(probeRef.Obj > 0 && table.TblId != 0 && uint64(probeRef.Obj) != table.TblId) ||
		(sourceRef.Obj > 0 && source.TableDef.TblId != 0 && uint64(sourceRef.Obj) != source.TableDef.TblId) ||
		probeRef.SubscriptionName != "" || sourceRef.SubscriptionName != "" ||
		probeRef.PubInfo != nil || sourceRef.PubInfo != nil || probeRef.Snapshot != nil || sourceRef.Snapshot != nil {
		return false
	}
	matched, unique := false, false
	for _, index := range source.TableDef.Indexes {
		if index == nil || index.IndexTableName != table.Name {
			continue
		}
		if matched || !index.TableExist || !catalog.IsRegularIndexAlgo(index.IndexAlgo) || catalog.IsRTreeIndexAlgo(index.IndexAlgo) {
			return false
		}
		matched = true
		if index.Unique {
			unique = true
			continue
		}
		prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(index.IndexAlgoParams)
		if err != nil || prefixLengths[source.TableDef.Pkey.PkeyColName] > 0 {
			return false
		}
		for _, part := range index.Parts {
			if catalog.ResolveAlias(part) == source.TableDef.Pkey.PkeyColName {
				unique = true
				break
			}
		}
	}
	return matched && unique
}

// The nested key must have the same value on every broadcast producer. A
// volatile or context-dependent expression could otherwise match one distinct
// unique probe row per CN, violating the global bound.
func (builder *QueryBuilder) singletonJoinKey(join, probe *plan.Node, buildID int32) bool {
	buildTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(buildID) {
		buildTags[tag] = true
	}
	probeTag := probe.BindingTags[0]
	if buildTags[probeTag] {
		return false
	}
	pkPos := probe.TableDef.Name2ColIndex[probe.TableDef.Pkey.PkeyColName]
	args := join.OnList[0].GetF().Args
	for i := range args {
		col := args[i].GetCol()
		if col != nil && col.RelPos == probeTag && col.ColPos == pkPos &&
			keycodec.ExactRuntimeFilterEncodingForPair(makeTypeByPlan2Expr(args[i]), makeTypeByPlan2Expr(args[1-i])) == keycodec.ExactRuntimeFilterRaw &&
			singletonStableJoinKey(args[1-i], buildTags) {
			return true
		}
	}
	return false
}

func singletonStableJoinKey(expr *plan.Expr, buildTags map[int32]bool) bool {
	if col := expr.GetCol(); col != nil {
		return col.ColPos >= 0 && buildTags[col.RelPos]
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) == 0 ||
		!((fn.Func.ObjName == function.SerialFunctionName && fn.Func.Obj == function.SerialFunctionEncodeID) ||
			(fn.Func.ObjName == function.SerialFullFunctionName && fn.Func.Obj == function.SerialFullFunctionEncodeID)) {
		return false
	}
	for _, arg := range fn.Args {
		if arg == nil || arg.GetCol() == nil || arg.GetCol().ColPos < 0 || !buildTags[arg.GetCol().RelPos] ||
			!function.SerialTypeSupported(types.T(arg.Typ.Id)) {
			return false
		}
	}
	return true
}

func singletonIntegerPKScan(scan *plan.Node) bool {
	table := scan.TableDef
	if scan.ObjRef == nil || len(scan.Children) != 0 || table == nil || table.TableType != catalog.SystemOrdinaryRel || table.IsTemporary || table.IsDynamic ||
		table.Partition != nil || table.FeatureFlag&(features.Partitioned|features.Partition) != 0 || table.ClusterBy != nil ||
		table.Pkey == nil || len(table.Pkey.Names) == 0 || len(scan.BindingTags) != 1 ||
		catalog.IsFakePkName(table.Pkey.PkeyColName) {
		return false
	}
	pk := table.Pkey
	if len(pk.Names) == 1 {
		if pk.PkeyColName != pk.Names[0] {
			return false
		}
	} else if pk.PkeyColName != catalog.CPrimaryKeyColName {
		return false
	} else if pos, ok := table.Name2ColIndex[pk.PkeyColName]; !ok || pos < 0 || int(pos) >= len(table.Cols) ||
		table.Cols[pos] == nil || table.Cols[pos].Name != pk.PkeyColName || !table.Cols[pos].Hidden {
		return false
	}
	for i, name := range pk.Names {
		pos, ok := table.Name2ColIndex[name]
		if !ok || pos < 0 || int(pos) >= len(table.Cols) || table.Cols[pos] == nil || (len(pk.Names) > 1 && name == pk.PkeyColName) ||
			table.Cols[pos].Name != name || !types.T(table.Cols[pos].Typ.Id).IsInteger() {
			return false
		}
		for _, previous := range pk.Names[:i] {
			if previous == name || table.Name2ColIndex[previous] == pos {
				return false
			}
		}
		found := false
		for _, expr := range scan.FilterList {
			if pointIntegerEquality(expr, scan.BindingTags[0], pos, table.Cols[pos].Typ.Id) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// pointIntegerEquality reuses the index classifier on a local argument slice:
// its normalization of commuted equality must not rewrite the original plan.
func pointIntegerEquality(expr *plan.Expr, tag, pos, typeID int32) bool {
	if expr == nil || expr.GetF() == nil {
		return false
	}
	fn := expr.GetF()
	if fn.Func == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil {
		return false
	}
	functionID, _ := function.DecodeOverloadID(fn.Func.Obj)
	if functionID != function.EQUAL {
		return false
	}
	args := [2]*plan.Expr{fn.Args[0], fn.Args[1]}
	// Only a literal or prepared parameter, optionally converted by a supported
	// context-independent scalar CAST, is known to be invariant across workers.
	if !(args[0].GetCol() != nil && pointScalar(args[1])) &&
		!(args[1].GetCol() != nil && pointScalar(args[0])) {
		return false
	}
	local := *fn
	local.Args = args[:]
	kind, col := checkIndexFilter(&local)
	return kind == EqualIndexCondition && col != nil && col.RelPos == tag && col.ColPos == pos &&
		local.Args[0].Typ.Id == typeID && local.Args[1].Typ.Id == typeID && pointScalar(local.Args[1])
}

func pointScalar(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetLit() != nil || expr.GetP() != nil {
		return true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || len(fn.Args) != 2 ||
		fn.Args[0] == nil || fn.Args[1] == nil || fn.Args[1].GetT() == nil ||
		(fn.Args[0].GetLit() == nil && fn.Args[0].GetP() == nil) ||
		fn.Args[1].Typ.Id != expr.Typ.Id || !types.T(expr.Typ.Id).IsInteger() {
		return false
	}
	functionID, overload := function.DecodeOverloadID(fn.Func.Obj)
	sourceType := types.T(fn.Args[0].Typ.Id)
	// CAST has the ordinary and explicit scalar overloads. Restricting the
	// index also keeps the existing index classifier safe on malformed plans.
	// For example, timestamp-to-integer casts depend on the session timezone,
	// whose interpretation need not be identical on all scan workers.
	return functionID == function.CAST && (overload == 0 || overload == 1) &&
		(sourceType.IsInteger() || sourceType.IsMySQLString()) &&
		function.IfTypeCastSupported(sourceType, types.T(expr.Typ.Id))
}
