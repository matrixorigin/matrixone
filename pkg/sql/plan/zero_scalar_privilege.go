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
	"slices"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// These nodes carry authorization metadata only; they are never query steps.
const zeroScalarPrivilegeCarrier = "mo:zero-scalar-privilege-carrier"

// IsZeroScalarPrivilegeCarrier identifies non-executable authorization metadata.
func IsZeroScalarPrivilegeCarrier(node *plan.Node) bool {
	return node != nil && node.ExtraOptions == zeroScalarPrivilegeCarrier &&
		(node.NodeType == plan.Node_TABLE_SCAN || node.NodeType == plan.Node_VALUE_SCAN)
}

func (builder *QueryBuilder) retainZeroScalarAuthorizationSources(root int32) error {
	pending := []int32{root}
	seen := make(map[int32]struct{})
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if id < 0 || int(id) >= len(builder.qry.Nodes) || builder.qry.Nodes[id] == nil {
			return moerr.NewInternalError(builder.GetContext(), "invalid zero-limit authorization source")
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		node := builder.qry.Nodes[id]
		pending = append(pending, node.Children...)
		for _, step := range node.SourceStep {
			if step < 0 || int(step) >= len(builder.qry.Steps) {
				return moerr.NewInternalError(builder.GetContext(), "invalid zero-limit authorization source step")
			}
			pending = append(pending, builder.qry.Steps[step])
		}
		if node.NodeType == plan.Node_PROJECT {
			for _, expr := range node.ProjectList {
				if expr != nil && HasMoCtrl(expr) {
					return moerr.NewNYI(builder.GetContext(), "control function in correlated scalar LIMIT 0 wrapper")
				}
			}
		}
		if node.NodeType == plan.Node_EXTERNAL_SCAN || node.NodeType == plan.Node_FUNCTION_SCAN {
			// These scans may need separate frontend authorization; the original
			// node must remain reachable for that consumer to validate it.
			if builder.zeroScalarRequiredLiveSources == nil {
				builder.zeroScalarRequiredLiveSources = make(map[int32]struct{})
			}
			builder.zeroScalarRequiredLiveSources[id] = struct{}{}
			continue
		}
		isScan := node.NodeType == plan.Node_TABLE_SCAN && node.ObjRef != nil
		if !isScan && len(node.OriginViews) == 0 && node.DirectView == "" && !node.NotCacheable {
			continue
		}
		if _, ok := builder.zeroScalarPrivilegeSources[id]; ok {
			continue
		}
		if builder.zeroScalarPrivilegeSources == nil {
			builder.zeroScalarPrivilegeSources = make(map[int32]struct{})
		}
		builder.zeroScalarPrivilegeSources[id] = struct{}{}
		carrier := &plan.Node{
			// Until publication NodeId identifies the source occurrence. The
			// final append assigns the carrier's own plan node ID.
			NodeId: id, NodeType: plan.Node_VALUE_SCAN, ExtraOptions: zeroScalarPrivilegeCarrier,
			OriginViews: append([]string(nil), node.OriginViews...), DirectView: node.DirectView,
			ScanSnapshot: DeepCopySnapshot(node.ScanSnapshot), NotCacheable: node.NotCacheable,
		}
		if isScan {
			carrier.NodeType = plan.Node_TABLE_SCAN
			carrier.ObjRef = DeepCopyObjectRef(node.ObjRef)
			carrier.ParentObjRef = DeepCopyObjectRef(node.ParentObjRef)
			if node.TableDef != nil {
				carrier.TableDef = &plan.TableDef{TableType: node.TableDef.TableType,
					Name: node.TableDef.Name, TblId: node.TableDef.TblId, Version: node.TableDef.Version,
					DbName: node.TableDef.DbName, DbId: node.TableDef.DbId}
			}
		}
		builder.zeroScalarPrivilegeScans = append(builder.zeroScalarPrivilegeScans, carrier)
	}
	return nil
}

// A reachable original can replace its carrier only while it still supplies
// the same authorization, snapshot and cache metadata.
func zeroScalarSourceCoversCarrier(source, carrier *plan.Node) bool {
	if source == nil || carrier == nil ||
		!proto.Equal(source.ObjRef, carrier.ObjRef) ||
		!proto.Equal(source.ParentObjRef, carrier.ParentObjRef) ||
		!proto.Equal(source.ScanSnapshot, carrier.ScanSnapshot) ||
		!slices.Equal(source.OriginViews, carrier.OriginViews) || source.DirectView != carrier.DirectView ||
		(carrier.NotCacheable && !source.NotCacheable) {
		return false
	}
	if carrier.NodeType != plan.Node_TABLE_SCAN {
		return true
	}
	if source.NodeType != plan.Node_TABLE_SCAN || source.TableDef == nil || carrier.TableDef == nil {
		return false
	}
	a, b := source.TableDef, carrier.TableDef
	return a.TableType == b.TableType && a.Name == b.Name && a.TblId == b.TblId &&
		a.Version == b.Version && a.DbName == b.DbName && a.DbId == b.DbId
}

// finalizeZeroScalarAuthorizationSources verifies that execution keeps the
// required live sources and publishes metadata for sources pruned from the plan.
func (builder *QueryBuilder) finalizeZeroScalarAuthorizationSources() error {
	var seen map[int32]struct{}
	if len(builder.zeroScalarScanIDs) != 0 || len(builder.zeroScalarRequiredLiveSources) != 0 || len(builder.zeroScalarPrivilegeScans) != 0 {
		seen = make(map[int32]struct{})
		pending := append([]int32(nil), builder.qry.Steps...)
		for len(pending) > 0 {
			id := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			if id < 0 || int(id) >= len(builder.qry.Nodes) || builder.qry.Nodes[id] == nil {
				return moerr.NewInternalError(builder.GetContext(), "invalid query node while preserving correlated scalar source")
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			pending = append(pending, builder.qry.Nodes[id].Children...)
		}
		for _, id := range builder.zeroScalarScanIDs {
			if _, ok := seen[id]; !ok {
				if _, retainedForAuthorization := builder.zeroScalarPrivilegeSources[id]; !retainedForAuthorization {
					return moerr.NewNYI(builder.GetContext(), "correlated scalar LIMIT 0 source discarded during planning")
				}
			}
			node := builder.qry.Nodes[id]
			if node.NodeType != plan.Node_TABLE_SCAN || node.TableDef == nil || node.ObjRef == nil {
				return moerr.NewInternalError(builder.GetContext(), "correlated scalar LIMIT 0 source is not a table scan")
			}
		}
		for id := range builder.zeroScalarRequiredLiveSources {
			if _, ok := seen[id]; !ok {
				return moerr.NewNYI(builder.GetContext(), "special source in correlated scalar LIMIT 0 wrapper")
			}
		}
	}
	for _, node := range builder.zeroScalarPrivilegeScans {
		if _, ok := seen[node.NodeId]; ok && zeroScalarSourceCoversCarrier(builder.qry.Nodes[node.NodeId], node) {
			continue
		}
		node.NodeId = int32(len(builder.qry.Nodes))
		builder.qry.Nodes = append(builder.qry.Nodes, node)
		builder.ctxByNode = append(builder.ctxByNode, nil)
	}
	return nil
}
