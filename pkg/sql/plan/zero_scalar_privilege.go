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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// These nodes carry authorization metadata only; they are never query steps.
const zeroScalarPrivilegeCarrier = "mo:zero-scalar-privilege-carrier"

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
		isScan := node.NodeType == plan.Node_TABLE_SCAN && node.ObjRef != nil
		if !isScan && len(node.OriginViews) == 0 && node.DirectView == "" {
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
			NodeType: plan.Node_VALUE_SCAN, ExtraOptions: zeroScalarPrivilegeCarrier,
			OriginViews: append([]string(nil), node.OriginViews...), DirectView: node.DirectView,
			ScanSnapshot: DeepCopySnapshot(node.ScanSnapshot),
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
