// Copyright 2022 Matrix Origin
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
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (builder *QueryBuilder) buildResultScan(tbl *tree.TableFunction, ctx *BindContext) (int32, error) {
	var err error
	val, err := builder.compCtx.ResolveVariable("save_query_result", true, false)
	if err == nil {
		if v, _ := val.(int8); v == 0 {
			return 0, moerr.NewNoConfig(builder.GetContext(), "save query result")
		} else {
			logutil.Infof("buildResultScan : save query result: %v", v)
		}
	} else {
		return 0, err
	}
	ctx.binder = NewTableBinder(builder, ctx)
	exprs := make([]*plan.Expr, 0, len(tbl.Func.Exprs))
	for _, v := range tbl.Func.Exprs {
		curExpr, err := ctx.binder.BindExpr(v, 0, false)
		if err != nil {
			return 0, err
		}
		exprs = append(exprs, curExpr)
	}
	if exprs[0].GetP() != nil {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "invalid argument of result_scan")
	}
	exprs[0], err = appendCastBeforeExpr(builder.GetContext(), exprs[0], plan.Type{
		Id:          int32(types.T_uuid),
		NotNullable: true,
	})
	if err != nil {
		return 0, err
	}

	// calculate uuid
	vec, free, err := colexec.GetReadonlyResultFromNoColumnExpression(builder.compCtx.GetProcess(), exprs[0])
	if err != nil {
		return 0, err
	}
	uuid := vector.MustFixedColWithTypeCheck[types.Uuid](vec)[0]
	free()

	// get cols
	cols, path, err := builder.compCtx.GetQueryResultMeta(uuid.String())
	if err != nil {
		return 0, err
	}
	logutil.Infof("buildResultScan : get save query result path is %s, uuid is %s", path, uuid.String())
	if len(path) == 0 {
		return 0, moerr.NewInvalidInputf(builder.GetContext(), "empty %s", "query result")
	}
	typs := make([]types.Type, len(cols))
	for i, c := range cols {
		typs[i] = types.New(types.T(c.Typ.Id), c.Typ.Width, c.Typ.Scale)
	}
	builder.compCtx.GetProcess().GetSessionInfo().ResultColTypes = typs
	name2ColIndex := map[string]int32{}
	for i := 0; i < len(cols); i++ {
		name2ColIndex[cols[i].Name] = int32(i)
	}
	tableDef := &plan.TableDef{
		Name:          uuid.String(),
		TableType:     "query_result",
		Cols:          cols,
		Name2ColIndex: name2ColIndex,
	}
	// build external param
	p := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			// ScanType: tree.S3,
			Filepath: path,
			// FileService: builder.compCtx.GetProcess().FileService,
			// S3Param:     &tree.S3Parameter{},
			Tail: &tree.TailParameter{},
		},
	}
	b, err := json.Marshal(p)
	if err != nil {
		return 0, err
	}
	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemExternalRel,
		},
		{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: string(b),
		},
	}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		}})
	tableDef.Createsql = string(b)
	node := &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		ExternScan: &plan.ExternScan{
			Type: int32(plan.ExternType_RESULT_SCAN),
		},
		Stats:        &plan.Stats{},
		TableDef:     tableDef,
		BindingTags:  []int32{builder.genNewTag()},
		NotCacheable: true,
	}
	nodeID := builder.appendNode(node, ctx)
	return nodeID, nil
}
