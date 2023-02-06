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
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_col/group_concat"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/single"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/update"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var constBat *batch.Batch

func init() {
	constBat = batch.NewWithSize(0)
	constBat.Zs = []int64{1}
}

func dupInstruction(sourceIns *vm.Instruction, regMap map[*process.WaitRegister]*process.WaitRegister) vm.Instruction {
	res := vm.Instruction{Op: sourceIns.Op, Idx: sourceIns.Idx, IsFirst: sourceIns.IsFirst, IsLast: sourceIns.IsLast}
	switch sourceIns.Op {
	case vm.Anti:
		t := sourceIns.Arg.(*anti.Argument)
		res.Arg = &anti.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Cond:       t.Cond,
			Typs:       t.Typs,
			Conditions: t.Conditions,
			Result:     t.Result,
		}
	case vm.Group:
		t := sourceIns.Arg.(*group.Argument)
		res.Arg = &group.Argument{
			NeedEval: t.NeedEval,
			Ibucket:  t.Ibucket,
			Nbucket:  t.Nbucket,
			Exprs:    t.Exprs,
			Types:    t.Types,
			Aggs:     t.Aggs,
		}
	case vm.Join:
		t := sourceIns.Arg.(*join.Argument)
		res.Arg = &join.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Result:     t.Result,
			Cond:       t.Cond,
			Typs:       t.Typs,
			Conditions: t.Conditions,
		}
	case vm.Left:
		t := sourceIns.Arg.(*left.Argument)
		res.Arg = &left.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Cond:       t.Cond,
			Result:     t.Result,
			Typs:       t.Typs,
			Conditions: t.Conditions,
		}
	case vm.Limit:
		t := sourceIns.Arg.(*limit.Argument)
		res.Arg = &limit.Argument{
			Limit: t.Limit,
		}
	case vm.LoopAnti:
		t := sourceIns.Arg.(*loopanti.Argument)
		res.Arg = &loopanti.Argument{
			Result: t.Result,
			Cond:   t.Cond,
			Typs:   t.Typs,
		}
	case vm.LoopJoin:
		t := sourceIns.Arg.(*loopanti.Argument)
		res.Arg = &loopanti.Argument{
			Result: t.Result,
			Cond:   t.Cond,
			Typs:   t.Typs,
		}
	case vm.LoopLeft:
		t := sourceIns.Arg.(*loopleft.Argument)
		res.Arg = &loopleft.Argument{
			Cond:   t.Cond,
			Typs:   t.Typs,
			Result: t.Result,
		}
	case vm.LoopSemi:
		t := sourceIns.Arg.(*loopsemi.Argument)
		res.Arg = &loopsemi.Argument{
			Result: t.Result,
			Cond:   t.Cond,
			Typs:   t.Typs,
		}
	case vm.LoopSingle:
		t := sourceIns.Arg.(*loopsingle.Argument)
		res.Arg = &loopsingle.Argument{
			Result: t.Result,
			Cond:   t.Cond,
			Typs:   t.Typs,
		}
	case vm.LoopMark:
		t := sourceIns.Arg.(*loopmark.Argument)
		res.Arg = &loopmark.Argument{
			Result: t.Result,
			Cond:   t.Cond,
			Typs:   t.Typs,
		}
	case vm.Offset:
		t := sourceIns.Arg.(*offset.Argument)
		res.Arg = &offset.Argument{
			Offset: t.Offset,
		}
	case vm.Order:
		t := sourceIns.Arg.(*order.Argument)
		res.Arg = &order.Argument{
			Fs: t.Fs,
		}
	case vm.Product:
		t := sourceIns.Arg.(*product.Argument)
		res.Arg = &product.Argument{
			Result: t.Result,
			Typs:   t.Typs,
		}
	case vm.Projection:
		t := sourceIns.Arg.(*projection.Argument)
		res.Arg = &projection.Argument{
			Es: t.Es,
		}
	case vm.Restrict:
		t := sourceIns.Arg.(*restrict.Argument)
		res.Arg = &restrict.Argument{
			E: t.E,
		}
	case vm.Semi:
		t := sourceIns.Arg.(*semi.Argument)
		res.Arg = &semi.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Result:     t.Result,
			Cond:       t.Cond,
			Typs:       t.Typs,
			Conditions: t.Conditions,
		}
	case vm.Single:
		t := sourceIns.Arg.(*single.Argument)
		res.Arg = &single.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Result:     t.Result,
			Cond:       t.Cond,
			Typs:       t.Typs,
			Conditions: t.Conditions,
		}
	case vm.Top:
		t := sourceIns.Arg.(*top.Argument)
		res.Arg = &top.Argument{
			Limit: t.Limit,
			Fs:    t.Fs,
		}
	case vm.Intersect:
		t := sourceIns.Arg.(*intersect.Argument)
		res.Arg = &intersect.Argument{
			IBucket: t.IBucket,
			NBucket: t.NBucket,
		}
	case vm.Minus: // 2
		t := sourceIns.Arg.(*minus.Argument)
		res.Arg = &minus.Argument{
			IBucket: t.IBucket,
			NBucket: t.NBucket,
		}
	case vm.IntersectAll:
		t := sourceIns.Arg.(*intersectall.Argument)
		res.Arg = &intersectall.Argument{
			IBucket: t.IBucket,
			NBucket: t.NBucket,
		}
	case vm.Merge:
		res.Arg = &merge.Argument{}
	case vm.MergeGroup:
		t := sourceIns.Arg.(*mergegroup.Argument)
		res.Arg = &mergegroup.Argument{
			NeedEval: t.NeedEval,
		}
	case vm.MergeLimit:
		t := sourceIns.Arg.(*mergelimit.Argument)
		res.Arg = &mergelimit.Argument{
			Limit: t.Limit,
		}
	case vm.MergeOffset:
		t := sourceIns.Arg.(*mergeoffset.Argument)
		res.Arg = &mergeoffset.Argument{
			Offset: t.Offset,
		}
	case vm.MergeTop:
		t := sourceIns.Arg.(*mergetop.Argument)
		res.Arg = &mergetop.Argument{
			Limit: t.Limit,
			Fs:    t.Fs,
		}
	case vm.MergeOrder:
		t := sourceIns.Arg.(*mergeorder.Argument)
		res.Arg = &mergeorder.Argument{
			Fs: t.Fs,
		}
	case vm.Mark:
		t := sourceIns.Arg.(*mark.Argument)
		res.Arg = &mark.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Result:     t.Result,
			Conditions: t.Conditions,
			Typs:       t.Typs,
			Cond:       t.Cond,
			OnList:     t.OnList,
		}
	case vm.TableFunction:
		t := sourceIns.Arg.(*table_function.Argument)
		res.Arg = &table_function.Argument{
			Name:   t.Name,
			Args:   t.Args,
			Rets:   t.Rets,
			Attrs:  t.Attrs,
			Params: t.Params,
		}

	case vm.HashBuild:
		t := sourceIns.Arg.(*hashbuild.Argument)
		res.Arg = &hashbuild.Argument{
			NeedHashMap: t.NeedHashMap,
			NeedExpr:    t.NeedExpr,
			Ibucket:     t.Ibucket,
			Nbucket:     t.Nbucket,
			Typs:        t.Typs,
			Conditions:  t.Conditions,
		}
	case vm.External:
		t := sourceIns.Arg.(*external.Argument)
		res.Arg = &external.Argument{
			Es: &external.ExternalParam{
				ExParamConst: external.ExParamConst{
					Attrs:         t.Es.Attrs,
					Cols:          t.Es.Cols,
					Name2ColIndex: t.Es.Name2ColIndex,
					CreateSql:     t.Es.CreateSql,
					FileList:      t.Es.FileList,

					Extern: t.Es.Extern,
				},
				ExParam: external.ExParam{
					Filter: t.Es.Filter,
					Fileparam: &external.ExFileparam{
						End:       t.Es.Fileparam.End,
						FileCnt:   t.Es.Fileparam.FileCnt,
						FileFin:   t.Es.Fileparam.FileFin,
						FileIndex: t.Es.Fileparam.FileIndex,
					},
				},
			},
		}
	case vm.Connector:
		ok := false
		if regMap != nil {
			arg := &connector.Argument{}
			sourceReg := sourceIns.Arg.(*connector.Argument).Reg
			if arg.Reg, ok = regMap[sourceReg]; !ok {
				panic("nonexistent wait register")
			}
			res.Arg = arg
		}
	case vm.Dispatch:
		ok := false
		if regMap != nil {
			sourceArg := sourceIns.Arg.(*dispatch.Argument)
			arg := &dispatch.Argument{
				All:        sourceArg.All,
				CrossCN:    sourceArg.CrossCN,
				SendFunc:   sourceArg.SendFunc,
				LocalRegs:  make([]*process.WaitRegister, len(sourceArg.LocalRegs)),
				RemoteRegs: make([]colexec.WrapperNode, len(sourceArg.RemoteRegs)),
			}
			for j := range arg.LocalRegs {
				sourceReg := sourceArg.LocalRegs[j]
				if arg.LocalRegs[j], ok = regMap[sourceReg]; !ok {
					panic("nonexistent wait register")
				}
			}
			for j := range arg.RemoteRegs {
				arg.RemoteRegs[j] = sourceArg.RemoteRegs[j]
			}
			res.Arg = arg
		}
	case vm.Insert:
		t := sourceIns.Arg.(*insert.Argument)
		res.Arg = &insert.Argument{
			Ts: t.Ts,
			// TargetTable:          t.TargetTable,
			// TargetColDefs:        t.TargetColDefs,
			Affected: t.Affected,
			Engine:   t.Engine,
			// DB:                   t.DB,
			// TableID:              t.TableID,
			// CPkeyColDef:          t.CPkeyColDef,
			// DBName:               t.DBName,
			// TableName:            t.TableName,
			// UniqueIndexTables:    t.UniqueIndexTables,
			// UniqueIndexDef:       t.UniqueIndexDef,
			// SecondaryIndexTables: t.SecondaryIndexTables,
			// SecondaryIndexDef:    t.SecondaryIndexDef,
			// ClusterTable:         t.ClusterTable,
			// ClusterByDef:         t.ClusterByDef,
			IsRemote:  t.IsRemote,
			InsertCtx: t.InsertCtx,
			// HasAutoCol:           t.HasAutoCol,
		}
	default:
		panic(fmt.Sprintf("unexpected instruction type '%d' to dup", sourceIns.Op))
	}
	return res
}

func constructRestrict(n *plan.Node) *restrict.Argument {
	return &restrict.Argument{
		E: colexec.RewriteFilterExprList(n.FilterList),
	}
}

func constructDeletion(n *plan.Node, eg engine.Engine, proc *process.Process) (*deletion.Argument, error) {
	oldCtx := n.DeleteCtx
	delCtx := &deletion.DeleteCtx{
		DelSource: make([]engine.Relation, len(oldCtx.Ref)),
		DelRef:    oldCtx.Ref,

		IdxSource: make([]engine.Relation, len(oldCtx.IdxRef)),
		IdxIdx:    oldCtx.IdxIdx,

		OnRestrictIdx: oldCtx.OnRestrictIdx,

		OnCascadeIdx:    oldCtx.OnCascadeIdx,
		OnCascadeSource: make([]engine.Relation, len(oldCtx.OnCascadeRef)),

		OnSetSource:       make([]engine.Relation, len(oldCtx.OnSetRef)),
		OnSetUniqueSource: make([][]engine.Relation, len(oldCtx.Ref)),
		OnSetIdx:          make([][]int32, len(oldCtx.OnSetIdx)),
		OnSetTableDef:     oldCtx.OnSetDef,
		OnSetRef:          oldCtx.OnSetRef,
		OnSetUpdateCol:    make([]map[string]int32, len(oldCtx.OnSetUpdateCol)),

		CanTruncate: oldCtx.CanTruncate,
	}

	if delCtx.CanTruncate {
		for i, ref := range oldCtx.Ref {
			rel, _, err := getRel(proc.Ctx, proc, eg, ref, nil)
			if err != nil {
				return nil, err
			}
			delCtx.DelSource[i] = rel
		}
	} else {
		for i, list := range oldCtx.OnSetIdx {
			delCtx.OnSetIdx[i] = make([]int32, len(list.List))
			for j, id := range list.List {
				delCtx.OnSetIdx[i][j] = int32(id)
			}
		}
		for i, ref := range oldCtx.Ref {
			rel, _, err := getRel(proc.Ctx, proc, eg, ref, nil)
			if err != nil {
				return nil, err
			}
			delCtx.DelSource[i] = rel
		}
		for i, ref := range oldCtx.IdxRef {
			rel, _, err := getRel(proc.Ctx, proc, eg, ref, nil)
			if err != nil {
				return nil, err
			}
			delCtx.IdxSource[i] = rel
		}
		for i, ref := range oldCtx.OnCascadeRef {
			rel, _, err := getRel(proc.Ctx, proc, eg, ref, nil)
			if err != nil {
				return nil, err
			}
			delCtx.OnCascadeSource[i] = rel
		}
		for i, ref := range oldCtx.OnSetRef {
			rel, uniqueRels, err := getRel(proc.Ctx, proc, eg, ref, oldCtx.OnSetDef[i])
			if err != nil {
				return nil, err
			}
			delCtx.OnSetSource[i] = rel
			delCtx.OnSetUniqueSource[i] = uniqueRels
		}
		for i, idxMap := range oldCtx.OnSetUpdateCol {
			delCtx.OnSetUpdateCol[i] = idxMap.Map
		}
	}

	return &deletion.Argument{
		DeleteCtx: delCtx,
		Engine:    eg,
	}, nil
}

func constructInsert(n *plan.Node, eg engine.Engine, proc *process.Process) (*insert.Argument, error) {
	oldCtx := n.InsertCtx
	ctx := proc.Ctx
	if oldCtx.GetClusterTable().GetIsClusterTable() {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	}
	newCtx := &insert.InsertCtx{
		Idx:      oldCtx.Idx,
		Ref:      oldCtx.Ref,
		TableDef: oldCtx.TableDef,

		ParentIdx:    oldCtx.ParentIdx,
		ClusterTable: oldCtx.ClusterTable,
	}

	originRel, indexRels, err := getRel(ctx, proc, eg, oldCtx.Ref, oldCtx.TableDef)
	if err != nil {
		return nil, err
	}
	newCtx.Source = originRel
	newCtx.UniqueSource = indexRels

	return &insert.Argument{
		InsertCtx: newCtx,
		Engine:    eg,
	}, nil
}

func constructUpdate(n *plan.Node, eg engine.Engine, proc *process.Process) (*update.Argument, error) {
	oldCtx := n.UpdateCtx
	updateCtx := &update.UpdateCtx{
		Source:       make([]engine.Relation, len(oldCtx.Ref)),
		Idxs:         make([][]int32, len(oldCtx.Idx)),
		TableDefs:    oldCtx.TableDefs,
		Ref:          oldCtx.Ref,
		UpdateCol:    make([]map[string]int32, len(oldCtx.UpdateCol)),
		UniqueSource: make([][]engine.Relation, len(oldCtx.Ref)),

		IdxSource: make([]engine.Relation, len(oldCtx.IdxRef)),
		IdxIdx:    oldCtx.IdxIdx,

		OnRestrictIdx: oldCtx.OnRestrictIdx,

		OnCascadeIdx:          make([][]int32, len(oldCtx.OnCascadeIdx)),
		OnCascadeSource:       make([]engine.Relation, len(oldCtx.OnCascadeRef)),
		OnCascadeUniqueSource: make([][]engine.Relation, len(oldCtx.OnCascadeRef)),
		OnCascadeRef:          oldCtx.OnCascadeRef,
		OnCascadeTableDef:     oldCtx.OnCascadeDef,
		OnCascadeUpdateCol:    make([]map[string]int32, len(oldCtx.OnCascadeUpdateCol)),

		OnSetSource:       make([]engine.Relation, len(oldCtx.OnSetRef)),
		OnSetUniqueSource: make([][]engine.Relation, len(oldCtx.OnSetRef)),
		OnSetIdx:          make([][]int32, len(oldCtx.OnSetIdx)),
		OnSetRef:          oldCtx.OnSetRef,
		OnSetTableDef:     oldCtx.OnSetDef,
		OnSetUpdateCol:    make([]map[string]int32, len(oldCtx.OnSetUpdateCol)),

		ParentIdx: make([]map[string]int32, len(oldCtx.ParentIdx)),
	}

	for i, idxMap := range oldCtx.UpdateCol {
		updateCtx.UpdateCol[i] = idxMap.Map
	}
	for i, list := range oldCtx.Idx {
		updateCtx.Idxs[i] = make([]int32, len(list.List))
		for j, id := range list.List {
			updateCtx.Idxs[i][j] = int32(id)
		}
	}
	for i, list := range oldCtx.OnSetIdx {
		updateCtx.OnSetIdx[i] = make([]int32, len(list.List))
		for j, id := range list.List {
			updateCtx.OnSetIdx[i][j] = int32(id)
		}
	}
	for i, list := range oldCtx.OnCascadeIdx {
		updateCtx.OnCascadeIdx[i] = make([]int32, len(list.List))
		for j, id := range list.List {
			updateCtx.OnCascadeIdx[i][j] = int32(id)
		}
	}
	for i, ref := range oldCtx.Ref {
		rel, uniqueRels, err := getRel(proc.Ctx, proc, eg, ref, oldCtx.TableDefs[i])
		if err != nil {
			return nil, err
		}
		updateCtx.Source[i] = rel
		updateCtx.UniqueSource[i] = uniqueRels
	}
	for i, ref := range oldCtx.IdxRef {
		rel, _, err := getRel(proc.Ctx, proc, eg, ref, nil)
		if err != nil {
			return nil, err
		}
		updateCtx.IdxSource[i] = rel
	}
	for i, ref := range oldCtx.OnCascadeRef {
		rel, uniqueRels, err := getRel(proc.Ctx, proc, eg, ref, oldCtx.OnCascadeDef[i])
		if err != nil {
			return nil, err
		}
		updateCtx.OnCascadeSource[i] = rel
		updateCtx.OnCascadeUniqueSource[i] = uniqueRels
	}
	for i, ref := range oldCtx.OnSetRef {
		rel, uniqueRels, err := getRel(proc.Ctx, proc, eg, ref, oldCtx.OnSetDef[i])
		if err != nil {
			return nil, err
		}
		updateCtx.OnSetSource[i] = rel
		updateCtx.OnSetUniqueSource[i] = uniqueRels
	}
	for i, idxMap := range oldCtx.OnCascadeUpdateCol {
		updateCtx.OnCascadeUpdateCol[i] = idxMap.Map
	}
	for i, idxMap := range oldCtx.OnSetUpdateCol {
		updateCtx.OnSetUpdateCol[i] = idxMap.Map
	}
	for i, idxMap := range oldCtx.ParentIdx {
		updateCtx.ParentIdx[i] = idxMap.Map
	}

	return &update.Argument{
		UpdateCtx: updateCtx,
		Engine:    eg,
	}, nil
}

func constructProjection(n *plan.Node) *projection.Argument {
	return &projection.Argument{
		Es: n.ProjectList,
	}
}

func constructExternal(n *plan.Node, param *tree.ExternParam, ctx context.Context, fileList []string, FileSize []int64, fileOffset [][2]int) *external.Argument {
	attrs := make([]string, len(n.TableDef.Cols))
	for j, col := range n.TableDef.Cols {
		attrs[j] = col.Name
	}
	return &external.Argument{
		Es: &external.ExternalParam{
			ExParamConst: external.ExParamConst{
				Attrs:         attrs,
				Cols:          n.TableDef.Cols,
				Extern:        param,
				Name2ColIndex: n.TableDef.Name2ColIndex,
				FileOffset:    fileOffset,
				CreateSql:     n.TableDef.Createsql,
				Ctx:           ctx,
				FileList:      fileList,
				FileSize:      FileSize,
				ClusterTable:  n.GetClusterTable(),
			},
			ExParam: external.ExParam{
				Fileparam: new(external.ExFileparam),
				Filter: &external.FilterParam{
					FilterExpr: colexec.RewriteFilterExprList(n.FilterList),
				},
			},
		},
	}
}
func constructTableFunction(n *plan.Node, ctx context.Context, name string) *table_function.Argument {
	attrs := make([]string, len(n.TableDef.Cols))
	for j, col := range n.TableDef.Cols {
		attrs[j] = col.Name
	}
	return &table_function.Argument{
		Attrs:  attrs,
		Rets:   n.TableDef.Cols,
		Args:   n.TblFuncExprList,
		Name:   name,
		Params: n.TableDef.TblFunc.Param,
	}
}

func constructTop(n *plan.Node, topN int64) *top.Argument {
	return &top.Argument{
		Fs:    n.OrderBy,
		Limit: topN,
	}
}

func constructJoin(n *plan.Node, typs []types.Type, proc *process.Process) *join.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &join.Argument{
		Typs:       typs,
		Result:     result,
		Cond:       cond,
		Conditions: constructJoinConditions(conds, proc),
	}
}

func constructSemi(n *plan.Node, typs []types.Type, proc *process.Process) *semi.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel != 0 {
			panic(moerr.NewNYI(proc.Ctx, "semi result '%s'", expr))
		}
		result[i] = pos
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &semi.Argument{
		Typs:       typs,
		Result:     result,
		Cond:       cond,
		Conditions: constructJoinConditions(conds, proc),
	}
}

func constructLeft(n *plan.Node, typs []types.Type, proc *process.Process) *left.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &left.Argument{
		Typs:       typs,
		Result:     result,
		Cond:       cond,
		Conditions: constructJoinConditions(conds, proc),
	}
}

func constructSingle(n *plan.Node, typs []types.Type, proc *process.Process) *single.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &single.Argument{
		Typs:       typs,
		Result:     result,
		Cond:       cond,
		Conditions: constructJoinConditions(conds, proc),
	}
}

func constructProduct(n *plan.Node, typs []types.Type, proc *process.Process) *product.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	return &product.Argument{Typs: typs, Result: result}
}

func constructAnti(n *plan.Node, typs []types.Type, proc *process.Process) *anti.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel != 0 {
			panic(moerr.NewNYI(proc.Ctx, "anti result '%s'", expr))
		}
		result[i] = pos
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &anti.Argument{
		Typs:       typs,
		Result:     result,
		Cond:       cond,
		Conditions: constructJoinConditions(conds, proc),
	}
}

/*
func constructMark(n *plan.Node, typs []types.Type, proc *process.Process) *mark.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel == 0 {
			result[i] = pos
		} else if rel == -1 {
			result[i] = -1
		} else {
			panic(moerr.NewNYI(proc.Ctx, "loop mark result '%s'", expr))
		}
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &mark.Argument{
		Typs:       typs,
		Result:     result,
		Cond:       cond,
		Conditions: constructJoinConditions(conds, proc),
		OnList:     n.OnList,
	}
}
*/

func constructOrder(n *plan.Node, proc *process.Process) *order.Argument {
	return &order.Argument{
		Fs: n.OrderBy,
	}
}

/*
func constructOffset(n *plan.Node, proc *process.Process) *offset.Argument {
	vec, err := colexec.EvalExpr(constBat, proc, n.Offset)
	if err != nil {
		panic(err)
	}
	return &offset.Argument{
		Offset: uint64(vec.Col.([]int64)[0]),
	}
}
*/

func constructLimit(n *plan.Node, proc *process.Process) *limit.Argument {
	vec, err := colexec.EvalExpr(constBat, proc, n.Limit)
	if err != nil {
		panic(err)
	}
	defer vec.Free(proc.Mp())
	return &limit.Argument{
		Limit: uint64(vec.Col.([]int64)[0]),
	}
}

func constructGroup(ctx context.Context, n, cn *plan.Node, ibucket, nbucket int, needEval bool, proc *process.Process) *group.Argument {
	var lenAggs, lenMultiAggs int
	aggs := make([]agg.Aggregate, len(n.AggList))
	// multiaggs: is not like the normal agg funcs which have only one arg exclude 'distinct'
	// for now, we have group_concat
	multiaggs := make([]group_concat.Argument, len(n.AggList))
	for _, expr := range n.AggList {
		if f, ok := expr.Expr.(*plan.Expr_F); ok {
			distinct := (uint64(f.F.Func.Obj) & function.Distinct) != 0
			if len(f.F.Args) > 1 {
				// vec is separator
				vec, _ := colexec.EvalExpr(constBat, proc, f.F.Args[len(f.F.Args)-1])
				sepa := vec.GetString(0)
				multiaggs[lenMultiAggs] = group_concat.Argument{
					Dist:      distinct,
					GroupExpr: f.F.Args[:len(f.F.Args)-1],
					Separator: sepa,
				}
				lenMultiAggs++
				continue
			}
			obj := int64(uint64(f.F.Func.Obj) & function.DistinctMask)
			fun, err := function.GetFunctionByID(ctx, obj)
			if err != nil {
				panic(err)
			}
			aggs[lenAggs] = agg.Aggregate{
				E:    f.F.Args[0],
				Dist: distinct,
				Op:   fun.AggregateInfo,
			}
			lenAggs++
		}
	}
	aggs = aggs[:lenAggs]
	multiaggs = multiaggs[:lenMultiAggs]
	typs := make([]types.Type, len(cn.ProjectList))
	for i, e := range cn.ProjectList {
		typs[i].Oid = types.T(e.Typ.Id)
		typs[i].Width = e.Typ.Width
		typs[i].Size = e.Typ.Size
		typs[i].Scale = e.Typ.Scale
		typs[i].Precision = e.Typ.Precision
	}
	return &group.Argument{
		Aggs:      aggs,
		MultiAggs: multiaggs,
		Types:     typs,
		NeedEval:  needEval,
		Exprs:     n.GroupBy,
		Ibucket:   uint64(ibucket),
		Nbucket:   uint64(nbucket),
	}
}

// ibucket: bucket number
// nbucket:
// construct operator argument
func constructIntersectAll(_ *plan.Node, proc *process.Process, ibucket, nbucket int) *intersectall.Argument {
	return &intersectall.Argument{
		IBucket: uint64(ibucket),
		NBucket: uint64(nbucket),
	}
}

func constructMinus(n *plan.Node, proc *process.Process, ibucket, nbucket int) *minus.Argument {
	return &minus.Argument{
		IBucket: uint64(ibucket),
		NBucket: uint64(nbucket),
	}
}

func constructIntersect(n *plan.Node, proc *process.Process, ibucket, nbucket int) *intersect.Argument {
	return &intersect.Argument{
		IBucket: uint64(ibucket),
		NBucket: uint64(nbucket),
	}
}

func constructDispatch(all bool, regs []*process.WaitRegister) *dispatch.Argument {
	arg := new(dispatch.Argument)
	arg.All = all
	arg.CrossCN = false
	arg.LocalRegs = regs
	return arg
}

// ShuffleJoinDispatch is a cross-cn dispath
// and it will send same batch to all register
func constructShuffleJoinDispatch(idx int, ss []*Scope, currentCNAddr string) *dispatch.Argument {
	arg := new(dispatch.Argument)
	arg.All = true

	scopeLen := len(ss)
	arg.RemoteRegs = make([]colexec.WrapperNode, 0, scopeLen)
	arg.LocalRegs = make([]*process.WaitRegister, 0, scopeLen)

	for _, s := range ss {
		if s.IsEnd {
			continue
		}

		if len(s.NodeInfo.Addr) == 0 || len(currentCNAddr) == 0 ||
			strings.Split(currentCNAddr, ":")[0] == strings.Split(s.NodeInfo.Addr, ":")[0] {
			// Local reg.
			// Put them into arg.LocalRegs
			arg.LocalRegs = append(arg.LocalRegs, s.Proc.Reg.MergeReceivers[idx])
		} else {
			// Remote reg.
			// Generate uuid for them and put into arg.RemoteRegs
			found := false
			newUuid := uuid.New()

			// Length of RemoteRegs must be very small, so find the same NodeAddr with traversal
			for _, reg := range arg.RemoteRegs {
				if reg.NodeAddr == s.NodeInfo.Addr {
					reg.Uuids = append(reg.Uuids, newUuid)
					found = true
					break
				}
			}

			if !found {
				uuids := make([]uuid.UUID, 0, scopeLen)
				uuids = append(uuids, newUuid)
				arg.RemoteRegs = append(arg.RemoteRegs, colexec.WrapperNode{
					NodeAddr: s.NodeInfo.Addr,
					Uuids:    uuids,
				})
			}

			s.UuidToRegIdx = append(s.UuidToRegIdx, UuidToRegIdx{
				Uuid: newUuid,
				Idx:  idx,
			})
		}
	}
	if len(arg.RemoteRegs) != 0 {
		arg.CrossCN = true
	}

	sendFunc := func(streams []*dispatch.WrapperStream, bat *batch.Batch, localChans []*process.WaitRegister, ctxs []context.Context, cnts [][]uint, proc *process.Process) error {
		// TODO: seperate local and remote to different goroutine?
		// send bat to streams
		{
			// TODO: Split the batch into small if it is too large
			encodeBatch, err := types.Encode(bat)
			if err != nil {
				return err
			}
			for i, stream := range streams {
				// seperate different uuid into different message
				for j, uuid := range stream.Uuids {
					message := cnclient.AcquireMessage()
					{
						message.Id = stream.Stream.ID()
						message.Cmd = pipeline.BatchMessage
						message.Data = encodeBatch
						message.Uuid = uuid[:]
					}
					errSend := stream.Stream.Send(ctxs[i], message)
					if errSend != nil {
						return errSend
					}
					cnts[i][j]++
				}
			}
		}

		// send bat to localChans
		{
			for i, vec := range bat.Vecs {
				if vec.IsOriginal() {
					cloneVec, err := vector.Dup(vec, proc.Mp())
					if err != nil {
						bat.Clean(proc.Mp())
						return err
					}
					bat.Vecs[i] = cloneVec
				}
			}

			refCountAdd := int64(len(localChans) - 1)
			atomic.AddInt64(&bat.Cnt, refCountAdd)
			if jm, ok := bat.Ht.(*hashmap.JoinMap); ok {
				jm.IncRef(refCountAdd)
			}

			for _, reg := range localChans {
				select {
				case <-reg.Ctx.Done():
					return moerr.NewInternalError(proc.Ctx, "pipeline context has done.")
				case reg.Ch <- bat:
				}
			}
		}

		return nil
	}
	arg.SendFunc = sendFunc

	return arg
}

func constructMergeGroup(_ *plan.Node, needEval bool) *mergegroup.Argument {
	return &mergegroup.Argument{
		NeedEval: needEval,
	}
}

func constructMergeTop(n *plan.Node, topN int64) *mergetop.Argument {
	return &mergetop.Argument{
		Fs:    n.OrderBy,
		Limit: topN,
	}
}

func constructMergeOffset(n *plan.Node, proc *process.Process) *mergeoffset.Argument {
	vec, err := colexec.EvalExpr(constBat, proc, n.Offset)
	if err != nil {
		panic(err)
	}
	defer vec.Free(proc.Mp())
	return &mergeoffset.Argument{
		Offset: uint64(vec.Col.([]int64)[0]),
	}
}

func constructMergeLimit(n *plan.Node, proc *process.Process) *mergelimit.Argument {
	vec, err := colexec.EvalExpr(constBat, proc, n.Limit)
	if err != nil {
		panic(err)
	}
	defer vec.Free(proc.Mp())
	return &mergelimit.Argument{
		Limit: uint64(vec.Col.([]int64)[0]),
	}
}

func constructMergeOrder(n *plan.Node, proc *process.Process) *mergeorder.Argument {
	return &mergeorder.Argument{
		Fs: n.OrderBy,
	}
}

func constructLoopJoin(n *plan.Node, typs []types.Type, proc *process.Process) *loopjoin.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	return &loopjoin.Argument{
		Typs:   typs,
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func constructLoopSemi(n *plan.Node, typs []types.Type, proc *process.Process) *loopsemi.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel != 0 {
			panic(moerr.NewNYI(proc.Ctx, "loop semi result '%s'", expr))
		}
		result[i] = pos
	}
	return &loopsemi.Argument{
		Typs:   typs,
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func constructLoopLeft(n *plan.Node, typs []types.Type, proc *process.Process) *loopleft.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	return &loopleft.Argument{
		Typs:   typs,
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func constructLoopSingle(n *plan.Node, typs []types.Type, proc *process.Process) *loopsingle.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	return &loopsingle.Argument{
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func constructLoopAnti(n *plan.Node, typs []types.Type, proc *process.Process) *loopanti.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel != 0 {
			panic(moerr.NewNYI(proc.Ctx, "loop anti result '%s'", expr))
		}
		result[i] = pos
	}
	return &loopanti.Argument{
		Typs:   typs,
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func constructLoopMark(n *plan.Node, typs []types.Type, proc *process.Process) *loopmark.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel == 0 {
			result[i] = pos
		} else if rel == -1 {
			result[i] = -1
		} else {
			panic(moerr.NewNYI(proc.Ctx, "loop mark result '%s'", expr))
		}
	}
	return &loopmark.Argument{
		Typs:   typs,
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func constructHashBuild(in vm.Instruction, proc *process.Process) *hashbuild.Argument {
	switch in.Op {
	case vm.Anti:
		arg := in.Arg.(*anti.Argument)
		return &hashbuild.Argument{
			NeedHashMap: true,
			Typs:        arg.Typs,
			Conditions:  arg.Conditions[1],
		}
	case vm.Mark:
		arg := in.Arg.(*mark.Argument)
		return &hashbuild.Argument{
			NeedHashMap: true,
			Typs:        arg.Typs,
			Conditions:  arg.Conditions[1],
		}
	case vm.Join:
		arg := in.Arg.(*join.Argument)
		return &hashbuild.Argument{
			NeedHashMap: true,
			Typs:        arg.Typs,
			Conditions:  arg.Conditions[1],
		}
	case vm.Left:
		arg := in.Arg.(*left.Argument)
		return &hashbuild.Argument{
			NeedHashMap: true,
			Typs:        arg.Typs,
			Conditions:  arg.Conditions[1],
		}
	case vm.Semi:
		arg := in.Arg.(*semi.Argument)
		return &hashbuild.Argument{
			NeedHashMap: true,
			Typs:        arg.Typs,
			Conditions:  arg.Conditions[1],
		}
	case vm.Single:
		arg := in.Arg.(*single.Argument)
		return &hashbuild.Argument{
			NeedHashMap: true,
			Typs:        arg.Typs,
			Conditions:  arg.Conditions[1],
		}
	case vm.Product:
		arg := in.Arg.(*product.Argument)
		return &hashbuild.Argument{
			NeedHashMap: false,
			Typs:        arg.Typs,
		}
	case vm.LoopAnti:
		arg := in.Arg.(*loopanti.Argument)
		return &hashbuild.Argument{
			NeedHashMap: false,
			Typs:        arg.Typs,
		}
	case vm.LoopJoin:
		arg := in.Arg.(*loopjoin.Argument)
		return &hashbuild.Argument{
			NeedHashMap: false,
			Typs:        arg.Typs,
		}
	case vm.LoopLeft:
		arg := in.Arg.(*loopleft.Argument)
		return &hashbuild.Argument{
			NeedHashMap: false,
			Typs:        arg.Typs,
		}
	case vm.LoopSemi:
		arg := in.Arg.(*loopsemi.Argument)
		return &hashbuild.Argument{
			NeedHashMap: false,
			Typs:        arg.Typs,
		}
	case vm.LoopSingle:
		arg := in.Arg.(*loopsingle.Argument)
		return &hashbuild.Argument{
			NeedHashMap: false,
			Typs:        arg.Typs,
		}
	case vm.LoopMark:
		arg := in.Arg.(*loopmark.Argument)
		return &hashbuild.Argument{
			NeedHashMap: false,
			Typs:        arg.Typs,
		}

	default:
		panic(moerr.NewInternalError(proc.Ctx, "unsupport join type '%v'", in.Op))
	}
}

func constructJoinResult(expr *plan.Expr, proc *process.Process) (int32, int32) {
	e, ok := expr.Expr.(*plan.Expr_Col)
	if !ok {
		panic(moerr.NewNYI(proc.Ctx, "join result '%s'", expr))
	}
	return e.Col.RelPos, e.Col.ColPos
}

func constructJoinConditions(exprs []*plan.Expr, proc *process.Process) [][]*plan.Expr {
	conds := make([][]*plan.Expr, 2)
	conds[0] = make([]*plan.Expr, len(exprs))
	conds[1] = make([]*plan.Expr, len(exprs))
	for i, expr := range exprs {
		conds[0][i], conds[1][i] = constructJoinCondition(expr, proc)
	}
	return conds
}

func constructJoinCondition(expr *plan.Expr, proc *process.Process) (*plan.Expr, *plan.Expr) {
	if e, ok := expr.Expr.(*plan.Expr_C); ok { // constant bool
		b, ok := e.C.Value.(*plan.Const_Bval)
		if !ok {
			panic(moerr.NewNYI(proc.Ctx, "join condition '%s'", expr))
		}
		if b.Bval {
			return expr, expr
		}
		return expr, &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Value: &plan.Const_Bval{Bval: true},
				},
			},
		}
	}
	e, ok := expr.Expr.(*plan.Expr_F)
	if !ok || !supportedJoinCondition(e.F.Func.GetObj()) {
		panic(moerr.NewNYI(proc.Ctx, "join condition '%s'", expr))
	}
	if exprRelPos(e.F.Args[0]) == 1 {
		return e.F.Args[1], e.F.Args[0]
	}
	return e.F.Args[0], e.F.Args[1]
}

func isEquiJoin(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if e, ok := expr.Expr.(*plan.Expr_F); ok {
			if !supportedJoinCondition(e.F.Func.GetObj()) {
				continue
			}
			lpos, rpos := hasColExpr(e.F.Args[0], -1), hasColExpr(e.F.Args[1], -1)
			if lpos == -1 || rpos == -1 || (lpos == rpos) {
				continue
			}
			return true
		}
	}
	return false || isEquiJoin0(exprs)
}

func isEquiJoin0(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if e, ok := expr.Expr.(*plan.Expr_F); ok {
			if !supportedJoinCondition(e.F.Func.GetObj()) {
				return false
			}
			lpos, rpos := hasColExpr(e.F.Args[0], -1), hasColExpr(e.F.Args[1], -1)
			if lpos == -1 || rpos == -1 || (lpos == rpos) {
				return false
			}
		}
	}
	return true
}

func extraJoinConditions(exprs []*plan.Expr) (*plan.Expr, []*plan.Expr) {
	exprs = colexec.SplitAndExprs(exprs)
	eqConds := make([]*plan.Expr, 0, len(exprs))
	notEqConds := make([]*plan.Expr, 0, len(exprs))
	for i, expr := range exprs {
		if e, ok := expr.Expr.(*plan.Expr_F); ok {
			if !supportedJoinCondition(e.F.Func.GetObj()) {
				notEqConds = append(notEqConds, exprs[i])
				continue
			}
			lpos, rpos := hasColExpr(e.F.Args[0], -1), hasColExpr(e.F.Args[1], -1)
			if lpos == -1 || rpos == -1 || (lpos == rpos) {
				notEqConds = append(notEqConds, exprs[i])
				continue
			}
			eqConds = append(eqConds, exprs[i])
		}
	}
	if len(notEqConds) == 0 {
		return nil, eqConds
	}
	return colexec.RewriteFilterExprList(notEqConds), eqConds
}

func supportedJoinCondition(id int64) bool {
	fid, _ := function.DecodeOverloadID(id)
	return fid == function.EQUAL
}

func hasColExpr(expr *plan.Expr, pos int32) int32 {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if pos == -1 {
			return e.Col.RelPos
		}
		if pos != e.Col.RelPos {
			return -1
		}
		return pos
	case *plan.Expr_F:
		for i := range e.F.Args {
			pos0 := hasColExpr(e.F.Args[i], pos)
			switch {
			case pos0 == -1:
			case pos == -1:
				pos = pos0
			case pos != pos0:
				return -1
			}
		}
		return pos
	default:
		return pos
	}
}

func exprRelPos(expr *plan.Expr) int32 {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		return e.Col.RelPos
	case *plan.Expr_F:
		for i := range e.F.Args {
			if relPos := exprRelPos(e.F.Args[i]); relPos >= 0 {
				return relPos
			}
		}
	}
	return -1
}

func buildIndexDefs(defs []*plan.TableDef_DefType) (*plan.UniqueIndexDef, *plan.SecondaryIndexDef) {
	var uIdxDef *plan.UniqueIndexDef = nil
	var sIdxDef *plan.SecondaryIndexDef = nil
	for _, def := range defs {
		if idxDef, ok := def.Def.(*plan.TableDef_DefType_UIdx); ok {
			uIdxDef = idxDef.UIdx
		}
		if idxDef, ok := def.Def.(*plan.TableDef_DefType_SIdx); ok {
			sIdxDef = idxDef.SIdx
		}
	}
	return uIdxDef, sIdxDef
}

// Get the 'engine.Relation' of the table by using 'ObjectRef' and 'TableDef', if 'TableDef' is nil, the relations of its index table will not be obtained
// the first return value is Relation of the original table
// the second return value is Relations of index tables
func getRel(ctx context.Context, proc *process.Process, eg engine.Engine, ref *plan.ObjectRef, tableDef *plan.TableDef) (engine.Relation, []engine.Relation, error) {
	var dbSource engine.Database
	var relation engine.Relation
	var err error
	var isTemp bool
	oldDbName := ref.SchemaName
	if ref.SchemaName != "" {
		dbSource, err = eg.Database(ctx, ref.SchemaName, proc.TxnOperator)
		if err != nil {
			return nil, nil, err
		}
		relation, err = dbSource.Relation(ctx, ref.ObjName)
		if err == nil {
			isTemp = false
		} else {
			dbSource, err = eg.Database(ctx, defines.TEMPORARY_DBNAME, proc.TxnOperator)
			if err != nil {
				return nil, nil, err
			}
			newObjeName := engine.GetTempTableName(ref.SchemaName, ref.ObjName)
			newSchemaName := defines.TEMPORARY_DBNAME
			ref.SchemaName = newSchemaName
			ref.ObjName = newObjeName
			relation, err = dbSource.Relation(ctx, newObjeName)
			if err != nil {
				return nil, nil, err
			}
			isTemp = true
		}
	} else {
		_, _, relation, err = eg.GetRelationById(ctx, proc.TxnOperator, uint64(ref.Obj))
		if err != nil {
			return nil, nil, err
		}
	}

	var uniqueIndexTables []engine.Relation
	if tableDef != nil {
		uniqueIndexTables = make([]engine.Relation, 0)
		uDef, _ := buildIndexDefs(tableDef.Defs)
		if uDef != nil {
			for i := range uDef.TableNames {
				var indexTable engine.Relation
				if uDef.TableExists[i] {
					if isTemp {
						indexTable, err = dbSource.Relation(ctx, engine.GetTempTableName(oldDbName, uDef.TableNames[i]))
					} else {
						indexTable, err = dbSource.Relation(ctx, uDef.TableNames[i])
					}
					if err != nil {
						return nil, nil, err
					}
					uniqueIndexTables = append(uniqueIndexTables, indexTable)
				}
			}
		}
	}
	return relation, uniqueIndexTables, err
}
