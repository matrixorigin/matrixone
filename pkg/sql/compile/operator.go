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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergededup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/times"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transform"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transformer"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/untransform"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

func dupInstruction(in vm.Instruction) vm.Instruction {
	rin := vm.Instruction{
		Op: in.Op,
	}
	switch arg := in.Arg.(type) {
	case *top.Argument:
		rin.Arg = &top.Argument{
			Fs:    arg.Fs,
			Limit: arg.Limit,
		}
	case *limit.Argument:
		rin.Arg = &limit.Argument{
			Limit: arg.Limit,
		}
	case *dedup.Argument:
		rin.Arg = &dedup.Argument{}
	case *order.Argument:
		rin.Arg = &order.Argument{
			Fs: arg.Fs,
		}
	case *projection.Argument:
		rin.Arg = &projection.Argument{
			Rs: arg.Rs,
			Es: arg.Es,
			As: arg.As,
		}
	case *transform.Argument:
		rin.Arg = &transform.Argument{
			Typ:        arg.Typ,
			IsMerge:    arg.IsMerge,
			FreeVars:   arg.FreeVars,
			Restrict:   arg.Restrict,
			Projection: arg.Projection,
			BoundVars:  arg.BoundVars,
		}
	case *join.Argument:
		rin.Arg = &join.Argument{
			Vars:   arg.Vars,
			Bats:   arg.Bats,
			Result: arg.Result,
		}
	case *times.Argument:
		rin.Arg = &times.Argument{
			Vars:   arg.Vars,
			Bats:   arg.Bats,
			Result: arg.Result,
		}
	case *restrict.Argument:
		rin.Arg = &restrict.Argument{
			E: arg.E,
		}
	}
	return rin
}

func constructDedup() *dedup.Argument {
	return &dedup.Argument{}
}

func constructMergeDedup() *mergededup.Argument {
	return &mergededup.Argument{}
}

func constructLimit(op *plan.Limit) *limit.Argument {
	return &limit.Argument{
		Limit: uint64(op.Limit),
	}
}

func constructMergeLimit(op *plan.Limit) *mergelimit.Argument {
	return &mergelimit.Argument{
		Limit: uint64(op.Limit),
	}
}

func constructOffset(op *plan.Offset) *offset.Argument {
	return &offset.Argument{
		Offset: uint64(op.Offset),
	}
}

func constructMergeOffset(op *plan.Offset) *mergeoffset.Argument {
	return &mergeoffset.Argument{
		Offset: uint64(op.Offset),
	}
}

func constructOrder(op *plan.Order) *order.Argument {
	arg := &order.Argument{
		Fs: make([]order.Field, len(op.Fs)),
	}
	for i, f := range op.Fs {
		arg.Fs[i].Attr = f.Attr
		arg.Fs[i].Type = order.Direction(f.Type)
	}
	return arg
}

func constructMergeOrder(op *plan.Order) *mergeorder.Argument {
	arg := &mergeorder.Argument{
		Fields: make([]order.Field, len(op.Fs)),
	}
	for i, f := range op.Fs {
		arg.Fields[i].Attr = f.Attr
		arg.Fields[i].Type = order.Direction(f.Type)
	}
	return arg

}

func constructTop(op interface{}, limit int64) *top.Argument {
	fs := op.(*order.Argument).Fs
	arg := &top.Argument{
		Limit: limit,
		Fs:    make([]top.Field, len(fs)),
	}
	for i, f := range fs {
		arg.Fs[i].Attr = f.Attr
		arg.Fs[i].Type = top.Direction(f.Type)
	}
	return arg
}

func constructMergeTop(op interface{}, limit int64) *mergetop.Argument {
	fs := op.(*mergeorder.Argument).Fields
	arg := &mergetop.Argument{
		Limit:  limit,
		Fields: make([]top.Field, len(fs)),
	}
	for i, f := range fs {
		arg.Fields[i].Attr = f.Attr
		arg.Fields[i].Type = top.Direction(f.Type)
	}
	return arg
}

func constructRestrict(op *plan.Restrict) *restrict.Argument {
	return &restrict.Argument{
		E: op.E,
	}
}

func constructRename(op *plan.Rename) *projection.Argument {
	arg := &projection.Argument{
		Rs: make([]uint64, len(op.Rs)),
		As: make([]string, len(op.As)),
		Es: make([]extend.Extend, len(op.Es)),
	}
	for i := range op.Rs {
		arg.As[i] = op.As[i]
		arg.Es[i] = op.Es[i]
		arg.Rs[i] = op.Rs[i]
	}
	return arg
}

func constructProjection(op *plan.Projection) *projection.Argument {
	arg := &projection.Argument{
		Rs: make([]uint64, len(op.Rs)),
		As: make([]string, len(op.As)),
		Es: make([]extend.Extend, len(op.Es)),
	}
	for i := range op.Rs {
		arg.As[i] = op.As[i]
		arg.Es[i] = op.Es[i]
		arg.Rs[i] = op.Rs[i]
	}
	return arg
}

func constructResultProjection(op *plan.ResultProjection) *projection.Argument {
	arg := &projection.Argument{
		Rs: make([]uint64, len(op.Rs)),
		As: make([]string, len(op.As)),
		Es: make([]extend.Extend, len(op.Es)),
	}
	for i := range op.Rs {
		arg.As[i] = op.As[i]
		arg.Es[i] = op.Es[i]
		arg.Rs[i] = op.Rs[i]
	}
	return arg
}

func constructUntransform(op *plan.Untransform) *untransform.Argument {
	return &untransform.Argument{
		FreeVars: op.FreeVars,
	}
}

func constructCAQUntransform(op *plan.Untransform) *untransform.Argument {
	return &untransform.Argument{
		FreeVars: op.FreeVars,
		Type:     untransform.CAQ,
	}
}

func constructBareTransform(op *plan.Relation) *transform.Argument {
	arg := &transform.Argument{
		Typ: transform.Bare,
	}
	if op.Cond != nil {
		arg.Restrict = &restrict.Argument{E: op.Cond}
	}
	if n := len(op.Proj.Es); n > 0 {
		proj := &projection.Argument{
			Rs: make([]uint64, n),
			As: make([]string, n),
			Es: make([]extend.Extend, n),
		}
		for i := range op.Proj.Es {
			proj.As[i] = op.Proj.As[i]
			proj.Es[i] = op.Proj.Es[i]
			proj.Rs[i] = op.Proj.Rs[i]
		}
		arg.Projection = proj
	}
	return arg
}

func constructBareTransformFromDerived(op *plan.DerivedRelation) *transform.Argument {
	arg := &transform.Argument{
		Typ: transform.Bare,
	}
	if op.Cond != nil {
		arg.Restrict = &restrict.Argument{E: op.Cond}
	}
	if n := len(op.Proj.Es); n > 0 {
		proj := &projection.Argument{
			Rs: make([]uint64, n),
			As: make([]string, n),
			Es: make([]extend.Extend, n),
		}
		for i := range op.Proj.Es {
			proj.As[i] = op.Proj.As[i]
			proj.Es[i] = op.Proj.Es[i]
			proj.Rs[i] = op.Proj.Rs[i]
		}
		arg.Projection = proj
	}
	return arg
}

func constructTransform(op *plan.Relation) *transform.Argument {
	arg := new(transform.Argument)
	if len(op.FreeVars) == 0 {
		arg.Typ = transform.BoundVars
	} else {
		arg.Typ = transform.FreeVarsAndBoundVars
		arg.FreeVars = append(arg.FreeVars, op.FreeVars...)
	}
	for _, bvar := range op.BoundVars {
		arg.BoundVars = append(arg.BoundVars, transformer.Transformer{
			Ref:   bvar.Ref,
			Op:    bvar.Op,
			Name:  bvar.Name,
			Alias: bvar.Alias,
		})
	}
	if op.Cond != nil {
		arg.Restrict = &restrict.Argument{E: op.Cond}
	}
	if n := len(op.Proj.Es); n > 0 {
		proj := &projection.Argument{
			Rs: make([]uint64, n),
			As: make([]string, n),
			Es: make([]extend.Extend, n),
		}
		for i := range op.Proj.Es {
			proj.As[i] = op.Proj.As[i]
			proj.Es[i] = op.Proj.Es[i]
			proj.Rs[i] = op.Proj.Rs[i]
		}
		arg.Projection = proj
	}
	return arg
}

func constructTransformFromDerived(op *plan.DerivedRelation) *transform.Argument {
	arg := new(transform.Argument)
	if len(arg.FreeVars) == 0 {
		arg.Typ = transform.BoundVars
	} else {
		arg.Typ = transform.FreeVarsAndBoundVars
		arg.FreeVars = append(arg.FreeVars, op.FreeVars...)
	}
	for _, bvar := range op.BoundVars {
		arg.BoundVars = append(arg.BoundVars, transformer.Transformer{
			Ref:   bvar.Ref,
			Op:    bvar.Op,
			Name:  bvar.Name,
			Alias: bvar.Alias,
		})
	}
	if op.Cond != nil {
		arg.Restrict = &restrict.Argument{E: op.Cond}
	}
	if n := len(op.Proj.Es); n > 0 {
		proj := &projection.Argument{
			Rs: make([]uint64, n),
			As: make([]string, n),
			Es: make([]extend.Extend, n),
		}
		for i := range op.Proj.Es {
			proj.As[i] = op.Proj.As[i]
			proj.Es[i] = op.Proj.Es[i]
			proj.Rs[i] = op.Proj.Rs[i]
		}
		arg.Projection = proj
	}
	return arg
}

func constructCAQTransform(op *plan.Relation) *transform.Argument {
	arg := new(transform.Argument)
	arg.IsMerge = true
	if len(op.FreeVars) == 0 {
		arg.Typ = transform.BoundVars
	} else {
		arg.Typ = transform.FreeVarsAndBoundVars
		arg.FreeVars = append(arg.FreeVars, op.FreeVars...)
	}
	for _, bvar := range op.BoundVars {
		arg.BoundVars = append(arg.BoundVars, transformer.Transformer{
			Ref:   bvar.Ref,
			Op:    bvar.Op,
			Name:  bvar.Name,
			Alias: bvar.Alias,
		})
	}
	if op.Cond != nil {
		arg.Restrict = &restrict.Argument{E: op.Cond}
	}
	if n := len(op.Proj.Es); n > 0 {
		proj := &projection.Argument{
			Rs: make([]uint64, n),
			As: make([]string, n),
			Es: make([]extend.Extend, n),
		}
		for i := range op.Proj.Es {
			proj.As[i] = op.Proj.As[i]
			proj.Es[i] = op.Proj.Es[i]
			proj.Rs[i] = op.Proj.Rs[i]
		}
		arg.Projection = proj
	}
	return arg
}

func constructCAQTransformFromDerived(op *plan.DerivedRelation) *transform.Argument {
	arg := new(transform.Argument)
	arg.IsMerge = true
	if len(arg.FreeVars) == 0 {
		arg.Typ = transform.BoundVars
	} else {
		arg.Typ = transform.FreeVarsAndBoundVars
		arg.FreeVars = append(arg.FreeVars, op.FreeVars...)
	}
	for _, bvar := range op.BoundVars {
		arg.BoundVars = append(arg.BoundVars, transformer.Transformer{
			Ref:   bvar.Ref,
			Op:    bvar.Op,
			Name:  bvar.Name,
			Alias: bvar.Alias,
		})
	}
	if op.Cond != nil {
		arg.Restrict = &restrict.Argument{E: op.Cond}
	}
	if n := len(op.Proj.Es); n > 0 {
		proj := &projection.Argument{
			Rs: make([]uint64, n),
			As: make([]string, n),
			Es: make([]extend.Extend, n),
		}
		for i := range op.Proj.Es {
			proj.As[i] = op.Proj.As[i]
			proj.Es[i] = op.Proj.Es[i]
			proj.Rs[i] = op.Proj.Rs[i]
		}
		arg.Projection = proj
	}
	return arg
}

func constructJoin(op *plan.Join) *join.Argument {
	arg := new(join.Argument)
	arg.Vars = make([][]string, len(op.Vars)-1)
	for i := 1; i < len(op.Vars); i++ {
		arg.Vars[i-1] = make([]string, len(op.Vars[i]))
		for j := range op.Vars[i] {
			arg.Vars[i-1][j] = strconv.Itoa(op.Vars[i][j])
		}
	}
	arg.Result = append(arg.Result, op.Result...)
	return arg
}

func constructTimes(op *plan.Join) *times.Argument {
	arg := new(times.Argument)
	arg.Vars = make([][]string, len(op.Vars)-1)
	for i := 1; i < len(op.Vars); i++ {
		arg.Vars[i-1] = make([]string, len(op.Vars[i]))
		for j := range op.Vars[i] {
			arg.Vars[i-1][j] = strconv.Itoa(op.Vars[i][j])
		}
	}
	arg.Result = append(arg.Result, op.Result...)
	return arg
}
