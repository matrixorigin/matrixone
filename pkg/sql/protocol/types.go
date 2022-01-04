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

package protocol

import (
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transform"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transformer"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

const (
	Attr = iota
	Unary
	Binary
	Multi
	Paren
	Func
	Star
	Value
)

const (
	DefaultRing = iota
	AvgRing
	CountRing
	StarCountRing
	// Max
	MaxInt8Ring
	MaxInt32Ring
	MaxInt16Ring
	MaxInt64Ring
	MaxUInt8Ring
	MaxUInt16Ring
	MaxUInt32Ring
	MaxUInt64Ring
	MaxFloat32Ring
	MaxFloat64Ring
	MaxStrRing
	// Min
	MinInt8Ring
	MinInt32Ring
	MinInt16Ring
	MinInt64Ring
	MinUInt8Ring
	MinUInt16Ring
	MinUInt32Ring
	MinUInt64Ring
	MinFloat32Ring
	MinFloat64Ring
	MinStrRing
	// Sum
	SumIntRing
	SumUIntRing
	SumFloatRing
)

// colexec

type Field struct {
	Attr string
	Type int8
}

type OffsetArgument struct {
	Seen   uint64
	Offset uint64
}

type LimitArgument struct {
	Seen   uint64
	Limit uint64
}

type OrderArgument struct {
	Fs []Field
}

type OplusArgument struct {
	Typ int
}

type OutputArgument struct {
	Attrs []string
}

type ProjectionArgument struct {
	Rs []uint64
	As []string
}

type RestrictArgument struct {
	Attrs []string
}

type TopArgument struct {
	Limit int64
	Fs    []Field
}

type MergeArgument struct {
}

type DedupArgument struct {
}

// viewexec

type PlusArgument struct {
	Typ int
}

type Transformer struct {
	Op    int
	Ref   int
	Name  string
	Alias string
}

type TransformArgument struct {
	IsNull      bool
	Typ			int
	IsMerge 	bool
	FreeVars 	[]string
	HasRestrict bool
	Restrict 	RestrictArgument
	HasProj     bool
	Projection 	ProjectionArgument
	BoundVars 	[]Transformer
}

func TransferTransformArg(arg *transform.Argument) TransformArgument {
	if arg == nil {
		return TransformArgument{IsNull: true}
	}
	var ra RestrictArgument
	hasRestrict := false
	if arg.Restrict != nil {
		hasRestrict = true
		ra = RestrictArgument{Attrs: arg.Restrict.Attrs}
	}
	var pa ProjectionArgument
	hasProjection := false
	if arg.Projection != nil {
		hasProjection = true
		pa = ProjectionArgument{
			Rs: arg.Projection.Rs,
			As:	arg.Projection.As,
		}
	}
	var bv []Transformer
	if arg.BoundVars != nil {
		bv = make([]Transformer, len(arg.BoundVars))
		for i, b := range arg.BoundVars {
			bv[i].Op = b.Op
			bv[i].Ref = b.Ref
			bv[i].Name = b.Name
			bv[i].Alias = b.Alias
		}
	}
	return TransformArgument{
		IsNull: false,
		Typ: arg.Typ,
		IsMerge: arg.IsMerge,
		FreeVars: arg.FreeVars,
		HasRestrict: hasRestrict,
		Restrict: ra,
		HasProj: hasProjection,
		Projection: pa,
		BoundVars: bv,
	}
}

func UntransferTransformArg(arg TransformArgument) *transform.Argument{
	if arg.IsNull {
		return nil
	}
	var ra *restrict.Argument
	if arg.HasRestrict {
		ra = &restrict.Argument{
			Attrs: arg.Restrict.Attrs,
		}
	}
	var pa *projection.Argument
	if arg.HasProj {
		pa = &projection.Argument{
			Rs: arg.Projection.Rs,
			As: arg.Projection.As,
		}
	}
	var bv []transformer.Transformer
	if arg.BoundVars != nil {
		bv = make([]transformer.Transformer, len(arg.BoundVars))
		for i, b := range arg.BoundVars {
			bv[i].Op = b.Op
			bv[i].Ref = b.Ref
			bv[i].Name = b.Name
			bv[i].Alias = b.Alias
		}
	}
	return &transform.Argument{
		Typ: arg.Typ,
		IsMerge: arg.IsMerge,
		FreeVars: arg.FreeVars,
		Restrict: ra,
		Projection: pa,
		BoundVars: bv,
	}
}

type TimesArgument struct {
	IsBare   bool
	R	     string
	Rvars    []string
	Ss       []string
	Svars    []string
	FreeVars []string
	VarsMap  map[string]int
	Arg		 TransformArgument
}

type UntransformArgument struct {
	Type     int
	FreeVars []string
}

type Source struct {
	IsMerge      bool
	SchemaName   string
	RelationName string
	RefCounts    []uint64
	Attributes   []string
}

type Node struct {
	Id	  string
	Addr  string
}

type Scope struct {
	Magic 	  		int
	DataSource      Source
	PreScopes 		[]Scope
	NodeInfo 		Node
	Ins		  		vm.Instructions
}