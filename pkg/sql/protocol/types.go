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
	"reflect"
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
	Int8Ring
	Int32Ring
	Int16Ring
	Int64Ring
	UInt8Ring
	UInt16Ring
	UInt32Ring
	UInt64Ring
	Float32Ring
	Float64Ring
	StrRing
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
	Typ			int
	IsMerge 	bool
	FreeVars 	[]string
	Restrict 	RestrictArgument
	Projection 	ProjectionArgument
	BoundVars 	[]Transformer
}

func TransferTransformArg(arg *transform.Argument) TransformArgument {
	if arg == nil {
		return TransformArgument{}
	}
	var ra RestrictArgument
	if arg.Restrict != nil {
		ra = RestrictArgument{Attrs: arg.Restrict.Attrs}
	}
	var pa ProjectionArgument
	if arg.Projection != nil {
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
		Typ: arg.Typ,
		IsMerge: arg.IsMerge,
		FreeVars: arg.FreeVars,
		Restrict: ra,
		Projection: pa,
		BoundVars: bv,
	}
}

func UntransferTransformArg(arg TransformArgument) *transform.Argument{
	if reflect.DeepEqual(arg, TransformArgument{}) {
		return nil
	}
	var ra *restrict.Argument
	if !reflect.DeepEqual(arg.Restrict, RestrictArgument{}) {
		ra = &restrict.Argument{
			Attrs: arg.Restrict.Attrs,
		}
	}
	var pa *projection.Argument
	if !reflect.DeepEqual(arg.Projection, ProjectionArgument{}) {
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
	IsBare  bool
	R	    string
	Rvars   []string
	Ss      []string
	Svars   []string
	VarsMap map[string]int
	Arg		TransformArgument
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