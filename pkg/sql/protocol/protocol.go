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
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/ring/bitand"
	"github.com/matrixorigin/matrixone/pkg/container/ring/bitor"
	"github.com/matrixorigin/matrixone/pkg/container/ring/bitxor"
	"github.com/matrixorigin/matrixone/pkg/container/ring/stddevpop"
	"github.com/matrixorigin/matrixone/pkg/container/ring/variance"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/ring/approxcd"
	"github.com/matrixorigin/matrixone/pkg/container/ring/avg"
	"github.com/matrixorigin/matrixone/pkg/container/ring/count"
	"github.com/matrixorigin/matrixone/pkg/container/ring/max"
	"github.com/matrixorigin/matrixone/pkg/container/ring/min"
	"github.com/matrixorigin/matrixone/pkg/container/ring/starcount"
	"github.com/matrixorigin/matrixone/pkg/container/ring/sum"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/oplus"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/plus"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/times"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transform"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/untransform"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	gob.Register(Field{})
	gob.Register(OffsetArgument{})
	gob.Register(LimitArgument{})
	gob.Register(OrderArgument{})
	gob.Register(OplusArgument{})
	gob.Register(OutputArgument{})
	gob.Register(ProjectionArgument{})
	gob.Register(RestrictArgument{})
	gob.Register(TopArgument{})
	gob.Register(MergeArgument{})
	gob.Register(DedupArgument{})

	gob.Register(PlusArgument{})
	gob.Register(TransformArgument{})
	gob.Register(Transformer{})
	gob.Register(JoinArgument{})
	gob.Register(TimesArgument{})
	gob.Register(UntransformArgument{})

	gob.Register(Source{})
	gob.Register(Node{})
	gob.Register(Scope{})

	gob.Register(types.Date(0))
	gob.Register(types.Datetime(0))
}

func EncodeScope(s Scope, buf *bytes.Buffer) error {
	// Magic
	buf.Write(encoding.EncodeUint32(uint32(s.Magic)))
	// DataSource
	data, err := encoding.Encode(s.DataSource)
	if err != nil {
		return err
	}
	buf.Write(encoding.EncodeUint32(uint32(len(data))))
	buf.Write(data)
	// PreScopes
	buf.Write(encoding.EncodeUint32(uint32(len(s.PreScopes))))
	for i := range s.PreScopes {
		if err := EncodeScope(s.PreScopes[i], buf); err != nil {
			return err
		}
	}
	// Node
	data, err = encoding.Encode(s.NodeInfo)
	if err != nil {
		return err
	}
	buf.Write(encoding.EncodeUint32(uint32(len(data))))
	buf.Write(data)
	// Ins
	if err := EncodeInstructions(s.Ins, buf); err != nil {
		return err
	}
	return nil
}

func DecodeScope(data []byte) (Scope, []byte, error) {
	var s Scope
	var err error
	// Magic
	s.Magic = int(encoding.DecodeUint32(data[:4]))
	data = data[4:]
	// DataSource
	n := encoding.DecodeUint32(data[:4])
	data = data[4:]
	if err = encoding.Decode(data[:n], &s.DataSource); err != nil {
		return s, nil, err
	}
	data = data[n:]
	// PreScopes
	n = encoding.DecodeUint32(data[:4])
	data = data[4:]
	s.PreScopes = make([]Scope, n)
	for i := uint32(0); i < n; i++ {
		if s.PreScopes[i], data, err = DecodeScope(data); err != nil {
			return s, nil, err
		}
	}
	// Node
	n = encoding.DecodeUint32(data[:4])
	data = data[4:]
	if err = encoding.Decode(data[:n], &s.NodeInfo); err != nil {
		return s, nil, err
	}
	data = data[n:]
	// Ins
	if s.Ins, data, err = DecodeInstructions(data); err != nil {
		return s, nil, err
	}
	return s, data, nil
}

func EncodeInstructions(ins vm.Instructions, buf *bytes.Buffer) error {
	buf.Write(encoding.EncodeUint32(uint32(len(ins))))
	for _, in := range ins {
		if err := EncodeInstruction(in, buf); err != nil {
			return err
		}
	}
	return nil
}

func DecodeInstructions(data []byte) (vm.Instructions, []byte, error) {
	var ins vm.Instructions

	n := encoding.DecodeUint32(data[:4])
	data = data[4:]
	ins = make(vm.Instructions, n)
	for i := uint32(0); i < n; i++ {
		in, d, err := DecodeInstruction(data)
		if err != nil {
			return ins, nil, err
		}
		ins[i] = in
		data = d
	}
	return ins, data, nil
}

func EncodeInstruction(in vm.Instruction, buf *bytes.Buffer) error {
	buf.Write(encoding.EncodeUint32(uint32(in.Op)))
	switch in.Op {
	case vm.Top:
		arg := in.Arg.(*top.Argument)
		fs := make([]Field, len(arg.Fs))
		{
			for i, f := range arg.Fs {
				fs[i].Attr = f.Attr
				fs[i].Type = int8(f.Type)
			}
		}
		data, err := encoding.Encode(TopArgument{Limit: arg.Limit, Fs: fs})
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return nil
	case vm.Plus:
		arg := in.Arg.(*plus.Argument)
		data, err := encoding.Encode(PlusArgument{Typ: arg.Typ})
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return nil
	case vm.Limit:
		arg := in.Arg.(*limit.Argument)
		data, err := encoding.Encode(LimitArgument{Seen: arg.Seen, Limit: arg.Limit})
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return nil
	case vm.Join:
		arg := in.Arg.(*join.Argument)
		data, err := encoding.Encode(JoinArgument{
			Vars:   arg.Vars,
			Result: arg.Result,
		})
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return nil
	case vm.Times:
		arg := in.Arg.(*times.Argument)
		data, err := encoding.Encode(TimesArgument{
			Vars:   arg.Vars,
			Result: arg.Result,
		})
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return nil
	case vm.Merge:
		// arg := in.Arg.(*merge.Argument)
		data, err := encoding.Encode(MergeArgument{})
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return nil
	case vm.Dedup:
		// arg := in.Arg.(*dedup.Argument)
		data, err := encoding.Encode(DedupArgument{})
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return nil
	case vm.Order:
		arg := in.Arg.(*order.Argument)
		fs := make([]Field, len(arg.Fs))
		{
			for i, f := range arg.Fs {
				fs[i].Attr = f.Attr
				fs[i].Type = int8(f.Type)
			}
		}
		data, err := encoding.Encode(OrderArgument{Fs: fs})
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return nil
	case vm.Oplus:
		arg := in.Arg.(*oplus.Argument)
		data, err := encoding.Encode(OplusArgument{Typ: arg.Typ})
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return nil
	case vm.Output:
		arg := in.Arg.(*output.Argument)
		data, err := encoding.Encode(OutputArgument{Attrs: arg.Attrs})
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return nil
	case vm.Offset:
		arg := in.Arg.(*offset.Argument)
		data, err := encoding.Encode(OffsetArgument{Seen: arg.Seen, Offset: arg.Offset})
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return nil
	case vm.Restrict:
		arg := in.Arg.(*restrict.Argument)
		data, err := encoding.Encode(RestrictArgument{Attrs: arg.Attrs})
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return EncodeExtend(arg.E, buf)
	case vm.Connector:
	case vm.Transform:
		arg := in.Arg.(*transform.Argument)
		transArg := TransferTransformArg(arg)
		data, err := encoding.Encode(transArg)
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		if arg != nil {
			if arg.Restrict != nil {
				if err = EncodeExtend(arg.Restrict.E, buf); err != nil {
					return err
				}
			}
			if arg.Projection != nil {
				buf.Write(encoding.EncodeUint32(uint32(len(arg.Projection.Es))))
				for _, e := range arg.Projection.Es {
					if err = EncodeExtend(e, buf); err != nil {
						return err
					}
				}
			}
		}
		return nil
	case vm.Projection:
		arg := in.Arg.(*projection.Argument)
		data, err := encoding.Encode(ProjectionArgument{Rs: arg.Rs, As: arg.As})
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		buf.Write(encoding.EncodeUint32(uint32(len(arg.Es))))
		for _, e := range arg.Es {
			if err = EncodeExtend(e, buf); err != nil {
				return err
			}
		}
		return nil
	case vm.UnTransform:
		arg := in.Arg.(*untransform.Argument)
		data, err := encoding.Encode(UntransformArgument{Type: arg.Type, FreeVars: arg.FreeVars})
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return nil
	}
	return nil
}

func DecodeInstruction(data []byte) (vm.Instruction, []byte, error) {
	var in vm.Instruction
	switch in.Op = int(encoding.DecodeUint32(data[:4])); in.Op {
	case vm.Top:
		var arg TopArgument
		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		fs := make([]top.Field, len(arg.Fs))
		{
			for i, f := range arg.Fs {
				fs[i].Attr = f.Attr
				fs[i].Type = top.Direction(f.Type)
			}
		}
		in.Arg = &top.Argument{
			Fs:    fs,
			Limit: arg.Limit,
		}
		data = data[n:]
	case vm.Plus:
		var arg PlusArgument
		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		in.Arg = &plus.Argument{
			Typ: arg.Typ,
		}
		data = data[n:]
	case vm.Limit:
		var arg LimitArgument
		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		in.Arg = &limit.Argument{
			Seen:  arg.Seen,
			Limit: arg.Limit,
		}
		data = data[n:]
	case vm.Join:
		var arg JoinArgument

		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		joinArg := &join.Argument{
			Vars:   arg.Vars,
			Result: arg.Result,
		}
		data = data[n:]
		in.Arg = joinArg
	case vm.Times:
		var arg TimesArgument

		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		timesArg := &times.Argument{
			Vars:   arg.Vars,
			Result: arg.Result,
		}
		data = data[n:]
		in.Arg = timesArg
	case vm.Merge:
		var arg MergeArgument
		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		in.Arg = &merge.Argument{}
		data = data[n:]
	case vm.Dedup:
		var arg DedupArgument
		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		in.Arg = &dedup.Argument{}
		data = data[n:]
	case vm.Order:
		var arg OrderArgument
		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		fs := make([]order.Field, len(arg.Fs))
		{
			for i, f := range arg.Fs {
				fs[i].Attr = f.Attr
				fs[i].Type = order.Direction(f.Type)
			}
		}
		in.Arg = &order.Argument{
			Fs: fs,
		}
		data = data[n:]
	case vm.Oplus:
		var arg OplusArgument
		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		in.Arg = &oplus.Argument{
			Typ: arg.Typ,
		}
		data = data[n:]
	case vm.Output:
		var arg OutputArgument
		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		in.Arg = &output.Argument{
			Attrs: arg.Attrs,
		}
		data = data[n:]
	case vm.Offset:
		var arg OffsetArgument
		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		in.Arg = &offset.Argument{
			Seen:   arg.Seen,
			Offset: arg.Offset,
		}
		data = data[n:]
	case vm.Restrict:
		var arg RestrictArgument
		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		data = data[n:]
		e, d, err := DecodeExtend(data)
		if err != nil {
			return in, nil, err
		}
		in.Arg = &restrict.Argument{
			Attrs: arg.Attrs,
			E:     e,
		}
		data = d
	case vm.Connector:
		data = data[4:]
		in.Arg = &connector.Argument{}
	case vm.Transform:
		var arg TransformArgument
		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		data = data[n:]
		transArg := UntransferTransformArg(arg)
		if transArg != nil {
			if transArg.Restrict != nil {
				e, d, err := DecodeExtend(data)
				if err != nil {
					return in, nil, err
				}
				data = d
				transArg.Restrict.E = e
			}
			if transArg.Projection != nil {
				n = encoding.DecodeUint32(data[:4])
				data = data[4:]
				es := make([]extend.Extend, n)
				for i := uint32(0); i < n; i++ {
					e, d, err := DecodeExtend(data)
					if err != nil {
						return in, nil, err
					}
					es[i] = e
					data = d
				}
				transArg.Projection.Es = es
			}
		}
		in.Arg = transArg
	case vm.Projection:
		var arg ProjectionArgument
		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		data = data[n:]
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		es := make([]extend.Extend, n)
		for i := uint32(0); i < n; i++ {
			e, d, err := DecodeExtend(data)
			if err != nil {
				return in, nil, err
			}
			es[i] = e
			data = d
		}
		in.Arg = &projection.Argument{
			Es: es,
			As: arg.As,
			Rs: arg.Rs,
		}
	case vm.UnTransform:
		var arg UntransformArgument
		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		in.Arg = &untransform.Argument{
			Type:     arg.Type,
			FreeVars: arg.FreeVars,
		}
		data = data[n:]
	}
	return in, data, nil
}

func EncodeExtend(e extend.Extend, buf *bytes.Buffer) error {
	switch v := e.(type) {
	case *extend.UnaryExtend:
		buf.WriteByte(Unary)
		buf.Write(encoding.EncodeUint32(uint32(v.Op)))
		return EncodeExtend(v.E, buf)
	case *extend.BinaryExtend:
		buf.WriteByte(Binary)
		buf.Write(encoding.EncodeUint32(uint32(v.Op)))
		if err := EncodeExtend(v.Left, buf); err != nil {
			return err
		}
		return EncodeExtend(v.Right, buf)
	case *extend.MultiExtend:
		buf.WriteByte(Multi)
		buf.Write(encoding.EncodeUint32(uint32(v.Op)))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Args))))
		for _, arg := range v.Args {
			if err := EncodeExtend(arg, buf); err != nil {
				return err
			}
		}
		return nil
	case *extend.ParenExtend:
		buf.WriteByte(Paren)
		return EncodeExtend(v.E, buf)
	case *extend.FuncExtend:
		buf.WriteByte(Func)
		buf.Write(encoding.EncodeUint32(uint32(len(v.Name))))
		buf.WriteString(v.Name)
		buf.Write(encoding.EncodeUint32(uint32(len(v.Args))))
		for _, arg := range v.Args {
			if err := EncodeExtend(arg, buf); err != nil {
				return err
			}
		}
		return nil
	case *extend.StarExtend:
		buf.WriteByte(Star)
		return nil
	case *extend.ValueExtend:
		buf.WriteByte(Value)
		return EncodeVector(v.V, buf)
	case *extend.Attribute:
		buf.WriteByte(Attr)
		buf.Write(encoding.EncodeUint16(uint16(v.Type)))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Name))))
		buf.WriteString(v.Name)
		return nil
	}
	return fmt.Errorf("'%v' not yet support", e)
}

func DecodeExtend(data []byte) (extend.Extend, []byte, error) {
	switch data[0] {
	case Unary:
		e := new(extend.UnaryExtend)
		data = data[1:]
		e.Op = int(encoding.DecodeUint32(data[:4]))
		data = data[4:]
		ext, data, err := DecodeExtend(data)
		if err != nil {
			return nil, nil, err
		}
		e.E = ext
		return e, data, nil
	case Binary:
		e := new(extend.BinaryExtend)
		data = data[1:]
		e.Op = int(encoding.DecodeUint32(data[:4]))
		data = data[4:]
		le, data, err := DecodeExtend(data)
		if err != nil {
			return nil, nil, err
		}
		re, data, err := DecodeExtend(data)
		if err != nil {
			return nil, nil, err
		}
		e.Left, e.Right = le, re
		return e, data, nil
	case Multi:
		e := new(extend.MultiExtend)
		data = data[1:]
		e.Op = int(encoding.DecodeUint32(data[:4]))
		data = data[4:]
		if n := encoding.DecodeUint32(data); n > 0 {
			data = data[4:]
			for i := uint32(0); i < n; i++ {
				ext, d, err := DecodeExtend(data)
				if err != nil {
					return nil, nil, err
				}
				e.Args = append(e.Args, ext)
				data = d
			}
		} else {
			data = data[4:]
		}
		return e, data, nil
	case Paren:
		e := new(extend.ParenExtend)
		data = data[1:]
		ext, data, err := DecodeExtend(data)
		if err != nil {
			return nil, nil, err
		}
		e.E = ext
		return e, data, nil
	case Func:
		e := new(extend.FuncExtend)
		data = data[1:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			e.Name = string(data[:n])
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data); n > 0 {
			data = data[4:]
			for i := uint32(0); i < n; i++ {
				ext, d, err := DecodeExtend(data)
				if err != nil {
					return nil, nil, err
				}
				e.Args = append(e.Args, ext)
				data = d
			}
		} else {
			data = data[4:]
		}
		return e, data, nil
	case Star:
		e := new(extend.StarExtend)
		return e, data, nil
	case Value:
		e := new(extend.ValueExtend)
		data = data[1:]
		v, data, err := DecodeVector(data)
		if err != nil {
			return nil, nil, err
		}
		e.V = v
		return e, data, nil
	case Attr:
		e := new(extend.Attribute)
		data = data[1:]
		e.Type = types.T(encoding.DecodeUint16(data[:2]))
		data = data[2:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			e.Name = string(data[:n])
			data = data[n:]
		} else {
			data = data[4:]
		}
		return e, data, nil
	}
	return nil, nil, fmt.Errorf("'%v' extend not yet support", data[0])
}

func EncodeBatch(bat *batch.Batch, buf *bytes.Buffer) error {
	// SelsData
	buf.Write(encoding.EncodeUint32(uint32(len(bat.SelsData))))
	buf.Write(bat.SelsData)
	// Sels
	sn := len(bat.Sels)
	buf.Write(encoding.EncodeUint32(uint32(sn)))
	if sn > 0 {
		buf.Write(encoding.EncodeInt64Slice(bat.Sels))
	}
	// Attrs
	data, err := encoding.Encode(bat.Attrs)
	if err != nil {
		return err
	}
	buf.Write(encoding.EncodeUint32(uint32(len(data))))
	buf.Write(data)
	// Vecs
	buf.Write(encoding.EncodeUint32(uint32(len(bat.Vecs))))
	for _, vec := range bat.Vecs {
		if err := EncodeVector(vec, buf); err != nil {
			return err
		}
	}
	// Zs
	zn := len(bat.Zs)
	buf.Write(encoding.EncodeUint32(uint32(zn)))
	if zn > 0 {
		buf.Write(encoding.EncodeInt64Slice(bat.Zs))
	}
	// As
	data, err = encoding.Encode(bat.As)
	if err != nil {
		return err
	}
	buf.Write(encoding.EncodeUint32(uint32(len(data))))
	buf.Write(data)
	// Refs
	rn := len(bat.Refs)
	buf.Write(encoding.EncodeUint32(uint32(rn)))
	if rn > 0 {
		buf.Write(encoding.EncodeUint64Slice(bat.Refs))
	}
	// Rs
	n := len(bat.Rs)
	buf.Write(encoding.EncodeUint32(uint32(n)))
	if n > 0 {
		for i := 0; i < n; i++ {
			err := EncodeRing(bat.Rs[i], buf)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func DecodeBatch(data []byte) (*batch.Batch, []byte, error) {
	bat := batch.New(true, []string{})
	// SelsData
	n := encoding.DecodeUint32(data[:4])
	data = data[4:]
	bat.SelsData = data[:n]
	data = data[n:]
	// Sels
	sn := encoding.DecodeUint32(data[:4])
	data = data[4:]
	if sn > 0 {
		bat.Sels = encoding.DecodeInt64Slice(data[:sn*8])
		data = data[sn*8:]
	}
	// Attrs
	if n := encoding.DecodeUint32(data); n > 0 {
		data = data[4:]
		if err := encoding.Decode(data[:n], &bat.Attrs); err != nil {
			return nil, nil, err
		}
		data = data[n:]
	} else {
		data = data[4:]
	}
	// Vecs
	if n := encoding.DecodeUint32(data); n > 0 {
		data = data[4:]
		for i := uint32(0); i < n; i++ {
			vec, remaining, err := DecodeVector(data)
			if err != nil {
				return nil, nil, err
			}
			bat.Vecs = append(bat.Vecs, vec)
			data = remaining
		}
	} else {
		data = data[4:]
	}
	// Zs
	zn := encoding.DecodeUint32(data[:4])
	data = data[4:]
	if zn > 0 {
		bat.Zs = encoding.DecodeInt64Slice(data[:zn*8])
		data = data[zn*8:]
	}
	// As
	if n := encoding.DecodeUint32(data); n > 0 {
		data = data[4:]
		if err := encoding.Decode(data[:n], &bat.As); err != nil {
			return nil, nil, err
		}
		data = data[n:]
	} else {
		data = data[4:]
	}
	// Refs
	rn := encoding.DecodeUint32(data[:4])
	data = data[4:]
	if rn > 0 {
		bat.Refs = encoding.DecodeUint64Slice(data[:rn*8])
		data = data[rn*8:]
	}
	// Rs
	n = encoding.DecodeUint32(data[:4])
	data = data[4:]
	if n > 0 {
		for i := uint32(0); i < n; i++ {
			r, d, err := DecodeRing(data)
			if err != nil {
				return nil, nil, err
			}
			data = d
			bat.Rs = append(bat.Rs, r)
		}
	}
	return bat, data, nil
}

func DecodeBatchWithProcess(data []byte, proc *process.Process) (*batch.Batch, []byte, error) {
	bat := batch.New(true, []string{})
	// SelsData
	n := encoding.DecodeUint32(data[:4])
	data = data[4:]
	bat.SelsData = data[:n]
	data = data[n:]
	// Sels
	sn := encoding.DecodeUint32(data[:4])
	data = data[4:]
	if sn > 0 {
		bat.Sels = encoding.DecodeInt64Slice(data[:sn*8])
		data = data[sn*8:]
	}
	// Attrs
	if n := encoding.DecodeUint32(data); n > 0 {
		data = data[4:]
		if err := encoding.Decode(data[:n], &bat.Attrs); err != nil {
			return nil, nil, err
		}
		data = data[n:]
	} else {
		data = data[4:]
	}
	// Vecs
	if n := encoding.DecodeUint32(data); n > 0 {
		data = data[4:]
		for i := uint32(0); i < n; i++ {
			vec, remaining, err := DecodeVector(data)
			if err != nil {
				return nil, nil, err
			}
			bat.Vecs = append(bat.Vecs, vec)
			data = remaining
		}
		var err error
		for i, vec := range bat.Vecs {
			if bat.Vecs[i], err = vector.Dup(vec, proc.Mp); err != nil {
				return nil, nil, err
			}
		}
	} else {
		data = data[4:]
	}
	// Zs
	zn := encoding.DecodeUint32(data[:4])
	data = data[4:]
	if zn > 0 {
		bat.Zs = make([]int64, zn)
		copy(bat.Zs, encoding.DecodeInt64Slice(data[:zn*8]))
		data = data[zn*8:]
	}
	// As
	if n := encoding.DecodeUint32(data); n > 0 {
		data = data[4:]
		if err := encoding.Decode(data[:n], &bat.As); err != nil {
			return nil, nil, err
		}
		data = data[n:]
	} else {
		data = data[4:]
	}
	// Refs
	rn := encoding.DecodeUint32(data[:4])
	data = data[4:]
	if rn > 0 {
		bat.Refs = encoding.DecodeUint64Slice(data[:rn*8])
		data = data[rn*8:]
	}
	// Rs
	n = encoding.DecodeUint32(data[:4])
	data = data[4:]
	if n > 0 {
		for i := uint32(0); i < n; i++ {
			r, d, err := DecodeRingWithProcess(data, proc)
			if err != nil {
				return nil, nil, err
			}
			data = d
			bat.Rs = append(bat.Rs, r)
		}
	}
	return bat, data, nil
}

func EncodeRing(r ring.Ring, buf *bytes.Buffer) error {
	switch v := r.(type) {
	case *stddevpop.StdDevPopRing:
		buf.WriteByte(StdDevPopRing)
		n := len(v.NullCounts)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.NullCounts))
		}
		// Sumx2
		n = len(v.SumX2)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeFloat64Slice(v.SumX2))
		}
		// Sumx
		da := encoding.EncodeFloat64Slice(v.SumX)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *bitand.NumericRing:
		buf.WriteByte(BitAndNumericRing)
		// nsp
		n := len(v.NullCnt)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.NullCnt))
		}
		// BitAndResult
		data := encoding.EncodeUint64Slice(v.BitAndResult)
		n = len(data)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(data)
		}
		// typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *avg.AvgRing:
		buf.WriteByte(AvgRing)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeFloat64Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *count.CountRing:
		buf.WriteByte(CountRing)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeInt64Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *starcount.CountRing:
		buf.WriteByte(StarCountRing)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeInt64Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *approxcd.ApproxCountDistinctRing:
		buf.WriteByte(ApproxCountDistinctRing)
		return v.Marshal(buf)
	case *max.Int8Ring:
		buf.WriteByte(MaxInt8Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeInt8Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *max.Int16Ring:
		buf.WriteByte(MaxInt16Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeInt16Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *max.Int32Ring:
		buf.WriteByte(MaxInt32Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeInt32Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *max.DateRing:
		buf.WriteByte(MaxDateRing)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeDateSlice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *max.Int64Ring:
		buf.WriteByte(MaxInt64Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeInt64Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *max.DatetimeRing:
		buf.WriteByte(MaxDatetimeRing)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeDatetimeSlice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *max.UInt8Ring:
		buf.WriteByte(MaxUInt8Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeUint8Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *max.UInt16Ring:
		buf.WriteByte(MaxUInt16Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeUint16Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *max.UInt32Ring:
		buf.WriteByte(MaxUInt32Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeUint32Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *max.UInt64Ring:
		buf.WriteByte(MaxUInt64Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeUint64Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *max.Float32Ring:
		buf.WriteByte(MaxFloat32Ring)
		// IsE
		/*
			var isE uint8
			if v.IsE {
				isE = 1
			}
			buf.Write(encoding.EncodeUint8(isE))
		*/
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeFloat32Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *max.Float64Ring:
		buf.WriteByte(MaxFloat64Ring)
		// IsE
		/*
			var isE uint8
			if v.IsE {
				isE = 1
			}
			buf.Write(encoding.EncodeUint8(isE))
		*/
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeFloat64Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *max.StrRing:
		buf.WriteByte(MaxStrRing)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		n = len(v.Vs)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			for i := 0; i < n; i++ {
				m := len(v.Vs[i])
				buf.Write(encoding.EncodeUint32(uint32(m)))
				if m > 0 {
					buf.Write(v.Vs[i])
				}
			}
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *min.Int8Ring:
		buf.WriteByte(MinInt8Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeInt8Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *min.Int16Ring:
		buf.WriteByte(MinInt16Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeInt16Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *min.Int32Ring:
		buf.WriteByte(MinInt32Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeInt32Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *min.DateRing:
		buf.WriteByte(MinDateRing)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeDateSlice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *min.Int64Ring:
		buf.WriteByte(MinInt64Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeInt64Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *min.DatetimeRing:
		buf.WriteByte(MinDatetimeRing)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeDatetimeSlice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *min.UInt8Ring:
		buf.WriteByte(MinUInt8Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeUint8Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *min.UInt16Ring:
		buf.WriteByte(MinUInt16Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeUint16Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *min.UInt32Ring:
		buf.WriteByte(MinUInt32Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeUint32Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *min.UInt64Ring:
		buf.WriteByte(MinUInt64Ring)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeUint64Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *min.Float32Ring:
		buf.WriteByte(MinFloat32Ring)
		// IsE
		/*
			var isE uint8
			if v.IsE {
				isE = 1
			}
			buf.Write(encoding.EncodeUint8(isE))
		*/
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeFloat32Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *min.Float64Ring:
		buf.WriteByte(MinFloat64Ring)
		// IsE
		/*
			var isE uint8
			if v.IsE {
				isE = 1
			}
			buf.Write(encoding.EncodeUint8(isE))
		*/
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeFloat64Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *min.StrRing:
		buf.WriteByte(MinStrRing)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		n = len(v.Vs)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			for i := 0; i < n; i++ {
				m := len(v.Vs[i])
				buf.Write(encoding.EncodeUint32(uint32(m)))
				if m > 0 {
					buf.Write(v.Vs[i])
				}
			}
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *sum.IntRing:
		buf.WriteByte(SumIntRing)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeInt64Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *sum.UIntRing:
		buf.WriteByte(SumUIntRing)
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeUint64Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *sum.FloatRing:
		buf.WriteByte(SumFloatRing)
		// IsE
		/*
			var isE uint8
			if v.IsE {
				isE = 1
			}
			buf.Write(encoding.EncodeUint8(isE))
		*/
		// Ns
		n := len(v.Ns)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.Ns))
		}
		// Vs
		da := encoding.EncodeFloat64Slice(v.Vs)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *variance.VarRing:
		buf.WriteByte(VarianceRing)
		// NullCounts
		n := len(v.NullCounts)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.NullCounts))
		}
		// Sumx2
		n = len(v.SumX2)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeFloat64Slice(v.SumX2))
		}
		// Sumx
		da := encoding.EncodeFloat64Slice(v.SumX)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *bitor.BitOrRing:
		buf.WriteByte(BitOrRing)
		// NullCounts
		n := len(v.NullCounts)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.NullCounts))
		}
		// Values
		da := encoding.EncodeUint64Slice(v.Values)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	case *bitxor.BitXorRing:
		buf.WriteByte(BitXorRing)
		// NullCounts
		n := len(v.NullCounts)
		buf.Write((encoding.EncodeUint32(uint32(n))))
		if n > 0 {
			buf.Write(encoding.EncodeInt64Slice(v.NullCounts))
		}
		// BitXor Results

		da := encoding.EncodeUint64Slice(v.Values)
		n = len(da)
		buf.Write(encoding.EncodeUint32(uint32(n)))
		if n > 0 {
			buf.Write(da)
		}
		// Typ
		buf.Write(encoding.EncodeType(v.Typ))
		return nil
	}
	return fmt.Errorf("'%v' ring not yet support", r)
}

func DecodeRing(data []byte) (ring.Ring, []byte, error) {
	switch data[0] {
	case StdDevPopRing:
		r := new(stddevpop.StdDevPopRing)
		data = data[1:]

		// decode NullCounts
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.NullCounts = make([]int64, n)
			copy(r.NullCounts, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// decode Sumx2
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.SumX2 = make([]float64, n)
			copy(r.SumX2, encoding.DecodeFloat64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// decode Sumx
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Data = data[:n]
			data = data[n:]
		}
		r.SumX = encoding.DecodeFloat64Slice(r.Data)
		// decode typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		// return
		return r, data, nil
	case BitAndNumericRing:
		r := new(bitand.NumericRing)
		data = data[1:]
		// NullCnt
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.NullCnt = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Data
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Data = data[:n]
			data = data[n:]
		}
		// BitAndResult
		r.BitAndResult = encoding.DecodeUint64Slice(r.Data)
		//Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case AvgRing:
		r := new(avg.AvgRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeFloat64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case CountRing:
		r := new(count.CountRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case StarCountRing:
		r := new(starcount.CountRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case ApproxCountDistinctRing:
		data = data[1:]
		r := approxcd.NewApproxCountDistinct(types.Type{})
		data, err := r.Unmarshal(data)
		return r, data, err
	case MaxInt8Ring:
		r := new(max.Int8Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt8Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxInt16Ring:
		r := new(max.Int16Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt16Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxInt32Ring:
		r := new(max.Int32Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt32Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxDateRing:
		r := new(max.DateRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeDateSlice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxInt64Ring:
		r := new(max.Int64Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxDatetimeRing:
		r := new(max.DatetimeRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeDatetimeSlice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxUInt8Ring:
		r := new(max.UInt8Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint8Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxUInt16Ring:
		r := new(max.UInt16Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint16Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxUInt32Ring:
		r := new(max.UInt32Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint32Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxUInt64Ring:
		r := new(max.UInt64Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxFloat32Ring:
		r := new(max.Float32Ring)
		data = data[1:]
		// IsE
		/*
			isE := encoding.DecodeUint8(data[:1])
			if isE > 0 {
				r.IsE = true
			}
			data = data[1:]
		*/
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeFloat32Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxFloat64Ring:
		r := new(max.Float64Ring)
		data = data[1:]
		// IsE
		/*
			isE := encoding.DecodeUint8(data[:1])
			if isE > 0 {
				r.IsE = true
			}
			data = data[1:]
		*/
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeFloat64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxStrRing:
		r := new(max.StrRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Vs
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Vs = make([][]byte, n)
			for i := uint32(0); i < n; i++ {
				m := encoding.DecodeUint32(data[:4])
				data = data[4:]
				if m > 0 {
					r.Vs[i] = data[:m]
					data = data[m:]
				}
			}
		}
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinInt8Ring:
		r := new(min.Int8Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt8Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinInt16Ring:
		r := new(min.Int16Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt16Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinInt32Ring:
		r := new(min.Int32Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt32Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinDateRing:
		r := new(min.DateRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeDateSlice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinInt64Ring:
		r := new(min.Int64Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinDatetimeRing:
		r := new(min.DatetimeRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeDatetimeSlice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinUInt8Ring:
		r := new(min.UInt8Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint8Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinUInt16Ring:
		r := new(min.UInt16Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint16Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinUInt32Ring:
		r := new(min.UInt32Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint32Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinUInt64Ring:
		r := new(min.UInt64Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinFloat32Ring:
		r := new(min.Float32Ring)
		data = data[1:]
		// IsE
		/*
			isE := encoding.DecodeUint8(data[:1])
			if isE > 0 {
				r.IsE = true
			}
			data = data[1:]
		*/
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeFloat32Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinFloat64Ring:
		r := new(min.Float64Ring)
		data = data[1:]
		// IsE
		/*
			isE := encoding.DecodeUint8(data[:1])
			if isE > 0 {
				r.IsE = true
			}
			data = data[1:]
		*/
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeFloat64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinStrRing:
		r := new(min.StrRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Vs
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Vs = make([][]byte, n)
			for i := uint32(0); i < n; i++ {
				m := encoding.DecodeUint32(data[:4])
				data = data[4:]
				if m > 0 {
					r.Vs[i] = data[:m]
					data = data[m:]
				}
			}
		}
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case SumIntRing:
		r := new(sum.IntRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case SumUIntRing:
		r := new(sum.UIntRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case SumFloatRing:
		r := new(sum.FloatRing)
		data = data[1:]
		// IsE
		/*
			isE := encoding.DecodeUint8(data[:1])
			if isE > 0 {
				r.IsE = true
			}
			data = data[1:]
		*/
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Da = data[:n]
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeFloat64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case VarianceRing:
		r := new(variance.VarRing)
		data = data[1:]

		// decode NullCounts
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.NullCounts = make([]int64, n)
			copy(r.NullCounts, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// decode Sumx2
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.SumX2 = make([]float64, n)
			copy(r.SumX2, encoding.DecodeFloat64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// decode Sumx
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Data = data[:n]
			data = data[n:]
		}
		r.SumX = encoding.DecodeFloat64Slice(r.Data)
		// decode typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		// return
		return r, data, nil
	case BitOrRing:
		r := new(bitor.BitOrRing)
		data = data[1:]
		// NullCounts
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.NullCounts = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// Data
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Data = data[:n]
			data = data[n:]
		}
		// Values
		r.Values = encoding.DecodeUint64Slice(r.Data)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case BitXorRing:
		result := new(bitxor.BitXorRing)
		data = data[1:]
		// decode NullCounts
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			result.NullCounts = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		}
		// decode bitxor results
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			result.Data = data[:n]
			data = data[n:]
		}
		result.Values = encoding.DecodeUint64Slice(result.Data)
		// decode type
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		result.Typ = typ
		return result, data, nil
	}
	return nil, nil, fmt.Errorf("type '%v' ring not yet support", data[0])
}

func DecodeRingWithProcess(data []byte, proc *process.Process) (ring.Ring, []byte, error) {
	switch data[0] {
	case StdDevPopRing:
		r := new(stddevpop.StdDevPopRing)
		data = data[1:]

		// decode NullCounts
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.NullCounts = make([]int64, n)
			copy(r.NullCounts, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// decode Sumx2
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.SumX2 = make([]float64, n)
			copy(r.SumX2, encoding.DecodeFloat64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// decode Sumx
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Data = data[:n]
			data = data[n:]
		}
		r.SumX = encoding.DecodeFloat64Slice(r.Data)
		// decode typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		// return
		return r, data, nil
	case BitAndNumericRing:
		r := new(bitand.NumericRing)
		data = data[1:]
		// NullCnt
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.NullCnt = make([]int64, n)
			copy(r.NullCnt, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Data
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Data, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Data, data[:n])
			data = data[n:]
		}
		// BitAndResult
		r.BitAndResult = encoding.DecodeUint64Slice(r.Data)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case AvgRing:
		r := new(avg.AvgRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeFloat64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case CountRing:
		r := new(count.CountRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case StarCountRing:
		r := new(starcount.CountRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case ApproxCountDistinctRing:
		data = data[1:]
		r := approxcd.NewApproxCountDistinct(types.Type{})
		data, err := r.UnmarshalWithProc(data, proc)
		return r, data, err
	case MaxInt8Ring:
		r := new(max.Int8Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt8Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxInt16Ring:
		r := new(max.Int16Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt16Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxInt32Ring:
		r := new(max.Int32Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt32Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxDateRing:
		r := new(max.DateRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeDateSlice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxInt64Ring:
		r := new(max.Int64Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxDatetimeRing:
		r := new(max.DatetimeRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeDatetimeSlice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxUInt8Ring:
		r := new(max.UInt8Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint8Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxUInt16Ring:
		r := new(max.UInt16Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint16Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxUInt32Ring:
		r := new(max.UInt32Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint32Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxUInt64Ring:
		r := new(max.UInt64Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxFloat32Ring:
		r := new(max.Float32Ring)
		data = data[1:]
		// IsE
		/*
			isE := encoding.DecodeUint8(data[:1])
			if isE > 0 {
				r.IsE = true
			}
			data = data[1:]
		*/
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeFloat32Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxFloat64Ring:
		r := new(max.Float64Ring)
		data = data[1:]
		// IsE
		/*
			isE := encoding.DecodeUint8(data[:1])
			if isE > 0 {
				r.IsE = true
			}
			data = data[1:]
		*/
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeFloat64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MaxStrRing:
		r := new(max.StrRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Vs
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Vs = make([][]byte, n)
			for i := uint32(0); i < n; i++ {
				m := encoding.DecodeUint32(data[:4])
				data = data[4:]
				if m > 0 {
					var err error
					r.Vs[i], err = mheap.Alloc(proc.Mp, int64(m))
					if err != nil {
						return nil, nil, err
					}
					copy(r.Vs[i], data[:m])
					data = data[m:]
				}
			}
		}
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		r.Mp = proc.Mp
		return r, data, nil
	case MinInt8Ring:
		r := new(min.Int8Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt8Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinInt16Ring:
		r := new(min.Int16Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt16Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinInt32Ring:
		r := new(min.Int32Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt32Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinDateRing:
		r := new(min.DateRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeDateSlice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinInt64Ring:
		r := new(min.Int64Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinDatetimeRing:
		r := new(min.DatetimeRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeDatetimeSlice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinUInt8Ring:
		r := new(min.UInt8Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint8Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinUInt16Ring:
		r := new(min.UInt16Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint16Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinUInt32Ring:
		r := new(min.UInt32Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint32Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinUInt64Ring:
		r := new(min.UInt64Ring)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinFloat32Ring:
		r := new(min.Float32Ring)
		data = data[1:]
		// IsE
		/*
			isE := encoding.DecodeUint8(data[:1])
			if isE > 0 {
				r.IsE = true
			}
			data = data[1:]
		*/
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeFloat32Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinFloat64Ring:
		r := new(min.Float64Ring)
		data = data[1:]
		// IsE
		/*
			isE := encoding.DecodeUint8(data[:1])
			if isE > 0 {
				r.IsE = true
			}
			data = data[1:]
		*/
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeFloat64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case MinStrRing:
		r := new(min.StrRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Vs
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Vs = make([][]byte, n)
			for i := uint32(0); i < n; i++ {
				m := encoding.DecodeUint32(data[:4])
				data = data[4:]
				if m > 0 {
					var err error
					r.Vs[i], err = mheap.Alloc(proc.Mp, int64(m))
					if err != nil {
						return nil, nil, err
					}
					copy(r.Vs[i], data[:m])
					data = data[m:]
				}
			}
		}
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		r.Mp = proc.Mp
		return r, data, nil
	case SumIntRing:
		r := new(sum.IntRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeInt64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case SumUIntRing:
		r := new(sum.UIntRing)
		data = data[1:]
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeUint64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case SumFloatRing:
		r := new(sum.FloatRing)
		data = data[1:]
		// IsE
		/*
			isE := encoding.DecodeUint8(data[:1])
			if isE > 0 {
				r.IsE = true
			}
			data = data[1:]
		*/
		// Ns
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Ns = make([]int64, n)
			copy(r.Ns, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Da
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Da, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Da, data[:n])
			data = data[n:]
		}
		// Vs
		r.Vs = encoding.DecodeFloat64Slice(r.Da)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case VarianceRing:
		r := new(variance.VarRing)
		data = data[1:]

		// decode NullCounts
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.NullCounts = make([]int64, n)
			copy(r.NullCounts, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// decode Sumx2
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.SumX2 = make([]float64, n)
			copy(r.SumX2, encoding.DecodeFloat64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// decode Sumx
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.Data = data[:n]
			data = data[n:]
		}
		r.SumX = encoding.DecodeFloat64Slice(r.Data)
		// decode typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		// return
		return r, data, nil
	case BitOrRing:
		r := new(bitor.BitOrRing)
		data = data[1:]
		// NullCounts
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			r.NullCounts = make([]int64, n)
			copy(r.NullCounts, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// Data
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			r.Data, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(r.Data, data[:n])
			data = data[n:]
		}
		// Values
		r.Values = encoding.DecodeUint64Slice(r.Data)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		r.Typ = typ
		return r, data, nil
	case BitXorRing:
		result := new(bitxor.BitXorRing)
		data = data[1:]
		// decode NullCounts
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			result.NullCounts = make([]int64, n)
			copy(result.NullCounts, encoding.DecodeInt64Slice(data[:n*8]))
			data = data[n*8:]
		}
		// decode bitxor result
		n = encoding.DecodeUint32(data[:4])
		data = data[4:]
		if n > 0 {
			var err error
			result.Data, err = mheap.Alloc(proc.Mp, int64(n))
			if err != nil {
				return nil, nil, err
			}
			copy(result.Data, data[:n])
			data = data[n:]
		}
		result.Values = encoding.DecodeUint64Slice(result.Data)
		// Typ
		typ := encoding.DecodeType(data[:encoding.TypeSize])
		data = data[encoding.TypeSize:]
		result.Typ = typ
		return result, data, nil
	}
	return nil, nil, fmt.Errorf("type '%v' ring not yet support", data[0])
}

func EncodeVector(v *vector.Vector, buf *bytes.Buffer) error {
	switch v.Typ.Oid {
	case types.T_int8:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]int8)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeInt8Slice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_int16:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]int16)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeInt16Slice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_int32:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]int32)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeInt32Slice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_int64, types.T_sel:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]int64)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeInt64Slice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_uint8:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]uint8)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeUint8Slice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_uint16:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]uint16)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeUint16Slice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_uint32:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]uint32)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeUint32Slice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_uint64:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]uint64)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeUint64Slice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_float32:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]float32)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeFloat32Slice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_float64:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]float64)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeFloat64Slice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_char, types.T_varchar:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.(*types.Bytes)
		cnt := int32(len(vs.Offsets))
		buf.Write(encoding.EncodeInt32(cnt))
		if cnt != 0 {
			buf.Write(encoding.EncodeUint32Slice(vs.Lengths))
			size := uint64(0)
			for _, v := range vs.Lengths {
				size += uint64(v)
			}
			buf.Write(encoding.EncodeUint64(size))
			if size > 0 {
				for i, j := int64(0), int64(cnt); i < j; i++ {
					buf.Write(vs.Get(i))
				}
			}
		}
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_tuple:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		data, err := encoding.Encode(v.Col.([][]interface{}))
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		if len(data) > 0 {
			buf.Write(data)
		}
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_date:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]types.Date)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeDateSlice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_datetime:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]types.Datetime)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeDatetimeSlice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_timestamp:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]types.Timestamp)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeTimestampSlice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_decimal64:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]types.Decimal64)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeDecimal64Slice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	case types.T_decimal128:
		buf.Write(encoding.EncodeType(v.Typ))
		buf.Write(encoding.EncodeUint64(v.Ref))
		nb, err := v.Nsp.Show()
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		vs := v.Col.([]types.Decimal128)
		buf.Write(encoding.EncodeUint32(uint32(len(vs))))
		buf.Write(encoding.EncodeDecimal128Slice(vs))
		buf.Write(encoding.EncodeUint64(v.Link))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Data))))
		buf.Write(v.Data)
	default:
		return fmt.Errorf("unsupport vector type '%s'", v.Typ)
	}
	return nil
}

func DecodeVector(data []byte) (*vector.Vector, []byte, error) {
	typ := encoding.DecodeType(data[:encoding.TypeSize])
	data = data[encoding.TypeSize:]
	switch typ.Oid {
	case types.T_int8:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeInt8Slice(data[:n])
			data = data[n:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_int16:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeInt16Slice(data[:n*2])
			data = data[n*2:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_int32:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeInt32Slice(data[:n*4])
			data = data[n*4:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_int64, types.T_sel:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeInt64Slice(data[:n*8])
			data = data[n*8:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_uint8:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeUint8Slice(data[:n])
			data = data[n:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_uint16:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeUint16Slice(data[:n*2])
			data = data[n*2:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_uint32:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeUint32Slice(data[:n*4])
			data = data[n*4:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_uint64:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeUint64Slice(data[:n*8])
			data = data[n*8:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_float32:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeFloat32Slice(data[:n*4])
			data = data[n*4:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_float64:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeFloat64Slice(data[:n*8])
			data = data[n*8:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_char, types.T_varchar:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		col := new(types.Bytes)
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			col.Lengths = encoding.DecodeUint32Slice(data[:4*n])
			data = data[4*n:]
			m := encoding.DecodeUint64(data[:8])
			data = data[8:]
			col.Data = data[:m]
			data = data[m:]
			{
				o := uint32(0)
				col.Offsets = make([]uint32, n)
				for i, n := range col.Lengths {
					col.Offsets[i] = o
					o += n
				}
			}
			v.Col = col
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_tuple:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			col := v.Col.([][]interface{})
			if err := encoding.Decode(data[:n], &col); err != nil {
				return nil, nil, err
			}
			data = data[n:]
			v.Col = col
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_date:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeDateSlice(data[:n*4])
			data = data[n*4:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_datetime:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeDatetimeSlice(data[:n*8])
			data = data[n*8:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_timestamp:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeTimestampSlice(data[:n*8])
			data = data[n*8:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_decimal64:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeDecimal128Slice(data[:n*8])
			data = data[n*8:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	case types.T_decimal128:
		v := vector.New(typ)
		v.Or = true
		v.Ref = encoding.DecodeUint64(data[:8])
		data = data[8:]
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			if err := v.Nsp.Read(data[:n]); err != nil {
				return nil, nil, err
			}
			data = data[n:]
		} else {
			data = data[4:]
		}
		if n := encoding.DecodeUint32(data[:4]); n > 0 {
			data = data[4:]
			v.Col = encoding.DecodeDecimal128Slice(data[:n*16])
			data = data[n*16:]
		} else {
			data = data[4:]
		}
		v.Link = encoding.DecodeUint64(data[:8])
		data = data[8:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		v.Data = data[:n]
		data = data[n:]
		return v, data, nil
	}
	return nil, nil, fmt.Errorf("unsupport vector type '%s'", typ)
}
