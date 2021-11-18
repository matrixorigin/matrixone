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
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/colexec/dedup"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/colexec/limit"
	"matrixone/pkg/sql/colexec/offset"
	"matrixone/pkg/sql/colexec/order"
	"matrixone/pkg/sql/colexec/projection"
	"matrixone/pkg/sql/colexec/restrict"
	"matrixone/pkg/sql/colexec/top"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/engine"
)

func init() {
	gob.Register(Field{})
	gob.Register(Extend{})
	gob.Register(TopArgument{})
	gob.Register(JoinArgument{})
	gob.Register(GroupArgument{})
	gob.Register(ProjectionArgument{})

	gob.Register(Source{})
	gob.Register(Segment{})
}

func EncodeScope(s Scope, buf *bytes.Buffer) error {
	buf.Write(encoding.EncodeUint32(uint32(s.Magic)))
	{
		data, err := encoding.Encode(s.Data)
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
	}
	if err := EncodeInstructions(s.Ins, buf); err != nil {
		return err
	}
	buf.Write(encoding.EncodeUint32(uint32(len(s.Ss))))
	for i := range s.Ss {
		if err := EncodeScope(s.Ss[i], buf); err != nil {
			return err
		}
	}
	return nil
}

func DecodeScope(data []byte) (Scope, []byte, error) {
	var s Scope
	var err error

	s.Magic = int(encoding.DecodeUint32(data[:4]))
	data = data[4:]
	{
		n := int(encoding.DecodeUint32(data[:4]))
		data = data[4:]
		if n > 0 {
			if err = encoding.Decode(data, &s.Data); err != nil {
				return s, nil, err
			}
			data = data[n:]
		}
	}
	if s.Ins, data, err = DecodeInstructions(data); err != nil {
		return s, nil, err
	}
	n := int(encoding.DecodeUint32(data[:4]))
	data = data[4:]
	s.Ss = make([]Scope, n)
	for i := 0; i < n; i++ {
		if s.Ss[i], data, err = DecodeScope(data); err != nil {
			return s, nil, err
		}
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
		in, rdata, err := DecodeInstruction(data)
		if err != nil {
			return ins, nil, err
		}
		ins[i] = in
		data = rdata
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
	case vm.Dedup:
		arg := in.Arg.(*dedup.Argument)
		data, err := encoding.Encode(arg)
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return nil
	case vm.Limit:
		arg := in.Arg.(*limit.Argument)
		buf.Write(encoding.EncodeUint64(arg.Limit))
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
		data, err := encoding.Encode(fs)
		if err != nil {
			return err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(data))))
		buf.Write(data)
		return nil
	case vm.Offset:
		arg := in.Arg.(*offset.Argument)
		buf.Write(encoding.EncodeUint64(arg.Offset))
		return nil
	case vm.Restrict:
		arg := in.Arg.(*restrict.Argument)
		return EncodeExtend(arg.E, buf)
	case vm.Projection:
		arg := in.Arg.(*projection.Argument)
		data, err := encoding.Encode(ProjectionArgument{Rs: arg.Rs, As: arg.As, Es: arg.Es})
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
	case vm.Dedup:
		//var attrs []string
		//
		//data = data[4:]
		//n := encoding.DecodeUint32(data[:4])
		//data = data[4:]
		//if err := encoding.Decode(data[:n], &attrs); err != nil {
		//	return in, nil, err
		//}
		in.Arg = &dedup.Argument{}
		data = data[4:]
	case vm.Limit:
		data = data[4:]
		n := encoding.DecodeUint64(data[:8])
		in.Arg = &limit.Argument{Limit: n}
		data = data[8:]
	case vm.Order:
		var arg []Field

		data = data[4:]
		n := encoding.DecodeUint32(data[:4])
		data = data[4:]
		if err := encoding.Decode(data[:n], &arg); err != nil {
			return in, nil, err
		}
		fs := make([]order.Field, len(arg))
		{
			for i, f := range arg {
				fs[i].Attr = f.Attr
				fs[i].Type = order.Direction(f.Type)
			}
		}
		in.Arg = &order.Argument{
			Fs: fs,
		}
		data = data[n:]
	case vm.Offset:
		data = data[4:]
		n := encoding.DecodeUint64(data[:8])
		in.Arg = &offset.Argument{Offset: n}
		data = data[8:]
	case vm.Restrict:
		data = data[4:]
		{
			e, rdata, err := DecodeExtend(data)
			if err != nil {
				return in, nil, err
			}
			in.Arg = &restrict.Argument{E: e}
			data = rdata
		}
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
			e, rdata, err := DecodeExtend(data)
			if err != nil {
				return in, nil, err
			}
			es[i] = e
			data = rdata
		}
		in.Arg = &projection.Argument{
			Es: es,
			As: arg.As,
			Rs: arg.Rs,
		}
	}
	return in, data, nil
}

func EncodePartition(def *engine.PartitionByDef, buf *bytes.Buffer) error {
	data, err := encoding.Encode(def.Fields)
	if err != nil {
		return err
	}
	buf.Write(encoding.EncodeUint32(uint32(len(data))))
	buf.Write(data)
	if n := len(def.List); n > 0 {
		buf.Write(encoding.EncodeUint32(uint32(n)))
		for i := 0; i < n; i++ {
			if err := EncodeListPartition(def.List[i], buf); err != nil {
				return err
			}
		}
	} else {
		buf.Write(encoding.EncodeUint32(0))
	}
	if n := len(def.Range); n > 0 {
		buf.Write(encoding.EncodeUint32(uint32(n)))
		for i := 0; i < n; i++ {
			if err := EncodeRangePartition(def.Range[i], buf); err != nil {
				return err
			}
		}
	} else {
		buf.Write(encoding.EncodeUint32(0))
	}
	return nil
}

func DecodePartition(data []byte) (*engine.PartitionByDef, []byte, error) {
	def := new(engine.PartitionByDef)
	if n := encoding.DecodeUint32(data[:4]); n > 0 {
		data = data[4:]
		if err := encoding.Decode(data[:n], &def.Fields); err != nil {
			return nil, nil, err
		}
		data = data[n:]
	} else {
		data = data[4:]
	}
	if n := encoding.DecodeUint32(data[:4]); n > 0 {
		data = data[4:]
		for i := uint32(0); i < n; i++ {
			ldef, remaing, err := DecodeListPartition(data)
			if err != nil {
				return nil, nil, err
			}
			def.List = append(def.List, ldef)
			data = remaing
		}
	} else {
		data = data[4:]
	}
	if n := encoding.DecodeUint32(data[:4]); n > 0 {
		data = data[4:]
		for i := uint32(0); i < n; i++ {
			rdef, remaing, err := DecodeRangePartition(data)
			if err != nil {
				return nil, nil, err
			}
			def.Range = append(def.Range, rdef)
			data = remaing
		}
	} else {
		data = data[4:]
	}
	return def, data, nil
}

func EncodeListPartition(def engine.ListPartition, buf *bytes.Buffer) error {
	if def.Subpartition != nil {
		buf.WriteByte(ListWithSub)
	} else {
		buf.WriteByte(List)
	}
	buf.Write(encoding.EncodeUint32(uint32(len(def.Name))))
	buf.WriteString(def.Name)
	if n := len(def.Extends); n > 0 {
		buf.Write(encoding.EncodeUint32(uint32(n)))
		for _, e := range def.Extends {
			if err := EncodeExtend(e, buf); err != nil {
				return err
			}
		}
	}
	if def.Subpartition != nil {
		return EncodePartition(def.Subpartition, buf)
	}
	return nil
}

func DecodeListPartition(data []byte) (engine.ListPartition, []byte, error) {
	var def engine.ListPartition

	typ := data[0]
	data = data[1:]
	if n := encoding.DecodeUint32(data[:4]); n > 0 {
		data = data[4:]
		def.Name = string(data[:n])
		data = data[n:]
	} else {
		data = data[4:]
	}
	if n := encoding.DecodeUint32(data[:4]); n > 0 {
		data = data[4:]
		for i := uint32(0); i < n; i++ {
			e, remaing, err := DecodeExtend(data)
			if err != nil {
				return def, nil, err
			}
			def.Extends = append(def.Extends, e)
			data = remaing
		}
	} else {
		data = data[4:]
	}
	if typ == ListWithSub {
		sub, remaing, err := DecodePartition(data)
		if err != nil {
			return def, nil, err
		}
		data = remaing
		def.Subpartition = sub
	}
	return def, data, nil
}

func EncodeRangePartition(def engine.RangePartition, buf *bytes.Buffer) error {
	if def.Subpartition != nil {
		buf.WriteByte(RangeWithSub)
	} else {
		buf.WriteByte(Range)
	}
	buf.Write(encoding.EncodeUint32(uint32(len(def.Name))))
	buf.WriteString(def.Name)
	if n := len(def.From); n > 0 {
		buf.Write(encoding.EncodeUint32(uint32(n)))
		for _, e := range def.From {
			if err := EncodeExtend(e, buf); err != nil {
				return err
			}
		}
	}
	if n := len(def.To); n > 0 {
		buf.Write(encoding.EncodeUint32(uint32(n)))
		for _, e := range def.To {
			if err := EncodeExtend(e, buf); err != nil {
				return err
			}
		}
	}
	if def.Subpartition != nil {
		return EncodePartition(def.Subpartition, buf)
	}
	return nil
}

func DecodeRangePartition(data []byte) (engine.RangePartition, []byte, error) {
	var def engine.RangePartition

	typ := data[0]
	data = data[1:]
	if n := encoding.DecodeUint32(data[:4]); n > 0 {
		data = data[4:]
		def.Name = string(data[:n])
		data = data[n:]
	} else {
		data = data[4:]
	}
	if n := encoding.DecodeUint32(data[:4]); n > 0 {
		data = data[4:]
		for i := uint32(0); i < n; i++ {
			e, remaing, err := DecodeExtend(data)
			if err != nil {
				return def, nil, err
			}
			def.From = append(def.From, e)
			data = remaing
		}
	} else {
		data = data[4:]
	}
	if n := encoding.DecodeUint32(data[:4]); n > 0 {
		data = data[4:]
		for i := uint32(0); i < n; i++ {
			e, remaing, err := DecodeExtend(data)
			if err != nil {
				return def, nil, err
			}
			def.To = append(def.To, e)
			data = remaing
		}
	} else {
		data = data[4:]
	}
	if typ == ListWithSub {
		sub, remaing, err := DecodePartition(data)
		if err != nil {
			return def, nil, err
		}
		data = remaing
		def.Subpartition = sub
	}
	return def, data, nil
}

func EncodeExtend(e extend.Extend, buf *bytes.Buffer) error {
	switch v := e.(type) {
	case *extend.Attribute:
		buf.WriteByte(Attr)
		buf.Write(encoding.EncodeUint16(uint16(v.Type)))
		buf.Write(encoding.EncodeUint32(uint32(len(v.Name))))
		buf.WriteString(v.Name)
		return nil
	case *extend.UnaryExtend:
		buf.WriteByte(Unary)
		buf.Write(encoding.EncodeUint32(uint32(v.Op)))
		return EncodeExtend(v.E, buf)
	case *extend.ParenExtend:
		buf.WriteByte(Paren)
		return EncodeExtend(v.E, buf)
	case *extend.ValueExtend:
		buf.WriteByte(Value)
		return EncodeVector(v.V, buf)
	case *extend.BinaryExtend:
		buf.WriteByte(Binary)
		buf.Write(encoding.EncodeUint32(uint32(v.Op)))
		if err := EncodeExtend(v.Left, buf); err != nil {
			return err
		}
		return EncodeExtend(v.Right, buf)
	}
	return fmt.Errorf("'%s' not yet support", e)
}

func DecodeExtend(data []byte) (extend.Extend, []byte, error) {
	switch data[0] {
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
	case Paren:
		e := new(extend.ParenExtend)
		data = data[1:]
		ext, data, err := DecodeExtend(data)
		if err != nil {
			return nil, nil, err
		}
		e.E = ext
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
	}
	return nil, nil, fmt.Errorf("'%v' extend not yet support", data[0])
}

func EncodeBatch(bat *batch.Batch, buf *bytes.Buffer) error {
	sn := len(bat.Sels)
	buf.Write(encoding.EncodeUint32(uint32(sn)))
	if sn > 0 {
		buf.Write(encoding.EncodeInt64Slice(bat.Sels))
	}
	data, err := encoding.Encode(bat.Attrs)
	if err != nil {
		return err
	}
	buf.Write(encoding.EncodeUint32(uint32(len(data))))
	buf.Write(data)
	buf.Write(encoding.EncodeUint32(uint32(len(bat.Vecs))))
	for _, vec := range bat.Vecs {
		if err := EncodeVector(vec, buf); err != nil {
			return err
		}
	}
	return nil
}

func DecodeBatch(data []byte) (*batch.Batch, []byte, error) {
	bat := batch.New(true, []string{})
	sn := encoding.DecodeUint32(data[:4])
	data = data[4:]
	if sn > 0 {
		bat.Sels = encoding.DecodeInt64Slice(data[:sn*8])
		data = data[sn*8:]
	}
	if n := encoding.DecodeUint32(data); n > 0 {
		data = data[4:]
		if err := encoding.Decode(data[:n], &bat.Attrs); err != nil {
			return nil, nil, err
		}
		data = data[n:]
	} else {
		data = data[4:]
	}
	if n := encoding.DecodeUint32(data); n > 0 {
		data = data[4:]
		for i := uint32(0); i < n; i++ {
			vec, remaing, err := DecodeVector(data)
			if err != nil {
				return nil, nil, err
			}
			bat.Vecs = append(bat.Vecs, vec)
			data = remaing
		}
	} else {
		data = data[4:]
	}
	return bat, data, nil
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
		return v, data, nil
	}
	return nil, nil, fmt.Errorf("unsupport vector type '%s'", typ)
}
