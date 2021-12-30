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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/ring/avg"
	"github.com/matrixorigin/matrixone/pkg/container/ring/count"
	"github.com/matrixorigin/matrixone/pkg/container/ring/max"
	"github.com/matrixorigin/matrixone/pkg/container/ring/min"
	"github.com/matrixorigin/matrixone/pkg/container/ring/starcount"
	"github.com/matrixorigin/matrixone/pkg/container/ring/sum"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transform"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTransform(t *testing.T) {
	var buf bytes.Buffer
	ins := vm.Instruction{
		Op: vm.Transform,
		Arg: &transform.Argument{},
	}
	err := EncodeInstruction(ins, &buf)
	require.NoError(t, err)
	resultIns, _, err := DecodeInstruction(buf.Bytes())
	require.NoError(t, err)
	// Op
	if resultIns.Op != ins.Op {
		t.Errorf("Decode resultIns.Op failed. \nExpected/Got:\n%v\n%v", resultIns.Op, ins.Op)
		return
	}
	fmt.Println(resultIns)
}

func TestInstruction(t *testing.T) {
	var buf bytes.Buffer
	ins := vm.Instruction{
		Op: vm.Top,
		Arg: &top.Argument{
			Limit: 123,
			Fs: []top.Field{top.Field{Attr: "hello", Type: top.Ascending}},
		},
	}
	err := EncodeInstruction(ins, &buf)
	require.NoError(t, err)
	resultIns, _, err := DecodeInstruction(buf.Bytes())
	require.NoError(t, err)
	// Op
	if resultIns.Op != ins.Op {
		t.Errorf("Decode resultIns.Op failed. \nExpected/Got:\n%v\n%v", resultIns.Op, ins.Op)
		return
	}
	// Arg
	resultArg, ok := resultIns.Arg.(*top.Argument)
	if !ok {
		t.Errorf("Decode instruction type failed.")
		return
	}
	// Limit
	if resultArg.Limit != 123 {
		t.Errorf("Decode arg limit failed.")
		return
	}
	// Field Attr
	if resultArg.Fs[0].Attr != "hello" {
		t.Errorf("Decode arg Attr failed.")
		return
	}
	// Field Type
	if resultArg.Fs[0].Type != top.Ascending {
		t.Errorf("Decode arg Type failed.")
		return
	}
}

func TestExtend(t *testing.T) {
	var buf bytes.Buffer
	e := &extend.UnaryExtend{
		Op: Unary,
		E: &extend.ValueExtend{
			V: NewFloatVector(1.2),
		},
	}
	err := EncodeExtend(e, &buf)
	require.NoError(t, err)
	resultE, _, err := DecodeExtend(buf.Bytes())
	require.NoError(t, err)
	re, ok := resultE.(*extend.UnaryExtend)
	if !ok {
		t.Errorf("Decode extend type failed.")
		return
	}
	// Op
	if re.Op != e.Op {
		t.Errorf("Decode re.Op failed. \nExpected/Got:\n%v\n%v", e.Op, re.Op)
		return
	}
}

func TestVector(t *testing.T) {
	var buf bytes.Buffer
	vec := NewFloatVector(1.2)
	vec.Ref = 12839791322
	vec.Link = 123908123
	vec.Data = []byte("Unless required by applicable law or agreed to in writing")
	err := EncodeVector(vec, &buf)
	require.NoError(t, err)
	resultVec, _, err := DecodeVector(buf.Bytes())
	require.NoError(t, err)
	// Or
	if resultVec.Or != true {
		t.Errorf("Decode resultVec.Or failed. \nExpected/Got:\n%v\n%v", true, resultVec.Or)
		return
	}
	// Ref
	if resultVec.Ref != vec.Ref {
		t.Errorf("Decode resultVec.Ref failed. \nExpected/Got:\n%v\n%v", vec.Ref, resultVec.Ref)
		return
	}
	// Link
	if resultVec.Link != vec.Link {
		t.Errorf("Decode resultVec.Link failed. \nExpected/Got:\n%v\n%v", vec.Link, resultVec.Link)
		return
	}
	// Data
	if string(resultVec.Data) != string(vec.Data) {
		t.Errorf("Decode resultVec.Data failed. \nExpected/Got:\n%v\n%v", string(vec.Data), string(resultVec.Data))
		return
	}
	//
	if  resultVec.Col.([]float64)[0] != vec.Col.([]float64)[0] {
		t.Errorf("Decode Batch.Vecs failed. \nExpected/Got:\n%v\n%v", vec.Col.([]float64)[0], resultVec.Col.([]float64)[0])
		return
	}
}

func TestRing(t *testing.T) {
	ringArray := []ring.Ring{
		&avg.AvgRing{
			Ns: []int64{123123123, 123123908950, 9089374534},
			Vs: []float64{123.123, 34534.345, 234123.345345},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&count.CountRing{
			Ns: []int64{123123123, 123123908950, 9089374534},
			Vs: []int64{12312312, 34534345, 234123345345},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&starcount.CountRing{
			Ns: []int64{123123123, 123123908950, 9089374534},
			Vs: []int64{12312312, 34534345, 234123345345},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&max.Int8Ring{
			Ns: []int64{123123123, 123123908950, 9089374534},
			Vs: []int8{6, 6, 8, 0},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&max.Int16Ring{
			Ns: []int64{567567, 123123908950, 9089374534},
			Vs: []int16{62, 62, 8, 01},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&max.Int32Ring{
			Ns: []int64{789789, 123123908950, 9089374534},
			Vs: []int32{612, 632, 81, 0423},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&max.Int64Ring{
			Ns: []int64{178923123, 123123908950, 9089374534},
			Vs: []int64{6123, 123126, 2323328, 02342342},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&max.UInt8Ring{
			Ns: []int64{123123123, 123123908950, 9089374534},
			Vs: []uint8{6, 6, 8, 0},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&max.UInt16Ring{
			Ns: []int64{45634564, 123123908950, 9089374534},
			Vs: []uint16{6123, 1236, 8123, 12310},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&max.UInt32Ring{
			Ns: []int64{56784567, 123123908950, 9089374534},
			Vs: []uint32{6123, 3454346, 345348, 345340},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&max.UInt64Ring{
			Ns: []int64{8902345, 123123908950, 9089374534},
			Vs: []uint64{6112323, 34542345346, 234, 23412312},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&max.Float32Ring{
			Ns: []int64{3246457, 123123908950, 9089374534},
			Vs: []float32{123.123, 34534.345, 234123.345345},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&max.Float64Ring{
			Ns: []int64{996674, 123123908950, 9089374534},
			Vs: []float64{123.123, 34534.345, 234123.345345},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&max.StrRing{
			Ns: []int64{1231245234, 123123123908950, 123},
			Vs: [][]byte{[]byte("test1"), []byte("mysql1"), []byte("postgresql1")},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&min.Int8Ring{
			Ns: []int64{123123123, 123123908950, 9089374534},
			Vs: []int8{6, 6, 8, 0},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&min.Int16Ring{
			Ns: []int64{567567, 123123908950, 9089374534},
			Vs: []int16{62, 62, 8, 01},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&min.Int32Ring{
			Ns: []int64{789789, 123123908950, 9089374534},
			Vs: []int32{612, 632, 81, 0423},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&min.Int64Ring{
			Ns: []int64{178923123, 123123908950, 9089374534},
			Vs: []int64{6123, 123126, 2323328, 02342342},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&min.UInt8Ring{
			Ns: []int64{123123123, 123123908950, 9089374534},
			Vs: []uint8{6, 6, 8, 0},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&min.UInt16Ring{
			Ns: []int64{45634564, 123123908950, 9089374534},
			Vs: []uint16{6123, 1236, 8123, 12310},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&min.UInt32Ring{
			Ns: []int64{56784567, 123123908950, 9089374534},
			Vs: []uint32{6123, 3454346, 345348, 345340},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&min.UInt64Ring{
			Ns: []int64{8902345, 123123908950, 9089374534},
			Vs: []uint64{6112323, 34542345346, 234, 23412312},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&min.Float32Ring{
			Ns: []int64{3246457, 123123908950, 9089374534},
			Vs: []float32{123.123, 34534.345, 234123.345345},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&min.Float64Ring{
			Ns: []int64{996674, 123123908950, 9089374534},
			Vs: []float64{123.123, 34534.345, 234123.345345},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&min.StrRing{
			Ns: []int64{1231245234, 123123123908950, 123},
			Vs: [][]byte{[]byte("test1"), []byte("mysql1"), []byte("postgresql1")},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&sum.IntRing{
			Ns: []int64{178923123, 123123908950, 9089374534},
			Vs: []int64{6123, 123126, 2323328, 02342342},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&sum.UIntRing{
			Ns: []int64{8902345, 123123908950, 9089374534},
			Vs: []uint64{6112323, 34542345346, 234, 23412312},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
		&sum.FloatRing{
			Ns: []int64{996674, 123123908950, 9089374534},
			Vs: []float64{123.123, 34534.345, 234123.345345},
			Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
		},
	}
	for _, r := range ringArray {
		var buf bytes.Buffer
		err := EncodeRing(r, &buf)
		if err != nil {
			t.Errorf("Encode err: %v", err)
			return
		}
		resultRing, _, err := DecodeRing(buf.Bytes())
		if err != nil {
			t.Errorf("Decode ring error: %v", err)
			return
		}

		switch ExpectRing := resultRing.(type) {
		case *avg.AvgRing:
			oriRing := r.(*avg.AvgRing)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeFloat64Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *count.CountRing:
			oriRing := r.(*count.CountRing)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeInt64Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *starcount.CountRing:
			oriRing := r.(*starcount.CountRing)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeInt64Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *max.Int8Ring:
			oriRing := r.(*max.Int8Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeInt8Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *max.Int16Ring:
			oriRing := r.(*max.Int16Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeInt16Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *max.Int32Ring:
			oriRing := r.(*max.Int32Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeInt32Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *max.Int64Ring:
			oriRing := r.(*max.Int64Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeInt64Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *max.UInt8Ring:
			oriRing := r.(*max.UInt8Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeUint8Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *max.UInt16Ring:
			oriRing := r.(*max.UInt16Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeUint16Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *max.UInt32Ring:
			oriRing := r.(*max.UInt32Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeUint32Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *max.UInt64Ring:
			oriRing := r.(*max.UInt64Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeUint64Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *max.Float32Ring:
			oriRing := r.(*max.Float32Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeFloat32Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *max.Float64Ring:
			oriRing := r.(*max.Float64Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeFloat64Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *max.StrRing:
			oriRing := r.(*max.StrRing)
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if string(ExpectRing.Vs[i]) != string(v) {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", string(v), string(ExpectRing.Vs[i]))
					return
				}
			}
		case *min.Int8Ring:
			oriRing := r.(*min.Int8Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeInt8Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *min.Int16Ring:
			oriRing := r.(*min.Int16Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeInt16Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *min.Int32Ring:
			oriRing := r.(*min.Int32Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeInt32Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *min.Int64Ring:
			oriRing := r.(*min.Int64Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeInt64Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *min.UInt8Ring:
			oriRing := r.(*min.UInt8Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeUint8Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *min.UInt16Ring:
			oriRing := r.(*min.UInt16Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeUint16Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *min.UInt32Ring:
			oriRing := r.(*min.UInt32Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeUint32Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *min.UInt64Ring:
			oriRing := r.(*min.UInt64Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeUint64Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *min.Float32Ring:
			oriRing := r.(*min.Float32Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeFloat32Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *min.Float64Ring:
			oriRing := r.(*min.Float64Ring)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeFloat64Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *min.StrRing:
			oriRing := r.(*min.StrRing)
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if string(ExpectRing.Vs[i]) != string(v) {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", string(v), string(ExpectRing.Vs[i]))
					return
				}
			}
		case *sum.IntRing:
			oriRing := r.(*sum.IntRing)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeInt64Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *sum.UIntRing:
			oriRing := r.(*sum.UIntRing)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeUint64Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		case *sum.FloatRing:
			oriRing := r.(*sum.FloatRing)
			// Da
			if string(ExpectRing.Da) != string(encoding.EncodeFloat64Slice(oriRing.Vs)) {
				t.Errorf("Decode ring Da failed.")
				return
			}
			// Ns
			for i, n := range oriRing.Ns {
				if ExpectRing.Ns[i] != n {
					t.Errorf("Decode ring Ns failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.Ns[i])
					return
				}
			}
			// Vs
			for i, v := range oriRing.Vs {
				if ExpectRing.Vs[i] != v {
					t.Errorf("Decode ring Vs failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.Vs[i])
					return
				}
			}
		default:
			t.Error("Can not find the type of ring")
		}
	}
}

func TestBatch(t *testing.T) {
	var buf bytes.Buffer

	bat := batch.New(true, []string{"a", "b", "c"})
	bat.Vecs[0] = NewFloatVector(1.2)
	bat.Vecs[1] = NewFloatVector(2.1)
	bat.Vecs[2] = NewStrVector([]byte("x"))
	bat.SelsData = []byte("hello")
	bat.Sels = []int64{123, 456, 678}
	bat.Zs = []int64{908, 46, 1234, 23412}
	bat.Refs = []uint64{90123123128, 41231231236, 11231231234, 23412123123123}
	bat.As = []string{"hello", "world", "big", "small"}
	// encode
	err := EncodeBatch(bat, &buf)
	if err != nil {
		t.Errorf("Encode err: %v", err)
		return
	}
	// decode
	resultBat, _, err := DecodeBatch(buf.Bytes())
	if err != nil {
		t.Errorf("Decode err: %v", err)
		return
	}
	// result verify
	// Attrs
	for i, v := range bat.Attrs {
		if resultBat.Attrs[i] != v {
			t.Errorf("Decode Batch.Attrs failed. \nExpected/Got:\n%s\n%s", v, resultBat.Attrs[1])
			return
		}
	}
	// Vecs
	if  resultBat.Vecs[0].Col.([]float64)[0] != bat.Vecs[0].Col.([]float64)[0] {
		t.Errorf("Decode Batch.Vecs failed. \nExpected/Got:\n%v\n%v", bat.Vecs[0].Col.([]float64)[0], resultBat.Vecs[0].Col.([]float64)[0])
		return
	}
	if  resultBat.Vecs[1].Col.([]float64)[0] != bat.Vecs[1].Col.([]float64)[0] {
		t.Errorf("Decode Batch.Vecs failed. \nExpected/Got:\n%v\n%v", bat.Vecs[1].Col.([]float64)[0], resultBat.Vecs[1].Col.([]float64)[0])
		return
	}
	// SelsData
	if string(resultBat.SelsData) != string(bat.SelsData) {
		t.Errorf("Decode Batch.SelsData failed. \nExpected/Got:\n%v\\n%v", string(bat.SelsData), string(resultBat.SelsData))
		return
	}
	// Sels
	for i, v := range bat.Sels {
		if resultBat.Sels[i] != v {
			t.Errorf("Decode Batch.Sels failed. \nExpected/Got:\n%v\n%v", v, resultBat.Sels[i])
			return
		}
	}
	// Zs
	for i, v := range bat.Zs {
		if resultBat.Zs[i] != v {
			t.Errorf("Decode Batch.Zs failed. \nExpected/Got:\n%v\n%v", v, resultBat.Zs[i])
			return
		}
	}
	// Refs
	for i, v := range bat.Refs {
		if resultBat.Refs[i] != v {
			t.Errorf("Decode Batch.Refs failed. \nExpected/Got:\n%v\n%v", v, resultBat.Refs[i])
			return
		}
	}
	// As
	for i, v := range bat.As {
		if resultBat.As[i] != v {
			t.Errorf("Decode Batch.As failed. \nExpected/Got:\n%v\n%v", v, resultBat.As[i])
			return
		}
	}
}

func NewStrVector(v []byte) *vector.Vector {
	vec := vector.New(types.Type{Oid: types.T(types.T_varchar), Size: 24})
	vector.Append(vec, [][]byte{v, v, v})
	return vec
}

func NewFloatVector(v float64) *vector.Vector {
	vec := vector.New(types.Type{Oid: types.T(types.T_float64), Size: 8})
	vector.Append(vec, []float64{v, v, v})
	return vec
}