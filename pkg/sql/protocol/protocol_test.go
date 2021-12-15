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
	"github.com/stretchr/testify/require"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/ring/avg"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"testing"
)

// TestScope will be test in transfer_test.go, because here import cycle will happen

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
	var buf bytes.Buffer
	avgRing := &avg.AvgRing{
		Da: []byte("Copyright 2021 Matrix Origin"),
		Ns: []int64{123123123, 123123908950, 9089374534},
		Vs: []float64{123.123, 34534.345, 234123.345345},
		Typ: types.Type{Oid: types.T(types.T_varchar), Size: 24},
	}
	err := EncodeRing(avgRing, &buf)
	if err != nil {
		t.Errorf("Encode err: %v", err)
		return
	}
	resultRing, _, err := DecodeRing(buf.Bytes())
	if err != nil {
		t.Errorf("Decode err: %v", err)
		return
	}
	ar, ok := resultRing.(*avg.AvgRing)
	if !ok {
		t.Errorf("Decode ring type error")
		return
	}
	// Da
	if string(ar.Da) != string(avgRing.Da) {
		t.Errorf("Decode Batch.Attrs failed. \\nExpected/Got:\\n%v\\n%v", string(ar.Da), string(avgRing.Da))
		return
	}
	// Ns
	for i, v := range avgRing.Ns {
		if ar.Ns[i] != v {
			t.Errorf("Decode avgRing.Ns failed. \nExpected/Got:\n%v\n%v", v, ar.Ns[i])
			return
		}
	}
	// Vs
	for i, v := range avgRing.Vs {
		if ar.Vs[i] != v {
			t.Errorf("Decode avgRing.Vs failed. \nExpected/Got:\n%v\n%v", v, ar.Vs[i])
			return
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