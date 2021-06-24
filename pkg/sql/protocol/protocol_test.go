package protocol

import (
	"bytes"
	"fmt"
	"log"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/colexec/extend/overload"
	"matrixone/pkg/vm/engine"
	"testing"
)

/*
func TestEncode(t *testing.T) {
	var buf bytes.Buffer

	e := NewExtend()
	fmt.Printf("e: %v\n", e)
	if err := EncodeExtend(e, &buf); err != nil {
		log.Fatal(err)
	}
	ne, data, err := DecodeExtend(buf.Bytes())
	fmt.Printf("ne: %v, data: %v - %v\n", ne, data, err)
	v := vector.New(types.Type{types.T(types.T_tuple), 24, 0, 0})
	v.Append([][]interface{}{[]interface{}{1, 2, 3, 4}})
	if err := EncodeVector(v, &buf); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("v: %v\n", v)
	buf.Write([]byte{1, 2, 3})
	w, data, err := DecodeVector(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("w: %v\n", w)
	fmt.Printf("remaing: %v\n", data)
}
*/

/*
func TestBatch(t *testing.T) {
	var buf bytes.Buffer

	bat := batch.New(true, []string{"a", "b", "c"})
	bat.Vecs[0] = NewVector(1.2)
	bat.Vecs[1] = NewVector(2.1)
	bat.Vecs[2] = NewVector(3.0)
	fmt.Printf("bat: %v\n", bat)
	if err := EncodeBatch(bat, &buf); err != nil {
		log.Fatal(err)
	}
	nbat, data, err := DecodeBatch(buf.Bytes())
	fmt.Printf("nbat: %v\n", nbat)
	fmt.Printf("data: %v, err: %v\n", data, err)
}
*/

func TestPartition(t *testing.T) {
	var buf bytes.Buffer

	def := NewPartition()
	if err := EncodePartition(def, &buf); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("def: %v\n", def)
	ndef, data, err := DecodePartition(buf.Bytes())
	fmt.Printf("ndef: %v, data: %v, err: %v\n", ndef, data, err)
}

/*
func TestListPartition(t *testing.T) {
	var buf bytes.Buffer

	def := NewListPartition()
	if err := EncodeListPartition(def, &buf); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("def: %v\n", def)
	ndef, data, err := DecodeListPartition(buf.Bytes())
	fmt.Printf("ndef: %v, data: %v, err: %v\n", ndef, data, err)
}
*/

/*
func TestRangePartition(t *testing.T) {
	var buf bytes.Buffer

	def := NewRangePartition()
	if err := EncodeRangePartition(def, &buf); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("def: %v\n", def)
	ndef, data, err := DecodeRangePartition(buf.Bytes())
	fmt.Printf("ndef: %v, data: %v, err: %v\n", ndef, data, err)
}
*/

func NewPartition() *engine.PartitionBy {
	def := new(engine.PartitionBy)
	def.Fields = []string{"a", "b"}
	{
		def.List = append(def.List, NewListPartition())
		def.List = append(def.List, NewListPartition())
	}
	/*
		{
			def.Range = append(def.Range, NewRangePartition())
			def.Range = append(def.Range, NewRangePartition())
		}
	*/
	return def
}

func NewListPartition() engine.ListPartition {
	var def engine.ListPartition

	def.Name = "x"
	def.Extends = append(def.Extends, NewValue(1.2))
	def.Extends = append(def.Extends, NewValue(3.2))
	return def
}

func NewRangePartition() engine.RangePartition {
	var def engine.RangePartition

	def.Name = "x"
	{
		def.From = append(def.From, NewValue(1.2))
		def.From = append(def.From, NewValue(3.2))
	}
	{
		def.To = append(def.To, NewValue(3.2))
		def.To = append(def.To, NewValue(2.1))
	}
	return def
}

func NewVector(v float64) *vector.Vector {
	vec := vector.New(types.Type{types.T(types.T_float64), 8, 0, 0})
	vec.Append([]float64{v})
	return vec
}

func NewValue(v float64) extend.Extend {
	vec := vector.New(types.Type{types.T(types.T_float64), 8, 0, 0})
	vec.Append([]float64{v})
	return &extend.ValueExtend{vec}
}

func NewExtend() extend.Extend {
	v := vector.New(types.Type{types.T(types.T_varchar), 24, 0, 0})
	v.Append([][]byte{[]byte("1.2")})
	le := &extend.BinaryExtend{
		Op: overload.Plus,
		Left: &extend.Attribute{
			Name: "x",
			Type: types.T_int8,
		},
		Right: &extend.ValueExtend{v},
	}
	return &extend.BinaryExtend{
		Op:   overload.Minus,
		Left: le,
		Right: &extend.Attribute{
			Name: "y",
			Type: types.T_int8,
		},
	}
}
