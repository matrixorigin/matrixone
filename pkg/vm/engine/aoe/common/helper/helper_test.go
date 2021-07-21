package helper

import (
	"encoding/json"
	"fmt"
	"log"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/metadata"
	"testing"
)

func TestAoe(t *testing.T) {
	data, err := EncodeTable(aoe.TableInfo{})
	if err != nil {
		log.Fatal(err)
	}
	tbl, err := DecodeTable(data)
	if err != nil {
		log.Fatal(err)
	}
	{
		data, err := json.Marshal(tbl)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("tbl: %v\n", string(data))
	}
	pdef, _, err := protocol.DecodePartition(tbl.Partition)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("partition: %v\n", pdef)
}

func NewTableDefs() []engine.TableDef {
	var defs []engine.TableDef

	defs = append(defs, &engine.IndexTableDef{
		Typ:   0,
		Names: []string{"a", "b"},
	})
	defs = append(defs, &engine.AttributeDef{
		Attr: metadata.Attribute{
			Alg:  0,
			Name: "a",
			Type: types.Type{Oid: types.T_float64, Size: 8},
		},
	})
	defs = append(defs, &engine.AttributeDef{
		Attr: metadata.Attribute{
			Alg:  0,
			Name: "b",
			Type: types.Type{Oid: types.T_varchar, Size: 8},
		},
	})
	return defs
}

func NewPartition() *engine.PartitionBy {
	def := new(engine.PartitionBy)
	def.Fields = []string{"a", "b"}
	{
		def.List = append(def.List, NewListPartition())
		def.List = append(def.List, NewListPartition())
	}
	return def
}

func NewListPartition() engine.ListPartition {
	var def engine.ListPartition

	def.Name = "x"
	def.Extends = append(def.Extends, NewValue(1.2))
	def.Extends = append(def.Extends, NewValue(3.2))
	return def
}

func NewValue(v float64) extend.Extend {
	vec := vector.New(types.Type{types.T(types.T_float64), 8, 0, 0})
	vec.Append([]float64{v})
	return &extend.ValueExtend{vec}
}
