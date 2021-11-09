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

package helper

import (
	"encoding/json"
	"fmt"
	"log"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/protocol"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
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

	if len(tbl.Partition) > 0 {
		pdef, _, err := protocol.DecodePartition(tbl.Partition)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("partition: %v\n", pdef)
	}

}

func NewTableDefs() []engine.TableDef {
	var defs []engine.TableDef

	defs = append(defs, &engine.IndexTableDef{
		Typ:   0,
		ColNames: []string{"a", "b"},
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
	vec := vector.New(types.Type{Oid: types.T(types.T_float64), Size: 8})
	vec.Append([]float64{v})
	return &extend.ValueExtend{V: vec}
}
