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

package engine

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec"
	"github.com/smartystreets/goconvey/convey"
)

func TestTpeReader_Read(t *testing.T) {
	convey.Convey("read without primary key", t, func() {
		tpe, err := NewTpeEngine(&TpeConfig{
			KvType:                    tuplecodec.KV_MEMORY,
			SerialType:                tuplecodec.ST_JSON,
			ValueLayoutSerializerType: "default",
			KVLimit:                   10000})
		convey.So(err, convey.ShouldBeNil)
		err = tpe.Create(0, "test", 0)
		convey.So(err, convey.ShouldBeNil)

		dbDesc, err := tpe.Database("test")
		convey.So(err, convey.ShouldBeNil)

		//(a,b,c)
		//(uint64,uint64,uint64)
		_, attrDefs := tuplecodec.MakeAttributes(types.T_uint64, types.T_uint64, types.T_uint64)

		attrNames := []string{
			"a", "b", "c",
		}
		var defs []engine.TableDef
		var rawDefs []*engine.AttributeDef
		for i, def := range attrDefs {
			def.Attr.Name = attrNames[i]
			defs = append(defs, def)
			rawDefs = append(rawDefs, def)
		}

		err = dbDesc.Create(0, "A", defs)
		convey.So(err, convey.ShouldBeNil)

		tableDesc, err := dbDesc.Relation("A")
		convey.So(err, convey.ShouldBeNil)

		//make data
		bat := tuplecodec.MakeBatch(10, attrNames, rawDefs)

		bat.Zs = nil
		err = tableDesc.Write(0, bat)

		convey.So(err, convey.ShouldBeNil)

		var get *batch.Batch

		readers := tableDesc.NewReader(10, nil, nil)
		for i, reader := range readers {
			if i == 0 {
				for {
					get, err = reader.Read([]uint64{1, 1}, []string{"a", "b"})
					if get == nil {
						break
					}
					for j := 0; j < 2; j++ {
						a := bat.Vecs[j].Col.([]uint64)
						b := get.Vecs[j].Col.([]uint64)
						convey.So(a, convey.ShouldResemble, b)
					}
				}
			} else {
				get, err = reader.Read([]uint64{1, 1}, []string{"a", "b"})
				convey.So(get, convey.ShouldBeNil)
				convey.So(err, convey.ShouldBeNil)
			}
		}
	})

	convey.Convey("read with primary key", t, func() {
		tpe, err := NewTpeEngine(&TpeConfig{
			KvType:                    tuplecodec.KV_MEMORY,
			SerialType:                tuplecodec.ST_JSON,
			ValueLayoutSerializerType: "default",
			KVLimit:                   10000})
		convey.So(err, convey.ShouldBeNil)
		err = tpe.Create(0, "test", 0)
		convey.So(err, convey.ShouldBeNil)

		dbDesc, err := tpe.Database("test")
		convey.So(err, convey.ShouldBeNil)

		//(a,b,c)
		//(uint64,uint64,uint64)
		//primary key (a,b)
		_, attrDefs := tuplecodec.MakeAttributes(types.T_uint64, types.T_uint64, types.T_uint64)

		attrNames := []string{
			"a", "b", "c",
		}
		var defs []engine.TableDef
		var rawDefs []*engine.AttributeDef
		for i, def := range attrDefs {
			def.Attr.Name = attrNames[i]
			defs = append(defs, def)
			rawDefs = append(rawDefs, def)
		}
		pkDef := &engine.PrimaryIndexDef{Names: []string{"a", "b"}}

		defs = append(defs, pkDef)

		err = dbDesc.Create(0, "A", defs)
		convey.So(err, convey.ShouldBeNil)

		tableDesc, err := dbDesc.Relation("A")
		convey.So(err, convey.ShouldBeNil)

		//make data
		bat := tuplecodec.MakeBatch(10, attrNames, rawDefs)

		vec0 := bat.Vecs[0].Col.([]uint64)
		vec1 := bat.Vecs[1].Col.([]uint64)

		for i := 0; i < 10; i++ {
			vec0[i] = uint64(i)
			vec1[i] = uint64(i)
		}
		bat.Zs = nil
		err = tableDesc.Write(0, bat)
		convey.So(err, convey.ShouldBeNil)

		var get *batch.Batch

		readers := tableDesc.NewReader(10, nil, nil)
		for i, reader := range readers {
			if i == 0 {
				for {
					get, err = reader.Read([]uint64{1, 1},
						[]string{"a", "c"})
					if get == nil {
						break
					}

					columnMapping := [][]int{
						{0, 0},
						{2, 1},
					}
					for _, colIdx := range columnMapping {
						a := bat.Vecs[colIdx[0]].Col.([]uint64)
						b := get.Vecs[colIdx[1]].Col.([]uint64)
						convey.So(a, convey.ShouldResemble, b)
					}
				}
			} else {
				get, err = reader.Read([]uint64{1, 1}, []string{"a", "b"})
				convey.So(get, convey.ShouldBeNil)
				convey.So(err, convey.ShouldBeNil)
			}
		}
	})
}

func TestSort(t *testing.T) {
	attrs := []string{
		"P_CATEGORY", "P_BRAND", "LO_ORDERKEY", "LO_REVENUE", "S_REGION", "LO_ORDERDATE", "LO_LINENUMBER",
	}

	var indexes []int = make([]int, len(attrs))
	for i := 0; i < len(attrs); i++ {
		indexes[i] = i
	}

	sort.Slice(indexes, func(i, j int) bool {
		ai := indexes[i]
		bi := indexes[j]
		a := attrs[ai]
		b := attrs[bi]
		return strings.Compare(a, b) < 0
	})

	for _, index := range indexes {
		s := attrs[index]
		fmt.Printf("%s ", s)
	}
	fmt.Println()
	sort.Strings(attrs)
	fmt.Printf("%v\n", attrs)
}

func Test_ParallelReader(t *testing.T) {
	convey.Convey("read with ParallelReader or multiNode", t, func() {

		tpe, err := NewTpeEngine(&TpeConfig{
			KvType:                    tuplecodec.KV_MEMORY,
			SerialType:                tuplecodec.ST_JSON,
			ValueLayoutSerializerType: "default",
			KVLimit:                   10000})
		convey.So(err, convey.ShouldBeNil)
		err = tpe.Create(0, "test", 0)
		convey.So(err, convey.ShouldBeNil)

		dbDesc, err := tpe.Database("test")
		convey.So(err, convey.ShouldBeNil)

		//(a,b,c)
		//(uint64,uint64,uint64)
		_, attrDefs := tuplecodec.MakeAttributes(types.T_uint64, types.T_uint64, types.T_uint64)

		attrNames := []string{
			"a", "b", "c",
		}
		var defs []engine.TableDef
		var rawDefs []*engine.AttributeDef
		for i, def := range attrDefs {
			def.Attr.Name = attrNames[i]
			defs = append(defs, def)
			rawDefs = append(rawDefs, def)
		}

		err = dbDesc.Create(0, "A", defs)
		convey.So(err, convey.ShouldBeNil)

		tableDesc, err := dbDesc.Relation("A")
		convey.So(err, convey.ShouldBeNil)

		//make data
		bat := tuplecodec.MakeBatch(10, attrNames, rawDefs)

		bat.Zs = nil
		err = tableDesc.Write(0, bat)
		convey.So(err, convey.ShouldBeNil)

		readers := tableDesc.NewReader(1, nil, nil)
		readers[0].(*TpeReader).parallelReader = false
		readers[0].(*TpeReader).multiNode = false

		_, err = readers[0].Read([]uint64{1, 1}, []string{"a", "b"})
		convey.So(err, convey.ShouldBeNil)

		readers = tableDesc.NewReader(1, nil, nil)
		readers[0].(*TpeReader).parallelReader = true

		readers[0].(*TpeReader).shardInfos = make([]ShardInfo, 2)
		_, err = readers[0].Read([]uint64{1, 1}, []string{"a", "b"})
		convey.So(err, convey.ShouldResemble, errors.New("unsupported in memory kv"))

		readers[0].(*TpeReader).multiNode = true
		readers[0].(*TpeReader).printBatch = true
		readers[0].(*TpeReader).readCtx.CompleteInShard = true
		_, err = readers[0].Read([]uint64{1, 1}, []string{"a", "b"})
		convey.So(err, convey.ShouldResemble, errors.New("unsupported in memory kv"))

		_, err = readers[0].Read([]uint64{}, []string{"a", "b"})
		convey.So(err, convey.ShouldResemble, errorInvalidParameters)

		_, err = readers[0].Read([]uint64{1}, []string{"a", "b"})
		convey.So(err, convey.ShouldResemble, errorMismatchRefcntWithAttributeCnt)

		_, err = readers[0].Read([]uint64{1, 1}, []string{"a", "d"})
		convey.So(err, convey.ShouldResemble, errorSomeAttributeNamesAreNotInAttributeDesc)
	})
}


func Test_printBatch(t *testing.T) {
	convey.Convey("test printBatch", t, func() {
		tpe, err := NewTpeEngine(&TpeConfig{
			KvType:                    tuplecodec.KV_MEMORY,
			SerialType:                tuplecodec.ST_JSON,
			ValueLayoutSerializerType: "default",
			KVLimit:                   10000})
		convey.So(err, convey.ShouldBeNil)
		err = tpe.Create(0, "test", 0)
		convey.So(err, convey.ShouldBeNil)

		dbDesc, err := tpe.Database("test")
		convey.So(err, convey.ShouldBeNil)

		//(a,b,c)
		//(uint64,uint64,uint64)
		_, attrDefs := tuplecodec.MakeAttributes(types.T_int8, types.T_uint8, types.T_int16, types.T_uint16, types.T_int32, 
										types.T_uint32, types.T_int64, types.T_uint64, types.T_float32, types.T_float64, 
										types.T_char, types.T_varchar, types.T_date, types.T_datetime)

		attrNames := []string{
			"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n",
		}
		var defs []engine.TableDef
		var rawDefs []*engine.AttributeDef
		for i, def := range attrDefs {
			def.Attr.Name = attrNames[i]
			defs = append(defs, def)
			rawDefs = append(rawDefs, def)
		}

		err = dbDesc.Create(0, "A", defs)
		convey.So(err, convey.ShouldBeNil)

		tableDesc, err := dbDesc.Relation("A")
		convey.So(err, convey.ShouldBeNil)

		cnt := 1
		bat := tuplecodec.MakeBatch(cnt, attrNames, attrDefs)
		lines := [][]string{{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "2022-04-19", "2022-04-19 12:20:00"}}
		tuplecodec.FillBatch(lines, bat)

		bat.Zs = nil
		err = tableDesc.Write(0, bat)

		convey.So(err, convey.ShouldBeNil)

		var get *batch.Batch
		readers := tableDesc.NewReader(1, nil, nil)
		get, err = readers[0].Read(make([]uint64, 14), attrNames)
		printBatch(readers[0].(*TpeReader), get, attrNames)

		for i := 0; i < len(bat.Vecs); i++ {
			bat.Vecs[i].Nsp = &nulls.Nulls{Np: roaring.New()}
			bat.Vecs[i].Nsp.Np.AddInt(1)	
		}
		printBatch(readers[0].(*TpeReader), bat, attrNames)

		for i := 0; i < len(bat.Vecs); i++ {
			bat.Vecs[i].Nsp = &nulls.Nulls{Np: roaring.New()}
			bat.Vecs[i].Nsp.Np.AddInt(0)	
		}
		printBatch(readers[0].(*TpeReader), bat, attrNames)
	})
}