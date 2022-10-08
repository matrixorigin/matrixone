// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testengine

import (
	"context"
	"fmt"
	"log"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func CreateR(db engine.Database) {
	ctx := context.TODO()
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "orderid",
					Type: types.Type{
						Size:  24,
						Width: 10,
						Oid:   types.T(types.T_varchar),
					},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "uid",
					Type: types.Type{
						Size: 4,
						Oid:  types.T(types.T_uint32),
					},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "price",
					Type: types.Type{
						Size: 8,
						Oid:  types.T(types.T_float64),
					},
				}})
		}
		if err := db.Create(ctx, "r", attrs); err != nil {
			log.Fatal(err)
		}
	}
	r, err := db.Relation(ctx, "r")
	if err != nil {
		log.Fatal(err)
	}
	{
		bat := batch.New(true, []string{"orderid", "uid", "price"})
		{
			{
				vec := vector.New(types.Type{
					Size: 24,
					Oid:  types.T(types.T_varchar),
				})
				vs := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					vs[i] = []byte(fmt.Sprintf("%v", i))
				}
				if err := vector.AppendBytes(vec, vs, nil); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[0] = vec
			}
			{
				vec := vector.New(types.Type{
					Size: 4,
					Oid:  types.T(types.T_uint32),
				})
				vs := make([]uint32, 10)
				for i := 0; i < 10; i++ {
					vs[i] = uint32(i % 4)
				}
				if err := vector.AppendFixed(vec, vs, nil); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[1] = vec
			}
			{
				vec := vector.New(types.Type{
					Size: 8,
					Oid:  types.T(types.T_float64),
				})
				vs := make([]float64, 10)
				for i := 0; i < 10; i++ {
					vs[i] = float64(i)
				}
				if err := vector.AppendFixed(vec, vs, nil); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[2] = vec
			}
		}
		if err := r.Write(ctx, bat); err != nil {
			log.Fatal(err)
		}
	}
	{
		bat := batch.New(true, []string{"orderid", "uid", "price"})
		{
			vec := vector.New(types.Type{
				Size: 24,
				Oid:  types.T(types.T_varchar),
			})
			vs := make([][]byte, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = []byte(fmt.Sprintf("%v", i))
			}
			if err := vector.AppendBytes(vec, vs, nil); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[0] = vec
		}
		{
			vec := vector.New(types.Type{
				Size: 4,
				Oid:  types.T(types.T_uint32),
			})
			vs := make([]uint32, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = uint32(i % 4)
			}
			if err := vector.AppendFixed(vec, vs, nil); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[1] = vec
		}
		{
			vec := vector.New(types.Type{
				Size: 8,
				Oid:  types.T(types.T_float64),
			})
			vs := make([]float64, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = float64(i)
			}
			if err := vector.AppendFixed(vec, vs, nil); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[2] = vec
		}
		if err := r.Write(ctx, bat); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateS(db engine.Database) {
	ctx := context.TODO()
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "orderid",
					Type: types.Type{
						Size:  24,
						Width: 10,
						Oid:   types.T(types.T_varchar),
					},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "uid",
					Type: types.Type{
						Size: 4,
						Oid:  types.T(types.T_uint32),
					},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "price",
					Type: types.Type{
						Size: 8,
						Oid:  types.T(types.T_float64),
					},
				}})
		}
		if err := db.Create(ctx, "s", attrs); err != nil {
			log.Fatal(err)
		}
	}
	r, err := db.Relation(ctx, "s")
	if err != nil {
		log.Fatal(err)
	}
	{
		bat := batch.New(true, []string{"orderid", "uid", "price"})
		{
			{
				vec := vector.New(types.Type{
					Size: 24,
					Oid:  types.T(types.T_varchar),
				})
				vs := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					vs[i] = []byte(fmt.Sprintf("%v", i*2))
				}
				if err := vector.AppendBytes(vec, vs, nil); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[0] = vec
			}
			{
				vec := vector.New(types.Type{
					Size: 4,
					Oid:  types.T(types.T_uint32),
				})
				vs := make([]uint32, 10)
				for i := 0; i < 10; i++ {
					vs[i] = uint32(i % 2)
				}
				if err := vector.AppendFixed(vec, vs, nil); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[1] = vec
			}
			{
				vec := vector.New(types.Type{
					Size: 8,
					Oid:  types.T(types.T_float64),
				})
				vs := make([]float64, 10)
				for i := 0; i < 10; i++ {
					vs[i] = float64(i)
				}
				if err := vector.AppendFixed(vec, vs, nil); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[2] = vec
			}
		}
		if err := r.Write(ctx, bat); err != nil {
			log.Fatal(err)
		}
	}
	{
		bat := batch.New(true, []string{"orderid", "uid", "price"})
		{
			vec := vector.New(types.Type{
				Size: 24,
				Oid:  types.T(types.T_varchar),
			})
			vs := make([][]byte, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = []byte(fmt.Sprintf("%v", i*2))
			}
			if err := vector.AppendBytes(vec, vs, nil); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[0] = vec
		}
		{
			vec := vector.New(types.Type{
				Size: 4,
				Oid:  types.T(types.T_uint32),
			})
			vs := make([]uint32, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = uint32(i % 2)
			}
			if err := vector.AppendFixed(vec, vs, nil); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[1] = vec
		}
		{
			vec := vector.New(types.Type{
				Size: 8,
				Oid:  types.T(types.T_float64),
			})
			vs := make([]float64, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = float64(i)
			}
			if err := vector.AppendFixed(vec, vs, nil); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[2] = vec
		}
		if err := r.Write(ctx, bat); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateT(db engine.Database) {
	ctx := context.TODO()
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "id",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "price",
					Type: types.Type{Oid: types.T(types.T_float64), Size: 8, Width: 8, Precision: 0},
				}})
		}
		if err := db.Create(ctx, "t", attrs); err != nil {
			log.Fatal(err)
		}
	}

}

func CreateT1(db engine.Database) {
	ctx := context.TODO()
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "spid",
					Type: types.Type{Oid: types.T(types.T_int32), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "userid",
					Type: types.Type{Oid: types.T(types.T_int32), Size: 4, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "score",
					Type: types.Type{Oid: types.T(types.T_int8), Size: 1, Width: 8, Precision: 0},
				}})
		}
		if err := db.Create(ctx, "t1", attrs); err != nil {
			log.Fatal(err)
		}
	}
	r, err := db.Relation(ctx, "t1")
	if err != nil {
		log.Fatal(err)
	}
	{
		bat := batch.New(true, []string{"spid", "userid", "score"})
		{
			vec := vector.New(types.Type{Oid: types.T(types.T_int32), Size: 4, Width: 4, Precision: 0})
			vs := make([]int32, 5)
			vs[0] = 1
			vs[1] = 2
			vs[2] = 2
			vs[3] = 3
			vs[4] = 1
			if err := vector.AppendFixed(vec, vs, nil); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[0] = vec
		}
		{
			vec := vector.New(types.Type{Oid: types.T(types.T_int32), Size: 4, Width: 4, Precision: 0})
			vs := make([]int32, 5)
			vs[0] = 1
			vs[1] = 2
			vs[2] = 1
			vs[3] = 3
			vs[4] = 1
			if err := vector.AppendFixed(vec, vs, nil); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[1] = vec
		}
		{
			vec := vector.New(types.Type{Oid: types.T(types.T_int8), Size: 1, Width: 1, Precision: 0})
			vs := make([]int8, 5)
			vs[0] = 1
			vs[1] = 2
			vs[2] = 4
			vs[3] = 3
			vs[4] = 5
			if err := vector.AppendFixed(vec, vs, nil); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[2] = vec
		}
		if err := r.Write(ctx, bat); err != nil {
			log.Fatal(err)
		}
	}
	{
		bat := batch.New(true, []string{"spid", "userid", "score"})
		{
			vec := vector.New(types.Type{Oid: types.T(types.T_int32), Size: 4, Width: 4, Precision: 0})
			vs := make([]int32, 2)
			vs[0] = 4
			vs[1] = 5
			if err := vector.AppendFixed(vec, vs, nil); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[0] = vec
		}
		{
			vec := vector.New(types.Type{Oid: types.T(types.T_int32), Size: 4, Width: 4, Precision: 0})
			vs := make([]int32, 2)
			vs[0] = 6
			vs[1] = 11
			if err := vector.AppendFixed(vec, vs, nil); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[1] = vec
		}
		{
			vec := vector.New(types.Type{Oid: types.T(types.T_int8), Size: 1, Width: 1, Precision: 0})
			vs := make([]int8, 2)
			vs[0] = 10
			vs[1] = 99
			if err := vector.AppendFixed(vec, vs, nil); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[2] = vec
		}
		if err := r.Write(ctx, bat); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateCustomer(db engine.Database) {
	ctx := context.TODO()
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_custkey",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_name",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_address",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_city",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_nation",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_region",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_phone",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_mktsegment",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
		}
		if err := db.Create(ctx, "customer", attrs); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateLineorder(db engine.Database) {
	ctx := context.TODO()
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_orderkey",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_linenumber",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_custkey",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_partkey",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_suppkey",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_orderdate",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_orderpriority",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_shippriority",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_quantity",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_extendedprice",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_ordtotalprice",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_discount",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_revenue",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_supplycost",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_tax",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_commitdate",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_shipmode",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
		}
		if err := db.Create(ctx, "lineorder", attrs); err != nil {
			log.Fatal(err)
		}
	}
}

func CreatePart(db engine.Database) {
	ctx := context.TODO()
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_partkey",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_name",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_mfgr",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_category",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_brand",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_color",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_type",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_size",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_container",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
		}
		if err := db.Create(ctx, "part", attrs); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateSupplier(db engine.Database) {
	ctx := context.TODO()
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "s_suppkey",
					Type: types.Type{Oid: types.T(types.T_int64), Size: 8, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "s_name",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "s_address",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "s_city",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "s_nation",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "s_region",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "s_phone",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
		}
		if err := db.Create(ctx, "supplier", attrs); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateDate(db engine.Database) {
	ctx := context.TODO()
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_datekey",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_date",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_dayofweek",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_month",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_year",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_yearmonthnum",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_yearmonth",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_daynumnweek",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_weeknuminyear",
					Type: types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0},
				}})
		}
		if err := db.Create(ctx, "dates", attrs); err != nil {
			log.Fatal(err)
		}
	}
}
