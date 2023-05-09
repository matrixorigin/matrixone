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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var testEngineMp = testutil.TestUtilMp

func CreateR(db engine.Database) {
	ctx := context.TODO()
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "orderid",
					Type: types.New(types.T_varchar, 10, 0),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "uid",
					Type: types.T_uint32.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "price",
					Type: types.T_float64.ToType(),
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
				vec := vector.NewVec(types.T_varchar.ToType())
				vs := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					vs[i] = []byte(fmt.Sprintf("%v", i))
				}
				// XXX MPOOL
				if err := vector.AppendBytesList(vec, vs, nil, testEngineMp); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[0] = vec
			}
			{
				vec := vector.NewVec(types.T_uint32.ToType())
				vs := make([]uint32, 10)
				for i := 0; i < 10; i++ {
					vs[i] = uint32(i % 4)
				}
				if err := vector.AppendFixedList(vec, vs, nil, testEngineMp); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[1] = vec
			}
			{
				vec := vector.NewVec(types.T_float64.ToType())
				vs := make([]float64, 10)
				for i := 0; i < 10; i++ {
					vs[i] = float64(i)
				}
				if err := vector.AppendFixedList(vec, vs, nil, testEngineMp); err != nil {
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
			vec := vector.NewVec(types.T_varchar.ToType())
			vs := make([][]byte, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = []byte(fmt.Sprintf("%v", i))
			}
			if err := vector.AppendBytesList(vec, vs, nil, testEngineMp); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[0] = vec
		}
		{
			vec := vector.NewVec(types.T_uint32.ToType())
			vs := make([]uint32, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = uint32(i % 4)
			}
			if err := vector.AppendFixedList(vec, vs, nil, testEngineMp); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[1] = vec
		}
		{
			vec := vector.NewVec(types.T_float64.ToType())
			vs := make([]float64, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = float64(i)
			}
			if err := vector.AppendFixedList(vec, vs, nil, testEngineMp); err != nil {
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
					Type: types.New(types.T_varchar, 10, 0),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "uid",
					Type: types.T_uint32.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "price",
					Type: types.T_float64.ToType(),
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
				vec := vector.NewVec(types.T_varchar.ToType())
				vs := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					vs[i] = []byte(fmt.Sprintf("%v", i*2))
				}
				if err := vector.AppendBytesList(vec, vs, nil, testEngineMp); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[0] = vec
			}
			{
				vec := vector.NewVec(types.T_uint32.ToType())
				vs := make([]uint32, 10)
				for i := 0; i < 10; i++ {
					vs[i] = uint32(i % 2)
				}
				if err := vector.AppendFixedList(vec, vs, nil, testEngineMp); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[1] = vec
			}
			{
				vec := vector.NewVec(types.T_float64.ToType())
				vs := make([]float64, 10)
				for i := 0; i < 10; i++ {
					vs[i] = float64(i)
				}
				if err := vector.AppendFixedList(vec, vs, nil, testEngineMp); err != nil {
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
			vec := vector.NewVec(types.T_varchar.ToType())
			vs := make([][]byte, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = []byte(fmt.Sprintf("%v", i*2))
			}
			if err := vector.AppendBytesList(vec, vs, nil, testEngineMp); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[0] = vec
		}
		{
			vec := vector.NewVec(types.T_uint32.ToType())
			vs := make([]uint32, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = uint32(i % 2)
			}
			if err := vector.AppendFixedList(vec, vs, nil, testEngineMp); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[1] = vec
		}
		{
			vec := vector.NewVec(types.T_float64.ToType())
			vs := make([]float64, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = float64(i)
			}
			if err := vector.AppendFixedList(vec, vs, nil, testEngineMp); err != nil {
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
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "price",
					Type: types.T_float64.ToType(),
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
					Type: types.T_int32.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "userid",
					Type: types.T_int32.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "score",
					Type: types.T_int8.ToType(),
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
			vec := vector.NewVec(types.T_int32.ToType())
			vs := make([]int32, 5)
			vs[0] = 1
			vs[1] = 2
			vs[2] = 2
			vs[3] = 3
			vs[4] = 1
			if err := vector.AppendFixedList(vec, vs, nil, testEngineMp); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[0] = vec
		}
		{
			vec := vector.NewVec(types.T_int32.ToType())
			vs := make([]int32, 5)
			vs[0] = 1
			vs[1] = 2
			vs[2] = 1
			vs[3] = 3
			vs[4] = 1
			if err := vector.AppendFixedList(vec, vs, nil, testEngineMp); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[1] = vec
		}
		{
			vec := vector.NewVec(types.T_int8.ToType())
			vs := make([]int8, 5)
			vs[0] = 1
			vs[1] = 2
			vs[2] = 4
			vs[3] = 3
			vs[4] = 5
			if err := vector.AppendFixedList(vec, vs, nil, testEngineMp); err != nil {
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
			vec := vector.NewVec(types.T_int32.ToType())
			vs := make([]int32, 2)
			vs[0] = 4
			vs[1] = 5
			if err := vector.AppendFixedList(vec, vs, nil, testEngineMp); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[0] = vec
		}
		{
			vec := vector.NewVec(types.T_int32.ToType())
			vs := make([]int32, 2)
			vs[0] = 6
			vs[1] = 11
			if err := vector.AppendFixedList(vec, vs, nil, testEngineMp); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[1] = vec
		}
		{
			vec := vector.NewVec(types.T_int8.ToType())
			vs := make([]int8, 2)
			vs[0] = 10
			vs[1] = 99
			if err := vector.AppendFixedList(vec, vs, nil, testEngineMp); err != nil {
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
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_name",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_address",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_city",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_nation",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_region",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_phone",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c_mktsegment",
					Type: types.T_varchar.ToType(),
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
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_linenumber",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_custkey",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_partkey",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_suppkey",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_orderdate",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_orderpriority",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_shippriority",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_quantity",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_extendedprice",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_ordtotalprice",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_discount",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_revenue",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_supplycost",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_tax",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_commitdate",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "lo_shipmode",
					Type: types.T_varchar.ToType(),
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
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_name",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_mfgr",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_category",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_brand",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_color",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_type",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_size",
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "p_container",
					Type: types.T_varchar.ToType(),
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
					Type: types.T_int64.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "s_name",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "s_address",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "s_city",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "s_nation",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "s_region",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "s_phone",
					Type: types.T_varchar.ToType(),
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
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_date",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_dayofweek",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_month",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_year",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_yearmonthnum",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_yearmonth",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_daynumnweek",
					Type: types.T_varchar.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d_weeknuminyear",
					Type: types.T_varchar.ToType(),
				}})
		}
		if err := db.Create(ctx, "dates", attrs); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateCompressFileTable(db engine.Database) {
	ctx := context.TODO()
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "a",
					Type: types.T_int32.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "b",
					Type: types.T_int32.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "c",
					Type: types.T_int32.ToType(),
				}})
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.Lz4,
					Name: "d",
					Type: types.T_int32.ToType(),
				}})
		}
		if err := db.Create(ctx, "pressTbl", attrs); err != nil {
			log.Fatal(err)
		}
	}
}
