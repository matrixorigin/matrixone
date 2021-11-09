package memEngine

import (
	"fmt"
	"log"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/memEngine/kv"
)

func NewTestEngine() engine.Engine {
	e := New(kv.New(), engine.Node{Id: "0", Addr: "127.0.0.1"})
	db, _ := e.Database("test")
	CreateR(db)
	CreateS(db)
	CreateT(db)
	CreateT1(db)
	{ // star schema benchmark
		CreatePart(db)
		CreateDate(db)
		CreateSupplier(db)
		CreateCustomer(db)
		CreateLineorder(db)
	}
	return e
}

func CreateR(db engine.Database) {
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "orderId",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "uid",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "price",
				Type: types.Type{types.T(types.T_float64), 8, 8, 0},
			}})
		}
		if err := db.Create(0, "R", attrs); err != nil {
			log.Fatal(err)
		}
	}
	r, err := db.Relation("R")
	if err != nil {
		log.Fatal(err)
	}
	{
		bat := batch.New(true, []string{"orderId", "uid", "price"})
		{
			{
				vec := vector.New(types.Type{types.T(types.T_varchar), 24, 0, 0})
				vs := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					vs[i] = []byte(fmt.Sprintf("%v", i))
				}
				if err := vector.Append(vec, vs); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[0] = vec
			}
			{
				vec := vector.New(types.Type{types.T(types.T_varchar), 24, 0, 0})
				vs := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					vs[i] = []byte(fmt.Sprintf("%v", i%4))
				}
				if err := vector.Append(vec, vs); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[1] = vec
			}
			{
				vec := vector.New(types.Type{types.T(types.T_float64), 8, 8, 0})
				vs := make([]float64, 10)
				for i := 0; i < 10; i++ {
					vs[i] = float64(i)
				}
				if err := vector.Append(vec, vs); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[2] = vec
			}
		}
		if err := r.Write(0, bat); err != nil {
			log.Fatal(err)
		}
	}
	{
		bat := batch.New(true, []string{"orderId", "uid", "price"})
		{
			vec := vector.New(types.Type{types.T(types.T_varchar), 24, 0, 0})
			vs := make([][]byte, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = []byte(fmt.Sprintf("%v", i))
			}
			if err := vector.Append(vec, vs); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[0] = vec
		}
		{
			vec := vector.New(types.Type{types.T(types.T_varchar), 24, 0, 0})
			vs := make([][]byte, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = []byte(fmt.Sprintf("%v", i%4))
			}
			if err := vector.Append(vec, vs); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[1] = vec
		}
		{
			vec := vector.New(types.Type{types.T(types.T_float64), 8, 8, 0})
			vs := make([]float64, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = float64(i)
			}
			if err := vector.Append(vec, vs); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[2] = vec
		}
		if err := r.Write(0, bat); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateS(db engine.Database) {
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "orderId",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "uid",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "price",
				Type: types.Type{types.T(types.T_float64), 8, 8, 0},
			}})
		}
		if err := db.Create(0, "S", attrs); err != nil {
			log.Fatal(err)
		}
	}
	r, err := db.Relation("S")
	if err != nil {
		log.Fatal(err)
	}
	{
		bat := batch.New(true, []string{"orderId", "uid", "price"})
		{
			{
				vec := vector.New(types.Type{types.T(types.T_varchar), 24, 0, 0})
				vs := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					vs[i] = []byte(fmt.Sprintf("%v", i*2))
				}
				if err := vector.Append(vec, vs); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[0] = vec
			}
			{
				vec := vector.New(types.Type{types.T(types.T_varchar), 24, 0, 0})
				vs := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					vs[i] = []byte(fmt.Sprintf("%v", i%2))
				}
				if err := vector.Append(vec, vs); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[1] = vec
			}
			{
				vec := vector.New(types.Type{types.T(types.T_float64), 8, 8, 0})
				vs := make([]float64, 10)
				for i := 0; i < 10; i++ {
					vs[i] = float64(i)
				}
				if err := vector.Append(vec, vs); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[2] = vec
			}
		}
		if err := r.Write(0, bat); err != nil {
			log.Fatal(err)
		}
	}
	{
		bat := batch.New(true, []string{"orderId", "uid", "price"})
		{
			vec := vector.New(types.Type{types.T(types.T_varchar), 24, 0, 0})
			vs := make([][]byte, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = []byte(fmt.Sprintf("%v", i*2))
			}
			if err := vector.Append(vec, vs); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[0] = vec
		}
		{
			vec := vector.New(types.Type{types.T(types.T_varchar), 24, 0, 0})
			vs := make([][]byte, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = []byte(fmt.Sprintf("%v", i%2))
			}
			if err := vector.Append(vec, vs); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[1] = vec
		}
		{
			vec := vector.New(types.Type{types.T(types.T_float64), 8, 8, 0})
			vs := make([]float64, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = float64(i)
			}
			if err := vector.Append(vec, vs); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[2] = vec
		}
		if err := r.Write(0, bat); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateT(db engine.Database) {
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "orderId",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "uid",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "price",
				Type: types.Type{types.T(types.T_float64), 8, 8, 0},
			}})
		}
		if err := db.Create(0, "T", attrs); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateT1(db engine.Database) {
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "spID",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "userID",
				Type: types.Type{types.T(types.T_int32), 4, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "score",
				Type: types.Type{types.T(types.T_int8), 1, 8, 0},
			}})
		}
		if err := db.Create(0, "t1", attrs); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateCustomer(db engine.Database) {
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "c_custkey",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "c_name",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "c_address",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "c_city",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "c_nation",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "c_region",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "c_phone",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "c_mktsegment",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
		}
		if err := db.Create(0, "customer", attrs); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateLineorder(db engine.Database) {
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_orderkey",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_linenumber",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_custkey",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_partkey",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_suppkey",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_orderdate",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_orderpriority",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_shippriority",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_quantity",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_extendedprice",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_ordtotalprice",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_discount",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_revenue",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_supplycost",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_tax",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_commitdate",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "lo_shipmode",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
		}
		if err := db.Create(0, "lineorder", attrs); err != nil {
			log.Fatal(err)
		}
	}
}

func CreatePart(db engine.Database) {
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "p_partkey",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "p_name",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "p_mfgr",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "p_category",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "p_brand",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "p_color",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "p_type",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "p_size",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "p_container",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
		}
		if err := db.Create(0, "part", attrs); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateSupplier(db engine.Database) {
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "s_suppkey",
				Type: types.Type{types.T(types.T_int64), 8, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "s_name",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "s_address",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "s_city",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "s_nation",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "s_region",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "s_phone",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
		}
		if err := db.Create(0, "supplier", attrs); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateDate(db engine.Database) {
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "d_datekey",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "d_date",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "d_dayofweek",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "d_month",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "d_year",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "d_yearmonthnum",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "d_yearmonth",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "d_daynumnweek",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{engine.Attribute{
				Alg:  compress.Lz4,
				Name: "d_weeknuminyear",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
		}
		if err := db.Create(0, "dates", attrs); err != nil {
			log.Fatal(err)
		}
	}
}
