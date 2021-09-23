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
	"matrixone/pkg/vm/metadata"
)

func NewTestEngine() engine.Engine {
	e := New(kv.New(), metadata.Node{Id: "0", Addr: "127.0.0.1:40000"})
	db, _ := e.Database("test")
	CreateR(db)
	CreateS(db)
	return e
}

func CreateR(db engine.Database) {
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "orderId",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "uid",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "price",
				Type: types.Type{types.T(types.T_float64), 8, 8, 0},
			}})
		}
		if err := db.Create(0, "R", attrs, nil, nil, ""); err != nil {
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
				if err := vec.Append(vs); err != nil {
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
				if err := vec.Append(vs); err != nil {
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
				if err := vec.Append(vs); err != nil {
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
			if err := vec.Append(vs); err != nil {
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
			if err := vec.Append(vs); err != nil {
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
			if err := vec.Append(vs); err != nil {
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
			attrs = append(attrs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "orderId",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "uid",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "price",
				Type: types.Type{types.T(types.T_float64), 8, 8, 0},
			}})
		}
		if err := db.Create(0, "S", attrs, nil, nil, ""); err != nil {
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
				if err := vec.Append(vs); err != nil {
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
				if err := vec.Append(vs); err != nil {
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
				if err := vec.Append(vs); err != nil {
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
			if err := vec.Append(vs); err != nil {
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
			if err := vec.Append(vs); err != nil {
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
			if err := vec.Append(vs); err != nil {
				log.Fatal(err)
			}
			bat.Vecs[2] = vec
		}
		if err := r.Write(0, bat); err != nil {
			log.Fatal(err)
		}
	}
}
