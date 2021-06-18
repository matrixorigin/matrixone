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
	e := New(kv.New())
	db, _ := e.Database("default")
	CreateR(db)
	CreateS(db)
	CreateT(db)
	CreateW(db)
	CreateV(db)
	CreateVCount(db,10000)
	CreateVCount(db,100000)
	CreateVCount(db,1000000)

	tables := []string{
		"R",
		"S",
		"T",
		"V",
		"W",
		"V10000",
		"V100000",
		"V1000000",
	}
	for _,rel := range tables {
		fmt.Printf("test table: %s\n",rel)
	}
	return e
}

func CreateR(e engine.Database) {
	{
		var defs []engine.TableDef

		{
			defs = append(defs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "orderId",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			defs = append(defs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "uid",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			defs = append(defs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "price",
				Type: types.Type{types.T(types.T_float64), 8, 8, 0},
			}})
		}
		if err := e.Create("R", defs, nil, nil); err != nil {
			log.Fatal(err)
		}
	}
	r, err := e.Relation("R")
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
		if err := r.Write(bat); err != nil {
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
		if err := r.Write(bat); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateS(e engine.Database) {
	{
		var defs []engine.TableDef

		{
			defs = append(defs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "orderId",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			defs = append(defs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "uid",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			defs = append(defs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "price",
				Type: types.Type{types.T(types.T_float64), 8, 8, 0},
			}})
		}

		if err := e.Create("S", defs, nil, nil); err != nil {
			log.Fatal(err)
		}
	}
	r, err := e.Relation("S")
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
		if err := r.Write(bat); err != nil {
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
		if err := r.Write(bat); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateT(e engine.Database) {
	{
		var defs []engine.TableDef

		{
			defs = append(defs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "orderId",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			defs = append(defs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "uid",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			defs = append(defs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "amount",
				Type: types.Type{types.T(types.T_float64), 8, 8, 0},
			}})
		}

		if err := e.Create("T", defs, nil, nil); err != nil {
			log.Fatal(err)
		}
	}
	r, err := e.Relation("T")
	if err != nil {
		log.Fatal(err)
	}
	{
		bat := batch.New(true, []string{"orderId", "uid", "amount"})
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
		if err := r.Write(bat); err != nil {
			log.Fatal(err)
		}
	}
	{
		bat := batch.New(true, []string{"orderId", "uid", "amount"})
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
		if err := r.Write(bat); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateW(e engine.Database) {
	{
		var defs []engine.TableDef

		{
			defs = append(defs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "A",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			defs = append(defs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "B",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			defs = append(defs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "C",
				Type: types.Type{types.T(types.T_float64), 8, 8, 0},
			}})
		}

		if err := e.Create("W", defs, nil, nil); err != nil {
			log.Fatal(err)
		}
	}
	r, err := e.Relation("W")
	if err != nil {
		log.Fatal(err)
	}
	{
		bat := batch.New(true, []string{"A", "B", "C"})
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
		if err := r.Write(bat); err != nil {
			log.Fatal(err)
		}
	}
	{
		bat := batch.New(true, []string{"A", "B", "C"})
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
		if err := r.Write(bat); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateV(e engine.Database) {
	{
		var defs []engine.TableDef

		{
			defs = append(defs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "C",
				Type: types.Type{types.T(types.T_float64), 8, 8, 0},
			}})
		}

		if err := e.Create("V", defs, nil, nil); err != nil {
			log.Fatal(err)
		}
	}
	r, err := e.Relation("V")
	if err != nil {
		log.Fatal(err)
	}
	{
		bat := batch.New(true, []string{"C"})
		{
			{
				vec := vector.New(types.Type{types.T(types.T_float64), 8, 8, 0})
				vs := make([]float64, 10)
				for i := 0; i < 10; i++ {
					vs[i] = float64(i)
				}
				if err := vec.Append(vs); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[0] = vec
			}
		}
		if err := r.Write(bat); err != nil {
			log.Fatal(err)
		}
	}
}

func CreateVCount(e engine.Database,count int) {
	name := fmt.Sprintf("V%d",count)
	{
		var defs []engine.TableDef

		{
			defs = append(defs, &engine.AttributeDef{metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "C",
				Type: types.Type{types.T(types.T_float64), 8, 8, 0},
			}})
		}

		if err := e.Create(name, defs, nil, nil); err != nil {
			log.Fatal(err)
		}
	}
	r, err := e.Relation(name)
	if err != nil {
		log.Fatal(err)
	}
	{
		bat := batch.New(true, []string{"C"})
		{
			{
				vec := vector.New(types.Type{types.T(types.T_float64), 8, 8, 0})
				vs := make([]float64, count)
				for i := 0; i < count; i++ {
					vs[i] = float64(i)
				}
				if err := vec.Append(vs); err != nil {
					log.Fatal(err)
				}
				bat.Vecs[0] = vec
			}
		}
		if err := r.Write(bat); err != nil {
			log.Fatal(err)
		}
	}
}