package memEngine

import (
	"fmt"
	"log"
	"matrixbase/pkg/compress"
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/vm/engine"
	"matrixbase/pkg/vm/engine/memEngine/kv"
	"matrixbase/pkg/vm/metadata"
)

func NewTestEngine() engine.Engine {
	e := New(kv.New())
	{
		var attrs []metadata.Attribute

		{
			attrs = append(attrs, metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "orderId",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			})
			attrs = append(attrs, metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "uid",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			})
			attrs = append(attrs, metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "price",
				Type: types.Type{types.T(types.T_float64), 8, 8, 0},
			})
		}
		if err := e.Create("test", attrs); err != nil {
			log.Fatal(err)
		}
	}
	r, err := e.Relation("test")
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
	return e
}
