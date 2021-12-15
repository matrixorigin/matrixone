package testutil

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/rpcserver"
	//"github.com/matrixorigin/matrixone/pkg/sql/handler"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/kv"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func NewTestServer(e engine.Engine, proc *process.Process) (rpcserver.Server, error) {
	srv, err := rpcserver.New(fmt.Sprintf("127.0.0.1:%v", 40100), 1<<30, logutil.GetGlobalLogger())
	if err != nil {
		return nil, err
	}
	//hp := handler.New(e, proc)
	//srv.Register(hp.Process)
	return srv, nil
}

func NewTestEngine() (engine.Engine, error) {
	e := memEngine.New(kv.New(), engine.Node{Id: "0", Addr: "127.0.0.1:40000"})
	err := e.Create(0, "test", 0)
	if err != nil {
		return nil, err
	}
	db, _ := e.Database("test")
	if err := CreateR(db); err != nil {
		return nil, err
	}
	if err := CreateS(db); err != nil {
		return nil, err
	}
	return e, nil
}

func CreateR(db engine.Database) error {
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{
				engine.Attribute{
					Alg:  compress.Lz4,
					Name: "orderId",
					Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
				}})
			attrs = append(attrs, &engine.AttributeDef{
				engine.Attribute{
				Alg:  compress.Lz4,
				Name: "uid",
				Type: types.Type{types.T(types.T_varchar), 24, 0, 0},
			}})
			attrs = append(attrs, &engine.AttributeDef{
				engine.Attribute{
				Alg:  compress.Lz4,
				Name: "price",
				Type: types.Type{types.T(types.T_float64), 8, 8, 0},
			}})
		}
		if err := db.Create(0, "R", attrs); err != nil {
			return err
		}
	}
	r, err := db.Relation("R")
	if err != nil {
		return err
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
				if err := vector.Append(vec,vs); err != nil {
					return err
				}
				bat.Vecs[0] = vec
			}
			{
				vec := vector.New(types.Type{types.T(types.T_varchar), 24, 0, 0})
				vs := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					vs[i] = []byte(fmt.Sprintf("%v", i%4))
				}
				if err := vector.Append(vec,vs); err != nil {
					return err
				}
				bat.Vecs[1] = vec
			}
			{
				vec := vector.New(types.Type{types.T(types.T_float64), 8, 8, 0})
				vs := make([]float64, 10)
				for i := 0; i < 10; i++ {
					vs[i] = float64(i)
				}
				if err := vector.Append(vec,vs); err != nil {
					return err
				}
				bat.Vecs[2] = vec
			}
		}
		if err := r.Write(0, bat); err != nil {
			return err
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
			if err := vector.Append(vec,vs); err != nil {
				return err
			}
			bat.Vecs[0] = vec
		}
		{
			vec := vector.New(types.Type{types.T(types.T_varchar), 24, 0, 0})
			vs := make([][]byte, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = []byte(fmt.Sprintf("%v", i%4))
			}
			if err := vector.Append(vec,vs); err != nil {
				return err
			}
			bat.Vecs[1] = vec
		}
		{
			vec := vector.New(types.Type{types.T(types.T_float64), 8, 8, 0})
			vs := make([]float64, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = float64(i)
			}
			if err := vector.Append(vec,vs); err != nil {
				return err
			}
			bat.Vecs[2] = vec
		}
		if err := r.Write(0, bat); err != nil {
			return err
		}
	}
	return nil
}

func CreateS(db engine.Database) error {
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
			return err
		}
	}
	r, err := db.Relation("S")
	if err != nil {
		return err
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
				if err := vector.Append(vec,vs); err != nil {
					return err
				}
				bat.Vecs[0] = vec
			}
			{
				vec := vector.New(types.Type{types.T(types.T_varchar), 24, 0, 0})
				vs := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					vs[i] = []byte(fmt.Sprintf("%v", i%2))
				}
				if err := vector.Append(vec,vs); err != nil {
					return err
				}
				bat.Vecs[1] = vec
			}
			{
				vec := vector.New(types.Type{types.T(types.T_float64), 8, 8, 0})
				vs := make([]float64, 10)
				for i := 0; i < 10; i++ {
					vs[i] = float64(i)
				}
				if err := vector.Append(vec,vs); err != nil {
					return err
				}
				bat.Vecs[2] = vec
			}
		}
		if err := r.Write(0, bat); err != nil {
			return err
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
			if err := vector.Append(vec,vs); err != nil {
				return err
			}
			bat.Vecs[0] = vec
		}
		{
			vec := vector.New(types.Type{types.T(types.T_varchar), 24, 0, 0})
			vs := make([][]byte, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = []byte(fmt.Sprintf("%v", i%2))
			}
			if err := vector.Append(vec,vs); err != nil {
				return err
			}
			bat.Vecs[1] = vec
		}
		{
			vec := vector.New(types.Type{types.T(types.T_float64), 8, 8, 0})
			vs := make([]float64, 10)
			for i := 10; i < 20; i++ {
				vs[i-10] = float64(i)
			}
			if err := vector.Append(vec,vs); err != nil {
				return err
			}
			bat.Vecs[2] = vec
		}
		if err := r.Write(0, bat); err != nil {
			return err
		}
	}
	return nil
}