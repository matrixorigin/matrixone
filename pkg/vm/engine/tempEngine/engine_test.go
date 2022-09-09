// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package tempengine

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/require"
)

func TestEngine(t *testing.T) {
	e := NewTempEngine()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	ctx := context.TODO()
	db, _ := e.Database(ctx, "test", nil)
	TestBats := CreateR(db)
	rel, err := db.Relation(ctx, "test-r")
	require.NoError(t, err)
	read, err := rel.NewReader(ctx, 1, nil, nil)
	require.NoError(t, err)
	require.Equal(t, len(read), 1)
	var idx int
	// note that, append operation in for clause is not secure
	for {
		bat, err := read[0].Read([]string{"orderid", "uid", "price"}, nil, m)
		require.NoError(t, err)
		if bat == nil {
			break
		}
		require.Equal(t, TestBats[idx], bat)
		idx++
	}
	require.Equal(t, int(rel.Rows()), 20)
	read[0].Close()
	require.Equal(t, idx, len(TestBats))
	// these are just used to improve code coverage below, we will make it sense after func is implemented
	rel.AddTableDef(ctx, nil)
	rel.DelTableDef(ctx, nil)
	rel.Rows()
	rel.GetHideKeys(ctx)
	rel.GetPrimaryKeys(ctx)
	rel.Ranges(ctx)
	rel.TableDefs(ctx)
}

func CreateR(db engine.Database) (bats []*batch.Batch) {
	ctx := context.TODO()
	{
		var attrs []engine.TableDef

		{
			attrs = append(attrs, &engine.AttributeDef{
				Attr: engine.Attribute{
					Alg:  compress.None,
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
					Alg:  compress.None,
					Name: "price",
					Type: types.Type{
						Size: 8,
						Oid:  types.T(types.T_float64),
					},
				}})
		}
		if err := db.Create(ctx, "test-r", attrs); err != nil {
			log.Fatal(err)
		}
	}
	r, err := db.Relation(ctx, "test-r")
	if err != nil {
		log.Fatal(err)
	}
	{
		bat := batch.New(true, []string{"orderid", "uid", "price"})
		{
			{
				vec := vector.NewOriginal(types.Type{
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
				vec := vector.NewOriginal(types.Type{
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
				vec := vector.NewOriginal(types.Type{
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
		bat.InitZsOne(10)
		if err := r.Write(ctx, bat); err != nil {
			log.Fatal(err)
		}
		bats = append(bats, bat)
	}
	{
		bat := batch.New(true, []string{"orderid", "uid", "price"})
		{
			vec := vector.NewOriginal(types.Type{
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
			vec := vector.NewOriginal(types.Type{
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
			vec := vector.NewOriginal(types.Type{
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
		bat.InitZsOne(10)
		if err := r.Write(ctx, bat); err != nil {
			log.Fatal(err)
		}
		bats = append(bats, bat)
	}
	return
}
