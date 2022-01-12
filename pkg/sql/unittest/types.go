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

// Package unittest is a package only to test compute-layer
// We only validate the logic of the statement and not the result
// And There are some restrictions :
// you can test select and dml in this way but test ddl carefully due to memory-engine doesn't implement some interface (such as: drop index, create index)
package unittest

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/kv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const testEngineIP string = "127.0.0.1"

//type expectedResult struct {
//	null bool
//	attr []string
//	rows [][]string
//}

func newTestEngine() (engine.Engine, *process.Process) {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	proc := process.New(mheap.New(gm))
	//e := memEngine.NewTestEngine()
	e := memEngine.New(kv.New(), engine.Node{Id: "0", Addr: testEngineIP})
	return e, proc
}

func test(t *testing.T, e engine.Engine, proc *process.Process, noErrors []string, simpleSelect [][]string, errorSQLs [][]string) {
	// case will run success without error
	for _, sql := range noErrors {
		require.NoError(t, sqlRun(sql, e, proc), sql)
	}
	// simple query will do work as same as `select * from table`
	// First string is relation name, Second string is expected result.
	for _, r := range simpleSelect {
		require.Equal(t, r[1], tempSelect(e, "test", r[0]))
	}
	// case will return error in compute-layer
	for _, eql := range errorSQLs {
		r := sqlRun(eql[0], e, proc)
		if len(eql[1]) == 0 {
			require.NoError(t, r, eql[0])
		} else {
			if r == nil { // that should error but found no error
				require.Equal(t, eql[1], "no error occurs", eql[0])
			}
			require.Equal(t, eql[1], r.Error(), eql[0])
		}
	}
}

// sqlRun compile and run a sql, return error if happens
func sqlRun(sql string, e engine.Engine, proc *process.Process) error {
	compile.InitAddress(testEngineIP)
	c := compile.New("test", sql, "", e, proc)
	es, err := c.Build()
	if err != nil {
		return err
	}
	for _, e := range es {
		err := e.Compile(nil, func(i interface{}, batch *batch.Batch) error {
			return nil
		})
		if err != nil {
			return err
		}
		err = e.Run(0)
		if err != nil {
			return err
		}
	}
	return nil
}

//func convertBatch(b *batch.Batch) expectedResult {
//	res := expectedResult{}
//	if len(b.Zs) == 0 {
//		res.null = true
//		return res
//	}
//	res.attr = make([]string, len(b.Attrs))
//	res.rows = make([][]string, len(b.Attrs))
//	for i := range res.attr {
//		res.attr[i] = b.Attrs[i]
//		res.rows[i] = make([]string, vector.Length(b.Vecs[i]))
//	}
//
//}

// a simple select function
func tempSelect(e engine.Engine, schema, name string) string {
	var buff bytes.Buffer

	db, err := e.Database(schema)
	if err != nil {
		return err.Error()
	}
	r, err := db.Relation(name)
	if err != nil {
		return err.Error()
	}
	defs := r.TableDefs()
	attrs := make([]string, 0, len(defs))
	{
		for _, def := range defs {
			if v, ok := def.(*engine.AttributeDef); ok {
				attrs = append(attrs, v.Attr.Name)
			}
		}
	}
	cs := make([]uint64, len(attrs))
	for i := range cs {
		cs[i] = 1
	}
	rd := r.NewReader(1)[0]
	{
		bat, err := rd.Read(cs, attrs)
		if err != nil {
			return err.Error()
		}
		buff.WriteString(fmt.Sprintf("%s\n", bat))
	}
	return buff.String()
}
