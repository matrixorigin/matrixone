// Copyright 2021 - 2022 Matrix Origin
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

package ctl

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// MoTableRows returns an estimated row number of a table.
func MoTableRows(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec := vector.New(types.New(types.T_int64, 0, 0, 0))
	count := vecs[0].Length()
	dbs := vector.MustStrCols(vecs[0])
	tbls := vector.MustStrCols(vecs[1])
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn, err := proc.TxnClient.New()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback(proc.Ctx)
	if err := e.New(proc.Ctx, txn); err != nil {
		return nil, err
	}
	defer e.Rollback(proc.Ctx, txn)
	for i := 0; i < count; i++ {
		db, err := e.Database(proc.Ctx, dbs[i], txn)
		if err != nil {
			return nil, err
		}
		rel, err := db.Relation(proc.Ctx, tbls[i])
		if err != nil {
			return nil, err
		}
		rel.Ranges(proc.Ctx, nil)
		rows, err := rel.Rows(proc.Ctx)
		if err != nil {
			return nil, err
		}
		if err := vec.Append(rows, false, proc.Mp()); err != nil {
			vec.Free(proc.Mp())
			return nil, err
		}
	}
	return vec, nil
}

// MoTableSize returns an estimated size of a table.
func MoTableSize(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec := vector.New(types.New(types.T_int64, 0, 0, 0))
	count := vecs[0].Length()
	dbs := vector.MustStrCols(vecs[0])
	tbls := vector.MustStrCols(vecs[1])
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn, err := proc.TxnClient.New()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback(proc.Ctx)
	if err := e.New(proc.Ctx, txn); err != nil {
		return nil, err
	}
	defer e.Rollback(proc.Ctx, txn)
	for i := 0; i < count; i++ {
		db, err := e.Database(proc.Ctx, dbs[i], txn)
		if err != nil {
			return nil, err
		}
		rel, err := db.Relation(proc.Ctx, tbls[i])
		if err != nil {
			return nil, err
		}
		rel.Ranges(proc.Ctx, nil)
		rows, err := rel.Rows(proc.Ctx)
		if err != nil {
			return nil, err
		}
		attrs, err := rel.TableColumns(proc.Ctx)
		if err != nil {
			return nil, err
		}
		size := int64(0)
		for _, attr := range attrs {
			size += rows * int64(attr.Type.TypeSize())
		}
		if err := vec.Append(size, false, proc.Mp()); err != nil {
			vec.Free(proc.Mp())
			return nil, err
		}
	}
	return vec, nil

}
