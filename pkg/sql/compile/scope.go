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

package compile

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/update"

	"github.com/matrixorigin/matrixone/pkg/errno"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	y "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (s *Scope) CreateDatabase(ts uint64, snapshot engine.Snapshot, engine engine.Engine) error {
	ctx := context.TODO()
	dbName := s.Plan.GetDdl().GetCreateDatabase().GetDatabase()
	if _, err := engine.Database(ctx, dbName, snapshot); err == nil {
		if s.Plan.GetDdl().GetCreateDatabase().GetIfNotExists() {
			return nil
		}
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("database %s already exists", dbName))
	}
	return engine.Create(ctx, dbName, snapshot)
}

func (s *Scope) DropDatabase(ts uint64, snapshot engine.Snapshot, engine engine.Engine) error {
	ctx := context.TODO()
	dbName := s.Plan.GetDdl().GetDropDatabase().GetDatabase()
	if _, err := engine.Database(ctx, dbName, snapshot); err != nil {
		if s.Plan.GetDdl().GetDropDatabase().GetIfExists() {
			return nil
		}
		return err
	}
	return engine.Delete(ctx, dbName, snapshot)
}

func (s *Scope) CreateTable(ts uint64, snapshot engine.Snapshot, engine engine.Engine, dbName string) error {
	ctx := context.TODO()
	qry := s.Plan.GetDdl().GetCreateTable()
	// convert the plan's cols to the execution's cols
	planCols := qry.GetTableDef().GetCols()
	exeCols := planColsToExeCols(planCols)

	// convert the plan's defs to the execution's defs
	planDefs := qry.GetTableDef().GetDefs()
	exeDefs := planDefsToExeDefs(planDefs)

	if qry.GetDatabase() != "" {
		dbName = qry.GetDatabase()
	}
	dbSource, err := engine.Database(ctx, dbName, snapshot)
	if err != nil {
		return err
	}
	tblName := qry.GetTableDef().GetName()
	if _, err := dbSource.Relation(ctx, tblName); err == nil {
		if qry.GetIfNotExists() {
			return nil
		}
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("table '%s' already exists", tblName))
	}
	return dbSource.Create(ctx, tblName, append(exeCols, exeDefs...))
}

func (s *Scope) DropTable(ts uint64, snapshot engine.Snapshot, engine engine.Engine) error {
	ctx := context.TODO()
	qry := s.Plan.GetDdl().GetDropTable()

	dbName := qry.GetDatabase()
	dbSource, err := engine.Database(ctx, dbName, snapshot)
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}
	tblName := qry.GetTable()
	if _, err := dbSource.Relation(ctx, tblName); err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}
	return dbSource.Delete(ctx, tblName)
}

func (s *Scope) CreateIndex(ts uint64, snapshot engine.Snapshot, engine engine.Engine) error {
	return nil
}

func (s *Scope) DropIndex(ts uint64, snapshot engine.Snapshot, engine engine.Engine) error {
	return nil
}

func (s *Scope) Delete(ts uint64, snapshot engine.Snapshot, engine engine.Engine) (uint64, error) {
	s.Magic = Merge
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*deletion.Argument)
	arg.Ts = ts

	ctx := context.TODO()
	if arg.DeleteCtxs[0].CanTruncate {
		return arg.DeleteCtxs[0].TableSource.Truncate(ctx)
	}

	if err := s.MergeRun(engine); err != nil {
		return 0, err
	}
	return arg.AffectedRows, nil
}

func (s *Scope) Insert(ts uint64, snapshot engine.Snapshot, engine engine.Engine) (uint64, error) {
	s.Magic = Merge
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*insert.Argument)
	arg.Ts = ts
	if err := s.MergeRun(engine); err != nil {
		return 0, err
	}
	return arg.Affected, nil
}

func (s *Scope) Update(ts uint64, snapshot engine.Snapshot, engine engine.Engine) (uint64, error) {
	s.Magic = Merge
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*update.Argument)
	arg.Ts = ts
	if err := s.MergeRun(engine); err != nil {
		return 0, err
	}
	return arg.AffectedRows, nil
}

func (s *Scope) InsertValues(ts uint64, snapshot engine.Snapshot, engine engine.Engine, proc *process.Process, stmt *tree.Insert) (uint64, error) {
	p := s.Plan.GetIns()

	ctx := context.TODO()

	dbSource, err := engine.Database(ctx, p.DbName, snapshot)
	if err != nil {
		return 0, err
	}
	relation, err := dbSource.Relation(ctx, p.TblName)
	if err != nil {
		return 0, err
	}

	bat := makeInsertBatch(p)

	if p.OtherCols != nil {
		p.ExplicitCols = append(p.ExplicitCols, p.OtherCols...)
	}

	if err := fillBatch(bat, p, stmt.Rows.Select.(*tree.ValuesClause).Rows, proc); err != nil {
		return 0, err
	}
	batch.Reorder(bat, p.OrderAttrs)
	if err := relation.Write(ctx, bat); err != nil {
		return 0, err
	}

	return uint64(len(p.Columns[0].Column)), nil
}

func fillBatch(bat *batch.Batch, p *plan.InsertValues, rows []tree.Exprs, proc *process.Process) error {
	rowCount := len(p.Columns[0].Column)

	tmpBat := batch.NewWithSize(0)
	tmpBat.Zs = []int64{1}

	for i, v := range bat.Vecs {
		switch v.Typ.Oid {
		case types.T_json:
			vs := make([][]byte, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.(*types.Bytes).Data
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_bool:
			vs := make([]bool, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]bool)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_int8:
			vs := make([]int8, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]int8)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_int16:
			vs := make([]int16, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]int16)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_int32:
			vs := make([]int32, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]int32)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_int64:
			vs := make([]int64, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]int64)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_uint8:
			vs := make([]uint8, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]uint8)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_uint16:
			vs := make([]uint16, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]uint16)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_uint32:
			vs := make([]uint32, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]uint32)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_uint64:
			vs := make([]uint64, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]uint64)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_float32:
			vs := make([]float32, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]float32)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_float64:
			vs := make([]float64, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]float64)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_char, types.T_varchar, types.T_blob:
			vs := make([][]byte, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.(*types.Bytes).Data
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_date:
			vs := make([]types.Date, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]types.Date)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_datetime:
			vs := make([]types.Datetime, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]types.Datetime)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_timestamp:
			vs := make([]types.Timestamp, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]types.Timestamp)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_decimal64:
			vs := make([]types.Decimal64, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]types.Decimal64)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_decimal128:
			vs := make([]types.Decimal128, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]types.Decimal128)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		default:
			return fmt.Errorf("data truncation: type of '%v' doesn't implement", v.Typ)
		}
	}
	return nil
}

func makeInsertBatch(p *plan.InsertValues) *batch.Batch {
	attrs := make([]string, 0, len(p.OrderAttrs))

	for _, col := range p.ExplicitCols {
		attrs = append(attrs, col.Name)
	}
	for _, col := range p.OtherCols {
		attrs = append(attrs, col.Name)
	}

	bat := batch.New(true, attrs)
	idx := 0
	for _, col := range p.ExplicitCols {
		bat.Vecs[idx] = vector.New(types.Type{Oid: types.T(col.Typ.GetId())})
		idx++
	}
	for _, col := range p.OtherCols {
		bat.Vecs[idx] = vector.New(types.Type{Oid: types.T(col.Typ.GetId())})
		idx++
	}

	return bat
}

func planDefsToExeDefs(planDefs []*plan.TableDef_DefType) []engine.TableDef {
	exeDefs := make([]engine.TableDef, len(planDefs))
	for i, def := range planDefs {
		switch defVal := def.GetDef().(type) {
		case *plan.TableDef_DefType_Pk:
			exeDefs[i] = &engine.PrimaryIndexDef{
				Names: defVal.Pk.GetNames(),
			}
		case *plan.TableDef_DefType_Idx:
			exeDefs[i] = &engine.IndexTableDef{
				ColNames: defVal.Idx.GetColNames(),
				Name:     defVal.Idx.GetName(),
			}
		case *plan.TableDef_DefType_Properties:
			properties := make([]engine.Property, len(defVal.Properties.GetProperties()))
			for i, p := range defVal.Properties.GetProperties() {
				properties[i] = engine.Property{
					Key:   p.GetKey(),
					Value: p.GetValue(),
				}
			}
			exeDefs[i] = &engine.PropertiesDef{
				Properties: properties,
			}
		}
	}
	return exeDefs
}

func planColsToExeCols(planCols []*plan.ColDef) []engine.TableDef {
	exeCols := make([]engine.TableDef, len(planCols))
	for i, col := range planCols {
		var alg compress.T
		switch col.Alg {
		case plan.CompressType_None:
			alg = compress.None
		case plan.CompressType_Lz4:
			alg = compress.Lz4
		}
		colTyp := col.GetTyp()
		exeCols[i] = &engine.AttributeDef{
			Attr: engine.Attribute{
				Name: col.Name,
				Alg:  alg,
				Type: types.Type{
					Oid:       types.T(colTyp.GetId()),
					Width:     colTyp.GetWidth(),
					Precision: colTyp.GetPrecision(),
					Scale:     colTyp.GetScale(),
					Size:      colTyp.GetSize(),
				},
				Default: planCols[i].GetDefault(),
				Primary: col.GetPrimary(),
				Comment: col.GetComment(),
			},
		}
	}
	return exeCols
}

// PrintScope Print is to format scope list
func PrintScope(prefix []byte, ss []*Scope) {
	for _, s := range ss {
		if s.Magic == Merge || s.Magic == Remote {
			PrintScope(append(prefix, '\t'), s.PreScopes)
		}
		p := pipeline.NewMerge(s.Instructions, nil)
		fmt.Printf("%s:%v %v\n", prefix, s.Magic, p)
	}
}

// NumCPU Get the number of cpu's available for the current scope
func (s *Scope) NumCPU() int {
	return runtime.NumCPU()
}

// Run read data from storage engine and run the instructions of scope.
func (s *Scope) Run(e engine.Engine) (err error) {
	p := pipeline.New(s.DataSource.Attributes, s.Instructions, s.Reg)
	if s.DataSource.Bat != nil {
		if _, err = p.ConstRun(s.DataSource.Bat, s.Proc); err != nil {
			return err
		}
	} else {
		if _, err = p.Run(s.DataSource.R, s.Proc); err != nil {
			return err
		}
	}
	return nil
}

// MergeRun range and run the scope's pre-scopes by go-routine, and finally run itself to do merge work.
func (s *Scope) MergeRun(e engine.Engine) error {
	errChan := make(chan error, len(s.PreScopes))
	for i := range s.PreScopes {
		switch s.PreScopes[i].Magic {
		case Normal:
			go func(cs *Scope) {
				var err error
				defer func() {
					errChan <- err
				}()
				err = cs.Run(e)
			}(s.PreScopes[i])
		case Merge:
			go func(cs *Scope) {
				var err error
				defer func() {
					errChan <- err
				}()
				err = cs.MergeRun(e)
			}(s.PreScopes[i])
		case Remote:
			go func(cs *Scope) {
				var err error
				defer func() {
					errChan <- err
				}()
				err = cs.RemoteRun(e)
			}(s.PreScopes[i])
		case Parallel:
			go func(cs *Scope) {
				var err error
				defer func() {
					errChan <- err
				}()
				err = cs.ParallelRun(e)
			}(s.PreScopes[i])
		}
	}
	p := pipeline.NewMerge(s.Instructions, s.Reg)
	if _, err := p.MergeRun(s.Proc); err != nil {
		return err
	}

	// check sub-goroutine's error
	for i := 0; i < len(s.PreScopes); i++ {
		if err := <-errChan; err != nil {
			return err
		}
	}
	return nil
}

// RemoteRun send the scope to a remote node (if target node is itself, it is same to function ParallelRun) and run it.
func (s *Scope) RemoteRun(e engine.Engine) error {
	return s.ParallelRun(e)
}

// ParallelRun try to execute the scope in parallel way.
func (s *Scope) ParallelRun(e engine.Engine) error {
	var rds []engine.Reader

	if s.DataSource == nil {
		return s.MergeRun(e)
	}
	mcpu := s.NodeInfo.Mcpu
	snap := engine.Snapshot(s.Proc.Snapshot)
	{
		ctx := context.TODO()
		db, err := e.Database(ctx, s.DataSource.SchemaName, snap)
		if err != nil {
			return err
		}
		rel, err := db.Relation(ctx, s.DataSource.RelationName)
		if err != nil {
			return err
		}
		rds, _ = rel.NewReader(ctx, mcpu, nil, s.NodeInfo.Data)
	}
	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = &Scope{
			Magic: Normal,
			DataSource: &Source{
				R:            rds[i],
				SchemaName:   s.DataSource.SchemaName,
				RelationName: s.DataSource.RelationName,
				Attributes:   s.DataSource.Attributes,
			},
		}
		ss[i].Proc = process.NewFromProc(s.Proc, 0)
	}
	{
		var flg bool

		for i, in := range s.Instructions {
			if flg {
				break
			}
			switch in.Op {
			case vm.Top:
				flg = true
				arg := in.Arg.(*top.Argument)
				s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
				s.Instructions[0] = vm.Instruction{
					Op: vm.MergeTop,
					Arg: &mergetop.Argument{
						Fs:    arg.Fs,
						Limit: arg.Limit,
					},
				}
				for i := range ss {
					ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
						Op: vm.Top,
						Arg: &top.Argument{
							Fs:    arg.Fs,
							Limit: arg.Limit,
						},
					})
				}
			case vm.Order:
				flg = true
				arg := in.Arg.(*order.Argument)
				s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
				s.Instructions[0] = vm.Instruction{
					Op: vm.MergeOrder,
					Arg: &mergeorder.Argument{
						Fs: arg.Fs,
					},
				}
				for i := range ss {
					ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
						Op: vm.Order,
						Arg: &order.Argument{
							Fs: arg.Fs,
						},
					})
				}
			case vm.Limit:
				flg = true
				arg := in.Arg.(*limit.Argument)
				s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
				s.Instructions[0] = vm.Instruction{
					Op: vm.MergeLimit,
					Arg: &mergelimit.Argument{
						Limit: arg.Limit,
					},
				}
				for i := range ss {
					ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
						Op: vm.Limit,
						Arg: &limit.Argument{
							Limit: arg.Limit,
						},
					})
				}
			case vm.Group:
				flg = true
				arg := in.Arg.(*group.Argument)
				s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
				s.Instructions[0] = vm.Instruction{
					Op: vm.MergeGroup,
					Arg: &mergegroup.Argument{
						NeedEval: false,
					},
				}
				for i := range ss {
					ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
						Op: vm.Group,
						Arg: &group.Argument{
							Aggs:  arg.Aggs,
							Exprs: arg.Exprs,
							Types: arg.Types,
						},
					})
				}
			case vm.Offset:
				flg = true
				arg := in.Arg.(*offset.Argument)
				s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
				s.Instructions[0] = vm.Instruction{
					Op: vm.MergeOffset,
					Arg: &mergeoffset.Argument{
						Offset: arg.Offset,
					},
				}
				for i := range ss {
					ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
						Op: vm.Offset,
						Arg: &offset.Argument{
							Offset: arg.Offset,
						},
					})
				}
			default:
				for i := range ss {
					ss[i].Instructions = append(ss[i].Instructions, dupInstruction(in))
				}
			}
		}
		if !flg {
			for i := range ss {
				ss[i].Instructions = ss[i].Instructions[:len(ss[i].Instructions)-1]
			}
			s.Instructions[0] = vm.Instruction{
				Op:  vm.Merge,
				Arg: &merge.Argument{},
			}
			s.Instructions[1] = s.Instructions[len(s.Instructions)-1]
			s.Instructions = s.Instructions[:2]
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.Magic = Merge
	s.PreScopes = ss
	s.Proc.Cancel = cancel
	s.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ss))
	{
		for i := 0; i < len(ss); i++ {
			s.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Ctx: ctx,
				Ch:  make(chan *batch.Batch, 1),
			}
		}
	}
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Reg: s.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return s.MergeRun(e)
}

func (s *Scope) appendInstruction(in vm.Instruction) {
	if !s.IsEnd {
		s.Instructions = append(s.Instructions, in)
	}
}
