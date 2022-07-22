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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (s *Scope) CreateDatabase(ts uint64, snapshot engine.Snapshot, engine engine.Engine) error {
	dbName := s.Plan.GetDdl().GetCreateDatabase().GetDatabase()
	if _, err := engine.Database(dbName, snapshot); err == nil {
		if s.Plan.GetDdl().GetCreateDatabase().GetIfNotExists() {
			return nil
		}
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("database %s already exists", dbName))
	}
	return engine.Create(ts, dbName, 0, snapshot)
}

func (s *Scope) DropDatabase(ts uint64, snapshot engine.Snapshot, engine engine.Engine) error {
	dbName := s.Plan.GetDdl().GetDropDatabase().GetDatabase()
	if _, err := engine.Database(dbName, snapshot); err != nil {
		if s.Plan.GetDdl().GetDropDatabase().GetIfExists() {
			return nil
		}
		return err
	}
	return engine.Delete(ts, dbName, snapshot)
}

func (s *Scope) CreateTable(ts uint64, snapshot engine.Snapshot, engine engine.Engine, dbName string) error {
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
	dbSource, err := engine.Database(dbName, snapshot)
	if err != nil {
		return err
	}
	tblName := qry.GetTableDef().GetName()
	if relation, err := dbSource.Relation(tblName, snapshot); err == nil {
		relation.Close(snapshot)
		if qry.GetIfNotExists() {
			return nil
		}
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("table '%s' already exists", tblName))
	}
	return dbSource.Create(ts, tblName, append(exeCols, exeDefs...), snapshot)
}

func (s *Scope) DropTable(ts uint64, snapshot engine.Snapshot, engine engine.Engine) error {
	qry := s.Plan.GetDdl().GetDropTable()

	dbName := qry.GetDatabase()
	dbSource, err := engine.Database(dbName, snapshot)
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}
	tblName := qry.GetTable()
	if relation, err := dbSource.Relation(tblName, snapshot); err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	} else {
		relation.Close(snapshot)
	}
	return dbSource.Delete(ts, tblName, snapshot)
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

	if arg.DeleteCtxs[0].CanTruncate {
		return arg.DeleteCtxs[0].TableSource.Truncate(snapshot)
	}

	defer func() {
		for i := range arg.DeleteCtxs {
			arg.DeleteCtxs[i].TableSource.Close(snapshot)
		}
	}()

	if err := s.MergeRun(engine); err != nil {
		return 0, err
	}
	return arg.AffectedRows, nil
}

func (s *Scope) Insert(ts uint64, snapshot engine.Snapshot, engine engine.Engine) (uint64, error) {
	s.Magic = Merge
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*insert.Argument)
	arg.Ts = ts
	defer arg.TargetTable.Close(snapshot)
	if err := s.MergeRun(engine); err != nil {
		return 0, err
	}
	return arg.Affected, nil
}

func (s *Scope) Update(ts uint64, snapshot engine.Snapshot, engine engine.Engine) (uint64, error) {
	s.Magic = Merge
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*update.Argument)
	arg.Ts = ts
	defer arg.TableSource.Close(snapshot)
	if err := s.MergeRun(engine); err != nil {
		return 0, err
	}
	return arg.AffectedRows, nil
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
				Default: engine.DefaultExpr{
					Exist:  col.GetDefault().GetExist(),
					Value:  planValToExeVal(col.GetDefault().GetValue(), colTyp.GetId()),
					IsNull: col.GetDefault().GetIsNull(),
				},
				Primary: col.GetPrimary(),
				Comment: col.GetComment(),
			},
		}
	}
	return exeCols
}

func planValToExeVal(value *plan.ConstantValue, typ plan.Type_TypeId) interface{} {
	switch v := value.GetConstantValue().(type) {
	case *plan.ConstantValue_BoolV:
		return v.BoolV
	case *plan.ConstantValue_Int64V:
		switch typ {
		case plan.Type_INT8:
			return int8(v.Int64V)
		case plan.Type_INT16:
			return int16(v.Int64V)
		case plan.Type_INT32:
			return int32(v.Int64V)
		case plan.Type_INT64:
			return v.Int64V
		}
	case *plan.ConstantValue_Uint64V:
		switch typ {
		case plan.Type_UINT8:
			return uint8(v.Uint64V)
		case plan.Type_UINT16:
			return uint16(v.Uint64V)
		case plan.Type_UINT32:
			return uint32(v.Uint64V)
		case plan.Type_UINT64:
			return v.Uint64V
		}
	case *plan.ConstantValue_Float32V:
		return v.Float32V
	case *plan.ConstantValue_Float64V:
		switch typ {
		case plan.Type_FLOAT32:
			return float32(v.Float64V)
		case plan.Type_FLOAT64:
			return v.Float64V
		}
	case *plan.ConstantValue_StringV:
		return []byte(v.StringV)
	case *plan.ConstantValue_DateV:
		return types.Date(v.DateV)
	case *plan.ConstantValue_DateTimeV:
		return types.Datetime(v.DateTimeV)
	case *plan.ConstantValue_TimeStampV:
		return types.Timestamp(v.TimeStampV)
	case *plan.ConstantValue_Decimal64V:
		return types.Decimal64FromInt64Raw(v.Decimal64V.A)
	case *plan.ConstantValue_Decimal128V:
		return types.Decimal128FromInt64Raw(v.Decimal128V.A, v.Decimal128V.B)
	}
	return nil
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

func (s *Scope) DispatchRun(e engine.Engine) error {
	mcpu := s.NumCPU()
	ss := make([]*Scope, mcpu)
	regs := make([][]*process.WaitRegister, len(s.PreScopes))
	{
		for i := range regs {
			regs[i] = make([]*process.WaitRegister, mcpu)
		}
	}
	for i := 0; i < mcpu; i++ {
		ss[i] = &Scope{
			Magic: Merge,
		}
		ss[i].Proc = process.NewFromProc(mheap.New(s.Proc.Mp.Gm), s.Proc, len(s.PreScopes))
		for j := 0; j < len(s.PreScopes); j++ {
			regs[j][i] = ss[i].Proc.Reg.MergeReceivers[j]
		}
		ss[i].Instructions = append(ss[i].Instructions, dupInstruction(s.Instructions[0]))
	}
	for i := range s.PreScopes {
		s.PreScopes[i].Instructions[len(s.PreScopes[i].Instructions)-1] = vm.Instruction{
			Op: vm.Dispatch,
			Arg: &dispatch.Argument{
				Regs: regs[i],
				Mmu:  s.Proc.Mp.Gm,
				All:  s.PreScopes[i].DispatchAll,
			},
		}
	}
	s.PreScopes = append(s.PreScopes, ss...)
	s.Instructions[0] = vm.Instruction{
		Op:  vm.Merge,
		Arg: &merge.Argument{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.Proc.Cancel = cancel
	s.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ss))
	for i := range ss {
		s.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
			Ctx: ctx,
			Ch:  make(chan *batch.Batch, 1),
		}
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: s.Proc.Mp.Gm,
				Reg: s.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return s.MergeRun(e)
}

// RemoteRun send the scope to a remote node (if target node is itself, it is same to function ParallelRun) and run it.
func (s *Scope) RemoteRun(e engine.Engine) error {
	return s.ParallelRun(e)
}

// ParallelRun try to execute the scope in parallel way.
func (s *Scope) ParallelRun(e engine.Engine) error {
	var rds []engine.Reader

	if s.DataSource == nil {
		return s.DispatchRun(e)
	}
	mcpu := s.NumCPU()
	snap := engine.Snapshot(s.Proc.Snapshot)
	{
		db, err := e.Database(s.DataSource.SchemaName, snap)
		if err != nil {
			return err
		}
		rel, err := db.Relation(s.DataSource.RelationName, snap)
		if err != nil {
			return err
		}
		rds = rel.NewReader(mcpu, nil, s.NodeInfo.Data, snap)
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
		ss[i].Proc = process.New(mheap.New(s.Proc.Mp.Gm))
		ss[i].Proc.Id = s.Proc.Id
		ss[i].Proc.Lim = s.Proc.Lim
		ss[i].Proc.UnixTime = s.Proc.UnixTime
		ss[i].Proc.Snapshot = s.Proc.Snapshot
		ss[i].Proc.SessionInfo = s.Proc.SessionInfo
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
				Mmu: s.Proc.Mp.Gm,
				Reg: s.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return s.MergeRun(e)
}
