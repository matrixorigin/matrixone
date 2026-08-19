// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apply

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type vectorSource struct {
	spec          *plan.VectorIndexScan
	attrs         []string
	types         []types.Type
	txnOffset     int
	executors     []colexec.ExpressionExecutor
	queryVec      *vector.Vector
	limitVec      *vector.Vector
	firstRoundVec *vector.Vector
	reader        engine.Reader
	output        *batch.Batch
}

var _ AppliedSource = (*vectorSource)(nil)

func NewVectorSource(spec *plan.VectorIndexScan, attrs []string, typs []types.Type) AppliedSource {
	return &vectorSource{spec: spec, attrs: attrs, types: typs}
}

func (s *vectorSource) ApplyPrepare(proc *process.Process) error {
	if s.spec == nil || s.spec.Index == nil || s.spec.QueryVector == nil || s.spec.CandidateLimit == nil {
		return moerr.NewInvalidInput(proc.Ctx, "correlated vector scan has incomplete metadata")
	}
	if err := s.closeReader(); err != nil {
		return err
	}
	for _, executor := range s.executors {
		executor.Free()
	}
	s.executors = nil
	var err error
	for i := range s.spec.PreFilters {
		s.spec.PreFilters[i], err = plan2.ConstantFold(batch.EmptyForConstFoldBatch, s.spec.PreFilters[i], proc, true, true)
		if err != nil {
			return err
		}
	}
	exprs := []*plan.Expr{s.spec.QueryVector, s.spec.CandidateLimit}
	if s.spec.FirstRoundLimit != nil {
		exprs = append(exprs, s.spec.FirstRoundLimit)
	}
	s.executors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, exprs)
	return err
}

func (s *vectorSource) ApplyArgsEval(in *batch.Batch, proc *process.Process) error {
	var err error
	s.queryVec, err = s.executors[0].Eval(proc, []*batch.Batch{in}, nil)
	if err != nil {
		return err
	}
	s.limitVec, err = s.executors[1].Eval(proc, []*batch.Batch{in}, nil)
	if err != nil {
		return err
	}
	if len(s.executors) > 2 {
		s.firstRoundVec, err = s.executors[2].Eval(proc, []*batch.Batch{in}, nil)
	}
	return err
}

func (s *vectorSource) ApplyStart(row int, proc *process.Process, _ process.Analyzer) error {
	if err := s.closeReader(); err != nil {
		return err
	}
	if s.queryVec.IsNull(uint64(row)) {
		s.reader = new(readutil.EmptyReader)
		return nil
	}
	limit := vector.GetFixedAtNoTypeCheck[uint64](s.limitVec, row)
	p, ok := indexplugin.Get(s.spec.Index.IndexAlgo)
	if !ok {
		return moerr.NewNotSupportedf(proc.Ctx, "vector index algorithm %q is not registered", s.spec.Index.IndexAlgo)
	}
	searcher, ok := p.(indexplugin.SearchPlugin)
	if !ok {
		return moerr.NewNotSupportedf(proc.Ctx, "vector index algorithm %q has no scan reader", s.spec.Index.IndexAlgo)
	}
	runtimeSpec := *s.spec
	if s.firstRoundVec != nil {
		runtimeSpec.FirstRoundLimit = &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_uint64)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{
				U64Val: vector.GetFixedAtNoTypeCheck[uint64](s.firstRoundVec, row),
			}}},
		}
	}
	reader, err := searcher.Search().NewReader(proc, &runtimeSpec, searchplugin.Request{
		QueryVector:    append([]byte(nil), s.queryVec.GetBytesAt(row)...),
		QueryType:      s.spec.QueryVector.Typ,
		CandidateLimit: limit,
		PartitionCount: 1,
		TxnOffset:      s.txnOffset,
	})
	if err != nil {
		return err
	}
	s.reader = reader
	return nil
}

func (s *vectorSource) ApplyCall(proc *process.Process) (vm.CallResult, error) {
	if s.reader == nil {
		return vm.CancelResult, nil
	}
	if s.output == nil {
		s.output = batch.NewWithSize(len(s.attrs))
		s.output.Attrs = append(s.output.Attrs, s.attrs...)
		for i := range s.types {
			s.output.Vecs[i] = vector.NewVec(s.types[i])
		}
	}
	end, err := s.reader.Read(proc.Ctx, s.attrs, nil, proc.Mp(), s.output)
	if err != nil {
		return vm.CancelResult, err
	}
	if end {
		if err = s.closeReader(); err != nil {
			return vm.CancelResult, err
		}
		return vm.CancelResult, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: s.output}, nil
}

func (s *vectorSource) ApplyEnd(*process.Process) error { return s.closeReader() }

func (s *vectorSource) Reset(proc *process.Process, _ bool, _ error) {
	_ = s.closeReader()
	for _, executor := range s.executors {
		executor.ResetForNextQuery()
	}
	if s.output != nil {
		s.output.CleanOnlyData()
	}
	s.queryVec, s.limitVec, s.firstRoundVec = nil, nil, nil
}

func (s *vectorSource) Free(proc *process.Process, _ bool, _ error) {
	_ = s.closeReader()
	for _, executor := range s.executors {
		executor.Free()
	}
	s.executors = nil
	if s.output != nil {
		s.output.Clean(proc.Mp())
		s.output = nil
	}
}

func (s *vectorSource) closeReader() error {
	if s.reader == nil {
		return nil
	}
	err := s.reader.Close()
	s.reader = nil
	return err
}
