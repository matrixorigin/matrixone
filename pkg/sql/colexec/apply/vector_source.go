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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/vectorscan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type vectorSource struct {
	template  *plan.VectorIndexScan
	execution *vectorscan.Execution
	attrs     []string
	types     []types.Type
	txnOffset int
	reader    engine.Reader
	output    *batch.Batch
}

var _ AppliedSource = (*vectorSource)(nil)

func NewVectorSource(spec *plan.VectorIndexScan, attrs []string, typs []types.Type) AppliedSource {
	return &vectorSource{
		template: plan2.DeepCopyVectorIndexScan(spec),
		attrs:    attrs,
		types:    typs,
	}
}

func (s *vectorSource) ApplyPrepare(proc *process.Process) error {
	if s.template == nil || s.template.Index == nil || s.template.QueryVector == nil || s.template.CandidateLimit == nil {
		return moerr.NewInvalidInput(proc.Ctx, "correlated vector scan has incomplete metadata")
	}
	if err := s.closeGeneration(); err != nil {
		return err
	}
	execution, err := vectorscan.PrepareCorrelatedExecution(s.template, proc)
	if err == nil {
		s.execution = execution
	}
	return err
}

func (s *vectorSource) ApplyArgsEval(in *batch.Batch, proc *process.Process) error {
	if s.execution == nil {
		return moerr.NewInvalidState(proc.Ctx, "correlated vector scan is not prepared")
	}
	return s.execution.EvalBatch(in, proc)
}

func (s *vectorSource) ApplyStart(row int, proc *process.Process, _ process.Analyzer) error {
	if err := s.closeReader(); err != nil {
		return err
	}
	if s.execution == nil || s.execution.Spec() == nil {
		return moerr.NewInvalidState(proc.Ctx, "correlated vector scan is not prepared")
	}
	identity, err := vectorscan.Identity(s.execution.Spec(), s.txnOffset, 1, 0)
	if err != nil {
		return err
	}
	req, hasQuery, err := s.execution.RequestAt(row, identity)
	if err != nil {
		return err
	}
	if !hasQuery {
		s.reader = new(readutil.EmptyReader)
		return nil
	}
	spec := s.execution.Spec()
	p, ok := indexplugin.Get(spec.Index.IndexAlgo)
	if !ok {
		return moerr.NewNotSupportedf(proc.Ctx, "vector index algorithm %q is not registered", spec.Index.IndexAlgo)
	}
	searcher, ok := p.(indexplugin.SearchPlugin)
	if !ok {
		return moerr.NewNotSupportedf(proc.Ctx, "vector index algorithm %q has no scan reader", spec.Index.IndexAlgo)
	}
	reader, err := searcher.Search().NewReader(proc, spec, req)
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
	_ = s.closeGeneration()
	if s.output != nil {
		s.output.CleanOnlyData()
	}
}

func (s *vectorSource) Free(proc *process.Process, _ bool, _ error) {
	_ = s.closeGeneration()
	if s.output != nil {
		s.output.Clean(proc.Mp())
		s.output = nil
	}
}

func (s *vectorSource) closeGeneration() error {
	err := s.closeReader()
	if s.execution != nil {
		s.execution.Close()
		s.execution = nil
	}
	return err
}

func (s *vectorSource) closeReader() error {
	if s.reader == nil {
		return nil
	}
	err := s.reader.Close()
	s.reader = nil
	return err
}
