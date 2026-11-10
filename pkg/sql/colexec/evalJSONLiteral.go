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

package colexec

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// A typed literal's immutable plan string is only a reconstruction seed. Every
// query version is copied and admitted into a private builder, never a Vector
// that was exposed through ordinary expression evaluation.
type jsonLiteralExpressionExecutor struct {
	JSONReaderExpressionExecutor
	raw        string
	null       bool
	allocation *vector.AllocationAccountSelection
}

func newJSONLiteralExpressionExecutor(proc *process.Process, literal *plan.Literal, allocation *vector.AllocationAccountSelection) (*jsonLiteralExpressionExecutor, error) {
	value, isString := literal.GetValue().(*plan.Literal_Sval)
	if !literal.GetIsnull() && !isString {
		return nil, nil
	}
	if _, err := DecodeLiteralStringSource(literal); err != nil {
		return nil, err
	}
	source := &jsonLiteralExpressionExecutor{
		JSONReaderExpressionExecutor: JSONReaderExpressionExecutor{proc: proc},
		null:                         literal.GetIsnull(),
		allocation:                   allocation,
	}
	if isString {
		source.raw = value.Sval
	}
	if _, err := source.constantJSONReader(); err != nil {
		source.Free()
		return nil, err
	}
	return source, nil
}

func (source *jsonLiteralExpressionExecutor) constantJSONReader() (vector.JSONReader, error) {
	if source.proc == nil {
		return nil, io.ErrClosedPipe
	}
	if source.reader == nil {
		builder, err := vector.NewJSONReaderBuilder(source.proc.Mp(), source.allocation)
		if err != nil {
			return nil, err
		}
		defer builder.Close()
		// The seed is immutable. Append only reads this view and copies it into
		// accounted private storage; no intermediate payload copy is needed.
		if err = builder.Append(util.UnsafeStringToBytes(source.raw), source.null); err != nil {
			return nil, err
		}
		source.reader, err = builder.Finish()
		if err != nil {
			return nil, err
		}
	}
	return source.reader, nil
}

func (source *jsonLiteralExpressionExecutor) jsonReaderInput(proc *process.Process, batches []*batch.Batch, _ []bool) (vector.JSONReader, error) {
	reader, err := source.constantJSONReader()
	if err != nil {
		return nil, err
	}
	return jsonConstantReaderView(reader, expressionRowCount(batches))
}

func (source *jsonLiteralExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch, selected []bool) (*vector.Vector, error) {
	result, err := source.EvalWithoutResultReusing(proc, batches, selected)
	if err != nil {
		return nil, err
	}
	if source.materialized != nil {
		source.materialized.Free(source.proc.Mp())
	}
	source.materialized = result
	return result, nil
}

func (source *jsonLiteralExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, selected []bool) (*vector.Vector, error) {
	reader, err := source.jsonReaderInput(proc, batches, selected)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return reader.Materialize(source.proc.Mp())
}

func (source *jsonLiteralExpressionExecutor) Free() {
	source.JSONReaderExpressionExecutor.Free()
	source.raw = ""
}

func (source *jsonLiteralExpressionExecutor) IsColumnExpr() bool { return false }
func (source *jsonLiteralExpressionExecutor) TypeName() string   { return "json-reader-literal" }
