// Copyright 2026 Matrix Origin
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

package function

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type jsonConstructorSink interface{ AppendBytes([]byte, bool) error }
type jsonReaderConstructorSink struct {
	builder *vector.JSONReaderBuilder
	ctx     context.Context
}

func (sink jsonReaderConstructorSink) AppendBytes(raw []byte, null bool) error {
	if err := sink.ctx.Err(); err != nil {
		return err
	}
	return sink.builder.Append(raw, null)
}

func (sink jsonReaderConstructorSink) AppendByteJson(value bytejson.ByteJson, null bool) error {
	if err := sink.ctx.Err(); err != nil {
		return err
	}
	return sink.builder.AppendByteJson(value, null)
}

// EvalJSONConstructorReader constructs directly into a private admitted owner.
// Its sinks cannot return a Vector or writable alias. The ordinary constructor
// and this entry use the same row conversions; Finish does not scan again.
func EvalJSONConstructorReader(fid int32, params []*vector.Vector, proc *process.Process, length int, selected *FunctionSelectList, allocation *vector.AllocationAccountSelection) (vector.JSONReader, error) {
	if proc == nil || length < 0 || fid != JSON_ARRAY && fid != JSON_OBJECT && fid != CAST {
		return nil, moerr.NewInvalidInputNoCtx("unsupported JSON reader producer")
	}
	ctx := jsonValueEvaluationContext(proc)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fid == CAST {
		if len(params) != 2 || params[0] == nil || params[1] == nil || params[1].GetType().Oid != types.T_json ||
			!params[0].GetType().Oid.IsMySQLString() && params[0].GetType().Oid != types.T_json && params[0].GetType().Oid != types.T_any {
			return nil, moerr.NewInvalidInputNoCtx("unsupported JSON reader cast")
		}
		builder, err := newJSONReaderBuilderForEvaluation(proc, allocation)
		if err != nil {
			return nil, err
		}
		defer builder.Close()
		if err = castToJSONRows(params[0], jsonReaderConstructorSink{builder: builder, ctx: ctx}, proc, length, selected); err != nil {
			return nil, err
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		return builder.Finish()
	}
	if fid == JSON_OBJECT && len(params)%2 != 0 {
		return nil, moerr.NewInvalidInputNoCtx("JSON_OBJECT requires key/value pairs")
	}
	// JSON operands are ordinary mutable inputs. Validate them before the
	// constructor follows any descendant offsets; qualification belongs only
	// to the newly created private result.
	for row := 0; row < length; row++ {
		if selected != nil && (selected.IgnoreAllRow() || selected.Contains(uint64(row))) {
			continue
		}
		for _, parameter := range params {
			if parameter.GetType().Oid == types.T_json && !parameter.IsNull(uint64(row)) {
				raw, err := parameter.GetBytesAtChecked(row)
				if err != nil {
					return nil, err
				}
				if _, err := decodeJSONValueStored(raw); err != nil {
					return nil, err
				}
			}
		}
	}
	builder, err := newJSONReaderBuilderForEvaluation(proc, allocation)
	if err != nil {
		return nil, err
	}
	defer builder.Close()
	sink := jsonReaderConstructorSink{builder: builder, ctx: ctx}
	if fid == JSON_ARRAY {
		err = (&opBuiltInJsonArray{}).jsonArrayRows(params, sink, proc, length, selected)
	} else {
		err = (&opBuiltInJsonObject{}).jsonObjectRows(params, sink, proc, length, selected)
	}
	if err != nil {
		return nil, err
	}
	return builder.Finish()
}
