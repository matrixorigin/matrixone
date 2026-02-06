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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/monlp/llm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type opBuiltInLlmFunction struct {
	llmClient llm.LLMClient
}

func newOpBuiltInLlmFunction() *opBuiltInLlmFunction {
	return &opBuiltInLlmFunction{}
}

func getConstString(proc *process.Process, vec *vector.Vector, idx uint64, errPrefix string, nullOk bool) (string, error) {
	if !vec.IsConst() {
		return "", moerr.NewInvalidInputf(proc.Ctx, "%s must be constant", errPrefix)
	}
	if vec.IsConstNull() {
		if nullOk {
			return "", nil
		}
		return "", moerr.NewInvalidInputf(proc.Ctx, "%s must not be null", errPrefix)
	}
	return vec.GetStringAt(0), nil
}

func (op *opBuiltInLlmFunction) initLlmClient(params []*vector.Vector, proc *process.Process) error {
	// init llm client
	server, err := getConstString(proc, params[0], 0, "llm_chat: server", false)
	if err != nil {
		return err
	}
	addr, err := getConstString(proc, params[1], 0, "llm_chat: addr", false)
	if err != nil {
		return err
	}
	model, err := getConstString(proc, params[2], 0, "llm_chat: model", false)
	if err != nil {
		return err
	}
	options, err := getConstString(proc, params[3], 0, "llm_chat: options", true)
	if err != nil {
		return err
	}

	op.llmClient, err = llm.NewLLMClient(server, addr, model, options)
	return err
}

func (op *opBuiltInLlmFunction) llmChat(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList) error {
	var err error
	if op.llmClient == nil {
		if err = op.initLlmClient(params, proc); err != nil {
			return err
		}
	}

	queryVec := params[4]
	queryWrapper := vector.GenerateFunctionStrParameter(queryVec)
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		query, isNull := queryWrapper.GetStrValue(i)
		if isNull {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		} else {
			reply, err := op.llmClient.Chat(proc.Ctx, (string)(query))
			if err != nil {
				return err
			}
			if err = rs.AppendBytes([]byte(reply), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *opBuiltInLlmFunction) llmEmbedding(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList) error {
	var err error
	if op.llmClient == nil {
		if err = op.initLlmClient(params, proc); err != nil {
			return err
		}
	}
	queryVec := params[4]
	queryWrapper := vector.GenerateFunctionStrParameter(queryVec)
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		query, isNull := queryWrapper.GetStrValue(i)
		if isNull {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		} else {
			reply, err := op.llmClient.CreateEmbedding(proc.Ctx, string(query))
			if err != nil {
				return err
			}
			if err = rs.AppendBytes(types.ArrayToBytes(reply), false); err != nil {
				return err
			}
		}
	}
	return nil
}
