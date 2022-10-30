// Copyright 2022 Matrix Origin
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

package debug

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/json"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Handler used to handle mo_debug SQL function.
// Format of the function: mo_debug('service name', 'command', 'command parameter')
// service name: [CN|DN|LOG|HAKEEPER]
// command: command in supportedCmds
// command parameter: the parameter of the command
func Handler(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	service := serviceType(strings.ToUpper(vector.MustStrCols(vs[0])[0]))
	command := strings.ToUpper(vector.MustStrCols(vs[1])[0])
	parameter := vector.MustStrCols(vs[2])[0]

	if _, ok := supportedServiceTypes[service]; !ok {
		return nil, moerr.NewNotSupported("service type %s not supported", service)
	}

	f, ok := supportedCmds[command]
	if !ok {
		return nil, moerr.NewNotSupported("command %s not supported", command)
	}

	result, err := f(proc.Ctx,
		service,
		parameter,
		// We use a transaction client to send debug requests with the following in mind.
		// 1. reuse the RPC mechanism of cn and dn
		// 2. may support debug support for transactions in the future, such as testing the
		//    correctness of the transaction by forcing the timestamp of the transaction to
		//    be modified, etc.
		// TODO: add more ut tests for this.
		func(ctx context.Context, requests []txn.CNOpRequest) ([]txn.CNOpResponse, error) {
			txnOp := proc.TxnOperator
			if txnOp == nil {
				v, err := proc.TxnClient.New()
				if err != nil {
					return nil, err
				}
				txnOp = v
			}
			op, ok := txnOp.(client.DebugableTxnOperator)
			if !ok {
				return nil, moerr.NewNotSupported("debug function not supported")
			}

			debugRequests := make([]txn.TxnRequest, 0, len(requests))
			for _, req := range requests {
				tq := txn.NewTxnRequest(&req)
				tq.Method = txn.TxnMethod_DEBUG
				debugRequests = append(debugRequests, tq)
			}
			result, err := op.Debug(ctx, debugRequests)
			if err != nil {
				return nil, err
			}
			defer result.Release()

			responses := make([]txn.CNOpResponse, 0, len(requests))
			for _, resp := range result.Responses {
				responses = append(responses, *resp.CNOpResponse)
			}
			return responses, nil
		},
		proc.GetClusterDetails)
	if err != nil {
		return nil, err
	}

	value := vector.New(types.New(types.T_varchar, 0, 0, 0))
	if err := value.Append(json.Pretty(result), false, proc.Mp()); err != nil {
		return nil, err
	}
	return value, nil
}
