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

package ctl

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/util/json"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var MoCtlTNCmdSender = func(ctx context.Context, proc *process.Process, requests []txn.CNOpRequest) ([]txn.CNOpResponse, error) {
	txnOp := proc.TxnOperator
	if txnOp == nil {
		return nil, moerr.NewInternalError(ctx, "ctl: txn operator is nil")
	}

	debugRequests := make([]txn.TxnRequest, 0, len(requests))
	for _, req := range requests {
		tq := txn.NewTxnRequest(&req)
		tq.Method = txn.TxnMethod_DEBUG
		debugRequests = append(debugRequests, tq)
	}
	result, err := txnOp.Debug(ctx, debugRequests)
	if err != nil {
		return nil, err
	}
	defer result.Release()

	responses := make([]txn.CNOpResponse, 0, len(requests))
	for _, resp := range result.Responses {
		responses = append(responses, *resp.CNOpResponse)
	}
	return responses, nil
}

// mo_ctl functions are significantly different from oridinary functions and
// deserve its own package.
func MoCtl(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	args0 := vector.GenerateFunctionStrParameter(ivecs[0])
	args1 := vector.GenerateFunctionStrParameter(ivecs[1])
	args2 := vector.GenerateFunctionStrParameter(ivecs[2])

	if length != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "mo_ctl can only be called with const args")
	}

	arg0, _ := args0.GetStrValue(0)
	arg1, _ := args1.GetStrValue(0)
	arg2, _ := args2.GetStrValue(0)

	service := serviceType(strings.ToUpper(functionUtil.QuickBytesToStr(arg0)))
	command := strings.ToUpper(functionUtil.QuickBytesToStr(arg1))
	parameter := functionUtil.QuickBytesToStr(arg2)

	if _, ok := supportedServiceTypes[service]; !ok {
		return moerr.NewNotSupported(proc.Ctx, "service type %s not supported", service)
	}

	f, ok := supportedCmds[command]
	if !ok {
		return moerr.NewNotSupported(proc.Ctx, "command %s not supported", command)
	}

	res, err := f(proc,
		service,
		parameter,
		// We use a transaction client to send debug requests with the following in mind.
		// 1. reuse the RPC mechanism of cn and dn
		// 2. may support debug support for transactions in the future, such as testing the
		//    correctness of the transaction by forcing the timestamp of the transaction to
		//    be modified, etc.
		// TODO: add more ut tests for this.
		MoCtlTNCmdSender)

	if err != nil {
		return err
	}
	if command == InspectMethod || command == MergeObjectsMethod {
		obj := res.Data.([]any)[0].(*db.InspectResp)
		err = rs.AppendBytes([]byte(obj.ConsoleString()), false)
		return err
	}
	err = rs.AppendBytes(json.Pretty(res), false)
	return err
}
