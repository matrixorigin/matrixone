// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package iscp

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

func RegisterJob(ctx context.Context, cnUUID string, txn client.TxnOperator, pitr_name string, info *ConsumerInfo) (bool, error) {
	return true, nil
}

func UnregisterJob(ctx context.Context, cnUUID string, txn client.TxnOperator, info *ConsumerInfo) (bool, error) {
	return true, nil
}

func NewConsumer(cnUUID string,
	tableDef *plan.TableDef,
	info *ConsumerInfo) (Consumer, error) {

	if info.ConsumerType != int8(ConsumerType_IndexSync) {
		return nil, moerr.NewInternalErrorNoCtx("invalid index cdc consumer type")
	}

	return NewIndexConsumer(cnUUID, tableDef, info)
}
