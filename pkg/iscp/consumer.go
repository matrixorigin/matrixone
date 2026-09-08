// Copyright 2024 Matrix Origin
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

package iscp

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func NewConsumer(
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	tableDef *plan.TableDef,
	jobID JobID,
	info *ConsumerInfo,
) (Consumer, error) {

	if info == nil {
		return nil, moerr.NewInternalErrorNoCtx("missing ISCP consumer specification")
	}
	if info.ConsumerType == int8(ConsumerType_CNConsumer) {
		return NewInteralSqlConsumer(cnUUID, cnEngine, cnTxnClient, tableDef, jobID, info)
	}
	if info.ConsumerType == int8(ConsumerType_IndexSync) {
		return NewIndexConsumer(cnUUID, cnEngine, cnTxnClient, tableDef, jobID, info)
	}
	if info.ConsumerType == int8(ConsumerType_MaterializedView) {
		return NewMaterializedViewConsumer(cnUUID, cnEngine, cnTxnClient, jobID, info)
	}
	return nil, moerr.NewNotSupportedf(nil, "unsupported ISCP consumer type %d", info.ConsumerType)

}
