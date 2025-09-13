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

import "github.com/matrixorigin/matrixone/pkg/sql/plan"

func NewConsumer(
	cnUUID string,
	tableDef *plan.TableDef,
	jobID JobID,
	info *ConsumerInfo,
) (Consumer, error) {

	if info.ConsumerType == int8(ConsumerType_CNConsumer) {
		return NewInteralSqlConsumer(cnUUID, tableDef, jobID, info)
	}
	if info.ConsumerType == int8(ConsumerType_IndexSync) {
		return NewIndexConsumer(cnUUID, tableDef, jobID, info)
	}
	panic("todo")

}
