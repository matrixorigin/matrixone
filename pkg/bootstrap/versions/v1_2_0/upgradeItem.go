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

package v1_2_0

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var upgClusterList = []versions.UpgradeCluster{upg_mo_pub, upg_sys_async_task}

var upg_mo_pub = versions.UpgradeCluster{
	UpgType: versions.ADD_COLUMN,
	Comment: "upgrade mo_catalog.mo_pub",
	UpgResource: &versions.TableChangeResource{
		Schema:    "mo_catalog",
		TableName: "mo_pubs",
		HopeColumn: &plan.ColDef{
			Name: "update_time",
			Typ: &plan.Type{
				Id:          int32(types.T_timestamp),
				NotNullable: false,
			},
			Default: &plan.Default{
				OriginString: "",
				NullAbility:  true,
			},
			NotNull: false,
		},
	},
}

// "alter table `mo_task`.`sys_async_task` modify task_id bigint",
var upg_sys_async_task = versions.UpgradeCluster{
	UpgType: versions.MODIFY_COLUMN,
	Comment: "upgrade `mo_task`.`sys_async_task`",
	UpgResource: &versions.TableChangeResource{
		Schema:    "mo_task",
		TableName: "sys_async_task",
		HopeColumn: &plan.ColDef{
			Name: "task_id",
			Typ: &plan.Type{
				Id:          int32(types.T_int64),
				NotNullable: false,
			},
			Default: &plan.Default{
				OriginString: "",
				NullAbility:  true,
			},
			NotNull: false,
		},
	},
}
