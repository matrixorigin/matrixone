// Copyright 2021 Matrix Origin
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

package overload

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/deletion"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/complement"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/top"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var stringFunc = [...]func(interface{}, *bytes.Buffer){
	Top:        top.String,
	Join:       join.String,
	Semi:       semi.String,
	Left:       left.String,
	Limit:      limit.String,
	Order:      order.String,
	Group:      group.String,
	Merge:      merge.String,
	Output:     output.String,
	Offset:     offset.String,
	Product:    product.String,
	Restrict:   restrict.String,
	Dispatch:   dispatch.String,
	Connector:  connector.String,
	Projection: projection.String,
	Complement: complement.String,

	MergeTop:    mergetop.String,
	MergeLimit:  mergelimit.String,
	MergeOrder:  mergeorder.String,
	MergeGroup:  mergegroup.String,
	MergeOffset: mergeoffset.String,
	Deletion:    deletion.String,
}

var prepareFunc = [...]func(*process.Process, interface{}) error{
	Top:        top.Prepare,
	Join:       join.Prepare,
	Semi:       semi.Prepare,
	Left:       left.Prepare,
	Limit:      limit.Prepare,
	Order:      order.Prepare,
	Group:      group.Prepare,
	Merge:      merge.Prepare,
	Output:     output.Prepare,
	Offset:     offset.Prepare,
	Product:    product.Prepare,
	Restrict:   restrict.Prepare,
	Dispatch:   dispatch.Prepare,
	Connector:  connector.Prepare,
	Projection: projection.Prepare,
	Complement: complement.Prepare,

	MergeTop:    mergetop.Prepare,
	MergeLimit:  mergelimit.Prepare,
	MergeOrder:  mergeorder.Prepare,
	MergeGroup:  mergegroup.Prepare,
	MergeOffset: mergeoffset.Prepare,

	Deletion: deletion.Prepare,
}

var execFunc = [...]func(*process.Process, interface{}) (bool, error){
	Top:        top.Call,
	Join:       join.Call,
	Semi:       semi.Call,
	Left:       left.Call,
	Limit:      limit.Call,
	Order:      order.Call,
	Group:      group.Call,
	Merge:      merge.Call,
	Output:     output.Call,
	Offset:     offset.Call,
	Product:    product.Call,
	Restrict:   restrict.Call,
	Dispatch:   dispatch.Call,
	Connector:  connector.Call,
	Projection: projection.Call,
	Complement: complement.Call,

	MergeTop:    mergetop.Call,
	MergeLimit:  mergelimit.Call,
	MergeOrder:  mergeorder.Call,
	MergeGroup:  mergegroup.Call,
	MergeOffset: mergeoffset.Call,

	Deletion: deletion.Call,
}
