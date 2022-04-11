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

package vm

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deleteTag"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergededup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/oplus"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/plus"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/times"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transform"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/untransform"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var stringFunc = [...]func(interface{}, *bytes.Buffer){
	Top:         top.String,
	Join:        join.String,
	Plus:        plus.String,
	Times:       times.String,
	Limit:       limit.String,
	Dedup:       dedup.String,
	Order:       order.String,
	Merge:       merge.String,
	Oplus:       oplus.String,
	Output:      output.String,
	Offset:      offset.String,
	Restrict:    restrict.String,
	Connector:   connector.String,
	Transform:   transform.String,
	Projection:  projection.String,
	UnTransform: untransform.String,

	MergeDedup:  mergededup.String,
	MergeLimit:  mergelimit.String,
	MergeOffset: mergeoffset.String,
	MergeOrder:  mergeorder.String,
	MergeTop:    mergetop.String,

	DeleteTag: deleteTag.String,
}

var prepareFunc = [...]func(*process.Process, interface{}) error{
	Top:         top.Prepare,
	Join:        join.Prepare,
	Plus:        plus.Prepare,
	Times:       times.Prepare,
	Limit:       limit.Prepare,
	Dedup:       dedup.Prepare,
	Order:       order.Prepare,
	Merge:       merge.Prepare,
	Oplus:       oplus.Prepare,
	Output:      output.Prepare,
	Offset:      offset.Prepare,
	Restrict:    restrict.Prepare,
	Connector:   connector.Prepare,
	Transform:   transform.Prepare,
	Projection:  projection.Prepare,
	UnTransform: untransform.Prepare,

	MergeDedup:  mergededup.Prepare,
	MergeLimit:  mergelimit.Prepare,
	MergeOffset: mergeoffset.Prepare,
	MergeOrder:  mergeorder.Prepare,
	MergeTop:    mergetop.Prepare,

	DeleteTag: deleteTag.Prepare,
}

var execFunc = [...]func(*process.Process, interface{}) (bool, error){
	Top:         top.Call,
	Join:        join.Call,
	Plus:        plus.Call,
	Times:       times.Call,
	Limit:       limit.Call,
	Dedup:       dedup.Call,
	Order:       order.Call,
	Merge:       merge.Call,
	Oplus:       oplus.Call,
	Output:      output.Call,
	Offset:      offset.Call,
	Restrict:    restrict.Call,
	Connector:   connector.Call,
	Transform:   transform.Call,
	Projection:  projection.Call,
	UnTransform: untransform.Call,

	MergeDedup:  mergededup.Call,
	MergeLimit:  mergelimit.Call,
	MergeOffset: mergeoffset.Call,
	MergeOrder:  mergeorder.Call,
	MergeTop:    mergetop.Call,

	DeleteTag: deleteTag.Call,
}
