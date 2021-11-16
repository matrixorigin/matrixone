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
	"matrixone/pkg/sql/colexec/connector"
	"matrixone/pkg/sql/colexec/output"
	"matrixone/pkg/sql/colexec/projection"
	"matrixone/pkg/sql/colexec/restrict"
	"matrixone/pkg/sql/viewexec/plus"
	"matrixone/pkg/sql/viewexec/times"
	"matrixone/pkg/sql/viewexec/transform"
	"matrixone/pkg/sql/viewexec/untransform"
	"matrixone/pkg/vm/process"
)

var stringFunc = [...]func(interface{}, *bytes.Buffer){
	Plus:        plus.String,
	Times:       times.String,
	Output:      output.String,
	Restrict:    restrict.String,
	Connector:   connector.String,
	Transform:   transform.String,
	Projection:  projection.String,
	UnTransform: untransform.String,
}

var prepareFunc = [...]func(*process.Process, interface{}) error{
	Plus:        plus.Prepare,
	Times:       times.Prepare,
	Output:      output.Prepare,
	Restrict:    restrict.Prepare,
	Connector:   connector.Prepare,
	Transform:   transform.Prepare,
	Projection:  projection.Prepare,
	UnTransform: untransform.Prepare,
}

var execFunc = [...]func(*process.Process, interface{}) (bool, error){
	Plus:        plus.Call,
	Times:       times.Call,
	Output:      output.Call,
	Restrict:    restrict.Call,
	Connector:   connector.Call,
	Transform:   transform.Call,
	Projection:  projection.Call,
	UnTransform: untransform.Call,
}
