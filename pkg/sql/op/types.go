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

package op

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// OP represents an action to be taken on a single relational algebra operator.
// OP will generate a relation variable.
type OP interface {
	// Name returns the name of the variable, which is passed to the next op.
	Name() string
	// Rename applies an AS clause to the variable in this op.
	Rename(string)
	// String returns a format string to describe this operator.
	String() string
	// ResultColumns returns the column names of final result to client.
	ResultColumns() []string
	SetColumns([]string)
	Attribute() map[string]types.Type
}
