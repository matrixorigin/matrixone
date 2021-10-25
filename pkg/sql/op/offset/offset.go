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

package offset

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/op"
)

func New(prev op.OP, offset int64) *Offset {
	return &Offset{
		Prev:   prev,
		Offset: offset,
	}
}

func (n *Offset) String() string {
	return fmt.Sprintf("%s -> offset(%v)", n.Prev, n.Offset)
}

func (n *Offset) Name() string {
	return n.ID
}

func (n *Offset) Rename(name string) {
	n.ID = name
}

func (n *Offset) ResultColumns() []string {
	return n.Prev.ResultColumns()
}

func (n *Offset) SetColumns(cs []string) {
	n.Prev.SetColumns(cs)
}

func (n *Offset) Attribute() map[string]types.Type {
	return n.Prev.Attribute()
}
