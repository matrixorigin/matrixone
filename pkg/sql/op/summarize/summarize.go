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

package summarize

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sqlerror"
)

func New(prev op.OP, es []aggregation.Extend) (*Summarize, error) {
	as := make([]string, 0, len(es))
	attrs := make(map[string]types.Type)
	for _, e := range es {
		if len(e.Alias) == 0 {
			e.Alias = fmt.Sprintf("%s(%s)", aggregation.AggName[e.Op], e.Name)
		}
		if _, ok := attrs[e.Alias]; ok {
			return nil, sqlerror.New(errno.AmbiguousAlias, fmt.Sprintf("alias '%s' is ambiguous", e.Alias))
		}
		attrs[e.Alias] = aggregation.ReturnType(e.Op, e.Agg.Type())
		as = append(as, e.Alias)
	}
	return &Summarize{
		As:    as,
		Es:    es,
		Prev:  prev,
		Attrs: attrs,
	}, nil
}

func (n *Summarize) String() string {
	r := fmt.Sprintf("%s -> γ([", n.Prev)
	for i, e := range n.Es {
		switch i {
		case 0:
			r += fmt.Sprintf("%s(%s) -> %s", aggregation.AggName[e.Op], e.Name, e.Alias)
		default:
			r += fmt.Sprintf(", %s(%s) -> %s", aggregation.AggName[e.Op], e.Name, e.Alias)
		}
	}
	r += fmt.Sprintf("])")
	return r
}

func (n *Summarize) Name() string {
	return n.ID
}

func (n *Summarize) Rename(name string) {
	n.ID = name
}

func (n *Summarize) Columns() []string {
	return n.As
}

func (n *Summarize) Attribute() map[string]types.Type {
	return n.Attrs
}
