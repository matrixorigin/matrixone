// Copyright 2022 Matrix Origin
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

package memtable

type Row[K any, V any] interface {
	Key() K
	Value() V
	Indexes() []Tuple
}

type dumbRow int

var _ Row[dumbRow, dumbRow] = dumbRow(42)

func (d dumbRow) Key() dumbRow {
	return d
}

func (d dumbRow) Value() dumbRow {
	return d
}

func (d dumbRow) Indexes() []Tuple {
	return nil
}

func (d dumbRow) Less(than dumbRow) bool {
	return d < than
}
