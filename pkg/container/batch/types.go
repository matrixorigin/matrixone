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

package batch

import (
	"matrixone/pkg/container/ring"
	"matrixone/pkg/container/vector"
)

// Batch represents a part of a relationship
// including an optional list of row numbers, columns and list of attributes
//  (SelsData, Sels) - list of row numbers
//  (Attrs) - list of attributes
//  (vecs) 	- columns
type Batch struct {
	// Ro if true, Attrs is read only
	Ro bool
	// SelsData encoded row number list
	SelsData []byte
	// Sels row number list
	Sels []int64
	// Attrs column name list
	Attrs []string
	// Vecs col data
	Vecs []*vector.Vector
	// ring
	Zs   []int64
	As   []string // alias list
	Refs []uint64 // reference count
	Rs   []ring.Ring
	Ht   interface{}
}
