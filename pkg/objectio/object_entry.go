// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type ObjectIter interface {
	Next() bool
	Close() error
	Entry() ObjectEntry
}

type ObjectEntry struct {
	ObjectStats
	CreateTime types.TS
	DeleteTime types.TS
}

func (o ObjectEntry) String() string {
	return fmt.Sprintf(
		"%s; appendable: %v; sorted: %v; createTS: %s; deleteTS: %s",
		o.ObjectStats.String(),
		o.ObjectStats.GetAppendable(),
		o.ObjectStats.GetSorted(),
		o.CreateTime.ToString(),
		o.DeleteTime.ToString())
}

func (o ObjectEntry) Location() Location {
	return o.ObjectLocation()
}

func (o ObjectEntry) ObjectNameIndexLess(than ObjectEntry) bool {
	return bytes.Compare((*o.ObjectShortName())[:], (*than.ObjectShortName())[:]) < 0
}

// ObjectDTSIndexLess has the order:
// 1. if the delete time is empty, let it be the max ts
// 2. ascending object with delete ts.
// 3. ascending object with createts when same dts.
//
// sort by DELETE time and then CREATE time
func (o ObjectEntry) ObjectDTSIndexLess(than ObjectEntry) bool {
	// (c, d), (c, d), (c, d), (c, inf), (c, inf) ...
	x, y := o.DeleteTime, than.DeleteTime
	if x.IsEmpty() {
		x = types.MaxTs()
	}
	if y.IsEmpty() {
		y = types.MaxTs()
	}

	if !x.Equal(&y) {
		return x.LT(&y)
	}

	if !o.CreateTime.Equal(&than.CreateTime) {
		return o.CreateTime.LT(&than.CreateTime)
	}

	return bytes.Compare((*o.ObjectShortName())[:], (*than.ObjectShortName())[:]) < 0
}

func (o ObjectEntry) IsEmpty() bool {
	return o.Size() == 0
}

func (o ObjectEntry) Visible(ts types.TS) bool {
	return o.CreateTime.LE(&ts) &&
		(o.DeleteTime.IsEmpty() || ts.LT(&o.DeleteTime))
}

func (o ObjectEntry) StatsValid() bool {
	return o.ObjectStats.Rows() != 0
}
