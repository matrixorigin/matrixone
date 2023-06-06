// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type RuntimeFilterEvaluator interface {
	Evaluate(objectio.ZoneMap) bool
}

type RuntimeInFilter struct {
	InList *vector.Vector
}

type RuntimeZonemapFilter struct {
	Zm objectio.ZoneMap
}

func (f *RuntimeInFilter) Evaluate(zm objectio.ZoneMap) bool {
	return zm.AnyIn(f.InList)
}

func (f *RuntimeZonemapFilter) Evaluate(zm objectio.ZoneMap) bool {
	return f.Zm.FastIntersect(zm)
}
