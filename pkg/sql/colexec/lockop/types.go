// Copyright 2023 Matrix Origin
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

package lockop

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
)

// fetchRowsFunc fetch rows from vector.
type fetchRowsFunc func(
	vec *vector.Vector,
	parker *types.Packer,
	max int) ([][]byte, lock.Granularity)

type Argument struct {
	tableID    uint64
	tabaleName string
	pkIdx      int32
	pkType     types.Type
	mode       lock.LockMode
	fetcher    fetchRowsFunc
	packer     *types.Packer
}
