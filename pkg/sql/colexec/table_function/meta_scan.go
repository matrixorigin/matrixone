// Copyright 2022 Matrix Origin
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

package table_function

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// metaScanState still uses simpleOneBatchState
type metaScanState struct {
	simpleOneBatchState
}

func metaScanPrepare(proc *process.Process, tableFunction *TableFunction) (tvfState, error) {
	return &metaScanState{}, nil
}

func (s *metaScanState) start(tf *TableFunction, proc *process.Process, nthRow int) error {
	s.startPreamble(tf, proc, nthRow)

	// Get uuid
	uuid := vector.GetFixedAt[types.Uuid](tf.ctr.argVecs[0], nthRow)
	// get file size
	path := catalog.BuildQueryResultMetaPath(proc.GetSessionInfo().Account, uuid.String())

	// Get reader
	reader, err := blockio.NewFileReader(proc.GetService(), proc.Base.FileService, path)
	if err != nil {
		return err
	}

	var idxs []uint16
	for i, name := range catalog.MetaColNames {
		for _, attr := range tf.Attrs {
			if name == attr {
				idxs = append(idxs, uint16(i))
			}
		}
	}

	// read meta's data
	bats, closeCB, err := reader.LoadAllColumns(proc.Ctx, idxs, common.DefaultAllocator)
	if err != nil {
		return err
	}
	defer func() {
		if closeCB != nil {
			closeCB()
		}
	}()

	// Note that later s.batch.Vecs will be dupped from sbat.   So we must clean.
	s.batch.Clean(proc.Mp())
	for i, vec := range bats[0].Vecs {
		s.batch.Vecs[i], err = vec.Dup(proc.Mp())
		if err != nil {
			return err
		}
	}
	s.batch.SetRowCount(1)
	return nil
}
