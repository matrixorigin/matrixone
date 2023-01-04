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

package table_function

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func resultScanPrepare(_ *process.Process, arg *Argument) error {
	return nil
}

func resultScanCall(_ int, proc *process.Process, arg *Argument) (bool, error) {
	var (
		err  error
		rbat *batch.Batch
	)
	defer func() {
		if err != nil && rbat != nil {
			rbat.Clean(proc.Mp())
		}
	}()

	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	v, err := colexec.EvalExpr(bat, proc, arg.Args[0])
	if err != nil {
		return false, err
	}

	uuid := vector.MustTCols[types.Uuid](v)[0]
	// get file size
	fs := objectio.NewObjectFS(proc.FileService, catalog.QueryResultDir)
	dirs, err := fs.ListDir(catalog.QueryResultDir)
	if err != nil {
		return false, err
	}
	var size int64 = -1
	name := catalog.BuildQueryResultName(proc.SessionInfo.Account, uuid.ToString(), 1)
	for _, d := range dirs {
		if d.Name == name {
			size = d.Size
		}
	}
	if size == -1 {
		return false, moerr.NewInvalidArg(proc.Ctx, "query id", uuid.ToString())
	}
	// read result's meta
	path := catalog.BuildQueryResultPath(proc.SessionInfo.Account, uuid.ToString(), 1)
	reader, err := objectio.NewObjectReader(path, proc.FileService)
	if err != nil {
		return false, err
	}
	bs, err := reader.ReadAllMeta(proc.Ctx, size, proc.Mp())
	if err != nil {
		return false, err
	}
	cnt := len(proc.SessionInfo.ResultColTypes)
	idxs := make([]uint16, cnt)
	for i := range idxs {
		idxs[i] = uint16(i)
	}
	// read result's data
	for _, b := range bs {
		iov, err := reader.Read(proc.Ctx, b.GetExtent(), idxs, proc.Mp())
		if err != nil {
			return false, err
		}
		tmpBat := batch.NewWithSize(cnt)
		for i, e := range iov.Entries {
			tmpBat.Vecs[i] = vector.New(proc.SessionInfo.ResultColTypes[i])
			if err = tmpBat.Vecs[i].Read(e.Object.([]byte)); err != nil {
				return false, err
			}
		}
		rbat, err = rbat.Append(proc.Ctx, proc.Mp(), tmpBat)
		if err != nil {
			return false, err
		}
	}

	rbat.InitZsOne(rbat.Vecs[0].Length())
	proc.SetInputBatch(rbat)
	return false, nil
}
